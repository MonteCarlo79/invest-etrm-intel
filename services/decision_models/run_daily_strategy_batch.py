"""
services/decision_models/run_daily_strategy_batch.py

Batch pre-computation of LP dispatch and P&L for Inner Mongolia BESS assets.

Run this script AFTER actual prices and ops data have been loaded to DB.
It executes the perfect-foresight and forecast-optimal LP solves once and
persists the results so the daily-ops UI can read from DB without re-running
the CBC solver on every button click.

Results written to:
  reports.bess_asset_daily_scenario_pnl  — P&L summary (market + subsidy)
  reports.bess_strategy_dispatch_15min   — full 15-min dispatch time series

Usage:
    # Today only (default)
    python -m services.decision_models.run_daily_strategy_batch

    # Specific date
    python -m services.decision_models.run_daily_strategy_batch --date 2026-04-17

    # Specific asset
    python -m services.decision_models.run_daily_strategy_batch --date 2026-04-17 --asset suyou

    # Back-fill last N days for all assets
    python -m services.decision_models.run_daily_strategy_batch --lookback 7

    # Force re-compute even if DB already has results
    python -m services.decision_models.run_daily_strategy_batch --date 2026-04-17 --force
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FuturesTimeout
from datetime import date, timedelta
from typing import List, Optional

# Wall-clock limit per asset per date — CBC can hang indefinitely on some inputs.
# 8 minutes is enough for a full PF + forecast solve; hangs are typically infinite.
_ASSET_TIMEOUT_S: int = int(os.getenv("STRATEGY_BATCH_ASSET_TIMEOUT_S", "480"))

# Ensure repo root is on sys.path when run as a script
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# Set DB_DSN from PGURL if only the latter is provided
_url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
if _url:
    os.environ.setdefault("DB_DSN", _url)
    os.environ.setdefault("PGURL", _url)

from libs.decision_models.workflows.daily_strategy_report import (
    run_bess_daily_strategy_analysis,
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# Inner Mongolia 4-asset set
_IM_ASSETS: List[str] = ["suyou", "hangjinqi", "siziwangqi", "gushanliang"]


def _ensure_dispatch_table() -> None:
    """Create reports.bess_strategy_dispatch_15min if it doesn't exist."""
    try:
        from services.common.db_utils import get_engine
        from sqlalchemy import text
        engine = get_engine()
        ddl_path = os.path.join(
            _ROOT, "db", "ddl", "reports", "bess_strategy_dispatch_15min.sql"
        )
        if os.path.exists(ddl_path):
            with open(ddl_path, encoding="utf-8") as f:
                ddl = f.read()
            with engine.begin() as conn:
                # Execute statement-by-statement, skipping SQL comments and
                # COMMENT ON statements (which may contain semicolons inside
                # string literals and break naive semicolon-splitting).
                for stmt in ddl.split(";"):
                    sql = stmt.strip()
                    if sql and not sql.startswith("--") and not sql.upper().startswith("COMMENT"):
                        conn.execute(text(sql))
        logger.debug("Dispatch table ensured")
    except Exception as exc:
        logger.warning("Could not ensure dispatch table: %s", exc)


def _has_prices(asset_code: str, trade_date: date) -> bool:
    """Return True if canon.nodal_rt_price_15min has at least one row for this asset/date."""
    try:
        from libs.decision_models.resources.bess_context import _run_query_safe
        import pandas as pd
        sql = """
            SELECT 1
            FROM canon.nodal_rt_price_15min
            WHERE asset_code = %(asset_code)s
              AND time >= %(start_ts)s
              AND time < %(end_ts)s
            LIMIT 1
        """
        params = {
            "asset_code": asset_code,
            "start_ts": pd.Timestamp(trade_date).tz_localize("Asia/Shanghai"),
            "end_ts": (pd.Timestamp(trade_date) + pd.Timedelta(days=1)).tz_localize("Asia/Shanghai"),
        }
        df, err = _run_query_safe(sql, params)
        return not df.empty
    except Exception:
        return True  # can't check → let the LP attempt proceed


def _already_computed(asset_code: str, trade_date: date) -> bool:
    """
    Return True if both LP scenarios are already in
    reports.bess_asset_daily_scenario_pnl for this asset/date.
    """
    try:
        from libs.decision_models.resources.bess_context import (
            load_precomputed_scenario_pnl,
        )
        df, _ = load_precomputed_scenario_pnl(asset_code, trade_date, trade_date)
        if df.empty:
            return False
        lp_names = {"perfect_foresight_hourly", "forecast_ols_rt_time_v1"}
        present = set(
            df.loc[df["scenario_available"] == True, "scenario_name"].tolist()
        )
        return lp_names.issubset(present)
    except Exception:
        return False


def run_for_date(
    trade_date: date,
    asset_codes: Optional[List[str]] = None,
    forecast_models: Optional[List[str]] = None,
    force: bool = False,
) -> int:
    """
    Run LP pre-computation for all (or specified) assets on trade_date.

    Returns the number of assets successfully written to DB.
    """
    assets = asset_codes or _IM_ASSETS
    # Inner Mongolia Mengxi is a pure RT spot market — no DA prices exist.
    # Use ols_rt_time_v1 (rolling OLS on RT price history + time features).
    if forecast_models is None:
        forecast_models = ["ols_rt_time_v1"]
    written = 0

    for asset_code in assets:
        if not force and _already_computed(asset_code, trade_date):
            logger.info(
                "SKIP %s %s — LP results already in DB (use --force to recompute)",
                asset_code, trade_date,
            )
            continue

        # Quick price availability check before starting a multi-minute LP solve.
        if not _has_prices(asset_code, trade_date):
            logger.info(
                "SKIP %s %s — no rows in canon.nodal_rt_price_15min; "
                "run Canon ETL first",
                asset_code, trade_date,
            )
            continue

        logger.info("START %s %s (timeout=%ds)", asset_code, trade_date, _ASSET_TIMEOUT_S)
        try:
            # Run in a separate thread so we can enforce a wall-clock timeout.
            # CBC can hang indefinitely on certain price series; the timeout lets
            # the batch continue to the next asset rather than blocking forever.
            # Note: Python threads cannot be forcibly killed, so the hung thread
            # continues running in the background, but the main loop moves on.
            with ThreadPoolExecutor(max_workers=1) as _pool:
                _future = _pool.submit(
                    run_bess_daily_strategy_analysis,
                    asset_code=asset_code,
                    date=str(trade_date),
                    forecast_models=forecast_models,
                    use_ops_dispatch=True,
                )
                result = _future.result(timeout=_ASSET_TIMEOUT_S)

            # Check write notes in context
            write_notes = [
                n for n in result.get("context", {}).get("data_quality_notes", [])
                if "write_lp_results_to_db" in n or "write_ops_pnl_to_db" in n
            ]
            for note in write_notes:
                logger.info("%s %s — %s", asset_code, trade_date, note)

            pf_solved = result.get("pf_result", {}).get("pnl", {}).get("n_days_solved", 0)
            fc_strategies = len(
                result.get("forecast_suite", {}).get("strategies", [])
            )
            logger.info(
                "OK %s %s — PF n_days_solved=%d, forecast strategies=%d",
                asset_code, trade_date, pf_solved, fc_strategies,
            )
            written += 1

        except _FuturesTimeout:
            logger.warning(
                "TIMEOUT %s %s — LP solve exceeded %ds; skipping. "
                "Re-run this date with a higher STRATEGY_BATCH_ASSET_TIMEOUT_S or investigate CBC.",
                asset_code, trade_date, _ASSET_TIMEOUT_S,
            )
        except Exception as exc:
            logger.error("FAIL %s %s — %s", asset_code, trade_date, exc, exc_info=True)

    return written


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pre-compute LP dispatch + P&L for IM BESS assets"
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="ISO date to compute (default: today)",
    )
    parser.add_argument(
        "--asset", type=str, default=None,
        help=f"Asset code to run (default: all 4 IM assets: {_IM_ASSETS})",
    )
    parser.add_argument(
        "--lookback", type=int,
        default=int(os.getenv("STRATEGY_BATCH_LOOKBACK_DAYS", "1")),
        help="Number of days to back-fill ending on --date (default: 1 = date only)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-compute and overwrite even if DB already has results",
    )
    args = parser.parse_args()

    end_date = date.fromisoformat(args.date) if args.date else date.today()
    dates = [end_date - timedelta(days=i) for i in range(args.lookback - 1, -1, -1)]
    asset_codes = [args.asset] if args.asset else None

    logger.info(
        "run_daily_strategy_batch: dates=%s..%s (%d days), assets=%s, force=%s",
        dates[0], dates[-1], len(dates),
        asset_codes or _IM_ASSETS, args.force,
    )

    _ensure_dispatch_table()

    total = 0
    for d in dates:
        total += run_for_date(d, asset_codes=asset_codes, force=args.force)

    logger.info(
        "run_daily_strategy_batch complete — %d asset-days written across %d dates",
        total, len(dates),
    )


if __name__ == "__main__":
    main()
