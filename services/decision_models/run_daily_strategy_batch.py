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

# Every full-path 蒙西 BESS book in the trading-book registry (was a
# hard-coded 4: bameng/wulate/wuchuan/alashan never got attribution). Codes
# are registry codes end to end — prices, nominations, compensation and the
# scenario/attribution rows all key on them.
def _registry_assets() -> List[str]:
    from libs.trading_book.registry import get_stations
    return [s.asset_code for s in get_stations(asset_type="bess", data_path="full")]


_IM_ASSETS: List[str] = _registry_assets()


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


# A day counts as priced only when (nearly) all 96 slots are in. The filled
# table lands ~2 days late and fills a day progressively (2026-11-21 sat at
# 32 slots for a day): a LIMIT-1 check would solve the LP on a partial day
# and _already_computed would then freeze that wrong result forever.
_MIN_PRICE_SLOTS = 92


def _has_prices(asset_code: str, trade_date: date) -> bool:
    """True when a complete price day (>= _MIN_PRICE_SLOTS slots) exists for
    this asset/date — filled nodal table first (complete days from 2026-10),
    then raw md_rt_nodal_price (history back to 2025-05), then
    canon.nodal_rt_price_15min (died 2026-09-24)."""
    try:
        from libs.decision_models.resources.bess_context import _run_query_safe
        import pandas as pd
        node = None
        try:
            from libs.trading_book.registry import get_station
            station = get_station(asset_code)
            node = station.price_nodes[0] if station.price_nodes else None
            if node:
                from services.bess_map.nodal_pf_daily import _expand_price_nodes
                expanded = _expand_price_nodes([node])
                node = expanded[0] if expanded else None
        except Exception:
            node = None
        end_d = str(trade_date + pd.Timedelta(days=1))[:10]
        if node:
            for table, tscol in (("md_mengxi_nodal_price_96_filled", "metric_time"),
                                 ("md_rt_nodal_price", "datetime")):
                sql = f"""
                    SELECT COUNT(DISTINCT {tscol}) AS n
                    FROM marketdata.{table}
                    WHERE node_name = %(node)s
                      AND {tscol} >= %(start_ts)s
                      AND {tscol} < %(end_ts)s
                """
                df, err = _run_query_safe(sql, {
                    "node": node,
                    "start_ts": f"{trade_date} 00:00:00",
                    "end_ts": f"{end_d} 00:00:00",
                })
                if (err is None and not df.empty
                        and int(df["n"].iloc[0] or 0) >= _MIN_PRICE_SLOTS):
                    return True
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
    Return True if the perfect-foresight LP scenario is already in
    reports.bess_asset_daily_scenario_pnl for this asset/date — and the
    nominated/actual leg is not waiting on a recompute.

    Nominations and actuals come from the WeCom workbooks and can land days
    after prices. A day solved before they arrived has LP scenarios but no
    nominated_dispatch row; once ops rows exist for it, report "not computed"
    so the day is redone once and the ladder gets its nominated/cleared rungs.
    """
    try:
        from libs.decision_models.resources.bess_context import (
            load_ops_dispatch_15min,
            load_precomputed_scenario_pnl,
        )
        df, _ = load_precomputed_scenario_pnl(asset_code, trade_date, trade_date)
        if df.empty:
            return False
        # Only the PF scenario is required. forecast_ols_rt_time_v1 has never
        # solved in this path ("no forecast output for target dates"), so
        # requiring it made every day look uncomputed and re-ran the LP daily.
        lp_names = {"perfect_foresight_hourly"}
        present = set(
            df.loc[df["scenario_available"] == True, "scenario_name"].tolist()
        )
        if not lp_names.issubset(present):
            return False
        if "nominated_dispatch" in present:
            return True
        ops_df, _ = load_ops_dispatch_15min(asset_code, trade_date, trade_date)
        return ops_df.empty
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
                "SKIP %s %s — no prices in the filled nodal table or canon; "
                "check the price pipeline",
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


def catch_up(end_date: date, lookback_days: int,
             asset_codes: Optional[List[str]] = None) -> dict:
    """Batch + attribution for every day in [end_date - lookback_days + 1,
    end_date], oldest first. This is what the daily rule runs.

    A yesterday-only run never works: prices complete ~2 days late and
    nominations later still, so each day must be retried until its inputs
    land (attribution wrote nothing from 2026-11-07 to 11-22 this way).
    Days already complete are skipped by _already_computed, so the steady
    state costs one new day per run; attribution re-upserts are cheap.
    """
    from services.monitoring.ladder_rungs import write_ladder_rungs
    from services.monitoring.run_daily_attribution import run_for_date as _attrib
    out = {"batch": 0, "rungs": 0, "attribution": 0, "days": 0}
    for i in range(lookback_days - 1, -1, -1):
        d = end_date - timedelta(days=i)
        out["days"] += 1
        try:
            out["batch"] += run_for_date(d, asset_codes=asset_codes)
        except Exception as exc:
            logger.error("catch_up batch FAIL %s — %s", d, exc, exc_info=True)
        # Forecast-optimal + strategy rungs (book forecast / nodal-agent curve
        # valued at actual prices). Stored rungs are skipped inside.
        for asset_code in (asset_codes or _IM_ASSETS):
            try:
                r = write_ladder_rungs(asset_code, d)
                out["rungs"] += (int(r.get("grid_pf", False)) + int(r["forecast"])
                                 + int(r["strategy"]))
            except Exception as exc:
                logger.error("catch_up rungs FAIL %s %s — %s", asset_code, d, exc,
                             exc_info=True)
        try:
            out["attribution"] += _attrib(d)
        except Exception as exc:
            logger.error("catch_up attribution FAIL %s — %s", d, exc, exc_info=True)
    return out


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
        help=f"Asset code to run (default: all full-path IM assets: {_IM_ASSETS})",
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
