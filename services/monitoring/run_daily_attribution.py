"""
services/monitoring/run_daily_attribution.py

Batch job: read scenario PnL from reports.bess_asset_daily_scenario_pnl,
run dispatch_pnl_attribution model, upsert results to
reports.bess_asset_daily_attribution.

This job is model-backed — all attribution logic runs through the registered
dispatch_pnl_attribution model. It does not duplicate logic from
services/trading/bess/mengxi/run_pnl_refresh.py; that job populates
bess_asset_daily_scenario_pnl from raw data. This job reads from that table
and produces the attribution ladder via the registered model.

Usage:
    python -m services.monitoring.run_daily_attribution
    python -m services.monitoring.run_daily_attribution --date 2026-04-18
    python -m services.monitoring.run_daily_attribution --lookback 14
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import date, timedelta
from typing import Dict, List, Optional

import libs.decision_models.dispatch_pnl_attribution  # noqa: F401 — triggers registration

from libs.decision_models.runners.local import run
from services.common.db_utils import get_engine

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

_SCENARIO_COL_MAP: Dict[str, str] = {
    "perfect_foresight_unrestricted": "pf_unrestricted_pnl",
    "perfect_foresight_grid_feasible": "pf_grid_feasible_pnl",
    "tt_forecast_optimal": "tt_forecast_optimal_pnl",
    "tt_strategy": "tt_strategy_pnl",
    "nominated_dispatch": "nominated_pnl",
    "cleared_actual": "cleared_actual_pnl",
}


def _load_scenario_pnl(engine, trade_date: date) -> Dict[str, Dict[str, Optional[float]]]:
    """
    Load total_revenue_yuan per (asset_code, scenario_name) for the given date.
    Returns {asset_code: {scenario_name: yuan, ...}, ...}.
    """
    from sqlalchemy import text

    sql = text("""
        SELECT asset_code, scenario_name, total_revenue_yuan
        FROM reports.bess_asset_daily_scenario_pnl
        WHERE trade_date = :td
          AND scenario_available = TRUE
    """)
    result: Dict[str, Dict[str, Optional[float]]] = {}
    with engine.begin() as conn:
        rows = conn.execute(sql, {"td": trade_date}).fetchall()
    for row in rows:
        asset = row[0]
        scenario = row[1]
        pnl = float(row[2]) if row[2] is not None else None
        result.setdefault(asset, {})[scenario] = pnl
    return result


def _upsert_attribution(engine, rows: List[Dict]) -> None:
    """Upsert attribution rows into reports.bess_asset_daily_attribution."""
    from sqlalchemy import text

    sql = text("""
        INSERT INTO reports.bess_asset_daily_attribution (
            trade_date, asset_code,
            pf_unrestricted_pnl, pf_grid_feasible_pnl, cleared_actual_pnl,
            nominated_pnl, tt_forecast_optimal_pnl, tt_strategy_pnl,
            grid_restriction_loss, forecast_error_loss, strategy_error_loss,
            nomination_loss, execution_clearing_loss,
            realisation_gap_vs_pf, realisation_gap_vs_pf_grid,
            updated_at
        ) VALUES (
            :trade_date, :asset_code,
            :pf_unrestricted_pnl, :pf_grid_feasible_pnl, :cleared_actual_pnl,
            :nominated_pnl, :tt_forecast_optimal_pnl, :tt_strategy_pnl,
            :grid_restriction_loss, :forecast_error_loss, :strategy_error_loss,
            :nomination_loss, :execution_clearing_loss,
            :realisation_gap_vs_pf, :realisation_gap_vs_pf_grid,
            now()
        )
        ON CONFLICT (trade_date, asset_code) DO UPDATE SET
            pf_unrestricted_pnl       = EXCLUDED.pf_unrestricted_pnl,
            pf_grid_feasible_pnl      = EXCLUDED.pf_grid_feasible_pnl,
            cleared_actual_pnl        = EXCLUDED.cleared_actual_pnl,
            nominated_pnl             = EXCLUDED.nominated_pnl,
            tt_forecast_optimal_pnl   = EXCLUDED.tt_forecast_optimal_pnl,
            tt_strategy_pnl           = EXCLUDED.tt_strategy_pnl,
            grid_restriction_loss     = EXCLUDED.grid_restriction_loss,
            forecast_error_loss       = EXCLUDED.forecast_error_loss,
            strategy_error_loss       = EXCLUDED.strategy_error_loss,
            nomination_loss           = EXCLUDED.nomination_loss,
            execution_clearing_loss   = EXCLUDED.execution_clearing_loss,
            realisation_gap_vs_pf     = EXCLUDED.realisation_gap_vs_pf,
            realisation_gap_vs_pf_grid= EXCLUDED.realisation_gap_vs_pf_grid,
            updated_at                = now()
    """)
    with engine.begin() as conn:
        for row in rows:
            conn.execute(sql, row)


def run_for_date(trade_date: date, engine=None) -> int:
    """
    Run attribution for a single trade_date.
    Returns the number of asset rows written.
    """
    if engine is None:
        engine = get_engine()

    scenario_pnl = _load_scenario_pnl(engine, trade_date)
    if not scenario_pnl:
        logger.info("No scenario PnL found for %s — skipping", trade_date)
        return 0

    output_rows: List[Dict] = []
    for asset_code, pnl_by_scenario in scenario_pnl.items():
        # Map scenario names to model input field names
        model_input = {
            "asset_code": asset_code,
            "trade_date": trade_date,
        }
        for scenario_name, pnl_value in pnl_by_scenario.items():
            field_name = _SCENARIO_COL_MAP.get(scenario_name)
            if field_name:
                model_input[field_name] = pnl_value

        result = run("dispatch_pnl_attribution", model_input)

        output_rows.append({
            "trade_date": trade_date,
            "asset_code": asset_code,
            "pf_unrestricted_pnl": result.get("pf_unrestricted_pnl"),
            "pf_grid_feasible_pnl": result.get("pf_grid_feasible_pnl"),
            "cleared_actual_pnl": result.get("cleared_actual_pnl"),
            "nominated_pnl": result.get("nominated_pnl"),
            "tt_forecast_optimal_pnl": result.get("tt_forecast_optimal_pnl"),
            "tt_strategy_pnl": result.get("tt_strategy_pnl"),
            "grid_restriction_loss": result.get("grid_restriction_loss"),
            "forecast_error_loss": result.get("forecast_error_loss"),
            "strategy_error_loss": result.get("strategy_error_loss"),
            "nomination_loss": result.get("nomination_loss"),
            "execution_clearing_loss": result.get("execution_clearing_loss"),
            "realisation_gap_vs_pf": result.get("realisation_gap_vs_pf"),
            "realisation_gap_vs_pf_grid": result.get("realisation_gap_vs_pf_grid"),
        })

    _upsert_attribution(engine, output_rows)
    logger.info("Attribution upserted for %d assets on %s", len(output_rows), trade_date)
    return len(output_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run daily P&L attribution batch job")
    parser.add_argument("--date", type=str, default=None,
                        help="ISO date to process (default: today)")
    parser.add_argument("--lookback", type=int, default=int(os.getenv("ATTRIBUTION_LOOKBACK_DAYS", "1")),
                        help="Number of days to back-fill (default: 1 = today only)")
    args = parser.parse_args()

    end_date = date.fromisoformat(args.date) if args.date else date.today()
    dates = [end_date - timedelta(days=i) for i in range(args.lookback - 1, -1, -1)]

    engine = get_engine()
    total = 0
    for d in dates:
        total += run_for_date(d, engine)

    logger.info("run_daily_attribution complete — %d total rows written across %d dates", total, len(dates))


if __name__ == "__main__":
    main()
