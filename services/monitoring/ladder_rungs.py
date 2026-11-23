"""Strategy + forecast rungs of the attribution ladder (2026-11-23).

The ladder is
    PF grid-feasible -> forecast-optimal -> strategy -> nominated -> cleared actual
but nothing produced the two middle rungs: reports.bess_asset_daily_attribution
.tt_forecast_optimal_pnl / tt_strategy_pnl were NULL for every day, so the
Book-tab waterfall started from 0 and forecast error, strategy selection and
nomination loss could not be separated. This module fills them from what the
book already owns:

  tt_forecast_optimal  the SAME LP as the perfect-foresight benchmark
                       (run_perfect_foresight_dispatch: rating, round-trip
                       efficiency, cycle cap, ramp limit), solved on the
                       book's D-1 nodal price forecast
                       (reports.book_forecast_daily) instead of actual prices,
                       then valued at ACTUAL prices. PF minus this = what the
                       forecast cost, strategy held optimal.
  tt_strategy          the nodal-agent strategy curve
                       (marketdata.nodal_strategy_daily, summed over a
                       station's exchange accounts) valued at ACTUAL prices.
                       forecast-optimal minus this = strategy selection;
                       this minus nominated = what the trader changed.

Valuation is identical to the nominated / cleared-actual rungs
(_calc_15min_pnl): sum(MW x 0.25 h x actual nodal price) + discharge MWh x
compensation. All series are 96 CST slots, MW, positive = discharge.

Rows go to reports.bess_asset_daily_scenario_pnl under the scenario names
run_daily_attribution already maps (tt_forecast_optimal, tt_strategy).

Honest gaps, never fabricated values:
  - no forecast / incomplete forecast for the day  -> no forecast rung
  - no strategy row (or a multi-account station missing one account)
                                                    -> no strategy rung
  - strategy curve exceeds the station rating      -> no strategy rung. The
    writer's registry sized hangjinqi/siziwangqi at 4x and bameng at 0.4x
    until 2026-11-23; valuing those curves would be meaningless.
"""
from __future__ import annotations

import logging
import math
from datetime import date, datetime, timedelta
from typing import List, Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

SCENARIO_FORECAST = "tt_forecast_optimal"
SCENARIO_STRATEGY = "tt_strategy"
_RATING_TOLERANCE = 1.05   # curve may exceed nameplate by rounding only
_MIN_SLOTS = 92            # same completeness bar as the batch's price check

_EXISTING_SQL = text("""
    SELECT scenario_name FROM reports.bess_asset_daily_scenario_pnl
    WHERE asset_code = :ac AND trade_date = :td AND total_pnl IS NOT NULL
      AND scenario_name IN ('tt_forecast_optimal', 'tt_strategy')
""")

_UPSERT_SQL = text("""
    INSERT INTO reports.bess_asset_daily_scenario_pnl
        (trade_date, asset_code, scenario_name, scenario_available,
         market_revenue, compensation_revenue, total_pnl,
         discharge_mwh, charge_mwh, avg_daily_cycles, source_system, updated_at)
    VALUES
        (:trade_date, :asset_code, :scenario_name, TRUE,
         :market, :compensation, :total,
         :discharge_mwh, :charge_mwh, :cycles, :source_system, now())
    ON CONFLICT (asset_code, trade_date, scenario_name) DO UPDATE SET
        scenario_available   = TRUE,
        market_revenue       = EXCLUDED.market_revenue,
        compensation_revenue = EXCLUDED.compensation_revenue,
        total_pnl            = EXCLUDED.total_pnl,
        discharge_mwh        = EXCLUDED.discharge_mwh,
        charge_mwh           = EXCLUDED.charge_mwh,
        avg_daily_cycles     = EXCLUDED.avg_daily_cycles,
        source_system        = EXCLUDED.source_system,
        updated_at           = now()
""")


def _finite(v) -> bool:
    return v is not None and isinstance(v, (int, float)) and math.isfinite(v)


def slot_time(day: date, slot: int) -> str:
    """Naive CST interval-start timestamp of 0-based slot (matches the price
    records' wall-clock keys in _calc_15min_pnl)."""
    return str(datetime(day.year, day.month, day.day) + timedelta(minutes=15 * slot))


def value_curve(day: date, curve_mw: List[float], price_records: List[dict],
                compensation_yuan_per_mwh: float,
                energy_capacity_mwh: Optional[float] = None) -> Optional[dict]:
    """P&L of a 96-slot MW curve at actual prices, or None when the curve has
    too few finite slots. Same convention as the nominated/actual rungs."""
    from libs.decision_models.workflows.strategy_comparison import _calc_15min_pnl
    recs = [{"time": slot_time(day, i), "dispatch_mw": float(mw) * 0.25}
            for i, mw in enumerate(curve_mw) if _finite(mw)]
    if len(recs) < _MIN_SLOTS:
        return None
    pnl = _calc_15min_pnl(recs, price_records, compensation_yuan_per_mwh)
    disch = sum(max(r["dispatch_mw"], 0.0) for r in recs)
    chrg = sum(max(-r["dispatch_mw"], 0.0) for r in recs)
    cycles = round(disch / energy_capacity_mwh, 4) if energy_capacity_mwh else None
    return {"market": pnl["market"], "compensation": pnl["compensation"],
            "total": pnl["total"], "discharge_mwh": disch, "charge_mwh": chrg,
            "cycles": cycles}


def strategy_rung(day: date, curve_mw: Optional[List[float]], price_records: List[dict],
                  meta: dict) -> Optional[dict]:
    """tt_strategy row values, or None (no curve / curve exceeds the rating)."""
    if not curve_mw:
        return None
    peak = max((abs(v) for v in curve_mw if _finite(v)), default=0.0)
    if peak > float(meta["power_mw"]) * _RATING_TOLERANCE:
        logger.warning("ladder_rungs: %s %s strategy peak %.0f MW exceeds rating "
                       "%.0f MW — not valued (writer registry was mis-sized "
                       "before 2026-11-23)", meta.get("asset_code"), day, peak,
                       float(meta["power_mw"]))
        return None
    return value_curve(day, curve_mw, price_records, meta["compensation_yuan_per_mwh"],
                       meta["power_mw"] * meta["duration_h"])


def forecast_rung(day: date, forecast_prices: Optional[List[float]],
                  price_records: List[dict], meta: dict) -> Optional[dict]:
    """tt_forecast_optimal row values: PF LP on the D-1 forecast, valued at
    actual prices. None when the forecast is incomplete or the LP fails."""
    if not forecast_prices or sum(1 for p in forecast_prices if _finite(p)) < _MIN_SLOTS:
        return None
    from libs.decision_models.workflows.strategy_comparison import (
        run_perfect_foresight_dispatch,
    )
    fc_records = [{"time": slot_time(day, i), "price": float(p)}
                  for i, p in enumerate(forecast_prices) if _finite(p)]
    ctx = {"asset_code": meta.get("asset_code"), "date_from": str(day),
           "date_to": str(day), "asset_metadata": meta,
           "actual_prices_15min": fc_records, "actual_prices_hourly": []}
    res = run_perfect_foresight_dispatch(ctx)
    if not res.get("pnl", {}).get("n_days_solved", 0):
        return None
    by_time = {str(r["datetime"]).replace("T", " "): float(r["dispatch_grid_mw"])
               for r in res.get("dispatch_hourly", [])}
    curve = [by_time.get(slot_time(day, i)) for i in range(96)]
    return value_curve(day, curve, price_records, meta["compensation_yuan_per_mwh"],
                       meta["power_mw"] * meta["duration_h"])


def write_ladder_rungs(asset_code: str, trade_date: date, engine=None,
                       force: bool = False) -> dict:
    """Compute and upsert the forecast + strategy rungs for one station-day.

    Returns {"forecast": bool, "strategy": bool, "skipped": reason|None}.
    Rungs already stored are left alone unless force. Needs a complete day of
    actual prices; never raises on missing inputs.
    """
    from libs.decision_models.resources.bess_context import (
        load_actual_prices_15min,
        load_asset_metadata,
    )
    from libs.trading_book.asset_book import AssetBook
    from libs.trading_book.registry import get_station
    from services.mengxi_nodal import book_forecast

    out = {"forecast": False, "strategy": False, "skipped": None}
    if engine is None:
        from services.common.db_utils import get_engine
        engine = get_engine()
    try:
        station = get_station(asset_code)
    except KeyError:
        out["skipped"] = "unknown asset_code"
        return out
    if station.data_path != "full":
        out["skipped"] = f"{station.data_path} book"
        return out

    with engine.connect() as conn:
        have = {r[0] for r in conn.execute(_EXISTING_SQL,
                                           {"ac": asset_code, "td": trade_date})}
    want_fc = force or SCENARIO_FORECAST not in have
    want_st = force or SCENARIO_STRATEGY not in have
    if not (want_fc or want_st):
        out["skipped"] = "already stored"
        return out

    prices_df, _ = load_actual_prices_15min(asset_code, trade_date, trade_date)
    if len(prices_df) < _MIN_SLOTS:
        out["skipped"] = f"actual prices incomplete ({len(prices_df)} slots)"
        return out
    price_records = prices_df.to_dict("records")
    meta, _ = load_asset_metadata(asset_code, trade_date)

    rows = []
    if want_st:
        strategies = AssetBook(engine, station).strategies(trade_date)
        curve = next((s.series for s in strategies if s.primary), None)
        vals = strategy_rung(trade_date, curve, price_records, meta)
        if vals:
            rows.append((SCENARIO_STRATEGY, "nodal_agent", vals))
    if want_fc:
        fs = book_forecast.get_forecast(engine, station, trade_date)
        try:
            vals = forecast_rung(trade_date, fs.nodal_price_fc, price_records, meta)
        except Exception as exc:
            logger.warning("ladder_rungs: %s %s forecast LP failed — %s",
                           asset_code, trade_date, exc)
            vals = None
        if vals:
            rows.append((SCENARIO_FORECAST, "book_forecast_d1", vals))

    if rows:
        with engine.begin() as conn:
            for name, source, v in rows:
                conn.execute(_UPSERT_SQL, {
                    "trade_date": trade_date, "asset_code": asset_code,
                    "scenario_name": name, "source_system": source, **v})
    out["forecast"] = any(n == SCENARIO_FORECAST for n, _, _ in rows)
    out["strategy"] = any(n == SCENARIO_STRATEGY for n, _, _ in rows)
    return out
