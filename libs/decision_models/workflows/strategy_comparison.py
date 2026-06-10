"""
libs/decision_models/workflows/strategy_comparison.py

Six reusable skills for BESS dispatch strategy comparison, ranking,
discrepancy attribution, and report generation.

────────────────────────────────────────────────────────────
SKILL SUMMARY
────────────────────────────────────────────────────────────
Skill 1  load_bess_strategy_comparison_context(asset_code, date_from, date_to)
Skill 2  run_perfect_foresight_dispatch(context)
Skill 3  run_forecast_dispatch_suite(context, forecast_models)
Skill 4  rank_dispatch_strategies(context, pf_result, forecast_suite, db_pnl_df)
Skill 5  attribute_dispatch_discrepancy(context, ranking, attribution_df)
Skill 6  generate_asset_strategy_report(asset_code, date_from, date_to, period_type,
                                         context, ranking, attribution)

────────────────────────────────────────────────────────────
DESIGN RULES
────────────────────────────────────────────────────────────
- Model logic stays in libs/decision_models/<model>.py — never duplicated here.
- DB logic stays in libs/decision_models/resources/bess_context.py — never here.
- Each skill returns a JSON-serialisable dataclass (via dataclasses.asdict).
- All approximations are documented in output.caveats / output.data_quality_notes.
- Each skill can be called standalone or chained in a pipeline.

────────────────────────────────────────────────────────────
GRANULARITY NOTE
────────────────────────────────────────────────────────────
Perfect-foresight and forecast-driven strategies use HOURLY dispatch
(bess_dispatch_simulation_multiday) and are settled against hourly mean of
15-min actual prices.  Nominated / actual strategies come from the DB at
15-min granularity.  Do NOT directly compare hourly P&L to 15-min P&L as
absolute values — use them for relative gap / attribution only.

────────────────────────────────────────────────────────────
ATTRIBUTION METHOD
────────────────────────────────────────────────────────────
Rules-based waterfall.  Buckets do NOT prove causality.  Cascade:

  1. forecast_error         = PF_pnl − best_forecast_pnl
  2. grid_restriction       = from DB (PF_unrestricted − PF_grid_feasible) if available
  3. execution_nomination   = best_forecast_pnl − nominated_pnl (if nominated available)
  4. execution_clearing     = nominated_pnl − actual_pnl (if both available)
  5. asset_issue            = proxy from outage flags (always None until table exists)
  6. residual               = total_gap − sum(explained)
"""
from __future__ import annotations

import dataclasses
import datetime
import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from libs.decision_models.schemas.strategy_comparison import (
    AssetMetadata,
    AssetStrategyReport,
    DailyDiscrepancyRow,
    DiscrepancyAttributionResult,
    DiscrepancyBuckets,
    ForecastDispatchSuiteResult,
    ForecastStrategyResult,
    ForecastToYearEnd,
    PerfectForesightResult,
    PnLComparisonTable,
    StrategyComparisonContext,
    StrategyPnLResult,
    StrategyRankRow,
    StrategyRankingResult,
    YTDSummary,
)

logger = logging.getLogger(__name__)

_INTERVAL_HRS_15MIN = 0.25
_INTERVAL_HRS_HOURLY = 1.0

# Names of scenarios stored in the DB (canon.scenario_dispatch_15min)
_DB_SCENARIOS = [
    "perfect_foresight_unrestricted",
    "perfect_foresight_grid_feasible",
    "tt_forecast_optimal",
    "tt_strategy",
    "nominated_dispatch",
    "cleared_actual",
]


# ===========================================================================
# Skill 1 — Load context
# ===========================================================================

def load_bess_strategy_comparison_context(
    asset_code: str,
    date_from: str,
    date_to: str,
    skip_ops_fallback: bool = False,
) -> Dict[str, Any]:
    """
    Load all data needed to run the strategy comparison workflow.

    Parameters
    ----------
    asset_code : stable asset code, e.g. "suyou"
    date_from  : ISO date string, e.g. "2026-03-01"
    date_to    : ISO date string, e.g. "2026-03-31"

    Returns
    -------
    JSON-serialisable dict matching StrategyComparisonContext schema.
    Contains actual prices, DA prices, nominated/actual dispatch, asset metadata,
    and a list of data_quality_notes for any missing/degraded data.
    """
    from libs.decision_models.resources.bess_context import (
        load_actual_prices_15min,
        load_asset_metadata,
        load_available_scenarios,
        load_curtailment_flags,
        load_da_prices_hourly,
        load_id_cleared_energy,
        load_ops_dispatch_15min,
        load_outage_flags,
        load_scenario_dispatch_15min,
        resample_15min_to_hourly,
    )

    d_from = datetime.date.fromisoformat(date_from)
    d_to = datetime.date.fromisoformat(date_to)
    notes: List[str] = []

    # --- Asset metadata ---
    meta_dict, meta_notes = load_asset_metadata(asset_code, trade_month=d_from)
    notes.extend(meta_notes)

    # --- Actual prices ---
    prices_15min_df, price_notes = load_actual_prices_15min(asset_code, d_from, d_to)
    notes.extend(price_notes)

    prices_hourly_df = resample_15min_to_hourly(prices_15min_df)

    # --- DA prices (for forecast input) ---
    da_prices_df, da_notes = load_da_prices_hourly(d_from, d_to)
    notes.extend(da_notes)

    # --- Scenario availability ---
    available_scenarios, avail_notes = load_available_scenarios(asset_code, d_from, d_to)
    notes.extend(avail_notes)

    # --- Nominated dispatch ---
    # Primary: canon.scenario_dispatch_15min (populated by P&L refresh pipeline)
    # Fallback: marketdata.ops_bess_dispatch_15min (direct from Excel ingestion)
    nominated_df, nom_notes = load_scenario_dispatch_15min(
        asset_code, "nominated_dispatch", d_from, d_to
    )
    notes.extend(nom_notes)

    # --- Actual dispatch (cleared_actual scenario from canon table) ---
    # NOTE: "cleared_actual" in canon.scenario_dispatch_15min records the as-cleared
    # or as-dispatched schedule.  Do NOT conflate with id_cleared_energy_15min below.
    actual_df, act_notes = load_scenario_dispatch_15min(
        asset_code, "cleared_actual", d_from, d_to
    )
    notes.extend(act_notes)

    # --- Fallback: read nominated/actual directly from ops Excel ingestion table ---
    # canon.scenario_dispatch_15min is only populated by the P&L refresh batch job.
    # For recent dates (before the next batch run) ops data lives only in
    # marketdata.ops_bess_dispatch_15min — use it when canon is empty.
    # skip_ops_fallback=True when the caller will load ops data separately
    # (e.g. _enrich_context_with_ops_dispatch) to avoid a duplicate DB query.
    if not skip_ops_fallback and (nominated_df.empty or actual_df.empty):
        ops_df, ops_notes = load_ops_dispatch_15min(asset_code, d_from, d_to)
        notes.extend(ops_notes)
        if not ops_df.empty:
            if nominated_df.empty and ops_df["nominated_dispatch_mw"].notna().any():
                nominated_df = ops_df[["interval_start", "nominated_dispatch_mw"]].rename(
                    columns={"interval_start": "time", "nominated_dispatch_mw": "dispatch_mw"}
                )
                notes.append(
                    "nominated_dispatch: loaded from marketdata.ops_bess_dispatch_15min "
                    "(fallback — canon.scenario_dispatch_15min had no data)"
                )
            if actual_df.empty and ops_df["actual_dispatch_mw"].notna().any():
                actual_df = ops_df[["interval_start", "actual_dispatch_mw"]].rename(
                    columns={"interval_start": "time", "actual_dispatch_mw": "dispatch_mw"}
                )
                notes.append(
                    "actual_dispatch: loaded from marketdata.ops_bess_dispatch_15min "
                    "(fallback — canon.scenario_dispatch_15min had no data)"
                )

    # --- Inner Mongolia DA cleared energy (distinct from actual physical dispatch) ---
    # marketdata.md_id_cleared_energy = DA market-cleared trading energy.
    # Applies to Mengxi assets only; non-Mengxi assets get None with no error.
    id_cleared_df, id_cleared_notes = load_id_cleared_energy(asset_code, d_from, d_to)
    notes.extend(id_cleared_notes)
    if not id_cleared_df.empty:
        # Add standing caveat for any Mengxi asset with cleared energy data
        notes.append(
            "id_cleared_energy: Inner Mongolia DA cleared energy loaded — "
            "this is market-cleared trading volume, NOT actual physical dispatch; "
            "gap between id_cleared_energy and actual_dispatch may indicate "
            "asset issue / BOP constraint / grid restriction"
        )

    # --- Outage / curtailment (TODO placeholders) ---
    outage_flags, outage_notes = load_outage_flags(asset_code, d_from, d_to)
    notes.extend(outage_notes)
    curtailment_flags, curtailment_notes = load_curtailment_flags(asset_code, d_from, d_to)
    notes.extend(curtailment_notes)

    asset_meta = AssetMetadata(
        asset_code=meta_dict["asset_code"],
        display_name=meta_dict["display_name"],
        power_mw=meta_dict["power_mw"],
        duration_h=meta_dict["duration_h"],
        roundtrip_eff=meta_dict["roundtrip_eff"],
        compensation_yuan_per_mwh=meta_dict["compensation_yuan_per_mwh"],
        province=meta_dict["province"],
        max_cycles_per_day=meta_dict.get("max_cycles_per_day"),
        source=meta_dict["source"],
    )

    ctx = StrategyComparisonContext(
        asset_code=asset_code,
        date_from=date_from,
        date_to=date_to,
        asset_metadata=asset_meta,
        actual_prices_15min=prices_15min_df.to_dict("records") if not prices_15min_df.empty else [],
        actual_prices_hourly=prices_hourly_df.to_dict("records") if not prices_hourly_df.empty else [],
        da_prices_hourly=da_prices_df.to_dict("records") if not da_prices_df.empty else [],
        nominated_dispatch_15min=nominated_df.to_dict("records") if not nominated_df.empty else None,
        actual_dispatch_15min=actual_df.to_dict("records") if not actual_df.empty else None,
        id_cleared_energy_15min=(
            id_cleared_df.to_dict("records") if not id_cleared_df.empty else None
        ),
        available_scenarios=available_scenarios,
        outage_flags=outage_flags,
        curtailment_flags=curtailment_flags,
        data_quality_notes=notes,
    )
    return dataclasses.asdict(ctx)


# ===========================================================================
# Skill 2 — Perfect foresight dispatch
# ===========================================================================

def run_perfect_foresight_dispatch(
    context: Dict[str, Any],
    window_days: int = 1,
) -> Dict[str, Any]:
    """
    Compute perfect-foresight BESS dispatch using actual prices as input.

    Preferred: uses 15-min actual prices (96 intervals/day) so that PF P&L is a
    true upper bound on any 15-min settled strategy (cleared_actual, nominated, etc.).
    Falls back to hourly LP when 15-min prices are not in context.

    Parameters
    ----------
    context    : output dict from load_bess_strategy_comparison_context()
    window_days: number of consecutive days to optimise in one LP solve (default 1).
                 When > 1, SOC carries over across day boundaries within each window.

    Returns
    -------
    JSON-serialisable dict matching PerfectForesightResult schema.
    """
    meta = context["asset_metadata"]
    prices_15min: List[dict] = context.get("actual_prices_15min", [])
    hourly_prices: List[dict] = context.get("actual_prices_hourly", [])

    # ── Helper ────────────────────────────────────────────────────────────────
    def _ts_naive(val) -> pd.Timestamp:
        t = pd.Timestamp(val)
        if t.tzinfo is not None:
            # Convert to CST first so UTC timestamps land on the correct calendar
            # date (e.g. 2026-04-16 16:00 UTC → 2026-04-17 00:00 CST).
            # Without this, the date-grouping in compute_dispatch_from_15min_prices
            # splits a CST day across two UTC dates and finds no complete 96-interval
            # day → n_days_solved = 0.
            return t.tz_convert("Asia/Shanghai").replace(tzinfo=None)
        return t

    def _empty_result(reason: str, caveats: List[str]) -> Dict[str, Any]:
        caveats.append(reason)
        return dataclasses.asdict(PerfectForesightResult(
            strategy_name="perfect_foresight_hourly",
            pnl=StrategyPnLResult(
                strategy_name="perfect_foresight_hourly",
                pnl_market_yuan=0.0, pnl_compensation_yuan=0.0, pnl_total_yuan=0.0,
                discharge_mwh=0.0, charge_mwh=0.0, n_days_solved=0, granularity="hourly",
                notes=caveats,
            ),
            dispatch_hourly=[], daily_profit=[], energy_capacity_mwh=0.0,
            caveats=caveats,
        ))

    # ── 15-min branch: true upper bound ───────────────────────────────────────
    if prices_15min:
        from services.bess_map.optimisation_engine import compute_dispatch_from_15min_prices

        caveats: List[str] = [
            "perfect_foresight: 15-min granularity — LP optimised and settled at actual 15-min prices; "
            "true upper bound on all 15-min settled strategies",
            f"perfect_foresight: window_days={window_days} — "
            + ("SOC resets to 0 each day" if window_days == 1
               else f"SOC carries over across {window_days}-day windows"),
            "perfect_foresight: single-asset LP — no portfolio or grid constraints",
        ]

        price_records: List[tuple] = []
        for rec in prices_15min:
            ts = rec.get("time") or rec.get("datetime")
            price = rec.get("price")
            if ts is not None and price is not None:
                try:
                    p = float(price)
                    if not np.isnan(p):
                        price_records.append((_ts_naive(ts), p))
                except (TypeError, ValueError):
                    pass

        if not price_records:
            return _empty_result("perfect_foresight: 15-min prices are all null — returning empty result", caveats)

        price_series = pd.Series(
            {ts: p for ts, p in price_records}
        ).sort_index()
        price_series.index = pd.DatetimeIndex(price_series.index)

        max_cycles = meta.get("max_cycles_per_day")
        if max_cycles is not None:
            caveats.append(
                f"perfect_foresight: max_cycles_per_day={max_cycles} applied — "
                "LP discharge capped at max_cycles × energy_capacity_mwh per day; "
                "prevents unrealistic churning from compensation subsidy"
            )

        try:
            dispatch_df, profit_s = compute_dispatch_from_15min_prices(
                price_series,
                power_mw=meta["power_mw"],
                duration_h=meta["duration_h"],
                roundtrip_eff=meta["roundtrip_eff"],
                compensation_yuan_per_mwh=meta["compensation_yuan_per_mwh"],
                max_cycles_per_day=max_cycles,
                window_days=int(window_days),
            )
        except Exception as exc:
            return _empty_result(f"perfect_foresight: 15-min LP failed — {exc}", caveats)

        n_days_solved = len(profit_s)
        price_map_15 = {str(ts): p for ts, p in price_records}

        dispatch_records = []
        if not dispatch_df.empty:
            for dt_idx, row in dispatch_df.iterrows():
                dt_str = str(dt_idx)
                p = price_map_15.get(dt_str, 0.0)
                dispatch_records.append({
                    "datetime": dt_idx.isoformat(),
                    "charge_mw": float(row["charge_mw"]),
                    "discharge_mw": float(row["discharge_mw"]),
                    "dispatch_grid_mw": float(row["dispatch_grid_mw"]),
                    "soc_mwh": float(row["soc_mwh"]),
                    "price": p,
                })

        pnl_market = sum(
            r["dispatch_grid_mw"] * r["price"] * _INTERVAL_HRS_15MIN
            for r in dispatch_records
        )
        discharge_mwh = sum(max(r["discharge_mw"], 0) * _INTERVAL_HRS_15MIN for r in dispatch_records)
        charge_mwh = sum(max(r["charge_mw"], 0) * _INTERVAL_HRS_15MIN for r in dispatch_records)
        pnl_comp = discharge_mwh * meta["compensation_yuan_per_mwh"]

        daily_profit = [
            {"date": str(d), "profit_actual_prices": float(p)}
            for d, p in profit_s.items()
        ]

        result = PerfectForesightResult(
            strategy_name="perfect_foresight_hourly",
            pnl=StrategyPnLResult(
                strategy_name="perfect_foresight_hourly",
                pnl_market_yuan=pnl_market,
                pnl_compensation_yuan=pnl_comp,
                pnl_total_yuan=pnl_market + pnl_comp,
                discharge_mwh=discharge_mwh,
                charge_mwh=charge_mwh,
                n_days_solved=n_days_solved,
                granularity="15min",
            ),
            dispatch_hourly=dispatch_records,
            daily_profit=daily_profit,
            energy_capacity_mwh=meta["power_mw"] * meta["duration_h"],
            caveats=caveats,
        )
        return dataclasses.asdict(result)

    # ── Hourly fallback ───────────────────────────────────────────────────────
    import libs.decision_models.bess_dispatch_simulation_multiday  # noqa: F401
    from libs.decision_models.runners.local import run

    caveats = [
        "perfect_foresight: hourly granularity (15-min prices unavailable) — "
        "P&L settled on hourly mean of actual prices; "
        "intra-hour price spikes may allow 15-min strategies to exceed PF",
        f"perfect_foresight: window_days={window_days} — "
        + ("SOC resets to 0 each day" if window_days == 1
           else f"SOC carries over across {window_days}-day windows"),
        "perfect_foresight: single-asset LP — no portfolio or grid constraints",
    ]

    if not hourly_prices:
        return _empty_result("perfect_foresight: no actual prices available — returning empty result", caveats)

    price_records_h = []
    for rec in hourly_prices:
        ts = rec.get("datetime") or rec.get("time")
        price = rec.get("price")
        if ts is not None and price is not None and not (isinstance(price, float) and np.isnan(price)):
            price_records_h.append({"datetime": str(_ts_naive(ts)), "price": float(price)})

    if not price_records_h:
        return _empty_result("perfect_foresight: hourly prices are all null — returning empty result", caveats)

    opt_result = run("bess_dispatch_simulation_multiday", {
        "hourly_prices": price_records_h,
        "power_mw": meta["power_mw"],
        "duration_h": meta["duration_h"],
        "roundtrip_eff": meta["roundtrip_eff"],
        "compensation_yuan_per_mwh": meta["compensation_yuan_per_mwh"],
        "window_days": int(window_days),
    })

    price_map_h = {str(pd.Timestamp(rec["datetime"])): rec["price"] for rec in price_records_h}
    dispatch_hourly = []
    for rec in opt_result.get("dispatch_records", []):
        dt_str = str(pd.Timestamp(rec["datetime"]))
        p = price_map_h.get(dt_str, 0.0)
        dispatch_hourly.append({
            "datetime": dt_str,
            "charge_mw": rec["charge_mw"],
            "discharge_mw": rec["discharge_mw"],
            "dispatch_grid_mw": rec["dispatch_grid_mw"],
            "soc_mwh": rec["soc_mwh"],
            "price": p,
        })

    pnl_market = sum(
        r["dispatch_grid_mw"] * r["price"] * _INTERVAL_HRS_HOURLY
        for r in dispatch_hourly
    )
    discharge_mwh = sum(max(r["discharge_mw"], 0) * _INTERVAL_HRS_HOURLY for r in dispatch_hourly)
    charge_mwh = sum(max(r["charge_mw"], 0) * _INTERVAL_HRS_HOURLY for r in dispatch_hourly)
    pnl_comp = discharge_mwh * meta["compensation_yuan_per_mwh"]

    solver_statuses = {
        rec["date"]: str(
            next(
                (d["solver_status"] for d in opt_result.get("dispatch_records", [])
                 if d["datetime"].startswith(rec["date"])),
                "unknown",
            )
        )
        for rec in opt_result.get("daily_profit", [])
    }

    result = PerfectForesightResult(
        strategy_name="perfect_foresight_hourly",
        pnl=StrategyPnLResult(
            strategy_name="perfect_foresight_hourly",
            pnl_market_yuan=pnl_market,
            pnl_compensation_yuan=pnl_comp,
            pnl_total_yuan=pnl_market + pnl_comp,
            discharge_mwh=discharge_mwh,
            charge_mwh=charge_mwh,
            n_days_solved=opt_result.get("n_days_solved", 0),
            granularity="hourly",
        ),
        dispatch_hourly=dispatch_hourly,
        daily_profit=opt_result.get("daily_profit", []),
        energy_capacity_mwh=opt_result.get("energy_capacity_mwh", 0.0),
        solver_statuses=solver_statuses,
        caveats=caveats,
    )
    return dataclasses.asdict(result)


# ===========================================================================
# Skill 3 — Forecast dispatch suite
# ===========================================================================

def run_forecast_dispatch_suite(
    context: Dict[str, Any],
    forecast_models: Optional[List[str]] = None,
    window_days: int = 1,
) -> Dict[str, Any]:
    """
    Run one or more price forecast models, optimise dispatch on each forecast,
    and settle the resulting dispatch on actual prices.

    Uses the registered model assets directly:
      - forecast_engine.build_forecast()      — province-level RT price forecast
      - run("bess_dispatch_simulation_multiday") — LP dispatch over all target days

    Both engines handle multi-day input natively; no per-day Python loop needed.

    Parameters
    ----------
    context         : output of load_bess_strategy_comparison_context()
    forecast_models : list of model names. Defaults to ["ols_rt_time_v1"].
                      RT-only (Inner Mongolia): ols_rt_time_v1, naive_rt_lag1, naive_rt_lag7
                      DA-based (requires DA market): ols_da_time_v1, naive_da
    window_days     : number of consecutive days to optimise in one LP solve (default 1).
                      When > 1, SOC carries over across day boundaries within each window.

    Returns
    -------
    JSON-serialisable dict matching ForecastDispatchSuiteResult schema.

    Granularity approximation
    -------------------------
    Forecast and dispatch are hourly; P&L settled on hourly mean of actual
    15-min nodal prices.  Province-level forecast vs asset-level settlement
    introduces basis risk that is not modelled here.
    """
    import libs.decision_models.bess_dispatch_simulation_multiday  # noqa: F401
    from libs.decision_models.runners.local import run
    from services.bess_map.forecast_engine import RT_ONLY_MODELS, build_forecast

    if forecast_models is None:
        forecast_models = ["ols_rt_time_v1"]

    suite_caveats: List[str] = [
        "forecast_suite: price forecast is province-level (not nodal/asset) — "
        "dispatch optimised on province prices but settled on asset nodal prices",
        "forecast_suite: hourly granularity — settled on hourly mean of actual 15-min prices",
        f"forecast_suite: window_days={window_days} — "
        + ("SOC resets to 0 each day (no cross-day carryover)" if window_days == 1
           else f"SOC carries over across {window_days}-day windows"),
    ]

    meta = context["asset_metadata"]
    actual_prices_hourly: List[dict] = context.get("actual_prices_hourly", [])
    da_prices_hourly: List[dict] = context.get("da_prices_hourly", [])
    d_from = datetime.date.fromisoformat(context["date_from"])
    d_to = datetime.date.fromisoformat(context["date_to"])

    def _empty_strategy(model_name: str, notes: List[str]) -> ForecastStrategyResult:
        return ForecastStrategyResult(
            model_name=model_name,
            strategy_name=f"forecast_{model_name}",
            pnl=StrategyPnLResult(
                strategy_name=f"forecast_{model_name}",
                pnl_market_yuan=0.0, pnl_compensation_yuan=0.0, pnl_total_yuan=0.0,
                discharge_mwh=0.0, charge_mwh=0.0, n_days_solved=0, granularity="hourly",
            ),
            forecast_prices_hourly=[], dispatch_hourly=[], daily_profit=[],
            n_days_with_forecast=0, n_days_missing_da_prices=0,
            caveats=notes,
        )

    if not actual_prices_hourly:
        suite_caveats.append("forecast_suite: no actual prices in context — all models skipped")
        return dataclasses.asdict(ForecastDispatchSuiteResult(
            strategies=[_empty_strategy(m, ["no actual prices available"]) for m in forecast_models],
            requested_models=forecast_models,
            suite_caveats=suite_caveats,
        ))

    # Build unified hourly DataFrame (full history + target range in one pass).
    # build_forecast() and bess_dispatch_simulation_multiday both handle multi-day
    # input natively — no per-day loop needed here.

    def _naive_ts(val) -> str:
        """Return tz-naive ISO string, dropping timezone annotation without converting."""
        t = pd.Timestamp(val)
        if t.tzinfo is not None:
            t = t.tz_localize(None)
        return str(t)

    rt_map: Dict[str, float] = {
        _naive_ts(r.get("datetime") or r.get("time")): float(r.get("price", float("nan")))
        for r in actual_prices_hourly
    }
    da_map: Dict[str, float] = {
        _naive_ts(r["datetime"]): float(r.get("da_price", float("nan")))
        for r in da_prices_hourly
    }
    all_ts = sorted(set(rt_map.keys()) | set(da_map.keys()))
    hourly_df = pd.DataFrame({
        "datetime": [pd.Timestamp(ts) for ts in all_ts],
        "rt_price": [rt_map.get(ts, float("nan")) for ts in all_ts],
        "da_price": [da_map.get(ts, float("nan")) for ts in all_ts],
    }).set_index("datetime").sort_index()

    if not da_map:
        suite_caveats.append(
            "forecast_suite: no DA prices in DB — only RT-only models "
            "(ols_rt_time_v1, naive_rt_lag1, naive_rt_lag7) will produce results"
        )

    def _safe_price(v) -> float:
        try:
            f = float(v)
            return 0.0 if np.isnan(f) else f
        except (TypeError, ValueError):
            return 0.0

    actual_price_map = {
        _naive_ts(r.get("datetime") or r.get("time")): _safe_price(r.get("price", 0.0))
        for r in actual_prices_hourly
    }

    strategies: List[ForecastStrategyResult] = []

    for model_name in forecast_models:
        is_rt_only = model_name in RT_ONLY_MODELS
        model_caveats: List[str] = []
        strategy_name = f"forecast_{model_name}"

        # DA-based models require DA prices for the target period
        if not is_rt_only:
            target_da = hourly_df.loc[
                (hourly_df.index.date >= d_from) & (hourly_df.index.date <= d_to), "da_price"
            ]
            n_missing_da = int(
                target_da.groupby(target_da.index.date)
                .apply(lambda g: g.isna().all()).sum()
            )
            if target_da.isna().all():
                model_caveats.append(
                    f"{strategy_name}: no DA prices for target period — skipping"
                )
                strategies.append(ForecastStrategyResult(
                    model_name=model_name,
                    strategy_name=strategy_name,
                    pnl=StrategyPnLResult(
                        strategy_name=strategy_name,
                        pnl_market_yuan=0.0, pnl_compensation_yuan=0.0, pnl_total_yuan=0.0,
                        discharge_mwh=0.0, charge_mwh=0.0, n_days_solved=0, granularity="hourly",
                    ),
                    forecast_prices_hourly=[], dispatch_hourly=[], daily_profit=[],
                    n_days_with_forecast=0, n_days_missing_da_prices=n_missing_da,
                    caveats=model_caveats,
                ))
                continue
        else:
            n_missing_da = 0

        # Single call: build_forecast handles rolling OLS over all days internally
        try:
            rt_pred_series = build_forecast(hourly_df, model=model_name)
        except Exception as exc:
            model_caveats.append(f"{strategy_name}: forecast engine failed — {exc}")
            strategies.append(_empty_strategy(model_name, model_caveats))
            continue

        # Filter predictions to the target date range only
        target_mask = np.array([d_from <= d <= d_to for d in rt_pred_series.index.date])
        target_preds = rt_pred_series[target_mask].dropna()

        if target_preds.empty:
            model_caveats.append(
                f"{strategy_name}: no forecast output for target dates — check RT price history"
            )
            strategies.append(_empty_strategy(model_name, model_caveats))
            continue

        n_forecast_days = pd.Index(target_preds.index.date).nunique()
        model_used_per_day: Dict[str, str] = {
            str(d): model_name for d in pd.Index(target_preds.index.date).unique()
        }
        all_forecast_records = [
            {"datetime": ts.isoformat(), "rt_pred": float(p)}
            for ts, p in target_preds.items()
        ]

        # Single call to registered dispatch model over all forecast days at once
        opt_result = run("bess_dispatch_simulation_multiday", {
            "hourly_prices": [
                {"datetime": r["datetime"], "price": r["rt_pred"]}
                for r in all_forecast_records
            ],
            "power_mw": meta["power_mw"],
            "duration_h": meta["duration_h"],
            "roundtrip_eff": meta["roundtrip_eff"],
            "compensation_yuan_per_mwh": meta["compensation_yuan_per_mwh"],
            "window_days": int(window_days),
        })

        # Settle dispatch on actual nodal prices
        dispatch_hourly = []
        for rec in opt_result.get("dispatch_records", []):
            dt_str = str(pd.Timestamp(rec["datetime"]))
            dispatch_hourly.append({
                "datetime": rec["datetime"],
                "charge_mw": rec["charge_mw"],
                "discharge_mw": rec["discharge_mw"],
                "dispatch_grid_mw": rec["dispatch_grid_mw"],
                "soc_mwh": rec["soc_mwh"],
                "actual_price": actual_price_map.get(dt_str, 0.0),
            })

        _dgrid = np.array([r["dispatch_grid_mw"] for r in dispatch_hourly], dtype=float)
        _aprice = np.array([r["actual_price"] for r in dispatch_hourly], dtype=float)
        _dmw = np.array([r["discharge_mw"] for r in dispatch_hourly], dtype=float)
        _cmw = np.array([r["charge_mw"] for r in dispatch_hourly], dtype=float)
        pnl_market = float(np.nansum(_dgrid * _aprice * _INTERVAL_HRS_HOURLY))
        discharge_mwh = float(np.nansum(np.maximum(_dmw, 0.0) * _INTERVAL_HRS_HOURLY))
        charge_mwh = float(np.nansum(np.maximum(-_cmw, 0.0) * _INTERVAL_HRS_HOURLY))
        pnl_comp = discharge_mwh * meta["compensation_yuan_per_mwh"]

        daily_profit = []
        dispatch_df = pd.DataFrame(dispatch_hourly)
        if not dispatch_df.empty:
            dispatch_df["datetime"] = pd.to_datetime(dispatch_df["datetime"])
            dispatch_df["date"] = dispatch_df["datetime"].dt.date.astype(str)
            dispatch_df["hourly_pnl"] = (
                dispatch_df["dispatch_grid_mw"] * dispatch_df["actual_price"] * _INTERVAL_HRS_HOURLY
            )
            for d, grp in dispatch_df.groupby("date"):
                daily_profit.append({"date": d, "profit_actual_prices": float(grp["hourly_pnl"].sum())})

        strategies.append(ForecastStrategyResult(
            model_name=model_name,
            strategy_name=strategy_name,
            pnl=StrategyPnLResult(
                strategy_name=strategy_name,
                pnl_market_yuan=pnl_market,
                pnl_compensation_yuan=pnl_comp,
                pnl_total_yuan=pnl_market + pnl_comp,
                discharge_mwh=discharge_mwh,
                charge_mwh=charge_mwh,
                n_days_solved=opt_result.get("n_days_solved", 0),
                granularity="hourly",
            ),
            forecast_prices_hourly=all_forecast_records,
            dispatch_hourly=dispatch_hourly,
            daily_profit=daily_profit,
            n_days_with_forecast=n_forecast_days,
            n_days_missing_da_prices=n_missing_da,
            model_used_per_day=model_used_per_day,
            caveats=model_caveats,
        ))

    return dataclasses.asdict(ForecastDispatchSuiteResult(
        strategies=strategies,
        requested_models=forecast_models,
        suite_caveats=suite_caveats,
    ))


# ===========================================================================
# Skill 4 — Rank strategies
# ===========================================================================

def rank_dispatch_strategies(
    context: Dict[str, Any],
    pf_result: Dict[str, Any],
    forecast_suite: Dict[str, Any],
    db_pnl_df: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """
    Rank all available strategies by realised P&L.

    Parameters
    ----------
    context        : from load_bess_strategy_comparison_context()
    pf_result      : from run_perfect_foresight_dispatch()
    forecast_suite : from run_forecast_dispatch_suite()
    db_pnl_df      : optional DataFrame from load_precomputed_scenario_pnl()
                     If None, nominated/actual come from context dispatch + calc.

    Returns
    -------
    JSON-serialisable dict matching StrategyRankingResult schema.
    """
    asset_code = context["asset_code"]
    date_from = context["date_from"]
    date_to = context["date_to"]
    meta = context["asset_metadata"]
    caveats: List[str] = []

    # --- Pre-compute dispatch stats (cycles + spread) for strategies with dispatch data ---
    _energy_cap = float(meta.get("power_mw", 100.0)) * float(meta.get("duration_h", 2.0))
    _n_days = max(1, (
        datetime.date.fromisoformat(date_to) - datetime.date.fromisoformat(date_from)
    ).days + 1)
    _prices_15min = context.get("actual_prices_15min", [])
    _dstats: Dict[str, dict] = {}
    # ops dispatch is already in MWh (×0.25 applied in _enrich_context_with_ops_dispatch).
    # canon.scenario_dispatch_15min stores raw MW — must apply ×0.25 before stats/P&L calcs.
    _has_ops = bool(context.get("ops_dispatch_15min"))

    _pf_hourly = pf_result.get("dispatch_hourly", [])
    if _pf_hourly:
        _pf_is_15min = pf_result.get("pnl", {}).get("granularity") == "15min"
        _dstats["perfect_foresight_hourly"] = _calc_hourly_dispatch_stats(
            _pf_hourly, "price", _energy_cap,
            interval_h=_INTERVAL_HRS_15MIN if _pf_is_15min else _INTERVAL_HRS_HOURLY,
        )
    for _fs in forecast_suite.get("strategies", []):
        if _fs.get("dispatch_hourly") and _fs.get("n_days_with_forecast", 0) > 0:
            _dstats[_fs["strategy_name"]] = _calc_hourly_dispatch_stats(
                _fs["dispatch_hourly"], "actual_price", _energy_cap
            )
    for _sname, _dkey in [
        ("nominated_dispatch", "nominated_dispatch_15min"),
        ("cleared_actual", "actual_dispatch_15min"),
        ("tt_forecast_optimal", "tt_forecast_optimal_dispatch_15min"),
    ]:
        _disp = context.get(_dkey)
        if _disp and _prices_15min:
            # tt_forecast_optimal is always MWh (converted in _enrich_context_with_forecast_dispatch).
            # For non-ops nominated/actual, canon table stores raw MW — convert to MWh for stats.
            if not _has_ops and _sname != "tt_forecast_optimal":
                _disp = [
                    {**r, "dispatch_mw": float(r.get("dispatch_mw") or 0.0) * _INTERVAL_HRS_15MIN}
                    for r in _disp
                ]
            _dstats[_sname] = _calc_15min_dispatch_stats(_disp, _prices_15min, _energy_cap)
    _id_recs = context.get("id_cleared_energy_15min") or []
    if _id_recs:
        # Compute cycles/spread/efficiency from DA cleared energy records.
        # cleared_energy_mwh_15min is already in MWh per interval (positive=discharge, negative=charge).
        # Use cleared_price (DA price) for spread so figures are DA-settled, not RT-settled.
        _id_dispatch = [
            {"time": str(r.get("datetime")), "dispatch_mw": float(r.get("cleared_energy_mwh_15min") or 0.0)}
            for r in _id_recs if r.get("cleared_energy_mwh_15min") is not None
        ]
        _id_da_prices = [
            {"time": str(r.get("datetime")), "price": float(r.get("cleared_price") or 0.0)}
            for r in _id_recs if r.get("cleared_price") is not None
        ]
        if _id_dispatch and _id_da_prices:
            _dstats["id_cleared_energy_da"] = _calc_15min_dispatch_stats(
                _id_dispatch, _id_da_prices, _energy_cap
            )
        else:
            _dstats["id_cleared_energy_da"] = {"cycles": None, "captured_spread": None, "cycle_efficiency": None}

    # Collect all strategies as {name -> (pnl_total, granularity, available)}
    strategies: Dict[str, Dict[str, Any]] = {}

    # 1. Perfect foresight (hourly)
    pf_pnl = pf_result.get("pnl", {})
    if pf_pnl.get("n_days_solved", 0) > 0:
        strategies["perfect_foresight_hourly"] = {
            "pnl_total": pf_pnl.get("pnl_total_yuan", 0.0),
            "pnl_market": pf_pnl.get("pnl_market_yuan"),
            "pnl_compensation": pf_pnl.get("pnl_compensation_yuan"),
            "granularity": "hourly",
            "available": True,
        }
    else:
        caveats.append("ranking: perfect_foresight_hourly has no solved days — excluded from ranking")

    # 2. Forecast strategies (hourly)
    for strat in forecast_suite.get("strategies", []):
        strat_pnl = strat.get("pnl", {})
        n = strat.get("n_days_with_forecast", 0)
        name = strat["strategy_name"]
        if n > 0:
            strategies[name] = {
                "pnl_total": strat_pnl.get("pnl_total_yuan", 0.0),
                "pnl_market": strat_pnl.get("pnl_market_yuan"),
                "pnl_compensation": strat_pnl.get("pnl_compensation_yuan"),
                "granularity": "hourly",
                "available": True,
            }
        else:
            strategies[name] = {
                "pnl_total": None, "pnl_market": None, "pnl_compensation": None,
                "granularity": "hourly", "available": False,
            }
            caveats.append(f"ranking: {name} has no forecast days — marked unavailable")

    # 3. DB scenarios (15-min P&L from reports table)
    if db_pnl_df is not None and not db_pnl_df.empty:
        for scenario_name in _DB_SCENARIOS:
            hit = db_pnl_df[db_pnl_df["scenario_name"] == scenario_name]
            avail_hit = hit[hit["scenario_available"] == True]
            if not avail_hit.empty:
                total = avail_hit["total_revenue_yuan"].sum()
                market = avail_hit["market_revenue_yuan"].sum() if "market_revenue_yuan" in avail_hit.columns else None
                subsidy = avail_hit["subsidy_revenue_yuan"].sum() if "subsidy_revenue_yuan" in avail_hit.columns else None
                strategies[scenario_name] = {
                    "pnl_total": float(total),
                    "pnl_market": float(market) if market is not None else None,
                    "pnl_compensation": float(subsidy) if subsidy is not None else None,
                    "granularity": "15min",
                    "available": True,
                }
            else:
                strategies[scenario_name] = {
                    "pnl_total": None, "pnl_market": None, "pnl_compensation": None,
                    "granularity": "15min", "available": False,
                }
    else:
        # Fall back to raw dispatch from context + inline P&L calc
        for scenario_name, dispatch_key in [
            ("nominated_dispatch", "nominated_dispatch_15min"),
            ("cleared_actual", "actual_dispatch_15min"),
        ]:
            dispatch = context.get(dispatch_key)
            actual_prices = context.get("actual_prices_15min", [])
            if dispatch and actual_prices:
                pnl = _calc_15min_pnl(dispatch, actual_prices, meta["compensation_yuan_per_mwh"])
                strategies[scenario_name] = {
                    "pnl_total": pnl["total"],
                    "pnl_market": pnl["market"],
                    "pnl_compensation": pnl["compensation"],
                    "granularity": "15min",
                    "available": True,
                }
            else:
                strategies[scenario_name] = {
                    "pnl_total": None, "pnl_market": None, "pnl_compensation": None,
                    "granularity": "15min", "available": False,
                }
        caveats.append(
            "ranking: DB pnl_df not provided — nominated/actual P&L computed from raw dispatch"
        )

    # Always recalculate nominated_dispatch and cleared_actual P&L from context dispatch.
    # This corrects DB precomputed values that may have MW/MWh unit errors:
    #   - canon.scenario_dispatch_15min stores raw MW; ×0.25 must be applied here.
    #   - ops dispatch is already in MWh (applied in _enrich_context_with_ops_dispatch).
    # _has_ops is defined above near the stats section.
    _actual_prices = context.get("actual_prices_15min", [])
    for scenario_name, dispatch_key in [
        ("nominated_dispatch", "nominated_dispatch_15min"),
        ("cleared_actual", "actual_dispatch_15min"),
    ]:
        dispatch = context.get(dispatch_key)
        if dispatch and _actual_prices:
            if not _has_ops:
                # Canon dispatch is in raw MW — convert to MWh per 15-min interval.
                dispatch = [
                    {**r, "dispatch_mw": float(r.get("dispatch_mw") or 0.0) * _INTERVAL_HRS_15MIN}
                    for r in dispatch
                ]
            pnl = _calc_15min_pnl(dispatch, _actual_prices, meta["compensation_yuan_per_mwh"])
            strategies[scenario_name] = {
                "pnl_total": pnl["total"],
                "pnl_market": pnl["market"],
                "pnl_compensation": pnl["compensation"],
                "granularity": "15min",
                "available": True,
            }
            if _has_ops:
                caveats.append(
                    f"ranking: {scenario_name} P&L from ops dispatch "
                    "(ops is authoritative; DB precomputed value overridden)"
                )
            else:
                caveats.append(
                    f"ranking: {scenario_name} P&L from canon dispatch "
                    "(×0.25 MW→MWh applied; DB precomputed value overridden)"
                )

    # tt_forecast_optimal: if DB didn't populate it, compute from context forecast dispatch
    if not strategies.get("tt_forecast_optimal", {}).get("available"):
        tt_dispatch = context.get("tt_forecast_optimal_dispatch_15min")
        actual_prices = context.get("actual_prices_15min", [])
        if tt_dispatch and actual_prices:
            pnl = _calc_15min_pnl(tt_dispatch, actual_prices, meta["compensation_yuan_per_mwh"])
            strategies["tt_forecast_optimal"] = {
                "pnl_total": pnl["total"],
                "pnl_market": pnl["market"],
                "pnl_compensation": pnl["compensation"],
                "granularity": "15min",
                "available": True,
            }
            caveats.append(
                "ranking: tt_forecast_optimal P&L computed from forecast dispatch in context "
                "(settled at actual 15-min prices)"
            )
        else:
            strategies.setdefault("tt_forecast_optimal", {
                "pnl_total": None, "pnl_market": None, "pnl_compensation": None,
                "granularity": "15min", "available": False,
            })

    # 4. DA cleared energy P&L (md_id_cleared_energy) — Inner Mongolia only
    id_cleared_records = context.get("id_cleared_energy_15min") or []
    if id_cleared_records:
        comp_per_mwh = float(meta.get("compensation_yuan_per_mwh", 0.0) or 0.0)
        import numpy as _np
        cleared_mwh = _np.array(
            [float(r.get("cleared_energy_mwh_15min") or 0.0) for r in id_cleared_records],
            dtype=float,
        )
        cleared_price = _np.array(
            [float(r.get("cleared_price") or 0.0) for r in id_cleared_records],
            dtype=float,
        )
        # Market P&L: cleared energy * cleared DA price (MWh * CNY/MWh)
        # Negative cleared_mwh = charging (cost); positive = discharging (revenue).
        pnl_market_id = float(_np.nansum(cleared_mwh * cleared_price))
        # Subsidy: 350 CNY/MWh applies only to DISCHARGED energy (positive values).
        discharge_mwh_id = float(_np.nansum(_np.maximum(cleared_mwh, 0.0)))
        pnl_comp_id = discharge_mwh_id * comp_per_mwh
        strategies["id_cleared_energy_da"] = {
            "pnl_total": pnl_market_id + pnl_comp_id,
            "pnl_market": pnl_market_id,
            "pnl_compensation": pnl_comp_id,
            "granularity": "15min",
            "available": True,
        }
        caveats.append(
            "ranking: id_cleared_energy_da = DA market-cleared energy P&L "
            "(cleared_energy_mwh * cleared_price + subsidy); "
            "this is DA cleared trading revenue, NOT physical dispatch revenue"
        )
    else:
        strategies["id_cleared_energy_da"] = {
            "pnl_total": None, "pnl_market": None, "pnl_compensation": None,
            "granularity": "15min", "available": False,
        }

    # Build ranking
    available = {k: v for k, v in strategies.items() if v["available"] and v["pnl_total"] is not None}
    sorted_strategies = sorted(available.items(), key=lambda x: x[1]["pnl_total"], reverse=True)

    # PF benchmark: prefer hourly LP result; fall back to DB 15-min scenarios
    # (used when LP was skipped because DB already had PF data).
    _pf_candidates = [
        strategies.get("perfect_foresight_hourly", {}).get("pnl_total"),
        strategies.get("perfect_foresight_unrestricted", {}).get("pnl_total"),
        strategies.get("perfect_foresight_grid_feasible", {}).get("pnl_total"),
    ]
    pf_pnl_total = next((v for v in _pf_candidates if v is not None), None)
    actual_pnl_total = strategies.get("cleared_actual", {}).get("pnl_total")

    # Best forecast strategy
    forecast_strategy_names = [
        k for k in available if k.startswith("forecast_")
    ]
    best_forecast = None
    best_forecast_pnl = None
    if forecast_strategy_names:
        best_forecast = max(
            forecast_strategy_names,
            key=lambda k: available[k]["pnl_total"],
        )
        best_forecast_pnl = available[best_forecast]["pnl_total"]

    rows: List[StrategyRankRow] = []
    rank = 0
    for name, info in sorted_strategies:
        rank += 1
        pnl_val = info["pnl_total"]
        _stats = _dstats.get(name, {})
        rows.append(StrategyRankRow(
            rank=rank,
            strategy_name=name,
            pnl_total_yuan=pnl_val,
            pnl_market_yuan=info.get("pnl_market"),
            pnl_compensation_yuan=info.get("pnl_compensation"),
            gap_vs_perfect_foresight_yuan=(
                pf_pnl_total - pnl_val if pf_pnl_total is not None else None
            ),
            gap_vs_best_forecast_yuan=(
                best_forecast_pnl - pnl_val if best_forecast_pnl is not None else None
            ),
            gap_vs_nominated_yuan=(
                (strategies.get("nominated_dispatch", {}).get("pnl_total") or 0.0) - pnl_val
                if strategies.get("nominated_dispatch", {}).get("available") else None
            ),
            gap_vs_actual_yuan=(
                pnl_val - (actual_pnl_total or 0.0)
                if actual_pnl_total is not None else None
            ),
            capture_rate_vs_pf=(
                pnl_val / pf_pnl_total if pf_pnl_total and pf_pnl_total > 0 else None
            ),
            granularity=info["granularity"],
            data_available=True,
            avg_daily_cycles=_stats.get("cycles"),
            avg_daily_pnl_yuan=pnl_val / _n_days if pnl_val is not None else None,
            captured_spread_yuan_per_mwh=_stats.get("captured_spread"),
            cycle_efficiency=_stats.get("cycle_efficiency"),
        ))

    # Add unavailable strategies at the bottom
    for name, info in strategies.items():
        if not info["available"] or info["pnl_total"] is None:
            rows.append(StrategyRankRow(
                rank=len(rows) + 1,
                strategy_name=name,
                pnl_total_yuan=None,
                pnl_market_yuan=None,
                pnl_compensation_yuan=None,
                gap_vs_perfect_foresight_yuan=None,
                gap_vs_best_forecast_yuan=None,
                gap_vs_nominated_yuan=None,
                gap_vs_actual_yuan=None,
                capture_rate_vs_pf=None,
                granularity=info["granularity"],
                data_available=False,
                avg_daily_cycles=None,
                avg_daily_pnl_yuan=None,
                captured_spread_yuan_per_mwh=None,
                cycle_efficiency=None,
            ))

    caveats.append(
        "ranking: hourly and 15-min P&L figures are NOT directly comparable — "
        "use gaps for relative comparison only, not absolute amounts"
    )

    result = StrategyRankingResult(
        asset_code=asset_code,
        date_from=date_from,
        date_to=date_to,
        rows=rows,
        best_strategy=sorted_strategies[0][0] if sorted_strategies else None,
        best_forecast_strategy=best_forecast,
        perfect_foresight_pnl=pf_pnl_total,
        actual_pnl=actual_pnl_total,
        caveats=caveats,
    )
    return dataclasses.asdict(result)


def _calc_hourly_dispatch_stats(
    dispatch_records: List[dict],
    price_key: str,
    energy_capacity_mwh: float,
    interval_h: float = _INTERVAL_HRS_HOURLY,
) -> dict:
    """
    Cycles, captured spread, and cycle efficiency from LP dispatch records.

    LP convention: discharge_mw >= 0 (discharging), charge_mw >= 0 (charging).
    Use interval_h=_INTERVAL_HRS_15MIN for 15-min records, default 1.0 for hourly.

    Returns
    -------
    cycles           : charge_mwh / energy_capacity_mwh
                       (charge-based — consistent with daily cycling definition)
    captured_spread  : (discharge_revenue − charge_cost) / charge_mwh  [CNY/MWh]
                       = market P&L per MWh of charging energy consumed
    cycle_efficiency : discharge_mwh / charge_mwh  (dimensionless; ≈ round-trip eff)
    """
    discharge_mwh = 0.0
    charge_mwh = 0.0
    discharge_revenue = 0.0
    charge_cost = 0.0
    for r in dispatch_records:
        d_mw = float(r.get("discharge_mw") or 0.0)
        c_mw = float(r.get("charge_mw") or 0.0)
        price = float(r.get(price_key) or 0.0)
        if d_mw > 0:
            mwh = d_mw * interval_h
            discharge_mwh += mwh
            discharge_revenue += mwh * price
        if c_mw > 0:  # positive = charging (LP stores charge_mw >= 0)
            mwh = c_mw * interval_h
            charge_mwh += mwh
            charge_cost += mwh * price
    cycles = charge_mwh / energy_capacity_mwh if energy_capacity_mwh > 0 else None
    market_pnl = discharge_revenue - charge_cost
    captured_spread = market_pnl / charge_mwh if charge_mwh > 0 else None
    cycle_efficiency = discharge_mwh / charge_mwh if charge_mwh > 0 else None
    return {"cycles": cycles, "captured_spread": captured_spread, "cycle_efficiency": cycle_efficiency}


def _calc_15min_dispatch_stats(
    dispatch_records: List[dict],
    price_records: List[dict],
    energy_capacity_mwh: float,
) -> dict:
    """
    Cycles, captured spread, and cycle efficiency from 15-min dispatch records.

    dispatch_mw > 0  = discharge (MWh per interval)
    dispatch_mw < 0  = charging  (MWh per interval, negative)

    Returns
    -------
    cycles           : charge_mwh / energy_capacity_mwh
                       (charge-based — consistent with daily cycling definition)
    captured_spread  : (discharge_revenue − charge_cost) / charge_mwh  [CNY/MWh]
                       = market P&L per MWh of charging energy consumed
    cycle_efficiency : discharge_mwh / charge_mwh  (dimensionless; ≈ round-trip eff)
    """
    def _nts(val) -> str:
        t = pd.Timestamp(val)
        if t.tzinfo is not None:
            # Convert to CST (UTC+8) first so UTC and +08:00 timestamps
            # both normalise to the same naive CST wall-clock key.
            return str(t.tz_convert("Asia/Shanghai").tz_localize(None))
        return str(t)

    price_map = {
        _nts(r.get("time") or r.get("datetime")): float(r.get("price") or 0.0)
        for r in price_records
    }
    discharge_mwh = 0.0
    charge_mwh = 0.0
    discharge_revenue = 0.0
    charge_cost = 0.0
    for r in dispatch_records:
        ts = _nts(r.get("time") or r.get("datetime"))
        mwh = float(r.get("dispatch_mw") or 0.0)
        price = price_map.get(ts, 0.0)
        if mwh > 0:
            discharge_mwh += mwh
            discharge_revenue += mwh * price
        elif mwh < 0:
            charge_mwh += abs(mwh)
            charge_cost += abs(mwh) * price
    cycles = charge_mwh / energy_capacity_mwh if energy_capacity_mwh > 0 else None
    market_pnl = discharge_revenue - charge_cost
    captured_spread = market_pnl / charge_mwh if charge_mwh > 0 else None
    cycle_efficiency = discharge_mwh / charge_mwh if charge_mwh > 0 else None
    return {"cycles": cycles, "captured_spread": captured_spread, "cycle_efficiency": cycle_efficiency}


def _calc_15min_pnl(
    dispatch_records: List[dict],
    price_records: List[dict],
    compensation_yuan_per_mwh: float,
) -> dict:
    """
    Inline 15-min P&L helper (used when DB pnl_df not available).

    Returns dict with keys: total, market, compensation.
    """
    def _nts(val) -> str:
        t = pd.Timestamp(val)
        if t.tzinfo is not None:
            # Convert to CST (UTC+8) first so UTC and +08:00 timestamps
            # both normalise to the same naive CST wall-clock key.
            return str(t.tz_convert("Asia/Shanghai").tz_localize(None))
        return str(t)

    dispatch_map = {
        _nts(r.get("time") or r.get("datetime")): float(r.get("dispatch_mw", 0.0))
        for r in dispatch_records
    }
    price_map = {
        _nts(r.get("time") or r.get("datetime")): float(r.get("price", 0.0))
        for r in price_records
    }
    market_pnl = 0.0
    discharge_mwh = 0.0
    for ts, dispatch in dispatch_map.items():
        price = price_map.get(ts, 0.0)
        # dispatch values are in MWh per interval (already × 0.25)
        market_pnl += dispatch * price
        discharge_mwh += max(dispatch, 0)
    compensation = discharge_mwh * compensation_yuan_per_mwh
    return {"total": market_pnl + compensation, "market": market_pnl, "compensation": compensation}


# ===========================================================================
# Skill 5 — Attribute discrepancy
# ===========================================================================

def attribute_dispatch_discrepancy(
    context: Dict[str, Any],
    ranking: Dict[str, Any],
    attribution_df: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """
    Decompose the gap between perfect foresight and actual P&L into buckets.

    Attribution is a rules-based waterfall — NOT causal proof.

    Bucket cascade:
      grid_restriction    : from DB (PF_unrestricted - PF_grid_feasible), if available
      forecast_error      : PF_hourly_pnl - best_forecast_pnl
      execution_nomination: best_forecast_pnl - nominated_pnl (if available)
      execution_clearing  : nominated_pnl - actual_pnl (if both available)
      asset_issue         : None (outage table not yet implemented)
      residual            : total_gap - sum(non-None buckets)

    Parameters
    ----------
    context        : from load_bess_strategy_comparison_context()
    ranking        : from rank_dispatch_strategies()
    attribution_df : optional DataFrame from load_precomputed_attribution()
                     When provided, grid_restriction is sourced from it.

    Returns
    -------
    JSON-serialisable dict matching DiscrepancyAttributionResult schema.
    """
    asset_code = context["asset_code"]
    date_from = context["date_from"]
    date_to = context["date_to"]
    caveats: List[str] = [
        "attribution: rules-based waterfall — not causal proof",
        "attribution: hourly and 15-min P&L figures bridged by relative gaps, "
        "not absolute amounts",
    ]

    # Inner Mongolia: if DA cleared energy is available, note the cleared-vs-actual distinction
    id_cleared = context.get("id_cleared_energy_15min")
    if id_cleared:
        caveats.append(
            "attribution: id_cleared_energy_15min present (Inner Mongolia DA cleared energy) — "
            "cleared trading energy ≠ actual physical dispatch; "
            "execution_clearing bucket uses nominated vs actual (canon.scenario_dispatch_15min); "
            "gap between id_cleared_energy and actual_dispatch is NOT modelled here — "
            "may represent asset_issue / BOP / grid restriction when outage data is available"
        )

    # Collect key P&L values from ranking
    pnl_by_strategy: Dict[str, Optional[float]] = {
        r["strategy_name"]: r["pnl_total_yuan"] if r["data_available"] else None
        for r in ranking.get("rows", [])
    }

    pf_pnl = ranking.get("perfect_foresight_pnl")
    actual_pnl = ranking.get("actual_pnl")
    best_forecast = ranking.get("best_forecast_strategy")
    best_forecast_pnl = pnl_by_strategy.get(best_forecast) if best_forecast else None
    nominated_pnl = pnl_by_strategy.get("nominated_dispatch")

    total_gap = (pf_pnl - actual_pnl) if (pf_pnl is not None and actual_pnl is not None) else None

    # --- Grid restriction from DB (period aggregate) ---
    grid_restriction: Optional[float] = None
    if attribution_df is not None and not attribution_df.empty:
        if "grid_restriction_loss" in attribution_df.columns:
            gr_series = pd.to_numeric(attribution_df["grid_restriction_loss"], errors="coerce")
            if gr_series.notna().any():
                grid_restriction = float(gr_series.sum())
                caveats.append(
                    "grid_restriction: sourced from reports.bess_asset_daily_attribution "
                    "(PF_unrestricted - PF_grid_feasible from DB)"
                )
    if grid_restriction is None:
        db_pf_unres = pnl_by_strategy.get("perfect_foresight_unrestricted")
        db_pf_grid = pnl_by_strategy.get("perfect_foresight_grid_feasible")
        if db_pf_unres is not None and db_pf_grid is not None:
            grid_restriction = db_pf_unres - db_pf_grid
            caveats.append("grid_restriction: derived from DB scenario P&L difference")
        else:
            caveats.append(
                "grid_restriction: could not be estimated — "
                "neither precomputed attribution nor both PF scenarios available"
            )

    # --- Forecast error ---
    forecast_error: Optional[float] = None
    if pf_pnl is not None and best_forecast_pnl is not None:
        forecast_error = pf_pnl - best_forecast_pnl
        caveats.append(
            f"forecast_error: PF(hourly) - {best_forecast}(hourly) — "
            "both hourly; province-level forecast vs asset-level PF"
        )
    else:
        caveats.append("forecast_error: could not be estimated — PF or forecast P&L missing")

    # --- Execution / nomination ---
    execution_nomination: Optional[float] = None
    if best_forecast_pnl is not None and nominated_pnl is not None:
        execution_nomination = best_forecast_pnl - nominated_pnl
        caveats.append(
            "execution_nomination: forecast_optimal(hourly) - nominated(15min) — "
            "cross-granularity comparison; treat as directional only"
        )
    else:
        if nominated_pnl is None:
            caveats.append(
                "execution_nomination: nominated_dispatch P&L not available — bucket is None"
            )

    # --- Execution / clearing ---
    execution_clearing: Optional[float] = None
    if nominated_pnl is not None and actual_pnl is not None:
        execution_clearing = nominated_pnl - actual_pnl
    else:
        if actual_pnl is None:
            caveats.append("execution_clearing: actual P&L not available — bucket is None")

    # --- Asset issue (TODO) ---
    asset_issue: Optional[float] = None
    caveats.append("asset_issue: None — outage table not yet implemented")

    # --- Residual ---
    explained = sum(
        v for v in [grid_restriction, forecast_error, execution_nomination, execution_clearing]
        if v is not None
    )
    residual = (total_gap - explained) if total_gap is not None else None

    # --- Daily rows ---
    daily_rows: List[DailyDiscrepancyRow] = []
    if attribution_df is not None and not attribution_df.empty:
        for _, row in attribution_df.iterrows():
            d_str = str(pd.Timestamp(row["trade_date"]).date())
            daily_rows.append(DailyDiscrepancyRow(
                date=d_str,
                pf_pnl=_safe_float(row.get("pf_unrestricted_pnl")),
                forecast_pnl=_safe_float(row.get("tt_forecast_optimal_pnl")),
                nominated_pnl=_safe_float(row.get("nominated_pnl")),
                actual_pnl=_safe_float(row.get("cleared_actual_pnl")),
                forecast_error=_safe_float(row.get("forecast_error_loss")),
                execution_nomination=_safe_float(row.get("nomination_loss")),
                execution_clearing=_safe_float(row.get("execution_clearing_loss")),
                residual=None,  # not pre-computed in DB
            ))

    total_explained = sum(
        v for v in [grid_restriction, forecast_error, execution_nomination, execution_clearing]
        if v is not None
    ) if any(
        v is not None for v in [grid_restriction, forecast_error, execution_nomination, execution_clearing]
    ) else None

    result = DiscrepancyAttributionResult(
        asset_code=asset_code,
        date_from=date_from,
        date_to=date_to,
        total_pf_pnl=pf_pnl,
        total_actual_pnl=actual_pnl,
        total_gap=total_gap,
        buckets=DiscrepancyBuckets(
            forecast_error=forecast_error,
            asset_issue=asset_issue,
            grid_restriction=grid_restriction,
            execution_nomination=execution_nomination,
            execution_clearing=execution_clearing,
            residual=residual,
            total_explained=total_explained,
        ),
        attribution_method="rules_based_waterfall",
        daily_rows=daily_rows,
        caveats=caveats,
    )
    return dataclasses.asdict(result)


def _safe_float(v: Any) -> Optional[float]:
    try:
        f = float(v)
        return None if np.isnan(f) else f
    except (TypeError, ValueError):
        return None


# ===========================================================================
# Skill 6 — Generate asset strategy report
# ===========================================================================

def generate_asset_strategy_report(
    asset_code: str,
    date_from: str,
    date_to: str,
    period_type: str = "monthly",
    context: Optional[Dict[str, Any]] = None,
    ranking: Optional[Dict[str, Any]] = None,
    attribution: Optional[Dict[str, Any]] = None,
    db_pnl_df: Optional[pd.DataFrame] = None,
    db_attr_df: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """
    Generate a reusable daily / weekly / monthly asset strategy report.

    If context / ranking / attribution are not supplied, they are computed
    on the fly using the other 5 skills with default settings.

    period_type : "daily" | "weekly" | "monthly"

    Returns
    -------
    JSON-serialisable dict matching AssetStrategyReport schema.
    Includes:
      - sections dict (text, suitable for agent responses)
      - dataframes/rows (suitable for Streamlit display)
      - markdown string (suitable for Slack/email distribution)
    """
    from libs.decision_models.resources.bess_context import (
        load_precomputed_attribution,
        load_precomputed_scenario_pnl,
    )

    generated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    d_from = datetime.date.fromisoformat(date_from)
    d_to = datetime.date.fromisoformat(date_to)
    all_caveats: List[str] = []

    # --- Compute or reuse context ---
    if context is None:
        context = load_bess_strategy_comparison_context(asset_code, date_from, date_to)
    all_caveats.extend(context.get("data_quality_notes", []))

    # --- Load pre-computed DB P&L (most accurate for nominated/actual) ---
    # Accept pre-loaded DataFrames to avoid duplicate DB queries when called from
    # run_bess_daily_strategy_analysis (which already loaded these in Steps 5-6).
    if db_pnl_df is None:
        db_pnl_df, db_pnl_notes = load_precomputed_scenario_pnl(asset_code, d_from, d_to)
        all_caveats.extend(db_pnl_notes)

    if db_attr_df is None:
        db_attr_df, db_attr_notes = load_precomputed_attribution(asset_code, d_from, d_to)
        all_caveats.extend(db_attr_notes)

    # --- Compute or reuse ranking and attribution ---
    if ranking is None:
        pf_result = run_perfect_foresight_dispatch(context)
        forecast_suite = run_forecast_dispatch_suite(context)
        ranking = rank_dispatch_strategies(
            context, pf_result, forecast_suite,
            db_pnl_df=db_pnl_df if not db_pnl_df.empty else None,
        )
    all_caveats.extend(ranking.get("caveats", []))

    if attribution is None:
        attribution = attribute_dispatch_discrepancy(
            context, ranking,
            attribution_df=db_attr_df if not db_attr_df.empty else None,
        )
    all_caveats.extend(attribution.get("caveats", []))

    # --- Build period-aggregated P&L tables ---
    daily_rows: List[dict] = []
    weekly_rows: List[dict] = []
    monthly_rows: List[dict] = []

    if not db_pnl_df.empty:
        daily_rows = _build_period_rows(db_pnl_df, "D")
        weekly_rows = _build_period_rows(db_pnl_df, "W")
        monthly_rows = _build_period_rows(db_pnl_df, "M")

    # --- YTD summary ---
    ytd_summary = _build_ytd_summary(asset_code, d_to, db_pnl_df, ranking)

    # --- Forecast to year-end ---
    fy_summary = _build_fy_summary(asset_code, d_to, db_pnl_df, ytd_summary)

    # --- P&L comparison table ---
    pnl_rows = []
    for row in ranking.get("rows", []):
        if row["data_available"]:
            pnl_rows.append([
                row["strategy_name"],
                f"{row['pnl_total_yuan']:,.0f}" if row["pnl_total_yuan"] is not None else "—",
                f"{row['gap_vs_perfect_foresight_yuan']:,.0f}" if row["gap_vs_perfect_foresight_yuan"] is not None else "—",
                f"{row['capture_rate_vs_pf']:.1%}" if row["capture_rate_vs_pf"] is not None else "—",
                row["granularity"],
            ])
    pnl_table = PnLComparisonTable(
        headers=["strategy", "total_pnl_yuan", "gap_vs_pf", "capture_vs_pf", "granularity"],
        rows=pnl_rows,
    )

    # --- Sections ---
    buckets = attribution.get("buckets", {})
    total_gap = attribution.get("total_gap")
    sections: Dict[str, Any] = {
        "executive_summary": _build_exec_summary(
            asset_code, date_from, date_to, ranking, attribution, period_type
        ),
        "strategy_ranking": ranking.get("rows", []),
        "realised_pnl_comparison": dataclasses.asdict(pnl_table),
        "discrepancy_waterfall": {
            "method": "rules_based_waterfall",
            "total_gap_yuan": total_gap,
            "buckets": buckets,
        },
        "asset_issues": [
            "No outage data available — ops.asset_outage_log not yet implemented"
        ],
        "grid_restrictions": [
            f"Grid restriction loss (period): {buckets.get('grid_restriction'):,.0f} CNY"
            if buckets.get("grid_restriction") is not None
            else "Grid restriction data not available for this period"
        ],
        "ytd_and_forecast": {
            "ytd": dataclasses.asdict(ytd_summary) if ytd_summary else None,
            "forecast_to_year_end": dataclasses.asdict(fy_summary) if fy_summary else None,
        },
        "data_quality_caveats": list(dict.fromkeys(all_caveats)),  # deduplicated
    }

    # --- Markdown ---
    markdown = _build_markdown_report(
        asset_code, date_from, date_to, period_type,
        sections, ranking, attribution, ytd_summary, fy_summary,
        context=context,
    )

    result = AssetStrategyReport(
        asset_code=asset_code,
        date_from=date_from,
        date_to=date_to,
        period_type=period_type,
        generated_at=generated_at,
        pnl_comparison=pnl_table,
        strategy_ranking=ranking.get("rows", []),
        discrepancy_waterfall=sections["discrepancy_waterfall"],
        ytd_summary=ytd_summary,
        forecast_to_year_end=fy_summary,
        daily_rows=daily_rows,
        weekly_rows=weekly_rows,
        monthly_rows=monthly_rows,
        sections=sections,
        markdown=markdown,
        data_quality_caveats=list(dict.fromkeys(all_caveats)),
    )
    return dataclasses.asdict(result)


# ---------------------------------------------------------------------------
# Report helpers
# ---------------------------------------------------------------------------

def _build_period_rows(db_pnl_df: pd.DataFrame, freq: str) -> List[dict]:
    """Aggregate DB scenario P&L by period (D/W/M) across scenarios."""
    df = db_pnl_df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["period"] = df["trade_date"].dt.to_period(freq).astype(str)

    grp = df.groupby(["period", "scenario_name"]).agg(
        total_revenue_yuan=("total_revenue_yuan", "sum"),
        market_revenue_yuan=("market_revenue_yuan", "sum"),
        discharge_mwh=("discharge_mwh", "sum"),
    ).reset_index()

    rows = []
    for period, period_grp in grp.groupby("period"):
        row: dict = {"period": period}
        for _, r in period_grp.iterrows():
            prefix = r["scenario_name"]
            row[f"{prefix}_total"] = _safe_float(r["total_revenue_yuan"])
            row[f"{prefix}_market"] = _safe_float(r["market_revenue_yuan"])
            row[f"{prefix}_discharge_mwh"] = _safe_float(r["discharge_mwh"])
        rows.append(row)
    return rows


def _build_ytd_summary(
    asset_code: str,
    as_of_date: datetime.date,
    db_pnl_df: pd.DataFrame,
    ranking: Dict[str, Any],
) -> Optional[YTDSummary]:
    if db_pnl_df.empty:
        return None

    df = db_pnl_df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    ytd_df = df[df["trade_date"].dt.year == as_of_date.year]

    cleared = ytd_df[ytd_df["scenario_name"] == "cleared_actual"]
    ytd_actual = _safe_float(cleared["total_revenue_yuan"].sum()) if not cleared.empty else None

    pf_unres = ytd_df[ytd_df["scenario_name"] == "perfect_foresight_unrestricted"]
    ytd_pf = _safe_float(pf_unres["total_revenue_yuan"].sum()) if not pf_unres.empty else None

    ytd_capture = (ytd_actual / ytd_pf) if (ytd_actual and ytd_pf and ytd_pf > 0) else None
    ytd_days = int(ytd_df["trade_date"].dt.date.nunique())
    data_through = str(ytd_df["trade_date"].max().date()) if not ytd_df.empty else str(as_of_date)

    return YTDSummary(
        asset_code=asset_code,
        year=as_of_date.year,
        ytd_actual_pnl=ytd_actual,
        ytd_pf_pnl=ytd_pf,
        ytd_capture_rate=ytd_capture,
        ytd_days_with_data=ytd_days,
        data_through=data_through,
    )


def _build_fy_summary(
    asset_code: str,
    as_of_date: datetime.date,
    db_pnl_df: pd.DataFrame,
    ytd: Optional[YTDSummary],
) -> Optional[ForecastToYearEnd]:
    if ytd is None or ytd.ytd_actual_pnl is None or ytd.ytd_days_with_data == 0:
        return ForecastToYearEnd(
            asset_code=asset_code,
            year=as_of_date.year,
            realized_ytd=None,
            projected_remainder=None,
            projected_total=None,
            projection_method="ytd_daily_avg_run_rate",
            caveats=["insufficient YTD data for year-end projection"],
        )
    year_start = datetime.date(as_of_date.year, 1, 1)
    year_end = datetime.date(as_of_date.year, 12, 31)
    days_elapsed = max((as_of_date - year_start).days + 1, 1)
    days_remaining = max((year_end - as_of_date).days, 0)
    daily_avg = ytd.ytd_actual_pnl / ytd.ytd_days_with_data
    projected_remainder = daily_avg * days_remaining
    projected_total = ytd.ytd_actual_pnl + projected_remainder
    return ForecastToYearEnd(
        asset_code=asset_code,
        year=as_of_date.year,
        realized_ytd=ytd.ytd_actual_pnl,
        projected_remainder=projected_remainder,
        projected_total=projected_total,
        projection_method="ytd_daily_avg_run_rate",
        caveats=[
            "projection uses simple daily average run rate — does not account for "
            "seasonality, market changes, or curtailment",
        ],
    )


def _build_exec_summary(
    asset_code: str,
    date_from: str,
    date_to: str,
    ranking: Dict[str, Any],
    attribution: Dict[str, Any],
    period_type: str,
) -> str:
    best = ranking.get("best_strategy", "—")
    pf_pnl = ranking.get("perfect_foresight_pnl")
    actual_pnl = ranking.get("actual_pnl")
    total_gap = attribution.get("total_gap")

    parts = [
        f"Asset: {asset_code}  |  Period: {date_from} to {date_to} ({period_type})",
        f"Best strategy by realised P&L: {best}",
    ]
    if pf_pnl is not None:
        parts.append(f"Perfect foresight benchmark (hourly): {pf_pnl:,.0f} CNY")
    if actual_pnl is not None:
        parts.append(f"Actual (cleared) P&L: {actual_pnl:,.0f} CNY")
    if total_gap is not None:
        parts.append(f"Total gap (PF - actual): {total_gap:,.0f} CNY")
    buckets = attribution.get("buckets", {})
    for bucket, label in [
        ("forecast_error", "Forecast error"),
        ("grid_restriction", "Grid restriction"),
        ("execution_nomination", "Execution / nomination"),
        ("execution_clearing", "Execution / clearing"),
        ("asset_issue", "Asset issue"),
        ("residual", "Residual / unexplained"),
    ]:
        val = buckets.get(bucket)
        if val is not None:
            parts.append(f"  {label}: {val:,.0f} CNY")
    return "\n".join(parts)


def _build_markdown_report(
    asset_code: str,
    date_from: str,
    date_to: str,
    period_type: str,
    sections: Dict[str, Any],
    ranking: Dict[str, Any],
    attribution: Dict[str, Any],
    ytd: Optional[YTDSummary],
    fy: Optional[ForecastToYearEnd],
    context: Optional[Dict[str, Any]] = None,
) -> str:
    lines = [
        f"# BESS Strategy Report — {asset_code}",
        f"**Period:** {date_from} to {date_to} ({period_type})  ",
        "",
        "## 1. Executive Summary",
        "```",
        sections.get("executive_summary", ""),
        "```",
        "",
        "## 2. Strategy Ranking",
        "| Rank | Strategy | P&L (CNY) | Gap vs PF | Capture % | Granularity |",
        "|------|----------|-----------|-----------|-----------|-------------|",
    ]
    for row in ranking.get("rows", []):
        if not row["data_available"]:
            continue
        pnl_str = f"{row['pnl_total_yuan']:,.0f}" if row["pnl_total_yuan"] is not None else "—"
        gap_str = f"{row['gap_vs_perfect_foresight_yuan']:,.0f}" if row["gap_vs_perfect_foresight_yuan"] is not None else "—"
        cap_str = f"{row['capture_rate_vs_pf']:.1%}" if row["capture_rate_vs_pf"] is not None else "—"
        lines.append(
            f"| {row['rank']} | {row['strategy_name']} | {pnl_str} | {gap_str} | {cap_str} | {row['granularity']} |"
        )

    lines += [
        "",
        "## 3. Discrepancy Attribution (Waterfall)",
        "_Rules-based waterfall — not causal proof._  ",
        "",
    ]
    buckets = attribution.get("buckets", {})
    total_gap = attribution.get("total_gap")
    if total_gap is not None:
        lines.append(f"**Total gap (PF − actual): {total_gap:,.0f} CNY**  ")
    for bucket, label in [
        ("grid_restriction", "Grid restriction"),
        ("forecast_error", "Forecast error"),
        ("execution_nomination", "Execution / nomination"),
        ("execution_clearing", "Execution / clearing"),
        ("asset_issue", "Asset issue"),
        ("residual", "Residual"),
    ]:
        val = buckets.get(bucket)
        lines.append(
            f"- {label}: {'—' if val is None else f'{val:,.0f} CNY'}"
        )

    if ytd is not None:
        lines += [
            "",
            "## 4. YTD Summary",
            f"- YTD actual P&L: {'—' if ytd.ytd_actual_pnl is None else f'{ytd.ytd_actual_pnl:,.0f} CNY'}",
            f"- YTD PF benchmark: {'—' if ytd.ytd_pf_pnl is None else f'{ytd.ytd_pf_pnl:,.0f} CNY'}",
            f"- YTD capture rate: {'—' if ytd.ytd_capture_rate is None else f'{ytd.ytd_capture_rate:.1%}'}",
            f"- Data through: {ytd.data_through}",
        ]

    if fy is not None and fy.projected_total is not None:
        lines += [
            "",
            "## 5. Forecast to Year-End",
            f"- Realized YTD: {fy.realized_ytd:,.0f} CNY" if fy.realized_ytd else "",
            f"- Projected remainder: {fy.projected_remainder:,.0f} CNY" if fy.projected_remainder else "",
            f"- Projected full-year: {fy.projected_total:,.0f} CNY",
            f"- Method: {fy.projection_method}",
        ]
        if fy.caveats:
            lines.append(f"- _{fy.caveats[0]}_")

    # Inner Mongolia cleared-vs-actual note
    if context is not None and context.get("id_cleared_energy_15min"):
        lines += [
            "",
            "## 6. Inner Mongolia Market Data Note",
            "_Applies to Mengxi assets only._  ",
            "",
            "- **`id_cleared_energy`** = DA market-cleared trading energy "
            "(`marketdata.md_id_cleared_energy`) — NOT actual physical dispatch.",
            "- **`actual_dispatch`** = physical output as recorded in "
            "`canon.scenario_dispatch_15min` (cleared_actual scenario).",
            "- These two figures may differ. The gap may indicate asset issues, "
            "BOP constraints, or grid operator real-time re-dispatch.",
            "- `cleared_power_mw_implied_15min` = cleared_energy_mwh_15min / 0.25 "
            "(implied average power — informational only, not a measured value).",
            "- Do NOT interpret `id_cleared_energy` as the asset's physical output.",
        ]

    caveats = sections.get("data_quality_caveats", [])
    if caveats:
        lines += [
            "",
            "## 7. Data Quality Caveats",
        ]
        for c in caveats[:10]:  # cap at 10 in markdown
            lines.append(f"- {c}")
        if len(caveats) > 10:
            lines.append(f"- _...and {len(caveats) - 10} more — see full caveats in data_quality_caveats field_")

    return "\n".join(lines)
