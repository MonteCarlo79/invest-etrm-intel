"""
libs/decision_models/bess_dispatch_simulation_multiday.py

Reusable model asset: BESS dispatch simulation over multiple days.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SCOPE: MULTI-DAY BATCH, HOURLY PRICES, SOC RESETS EACH DAY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Wraps compute_dispatch_from_hourly_prices() in
services/bess_map/optimisation_engine.py.

Each day is still solved independently (SOC resets to 0 per day).
This matches the behaviour of the production capture pipeline.
No cross-day SOC carryover is modelled.

For single-day usage: bess_dispatch_optimization.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
USAGE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    import libs.decision_models.bess_dispatch_simulation_multiday  # register
    from libs.decision_models.runners.local import run

    result = run("bess_dispatch_simulation_multiday", {
        "hourly_prices": [
            {"datetime": "2026-01-01T00:00:00", "price": 30.0},
            {"datetime": "2026-01-01T01:00:00", "price": 35.0},
            ...  # 24 entries for each day included
        ],
        "power_mw": 100.0,
        "duration_h": 2.0,
        "roundtrip_eff": 0.85,
    })
    # result["dispatch_records"]  — list of per-hour dicts
    # result["daily_profit"]      — list of {date, profit} dicts
    # result["n_days_solved"]
    # result["n_days_skipped"]
    # result["energy_capacity_mwh"]

Direct engine call (no registry):

    from services.bess_map.optimisation_engine import compute_dispatch_from_hourly_prices
    dispatch_df, profit_s = compute_dispatch_from_hourly_prices(
        hourly_prices=my_series,  # pd.Series with DatetimeIndex
        power_mw=100.0, duration_h=2.0, roundtrip_eff=0.85,
    )
"""
from __future__ import annotations

import dataclasses
from typing import Any, Dict, List

import pandas as pd

from libs.decision_models.model_spec import ModelSpec
from libs.decision_models.registry import registry
from libs.decision_models.schemas.bess_dispatch_simulation_multiday import (
    DailyProfitRecord,
    MultiDayDispatchInput,
    MultiDayDispatchOutput,
    MultiDayDispatchRecord,
)


def _run(
    hourly_prices: List[dict],
    power_mw: float,
    duration_h: float,
    roundtrip_eff: float = 0.85,
    max_throughput_mwh: float = None,
    max_cycles_per_day: float = None,
) -> Dict[str, Any]:
    """
    Convert the JSON-serializable hourly_prices list into a pd.Series,
    then call the production multi-day engine.
    """
    from services.bess_map.optimisation_engine import compute_dispatch_from_hourly_prices

    # --- Input validation ---
    if not hourly_prices:
        raise ValueError("hourly_prices must not be empty")
    if power_mw <= 0:
        raise ValueError(f"power_mw must be positive, got {power_mw}")
    if duration_h <= 0:
        raise ValueError(f"duration_h must be positive, got {duration_h}")
    if not (0 < roundtrip_eff <= 1):
        raise ValueError(f"roundtrip_eff must be in (0, 1], got {roundtrip_eff}")

    # Build pd.Series with DatetimeIndex expected by the engine
    try:
        price_series = pd.Series(
            {pd.Timestamp(rec["datetime"]): float(rec["price"]) for rec in hourly_prices}
        ).sort_index()
        price_series.index = pd.DatetimeIndex(price_series.index)
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(
            f"hourly_prices must be a list of {{\"datetime\": str, \"price\": float}} dicts. "
            f"Parse error: {exc}"
        ) from exc

    # Count input days before solving
    n_input_days = price_series.index.normalize().nunique()

    dispatch_df, profit_s = compute_dispatch_from_hourly_prices(
        hourly_prices=price_series,
        power_mw=float(power_mw),
        duration_h=float(duration_h),
        roundtrip_eff=float(roundtrip_eff),
        max_throughput_mwh=float(max_throughput_mwh) if max_throughput_mwh is not None else None,
        max_cycles_per_day=float(max_cycles_per_day) if max_cycles_per_day is not None else None,
    )

    n_days_solved = len(profit_s)
    n_days_skipped = n_input_days - n_days_solved

    # Serialize dispatch_df → list of dicts
    dispatch_records: List[MultiDayDispatchRecord] = []
    if not dispatch_df.empty:
        for dt_idx, row in dispatch_df.iterrows():
            dispatch_records.append(MultiDayDispatchRecord(
                datetime=dt_idx.isoformat(),
                charge_mw=float(row["charge_mw"]),
                discharge_mw=float(row["discharge_mw"]),
                dispatch_grid_mw=float(row["dispatch_grid_mw"]),
                soc_mwh=float(row["soc_mwh"]),
                solver_status=str(row["solver_status"]),
            ))

    # Serialize profit_s → list of dicts
    daily_profit_records: List[DailyProfitRecord] = [
        DailyProfitRecord(date=str(d), profit=float(p))
        for d, p in profit_s.items()
    ]

    output = MultiDayDispatchOutput(
        dispatch_records=dispatch_records,
        daily_profit=daily_profit_records,
        n_days_solved=n_days_solved,
        n_days_skipped=max(n_days_skipped, 0),
        energy_capacity_mwh=float(power_mw) * float(duration_h),
    )

    # Convert nested dataclasses to plain dicts
    return {
        "dispatch_records": [dataclasses.asdict(r) for r in output.dispatch_records],
        "daily_profit": [dataclasses.asdict(r) for r in output.daily_profit],
        "n_days_solved": output.n_days_solved,
        "n_days_skipped": output.n_days_skipped,
        "energy_capacity_mwh": output.energy_capacity_mwh,
    }


_SPEC = ModelSpec(
    name="bess_dispatch_simulation_multiday",
    version="1.0.0",
    description=(
        "Multi-day BESS dispatch simulation over a contiguous or sparse hourly price series. "
        "Each day is solved independently (SOC resets to 0 per day; no cross-day carryover). "
        "Input: list of {datetime, price} records + battery params. "
        "Output: per-hour dispatch schedule and per-day profit summary. "
        "For single-day use bess_dispatch_optimization."
    ),
    input_schema=MultiDayDispatchInput,
    output_schema=MultiDayDispatchOutput,
    run_fn=_run,
    tags=["bess", "dispatch", "simulation", "perfect_foresight", "multi_day", "batch", "arbitrage"],
    metadata={
        "asset_type": "bess",
        "source_module": "services/bess_map/optimisation_engine.py",
        "production_pipeline": "services/bess_map/run_capture_pipeline.py",

        "scope": "multi_day",
        "granularity": "hourly",
        "cross_day_soc_carryover": False,
        "initial_soc_per_day": "zero",
        "terminal_soc_per_day": "unconstrained",
        "days_with_nan_prices": "skipped",
        "solver": "pulp_cbc",
        "solver_type": "milp",

        "singleday_model": "bess_dispatch_optimization",

        "limitations": [
            "No cross-day SOC carryover — each day resets to SOC=0",
            "Hourly granularity only",
            "Perfect foresight — requires complete price series up front",
            "Same per-day constraints as bess_dispatch_optimization",
        ],
    },
)

registry.register(_SPEC)
