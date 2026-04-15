"""
libs/decision_models/schemas/bess_dispatch_simulation_multiday.py

Input/output contracts for the bess_dispatch_simulation_multiday model.

SCOPE: multi-day batch simulation over a contiguous or sparse hourly price series.
Maps to compute_dispatch_from_hourly_prices() in services/bess_map/optimisation_engine.py.

IMPORTANT LIMITATION: each day is solved independently with SOC reset to 0.
There is NO cross-day SOC carryover. This is the same behaviour as the
production capture pipeline (run_capture_pipeline.py).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class MultiDayDispatchRecord:
    """Per-interval dispatch result (one row per hour across all days)."""
    datetime: str            # ISO8601, e.g. "2026-01-01T08:00:00"
    charge_mw: float
    discharge_mw: float
    dispatch_grid_mw: float  # discharge_mw − charge_mw
    soc_mwh: float           # end-of-interval SOC
    solver_status: str       # PuLP status for the day this interval belongs to


@dataclass
class DailyProfitRecord:
    """Daily profit summary."""
    date: str                # ISO date, e.g. "2026-01-01"
    profit: float            # sum(price[t] × dispatch_grid_mw[t]) for the day


@dataclass
class MultiDayDispatchInput:
    """
    Input for multi-day BESS dispatch simulation.

    SCOPE CONSTRAINTS:
      - hourly_prices must be hourly granularity (one entry per hour).
      - Each day is solved independently; SOC resets to 0 at the start of every day.
      - Days with any NaN price values are skipped (not solved).
      - Consecutive days are NOT linked via SOC. This is intentional and matches
        the production pipeline behaviour.

    Fields:
        hourly_prices:
            List of {"datetime": "<ISO8601>", "price": <float>} records.
            Must be hourly. Any day with a missing hour (NaN or absent) is skipped.
            Minimum 1 complete day required.

        power_mw:             Inverter / power rating (MW). Same as single-day model.
        duration_h:           Battery duration (hours). Same as single-day model.
        roundtrip_eff:        Round-trip efficiency in (0, 1]. Same as single-day model.
        max_throughput_mwh:   Optional daily discharge cap (applied independently per day).
        max_cycles_per_day:   Optional daily cycle cap (applied independently per day).
    """
    hourly_prices: List[dict]   # [{"datetime": str, "price": float}, ...]
    power_mw: float
    duration_h: float
    roundtrip_eff: float = 0.85
    max_throughput_mwh: Optional[float] = None
    max_cycles_per_day: Optional[float] = None


@dataclass
class MultiDayDispatchOutput:
    """
    Output for multi-day BESS dispatch simulation.

    Fields:
        dispatch_records:
            Per-hour dispatch results across all solved days.
            Ordered chronologically. Days with NaN prices are absent.

        daily_profit:
            Per-day profit summary. One entry per solved day.

        n_days_solved:    Number of days successfully solved.
        n_days_skipped:   Number of days skipped due to missing price data.
        energy_capacity_mwh: power_mw × duration_h (informational).
    """
    dispatch_records: List[MultiDayDispatchRecord]
    daily_profit: List[DailyProfitRecord]
    n_days_solved: int
    n_days_skipped: int
    energy_capacity_mwh: float
