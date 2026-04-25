"""
services/bess_map/optimisation_engine.py

Pure-computation BESS dispatch optimisation engine.

Extracted from run_capture_pipeline.py so it can be imported by:
  - run_capture_pipeline.py (unchanged behaviour)
  - libs/decision_models/bess_dispatch_optimization.py (shared model library)
  - any future ECS job, notebook, or agent tool

No DB, no argparse, no Streamlit, no side effects.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pulp


# ---------------------------------------------------------------------------
# Core dataclass
# ---------------------------------------------------------------------------

@dataclass
class DispatchResult:
    """Per-day optimisation result returned by optimise_day()."""
    charge_mw: np.ndarray      # shape (24,)
    discharge_mw: np.ndarray   # shape (24,)
    soc_mwh: np.ndarray        # shape (24,) — end-of-interval SoC
    profit: float              # Yuan (or price-unit · MWh) for the day
    status: str                # PuLP solver status string e.g. "Optimal"


# ---------------------------------------------------------------------------
# Single-day LP optimisation
# ---------------------------------------------------------------------------

def optimise_day(
    prices: np.ndarray,
    power_mw: float,
    duration_h: float,
    roundtrip_eff: float,
    max_throughput_mwh: Optional[float] = None,
    max_cycles_per_day: Optional[float] = None,
) -> DispatchResult:
    """
    Solve the BESS arbitrage LP for one day of 24 hourly prices.

    The round-trip efficiency is applied symmetrically:
        eta_c = eta_d = sqrt(roundtrip_eff)

    Degradation proxies (optional):
        max_throughput_mwh  — cap on total discharge energy per day
        max_cycles_per_day  — cap on equivalent full cycles/day
                              (discharge_energy <= cycles × energy_capacity)

    Args:
        prices:              Numpy array of 24 hourly prices.
        power_mw:            Inverter / power rating (MW).
        duration_h:          Battery duration (hours), giving energy_capacity = power_mw × duration_h.
        roundtrip_eff:       Round-trip efficiency, e.g. 0.85.
        max_throughput_mwh:  Optional daily discharge cap (MWh).
        max_cycles_per_day:  Optional daily cycle cap.

    Returns:
        DispatchResult with charge_mw, discharge_mw, soc_mwh arrays (length 24),
        total profit, and solver status string.

    Raises:
        ValueError: if prices does not have exactly 24 elements.
    """
    T = len(prices)
    if T != 24:
        raise ValueError(f"optimise_day expects 24 hourly prices, got {T}")

    eta_c = float(np.sqrt(roundtrip_eff))
    eta_d = float(np.sqrt(roundtrip_eff))
    e_cap = float(power_mw * duration_h)

    prob = pulp.LpProblem("bess_arbitrage", pulp.LpMaximize)

    ch = pulp.LpVariable.dicts("ch", range(T), lowBound=0, upBound=power_mw, cat="Continuous")
    dis = pulp.LpVariable.dicts("dis", range(T), lowBound=0, upBound=power_mw, cat="Continuous")
    soc = pulp.LpVariable.dicts("soc", range(T + 1), lowBound=0, upBound=e_cap, cat="Continuous")

    # Binary variable prevents simultaneous charge and discharge
    y = pulp.LpVariable.dicts("y", range(T), lowBound=0, upBound=1, cat="Binary")
    M = power_mw

    # Initial SoC = 0 (no cross-day carryover)
    prob += soc[0] == 0

    for t in range(T):
        prob += soc[t + 1] == soc[t] + ch[t] * eta_c - dis[t] * (1.0 / eta_d)
        prob += ch[t] <= M * y[t]
        prob += dis[t] <= M * (1 - y[t])

    # Optional degradation constraints
    if max_throughput_mwh is not None:
        prob += pulp.lpSum(dis[t] for t in range(T)) <= float(max_throughput_mwh)

    if max_cycles_per_day is not None:
        prob += pulp.lpSum(dis[t] for t in range(T)) <= float(max_cycles_per_day) * e_cap

    # Objective: maximise (discharge - charge) revenue
    prob += pulp.lpSum(float(prices[t]) * (dis[t] - ch[t]) for t in range(T))
    prob.solve(pulp.PULP_CBC_CMD(msg=False))

    status = pulp.LpStatus.get(prob.status, str(prob.status))

    chv = np.array([pulp.value(ch[t]) for t in range(T)], dtype=float)
    disv = np.array([pulp.value(dis[t]) for t in range(T)], dtype=float)
    socv = np.array([pulp.value(soc[t + 1]) for t in range(T)], dtype=float)
    profit = float(np.nansum(prices * (disv - chv)))

    return DispatchResult(chv, disv, socv, profit, status)


# ---------------------------------------------------------------------------
# Multi-day orchestration
# ---------------------------------------------------------------------------

def compute_dispatch_from_hourly_prices(
    hourly_prices: pd.Series,
    power_mw: float,
    duration_h: float,
    roundtrip_eff: float,
    max_throughput_mwh: Optional[float] = None,
    max_cycles_per_day: Optional[float] = None,
) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Run optimise_day() for every complete day in hourly_prices.

    Args:
        hourly_prices:       pd.Series with DatetimeIndex, hourly granularity.
                             Rows with NaN are dropped (day is skipped if any hour is NaN).
        power_mw:            Inverter power rating (MW).
        duration_h:          Battery duration (hours).
        roundtrip_eff:       Round-trip efficiency.
        max_throughput_mwh:  Optional daily discharge cap.
        max_cycles_per_day:  Optional daily cycle cap.

    Returns:
        dispatch_df:  DataFrame indexed by datetime with columns:
                      charge_mw, discharge_mw, dispatch_grid_mw, soc_mwh, solver_status
        profit_s:     pd.Series indexed by date with daily profit values.
    """
    s = hourly_prices.dropna().copy()
    if s.empty:
        return pd.DataFrame(), pd.Series(dtype=float)

    if not isinstance(s.index, pd.DatetimeIndex):
        s.index = pd.to_datetime(s.index)
    # Strip timezone so pd.date_range (tz-naive) can reindex each day's group
    if s.index.tz is not None:
        s.index = s.index.tz_localize(None)

    df = s.to_frame("price")
    df["date"] = df.index.date
    df["hour"] = df.index.hour

    dispatch_rows: List[pd.DataFrame] = []
    daily_profit: Dict[dt.date, float] = {}

    for d, g in df.groupby("date"):
        idx = pd.date_range(pd.Timestamp(d), periods=24, freq="h")
        g2 = g.reindex(idx)
        prices = g2["price"].to_numpy(dtype=float)

        if np.isnan(prices).any():
            continue

        res = optimise_day(
            prices,
            power_mw=power_mw,
            duration_h=duration_h,
            roundtrip_eff=roundtrip_eff,
            max_throughput_mwh=max_throughput_mwh,
            max_cycles_per_day=max_cycles_per_day,
        )

        out = pd.DataFrame({
            "datetime": idx,
            "charge_mw": res.charge_mw,
            "discharge_mw": res.discharge_mw,
            "dispatch_grid_mw": res.discharge_mw - res.charge_mw,
            "soc_mwh": res.soc_mwh,
            "solver_status": res.status,
        }).set_index("datetime")

        dispatch_rows.append(out)
        daily_profit[d] = res.profit

    dispatch_df = pd.concat(dispatch_rows).sort_index() if dispatch_rows else pd.DataFrame()
    profit_s = pd.Series(daily_profit).sort_index()
    profit_s.name = "profit"
    return dispatch_df, profit_s
