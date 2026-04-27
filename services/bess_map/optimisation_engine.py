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
# Core LP engine — arbitrary horizon
# ---------------------------------------------------------------------------

def optimise_window(
    prices: np.ndarray,
    power_mw: float,
    duration_h: float,
    roundtrip_eff: float,
    max_throughput_mwh: Optional[float] = None,
    max_cycles_per_day: Optional[float] = None,
    compensation_yuan_per_mwh: float = 0.0,
    dt: float = 1.0,
) -> DispatchResult:
    """
    Solve the BESS arbitrage LP for an arbitrary number of intervals T.

    SOC starts at 0 at t=0 and carries over naturally across the full window,
    enabling cross-day SOC continuity when T > intervals_per_day.

    The round-trip efficiency is applied symmetrically:
        eta_c = eta_d = sqrt(roundtrip_eff)

    The compensation (subsidy) is added to the discharge incentive so the LP
    dispatches when (market_price + compensation) > 0, not just when price > 0.
    The reported profit field is market-only cash flow.

    Degradation proxies (optional) are treated as *per-day* limits and scaled
    linearly by the window length so the cap is fair across windows of different
    sizes and granularities:
        max_throughput_mwh  — per-day cap on total discharge energy (MWh)
        max_cycles_per_day  — per-day cap on equivalent full cycles
    Scaling factor: n_days_window = T * dt / 24.0

    Args:
        prices:                    Numpy array of T prices (CNY/MWh).
        power_mw:                  Inverter / power rating (MW).
        duration_h:                Battery duration (hours), e_cap = power_mw × duration_h.
        roundtrip_eff:             Round-trip efficiency, e.g. 0.85.
        max_throughput_mwh:        Optional per-day discharge cap (MWh); scaled by n_days_window.
        max_cycles_per_day:        Optional per-day cycle cap; scaled by n_days_window.
        compensation_yuan_per_mwh: Discharge subsidy (CNY/MWh) added to LP objective.
        dt:                        Interval duration in hours (default 1.0 for hourly,
                                   use 0.25 for 15-min intervals).

    Returns:
        DispatchResult with charge_mw, discharge_mw, soc_mwh arrays (length T),
        market-only profit, and solver status string.
    """
    T = len(prices)
    eta_c = float(np.sqrt(roundtrip_eff))
    eta_d = float(np.sqrt(roundtrip_eff))
    e_cap = float(power_mw * duration_h)

    prob = pulp.LpProblem("bess_arbitrage_window", pulp.LpMaximize)

    ch = pulp.LpVariable.dicts("ch", range(T), lowBound=0, upBound=power_mw, cat="Continuous")
    dis = pulp.LpVariable.dicts("dis", range(T), lowBound=0, upBound=power_mw, cat="Continuous")
    soc = pulp.LpVariable.dicts("soc", range(T + 1), lowBound=0, upBound=e_cap, cat="Continuous")

    # Binary variable prevents simultaneous charge and discharge
    y = pulp.LpVariable.dicts("y", range(T), lowBound=0, upBound=1, cat="Binary")
    M = power_mw

    # Initial SoC = 0 at the start of the window
    prob += soc[0] == 0

    for t in range(T):
        # SOC update: power [MW] × dt [h] = energy [MWh]
        prob += soc[t + 1] == soc[t] + ch[t] * eta_c * dt - dis[t] * (1.0 / eta_d) * dt
        prob += ch[t] <= M * y[t]
        prob += dis[t] <= M * (1 - y[t])

    # Degradation constraints — scale per-day limits by window size
    # n_days_window = total hours covered / 24 = T * dt / 24
    n_days_window = T * dt / 24.0
    if max_throughput_mwh is not None:
        prob += pulp.lpSum(dis[t] for t in range(T)) <= float(max_throughput_mwh) * n_days_window

    if max_cycles_per_day is not None:
        prob += (
            pulp.lpSum(dis[t] for t in range(T))
            <= float(max_cycles_per_day) * e_cap * n_days_window
        )

    # Objective: maximise (discharge + subsidy - charge) revenue × interval duration
    prob += pulp.lpSum(
        ((float(prices[t]) + float(compensation_yuan_per_mwh)) * dis[t]
         - float(prices[t]) * ch[t]) * dt
        for t in range(T)
    )
    prob.solve(pulp.PULP_CBC_CMD(msg=False))

    status = pulp.LpStatus.get(prob.status, str(prob.status))

    chv = np.array([pulp.value(ch[t]) for t in range(T)], dtype=float)
    disv = np.array([pulp.value(dis[t]) for t in range(T)], dtype=float)
    socv = np.array([pulp.value(soc[t + 1]) for t in range(T)], dtype=float)
    profit = float(np.nansum(prices * (disv - chv) * dt))

    return DispatchResult(chv, disv, socv, profit, status)


# ---------------------------------------------------------------------------
# Single-day LP optimisation (backward-compatible wrapper)
# ---------------------------------------------------------------------------

def optimise_day(
    prices: np.ndarray,
    power_mw: float,
    duration_h: float,
    roundtrip_eff: float,
    max_throughput_mwh: Optional[float] = None,
    max_cycles_per_day: Optional[float] = None,
    compensation_yuan_per_mwh: float = 0.0,
) -> DispatchResult:
    """
    Solve the BESS arbitrage LP for one day of 24 hourly prices.

    Thin wrapper around optimise_window() that enforces the 24-hour constraint.
    All LP logic lives in optimise_window.

    Returns:
        DispatchResult with charge_mw, discharge_mw, soc_mwh arrays (length 24),
        market-only profit, and solver status string.

    Raises:
        ValueError: if prices does not have exactly 24 elements.
    """
    T = len(prices)
    if T != 24:
        raise ValueError(f"optimise_day expects 24 hourly prices, got {T}")
    return optimise_window(
        prices,
        power_mw=power_mw,
        duration_h=duration_h,
        roundtrip_eff=roundtrip_eff,
        max_throughput_mwh=max_throughput_mwh,
        max_cycles_per_day=max_cycles_per_day,
        compensation_yuan_per_mwh=compensation_yuan_per_mwh,
    )


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
    compensation_yuan_per_mwh: float = 0.0,
    window_days: int = 1,
    dt: float = 1.0,
    intervals_per_day: int = 24,
    freq: str = "h",
) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Run the BESS arbitrage LP over every complete day in ``hourly_prices``,
    grouping consecutive days into windows of ``window_days`` for a single LP solve.

    When window_days == 1 (default) behaviour is identical to calling optimise_day
    independently for each calendar day (SOC resets to 0 each day).

    When window_days > 1, consecutive calendar days are grouped into windows and
    solved as a single LP via optimise_window().  SOC carries over naturally within
    each window, giving the optimiser cross-day flexibility.  Days that are not
    consecutive (gap in the price series) start a new window automatically.
    Incomplete trailing windows (fewer days than window_days) are solved as-is.

    Args:
        hourly_prices:       pd.Series with DatetimeIndex.  Use any granularity with
                             matching ``dt``, ``intervals_per_day``, and ``freq``.
                             Days with any NaN value are skipped.
        power_mw:            Inverter power rating (MW).
        duration_h:          Battery duration (hours).
        roundtrip_eff:       Round-trip efficiency.
        max_throughput_mwh:  Optional per-day discharge cap; scaled by window size.
        max_cycles_per_day:  Optional per-day cycle cap; scaled by window size.
        compensation_yuan_per_mwh: Discharge subsidy (CNY/MWh) added to LP objective.
        window_days:         Number of consecutive days to optimise in one LP solve.
                             Default 1 (original per-day behaviour). Must be >= 1.
        dt:                  Interval duration in hours (default 1.0 for hourly,
                             0.25 for 15-min).  Passed to optimise_window.
        intervals_per_day:   Number of intervals per calendar day (default 24 for hourly,
                             96 for 15-min).
        freq:                Pandas frequency string for the DatetimeIndex of each day
                             (default "h" for hourly, "15min" for quarter-hourly).

    Returns:
        dispatch_df:  DataFrame indexed by datetime with columns:
                      charge_mw, discharge_mw, dispatch_grid_mw, soc_mwh, solver_status
        profit_s:     pd.Series indexed by date with daily profit values.
    """
    if window_days < 1:
        raise ValueError(f"window_days must be >= 1, got {window_days}")

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

    # ── Collect complete days (all intervals present, no NaN) ────────────────
    complete_days: Dict[dt.date, np.ndarray] = {}
    for d, g in df.groupby("date"):
        idx = pd.date_range(pd.Timestamp(d), periods=intervals_per_day, freq=freq)
        g2 = g.reindex(idx)
        prices_arr = g2["price"].to_numpy(dtype=float)
        if not np.isnan(prices_arr).any():
            complete_days[d] = prices_arr

    if not complete_days:
        return pd.DataFrame(), pd.Series(dtype=float)

    # ── Group complete days into consecutive windows ──────────────────────────
    sorted_days = sorted(complete_days.keys())
    windows: List[List[dt.date]] = []
    seq: List[dt.date] = [sorted_days[0]]
    for d in sorted_days[1:]:
        if (d - seq[-1]).days == 1:
            seq.append(d)
        else:
            # gap — flush current consecutive run into window_days-sized chunks
            for i in range(0, len(seq), window_days):
                windows.append(seq[i : i + window_days])
            seq = [d]
    for i in range(0, len(seq), window_days):
        windows.append(seq[i : i + window_days])

    # ── Solve each window, split results back into per-day records ────────────
    dispatch_rows: List[pd.DataFrame] = []
    daily_profit: Dict[dt.date, float] = {}

    for window_dates in windows:
        all_prices = np.concatenate([complete_days[d] for d in window_dates])
        res = optimise_window(
            all_prices,
            power_mw=power_mw,
            duration_h=duration_h,
            roundtrip_eff=roundtrip_eff,
            max_throughput_mwh=max_throughput_mwh,
            max_cycles_per_day=max_cycles_per_day,
            compensation_yuan_per_mwh=compensation_yuan_per_mwh,
            dt=dt,
        )

        for i, d in enumerate(window_dates):
            day_slice = slice(i * intervals_per_day, (i + 1) * intervals_per_day)
            idx = pd.date_range(pd.Timestamp(d), periods=intervals_per_day, freq=freq)
            ch_day = res.charge_mw[day_slice]
            dis_day = res.discharge_mw[day_slice]
            soc_day = res.soc_mwh[day_slice]

            out = pd.DataFrame({
                "datetime": idx,
                "charge_mw": ch_day,
                "discharge_mw": dis_day,
                "dispatch_grid_mw": dis_day - ch_day,
                "soc_mwh": soc_day,
                "solver_status": res.status,
            }).set_index("datetime")

            dispatch_rows.append(out)
            daily_profit[d] = float(np.nansum(complete_days[d] * (dis_day - ch_day) * dt))

    dispatch_df = pd.concat(dispatch_rows).sort_index() if dispatch_rows else pd.DataFrame()
    profit_s = pd.Series(daily_profit).sort_index()
    profit_s.name = "profit"
    return dispatch_df, profit_s


def compute_dispatch_from_15min_prices(
    prices_15min: pd.Series,
    power_mw: float,
    duration_h: float,
    roundtrip_eff: float,
    max_throughput_mwh: Optional[float] = None,
    max_cycles_per_day: Optional[float] = None,
    compensation_yuan_per_mwh: float = 0.0,
    window_days: int = 1,
) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Run the BESS arbitrage LP over 15-min price intervals (96 per day).

    Thin wrapper around compute_dispatch_from_hourly_prices with dt=0.25,
    intervals_per_day=96, freq="15min".

    Using 15-min prices ensures perfect-foresight P&L is a true upper bound
    on any 15-min settled strategy (cleared_actual, nominated, etc.).

    Args:
        prices_15min: pd.Series with 15-min DatetimeIndex.
        Other args:   same as compute_dispatch_from_hourly_prices.

    Returns:
        dispatch_df:  DataFrame indexed by 15-min datetime.
        profit_s:     pd.Series of daily profit (settled at 15-min prices).
    """
    return compute_dispatch_from_hourly_prices(
        hourly_prices=prices_15min,
        power_mw=power_mw,
        duration_h=duration_h,
        roundtrip_eff=roundtrip_eff,
        max_throughput_mwh=max_throughput_mwh,
        max_cycles_per_day=max_cycles_per_day,
        compensation_yuan_per_mwh=compensation_yuan_per_mwh,
        window_days=window_days,
        dt=0.25,
        intervals_per_day=96,
        freq="15min",
    )
