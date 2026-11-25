# services/nodal_agents/agent.py
"""L3 asset agent: deterministic LP dispatch of one BESS against a forecast
nodal curve, under a shared substation discharge cap and traffic-light zones.

Wraps the prod engine services/bess_map/optimisation_engine
.compute_dispatch_from_15min_prices (perfect foresight, SOC resets daily —
right shape for a D+1 nominated strategy).

Substation cap: the engine takes a scalar power_mw, so the per-interval rule
"available = cap - max(others_discharge_t, 0)" is enforced through the
tightest interval: power_mw_eff = min(capacity, min_t headroom_t). This
guarantees discharge <= cap - others at EVERY interval (conservative when
others varies within the day; coordination across same-substation agents
happens via the recursion loop, which re-solves against updated others).

Zone restrictions (integer codes, one per 15-min interval) — the TSO's
traffic-light windows, the same convention asset-risk reads from the
operator's cell colours (charge_only / discharge_only):
    ZONE_NO_DISCHARGE (=1): 蒙西 yellow — charge-only window
    ZONE_NO_CHARGE    (=2): 蒙西 red    — discharge-only window
Enforced as HARD LP constraints (the engine's no_charge_slots /
no_discharge_slots, as in services/bess_map/nodal_pf_daily) since 2026-11-24.
The earlier price-mask stand-in (price 0 / +1e6 in restricted intervals) was
violable when the battery is paid to charge (reviewer probe 2026-09-22) and
distorted expected_profit; it was never exercised because zones were
all-green until the zone forecast was wired into the writer.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

import os

from services.bess_map.optimisation_engine import (
    compute_dispatch_from_15min_prices,
    optimise_window,
)

# 蒙西 operator rules for an asset flagged operator_rules=True (owned stations,
# user decision 2026-11-25): the discharge compensation is in the objective,
# with the 1.5 cycles/day cap and the 3.3 %/min ramp limit — the constraint
# set of the PF benchmark and asset-risk's investment-standard leg. Without
# them the LP chased price spread only and under-discharged: valued at actual
# prices over 2026-11-12..20, the six original stations' strategies were worth
# 4-9x more with the rules on.
# That MILP rarely proves optimality (every solve ran to the engine's 120 s
# limit), so each solve is capped: 9 accounts x 3 iterations must finish well
# inside the 16:00 -> 17:00 nomination window. Plants we do not own stay on
# the sub-second spread-only LP — they only shape the fleet price feedback.
OPERATOR_MAX_CYCLES_PER_DAY = 1.5
OPERATOR_RAMP_PCT_PER_MIN = 3.3
OPERATOR_SOLVE_LIMIT_S = float(os.getenv("NODAL_AGENT_SOLVE_LIMIT_S", "15"))

ZONE_GREEN = 0
ZONE_NO_DISCHARGE = 1   # no-discharge window: price masked to 0
ZONE_NO_CHARGE = 2      # discharge-only window: price masked to +BIG
_SYNTH_DAY = "2000-01-01"  # index placeholder; the LP only needs 96 intervals


def optimize_asset(asset: dict, curve: np.ndarray, others: np.ndarray,
                   cap_mw: float | None, zones: np.ndarray) -> dict:
    """Optimize one asset for one day. Returns {"curve": (96,) MW net grid
    power (discharge - charge), "assumptions": {...}}.

    asset keys: plant_name, capacity_mw, duration_h, rte_pct (percent, e.g.
    85.0); node/substation/zone/settle_node pass through to assumptions.
    curve:  (96,) forecast price CNY/MWh — must be finite.
    others: (96,) MW, positive = other BESS discharging at the same substation.
    cap_mw: shared substation discharge cap MW; None = uncapped.
    zones:  (96,) int codes — ZONE_GREEN / ZONE_NO_DISCHARGE / ZONE_NO_CHARGE.
    """
    prices = np.asarray(curve, dtype=float)
    if prices.shape != (96,):
        raise ValueError(f"curve must be (96,), got {prices.shape}")
    if not np.isfinite(prices).all():
        raise ValueError("curve contains NaN/inf — forecast must be complete")
    others = np.nan_to_num(np.asarray(others, dtype=float), nan=0.0)
    if others.shape != (96,):
        raise ValueError(f"others must be (96,), got {others.shape}")
    zones = np.asarray(zones)
    if zones.shape != (96,):
        raise ValueError(f"zones must be (96,), got {zones.shape}")
    bad = set(np.unique(zones)) - {ZONE_GREEN, ZONE_NO_DISCHARGE, ZONE_NO_CHARGE}
    if bad:
        raise ValueError(f"unknown zone codes: {sorted(bad)}")

    capacity = float(asset["capacity_mw"])
    duration_h = float(asset["duration_h"])
    rte = float(asset["rte_pct"]) / 100.0
    if capacity <= 0 or duration_h <= 0 or not (0.0 < rte <= 1.0):
        raise ValueError(f"bad asset params for {asset.get('plant_name')}: "
                         f"capacity={capacity}, duration_h={duration_h}, rte={rte}")

    # Substation cap — tightest-interval headroom as the LP power rating.
    if cap_mw is None:
        power_eff = capacity
        headroom_min = None
    else:
        headroom = float(cap_mw) - np.maximum(others, 0.0)
        headroom_min = float(headroom.min())
        power_eff = float(np.clip(min(capacity, headroom_min), 0.0, capacity))

    # 1-based slot numbers, the engine's convention (zones_to_masks).
    no_charge = {int(i) + 1 for i in np.flatnonzero(zones == ZONE_NO_CHARGE)}
    no_discharge = {int(i) + 1 for i in np.flatnonzero(zones == ZONE_NO_DISCHARGE)}

    operator_rules = bool(asset.get("operator_rules"))
    compensation = float(asset.get("compensation_yuan_per_mwh") or 0.0)

    if power_eff <= 0.0:
        dispatch = np.zeros(96, dtype=float)
        profit, status = 0.0, "SkippedZeroHeadroom"
    elif operator_rules:
        res = optimise_window(
            prices, power_mw=power_eff, duration_h=duration_h, roundtrip_eff=rte,
            max_cycles_per_day=OPERATOR_MAX_CYCLES_PER_DAY,
            compensation_yuan_per_mwh=compensation, dt=0.25,
            ramp_rate_pct_per_min=OPERATOR_RAMP_PCT_PER_MIN,
            solver_time_limit_s=OPERATOR_SOLVE_LIMIT_S,
            # the engine core takes 0-based window indices
            no_charge_slots={i - 1 for i in no_charge} or None,
            no_discharge_slots={i - 1 for i in no_discharge} or None)
        dispatch = np.asarray(res.discharge_mw, dtype=float) - np.asarray(res.charge_mw, dtype=float)
        status = str(res.status)
        if not np.isfinite(dispatch).all():
            # time limit hit before any feasible point: stand down, loudly
            dispatch = np.zeros(96, dtype=float)
            status = f"NoIncumbent({status})"
        # market cash flow + discharge compensation on the forecast curve
        profit = float((prices * dispatch).sum() * 0.25
                       + np.clip(dispatch, 0.0, None).sum() * 0.25 * compensation)
    else:
        s = pd.Series(prices, index=pd.date_range(_SYNTH_DAY, periods=96, freq="15min"))
        dispatch_df, profit_s = compute_dispatch_from_15min_prices(
            s, power_mw=power_eff, duration_h=duration_h, roundtrip_eff=rte,
            no_charge_slots=no_charge or None,
            no_discharge_slots=no_discharge or None)
        if dispatch_df.empty:
            dispatch = np.zeros(96, dtype=float)
            profit, status = 0.0, "NoSolution"
        else:
            dispatch = dispatch_df["dispatch_grid_mw"].to_numpy(dtype=float)
            profit = float(profit_s.iloc[0]) if len(profit_s) else 0.0
            status = str(dispatch_df["solver_status"].iloc[0])

    assumptions = {
        "plant_name": asset.get("plant_name"),
        "node": asset.get("node"),
        "substation": asset.get("substation"),
        "zone": asset.get("zone"),
        "settle_node": asset.get("settle_node"),
        "capacity_mw": capacity,
        "duration_h": duration_h,
        "rte_pct": float(asset["rte_pct"]),
        "cap_mw": None if cap_mw is None else float(cap_mw),
        "power_mw_eff": power_eff,
        "substation_headroom_min_mw": headroom_min,
        "operator_rules": operator_rules,
        "compensation_yuan_per_mwh": compensation if operator_rules else None,
        "zone_no_discharge_intervals": int((zones == ZONE_NO_DISCHARGE).sum()),
        "zone_no_charge_intervals": int((zones == ZONE_NO_CHARGE).sum()),
        "price_mean": float(prices.mean()),
        "expected_profit_yuan": profit,
        "solver_status": status,
    }
    return {"curve": np.nan_to_num(dispatch, nan=0.0), "assumptions": assumptions}
