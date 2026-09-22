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

Zone masks (integer codes, one per 15-min interval): restricted intervals
have their price replaced — the spec's "-inf (charge) / +inf (discharge)"
mechanism, with one correction found in testing:
    ZONE_NO_DISCHARGE (=1): price -> 0.0   (no-discharge interval)
    ZONE_NO_CHARGE    (=2): price -> +BIG  (discharge-only interval)
A literal -BIG price does NOT forbid discharge: with round-trip efficiency
< 1 the LP churns (charge 1/eta MWh at -BIG, discharge 1 MWh at -BIG, net
gain from the efficiency gap) and max-power alternating garbage results at
ANY big-M (verified 1e3..1e6). Price 0 instead makes discharge strictly
dominated whenever any later positive-price opportunity exists and gives no
churn incentive. +BIG is churn-proof (recharging costs BIG/eta > BIG earned)
so the discharge-only side keeps the spec's big finite stand-in (1e6; PuLP/
CBC cannot take true infinities, and 1e6 dominates any realistic spread).

CAVEAT (reviewer probe, 2026-09-22): the price-0 no-discharge mask is
violable when the battery is PAID to charge (negative prices) and
positive-price room is too small to empty SOC — disposal at 0 becomes
the profit-enabling valve (probe: −50 charge window, 4 positive
intervals, mask on 52:96 → 100 MW discharge inside the forbidden
window, solver Optimal). Per-interval discharge upper bounds in the
engine are a PREREQUISITE for wiring zone learning. Zones are
all-green in v1 — no production impact today.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from services.bess_map.optimisation_engine import compute_dispatch_from_15min_prices

ZONE_GREEN = 0
ZONE_NO_DISCHARGE = 1   # no-discharge window: price masked to 0
ZONE_NO_CHARGE = 2      # discharge-only window: price masked to +BIG
_ZONE_BIG = 1e6
_ZONE_BLOCK_DIS = 0.0
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

    masked = prices.copy()
    masked[zones == ZONE_NO_DISCHARGE] = _ZONE_BLOCK_DIS
    masked[zones == ZONE_NO_CHARGE] = _ZONE_BIG

    if power_eff <= 0.0:
        dispatch = np.zeros(96, dtype=float)
        profit, status = 0.0, "SkippedZeroHeadroom"
    else:
        s = pd.Series(masked, index=pd.date_range(_SYNTH_DAY, periods=96, freq="15min"))
        dispatch_df, profit_s = compute_dispatch_from_15min_prices(
            s, power_mw=power_eff, duration_h=duration_h, roundtrip_eff=rte)
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
        "zone_no_discharge_intervals": int((zones == ZONE_NO_DISCHARGE).sum()),
        "zone_no_charge_intervals": int((zones == ZONE_NO_CHARGE).sum()),
        "price_mean": float(prices.mean()),
        "expected_profit_yuan": profit,
        "solver_status": status,
    }
    return {"curve": np.nan_to_num(dispatch, nan=0.0), "assumptions": assumptions}
