"""libs/deal_models/dispatch_valuation.py — Annual revenue estimation via spread call strip."""
from __future__ import annotations

import numpy as np
from libs.deal_models.contracts import DispatchRequest, DispatchResult


def _percentiles(arr: np.ndarray) -> tuple[float, float, float]:
    return float(np.percentile(arr, 10)), float(np.percentile(arr, 50)), float(np.percentile(arr, 90))


def _dispatch_bess(price_paths: np.ndarray, req: DispatchRequest) -> np.ndarray:
    """
    Greedy daily dispatch: charge in cheapest n_cycles hours, discharge in most expensive.
    Returns (n_sim,) annual revenue yuan.
    """
    n_sim, n_hours = price_paths.shape
    n_days = n_hours // 24
    n_cycles = max(1, int(req.cycles_per_day))
    # MWh discharged per cycle slot (constrained by both power and capacity)
    energy_mwh = min(req.power_mw * 1.0, req.capacity_mwh / n_cycles)

    daily = price_paths[:, : n_days * 24].reshape(n_sim, n_days, 24)
    sorted_prices = np.sort(daily, axis=2)  # ascending

    charge_prices = sorted_prices[:, :, :n_cycles]           # (n_sim, n_days, n_cycles)
    discharge_prices = sorted_prices[:, :, -n_cycles:]        # (n_sim, n_days, n_cycles)

    # Revenue from discharging energy_mwh * eta back to grid per cycle
    discharge_rev = discharge_prices.sum(axis=2) * energy_mwh * req.roundtrip_eff
    # Cost to charge energy_mwh from grid per cycle
    charge_cost = charge_prices.sum(axis=2) * energy_mwh
    # O&M cost per MWh discharged
    om = req.om_cost_yuan_per_mwh * energy_mwh * req.roundtrip_eff * n_cycles

    daily_rev = np.maximum(discharge_rev - charge_cost - om, 0.0)
    return daily_rev.sum(axis=1)


def _dispatch_wind(price_paths: np.ndarray, req: DispatchRequest) -> np.ndarray:
    """Simple energy revenue: price * installed_mw * CF per hour."""
    n_hours = price_paths.shape[1]
    if req.capacity_factor_profile:
        cf = np.asarray(req.capacity_factor_profile[:n_hours], dtype=float)
    else:
        cf = np.full(n_hours, 0.30)   # default 30% CF
    hourly_gen = req.installed_mw * cf  # MWh/h
    return (price_paths * hourly_gen).sum(axis=1)


def dispatch_annual(price_paths: np.ndarray, req: DispatchRequest) -> DispatchResult:
    """
    Compute annual revenue for each simulation path.

    price_paths: np.ndarray (n_sim, n_hours)  — yuan/MWh hourly prices
    Returns DispatchResult with revenue_paths (n_sim,) annual yuan + statistics.
    """
    if req.asset_type == "bess":
        rev = _dispatch_bess(price_paths, req)
    elif req.asset_type == "wind":
        rev = _dispatch_wind(price_paths, req)
    elif req.asset_type == "wind_bess":
        rev = _dispatch_wind(price_paths, req) + _dispatch_bess(price_paths, req)
    else:
        raise ValueError(f"Unknown asset_type: {req.asset_type!r}")

    p10, p50, p90 = _percentiles(rev)
    return DispatchResult(
        revenue_paths=rev,
        p10=p10, p50=p50, p90=p90,
        mean=float(rev.mean()),
        std=float(rev.std()),
    )
