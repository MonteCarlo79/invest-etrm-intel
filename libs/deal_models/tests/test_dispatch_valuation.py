"""libs/deal_models/tests/test_dispatch_valuation.py — TDD tests for dispatch_valuation."""
from __future__ import annotations
import numpy as np
import pytest
from libs.deal_models.contracts import DispatchRequest


def _flat_paths(n_sim: int, price: float, n_hours: int = 8760) -> np.ndarray:
    return np.full((n_sim, n_hours), price)


def _spread_paths(n_sim: int, offpeak: float, peak: float, n_hours: int = 8760) -> np.ndarray:
    """12h offpeak then 12h peak each day."""
    day = np.array([offpeak] * 12 + [peak] * 12)
    return np.tile(day, (n_sim, n_hours // 24))


def test_bess_shape():
    from libs.deal_models.dispatch_valuation import dispatch_annual
    paths = _spread_paths(50, 100.0, 500.0)
    req = DispatchRequest(asset_type="bess", capacity_mwh=100.0, power_mw=50.0)
    result = dispatch_annual(paths, req)
    assert result.revenue_paths.shape == (50,)


def test_bess_positive_on_high_spread():
    from libs.deal_models.dispatch_valuation import dispatch_annual
    paths = _spread_paths(20, 50.0, 800.0)
    req = DispatchRequest(
        asset_type="bess", capacity_mwh=100.0, power_mw=50.0,
        roundtrip_eff=0.85, cycles_per_day=1.0, om_cost_yuan_per_mwh=5.0,
    )
    result = dispatch_annual(paths, req)
    assert (result.revenue_paths > 0).all()


def test_bess_zero_on_flat_prices():
    from libs.deal_models.dispatch_valuation import dispatch_annual
    paths = _flat_paths(10, 300.0)
    req = DispatchRequest(
        asset_type="bess", capacity_mwh=100.0, power_mw=50.0,
        roundtrip_eff=0.85, om_cost_yuan_per_mwh=10.0,
    )
    result = dispatch_annual(paths, req)
    # Flat prices → no spread → discharge_rev <= charge_cost → revenue = 0
    assert (result.revenue_paths == 0.0).all()


def test_wind_shape_and_percentiles():
    from libs.deal_models.dispatch_valuation import dispatch_annual
    rng = np.random.default_rng(42)
    paths = rng.uniform(200, 400, (30, 8760))
    req = DispatchRequest(asset_type="wind", installed_mw=100.0)
    result = dispatch_annual(paths, req)
    assert result.revenue_paths.shape == (30,)
    assert result.p10 < result.p50 < result.p90


def test_wind_scales_with_installed_mw():
    from libs.deal_models.dispatch_valuation import dispatch_annual
    paths = _flat_paths(5, 300.0)
    req50 = DispatchRequest(asset_type="wind", installed_mw=50.0)
    req100 = DispatchRequest(asset_type="wind", installed_mw=100.0)
    r50 = dispatch_annual(paths, req50)
    r100 = dispatch_annual(paths, req100)
    assert abs(r100.mean / r50.mean - 2.0) < 0.01


def test_wind_bess_exceeds_wind_alone():
    from libs.deal_models.dispatch_valuation import dispatch_annual
    rng = np.random.default_rng(42)
    paths = rng.uniform(100, 600, (50, 8760))
    wind_req = DispatchRequest(asset_type="wind", installed_mw=50.0)
    wb_req = DispatchRequest(
        asset_type="wind_bess", installed_mw=50.0,
        capacity_mwh=50.0, power_mw=50.0, roundtrip_eff=0.85,
    )
    assert dispatch_annual(paths, wb_req).p50 > dispatch_annual(paths, wind_req).p50
