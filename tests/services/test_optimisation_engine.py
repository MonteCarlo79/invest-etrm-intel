"""Tests for services/bess_map/optimisation_engine.py — ramp-rate constraint.

Grid operator rule (蒙西, per user 2026-08-30): BESS ramp limit 爬坡限速
3.3%/min of rated power → |ΔP_grid| ≤ 49.5% of rated per 15-min interval,
symmetric for charge/discharge, idle (0 MW) window start.

The constraint is opt-in (ramp_rate_pct_per_min=None default) so valuation
models (capture pipeline) are unaffected.
"""
import numpy as np
import pytest

from services.bess_map.optimisation_engine import (
    compute_dispatch_from_15min_prices,
    optimise_window,
)

POWER = 100.0      # MW
DUR = 4.0          # h
RTE = 0.85
RAMP_PCT = 3.3     # %/min (蒙西 operator rule)
DT = 0.25          # h (15-min)
RAMP_MAX_MW = POWER * RAMP_PCT / 100.0 * 60.0 * DT   # 49.5 MW per interval


def _spike_prices() -> np.ndarray:
    """96 intervals: flat 30, one 4-interval spike at 300 (t=48..51), flat 30."""
    p = np.full(96, 30.0)
    p[48:52] = 300.0
    return p


def _grid(res):
    return res.discharge_mw - res.charge_mw


def test_ramp_constraint_limits_interval_swing():
    """With 3.3%/min, adjacent-interval |ΔP_grid| never exceeds 49.5 MW (100 MW asset)."""
    res = optimise_window(
        _spike_prices(), power_mw=POWER, duration_h=DUR, roundtrip_eff=RTE,
        dt=DT, ramp_rate_pct_per_min=RAMP_PCT,
    )
    g = _grid(res)
    swings = np.abs(np.diff(g))
    assert swings.max() <= RAMP_MAX_MW + 1e-6
    # and the day starts from idle: first interval within one ramp step of 0
    assert abs(g[0]) <= RAMP_MAX_MW + 1e-6


def test_ramp_constraint_binds_on_spike_day():
    """Unconstrained flips 0→100 MW in one interval at the spike; ramped cannot,
    so ramped profit is strictly lower."""
    unramped = optimise_window(
        _spike_prices(), power_mw=POWER, duration_h=DUR, roundtrip_eff=RTE, dt=DT,
    )
    g_un = _grid(unramped)
    assert np.abs(np.diff(g_un)).max() > RAMP_MAX_MW + 1.0  # sanity: flip really happens

    ramped = optimise_window(
        _spike_prices(), power_mw=POWER, duration_h=DUR, roundtrip_eff=RTE,
        dt=DT, ramp_rate_pct_per_min=RAMP_PCT,
    )
    assert ramped.profit < unramped.profit


def test_ramp_default_is_unconstrained():
    """ramp_rate_pct_per_min=None (default) reproduces the unconstrained solution
    (backward compatibility for valuation callers)."""
    a = optimise_window(_spike_prices(), power_mw=POWER, duration_h=DUR,
                        roundtrip_eff=RTE, dt=DT)
    b = optimise_window(_spike_prices(), power_mw=POWER, duration_h=DUR,
                        roundtrip_eff=RTE, dt=DT, ramp_rate_pct_per_min=None)
    np.testing.assert_allclose(_grid(a), _grid(b), atol=1e-6)
    assert a.profit == pytest.approx(b.profit)


def test_ramp_threads_through_15min_wrapper():
    """compute_dispatch_from_15min_prices accepts and enforces the parameter."""
    idx = pd_index = __import__("pandas").date_range("2026-06-01", periods=96, freq="15min")
    prices = __import__("pandas").Series(_spike_prices(), index=idx)
    dispatch_df, _ = compute_dispatch_from_15min_prices(
        prices, power_mw=POWER, duration_h=DUR, roundtrip_eff=RTE,
        ramp_rate_pct_per_min=RAMP_PCT,
    )
    g = (dispatch_df["discharge_mw"] - dispatch_df["charge_mw"]).to_numpy()
    assert np.abs(np.diff(g)).max() <= RAMP_MAX_MW + 1e-6
