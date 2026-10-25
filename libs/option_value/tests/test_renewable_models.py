# test_renewable_models.py
import numpy as np, pandas as pd
from datetime import date
from libs.option_value.curves import build_expected_curve
from libs.option_value.contracts import RenewableParams
from libs.option_value.renewable_models import (simulate_cf_paths,
    renewable_revenue_paths, renewable_intrinsic)

def _curve_and_paths():
    rng = np.random.default_rng(11)
    idx = pd.date_range("2025-11-01", periods=400*24, freq="h")
    s = pd.Series(300 + 80*np.sin(2*np.pi*(idx.hour-14)/24) + rng.normal(0, 25, len(idx)), index=idx)
    c = build_expected_curve(s, date(2026, 11, 30))
    paths = np.tile(c.hourly[:24*365], (300, 1)) + rng.normal(0, 25, (300, 24*365))
    return c, paths

def _params(rho):
    cf = np.tile(np.clip(np.sin(2*np.pi*(np.arange(24)-6)/24), 0, None), (12, 1)) * 0.4
    return RenewableParams(capacity_mw=1.0, cf_profile=cf, price_volume_corr=rho)

def _cf_mean(p: RenewableParams, c, n_days):
    idx = pd.date_range(c.window_end, periods=n_days*24, freq="h")
    return np.array([p.cf_profile[ts.month % 12, ts.hour] for ts in idx])

def test_correlation_recovered():
    c, paths = _curve_and_paths()
    cf = simulate_cf_paths(paths, c, _params(-0.4), seed=42)
    z_p = (paths[:, :24*365] - c.hourly[:24*365]) / 25.0
    cf_mean = _cf_mean(_params(-0.4), c, 365)
    z_c = (cf - cf_mean[:24*365]) / 0.15
    assert abs(np.corrcoef(z_p.ravel(), z_c.ravel())[0,1] - (-0.4)) < 0.05

def test_curtailment_floor_blocks_negative_revenue():
    c, _ = _curve_and_paths()
    paths = np.full((4, 24*365), -50.0)   # all-negative price year
    cf = simulate_cf_paths(paths, c, _params(0.0), seed=1)
    rev_floor, _ = renewable_revenue_paths(paths, cf, _params(0.0))
    p2 = _params(0.0); p2.curtailment_floor = False
    rev_nofloor, _ = renewable_revenue_paths(paths, cf, p2)
    assert (rev_floor == 0).all() and (rev_nofloor < 0).all()

def test_negative_extrinsic_construction():
    # strong negative corr + CF concentrated in low-price hours → E[p·cf] < E[p]·E[cf]
    c, paths = _curve_and_paths()
    p = _params(-0.9); p.cf_sigma = 0.5
    cf = simulate_cf_paths(paths, c, p, seed=42)
    rev, _ = renewable_revenue_paths(paths, cf, p)
    int_rev, _ = renewable_intrinsic(c, p)
    assert rev.mean() - int_rev < 0      # extrinsic negative, reported with sign
