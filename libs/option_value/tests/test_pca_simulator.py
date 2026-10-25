# test_pca_simulator.py
import numpy as np, pandas as pd
from datetime import date
from libs.option_value.curves import build_expected_curve
from libs.option_value.contracts import PCASimConfig
from libs.option_value.pca_simulator import fit_pca_v2, simulate_pca_v2

def _synthetic(n_days=400, seed=7):
    # two known shape factors; PC1 scores AR(1) phi=0.6, PC2 iid
    rng = np.random.default_rng(seed)
    L1 = np.sin(np.linspace(0, np.pi, 24)); L2 = np.cos(np.linspace(0, np.pi, 24))
    s1 = np.zeros(n_days); e1 = rng.normal(0, 3.0, n_days)
    for d in range(1, n_days): s1[d] = 0.6*s1[d-1] + e1[d]
    s2 = rng.normal(0, 2.0, n_days)
    resid = np.outer(s1, L1) + np.outer(s2, L2)
    idx = pd.date_range("2025-11-01", periods=n_days*24, freq="h")
    base = np.array([300.0 + ts.month*5 + 20*np.sin(2*np.pi*ts.hour/24) for ts in idx])
    return pd.Series(base + resid.reshape(-1), index=idx)

def test_ar1_phi_recovered():
    s = _synthetic(); c = build_expected_curve(s, date(2026, 11, 30))
    p = fit_pca_v2(s, c, PCASimConfig(n_components=2))
    assert abs(p.phi[0] - 0.6) < 0.1

def test_simulated_score_autocorr_and_moments():
    s = _synthetic(); c = build_expected_curve(s, date(2026, 11, 30))
    cfg = PCASimConfig(n_components=2, n_paths=400, seed=42)
    p = fit_pca_v2(s, c, cfg)
    sim = simulate_pca_v2(p, c, 365, cfg)
    assert sim.shape == (400, 365*24)
    assert sim.min() >= c.floor and sim.max() <= c.cap
    hist_spread = np.ptp(s.values.reshape(-1, 24), axis=1).mean()  # np.ptp: ndarray.ptp removed in numpy 2.x
    sim_spread = np.ptp(sim.reshape(400, 365, 24), axis=2).mean()
    assert abs(sim_spread - hist_spread) / hist_spread < 0.15

def test_seed_reproducible():
    s = _synthetic(); c = build_expected_curve(s, date(2026, 11, 30))
    cfg = PCASimConfig(n_components=2, n_paths=8, seed=42)
    p = fit_pca_v2(s, c, cfg)
    assert np.array_equal(simulate_pca_v2(p, c, 10, cfg), simulate_pca_v2(p, c, 10, cfg))

def test_degenerate_pc_falls_back_to_normal():
    s = _synthetic(); c = build_expected_curve(s, date(2026, 11, 30))
    p = fit_pca_v2(s, c, PCASimConfig(n_components=4))  # PCs 3-4 are pure noise
    assert np.isnan(p.inno_df[2:]).all() or (p.inno_df[2:] >= 3).all()  # no garbage df
