# test_value_stack.py
import numpy as np, pandas as pd, pytest
from datetime import date
from libs.option_value.curves import build_expected_curve
from libs.option_value.contracts import BESSParams, PolicyLevers, PCASimConfig
from libs.option_value.asset_models import ASSET_MODELS, BESSDispatchOption
from libs.option_value.pca_simulator import fit_pca_v2
from libs.option_value.value_stack import (apply_policy, compute_value_stack,
    annuitize, compute_greeks)

def test_apply_policy_signs_and_magnitude():
    lv = PolicyLevers(capacity_price_yuan_kw_yr=100.0, discharge_comp_yuan_kwh=0.28,
                      sysopfee_yuan_kwh=0.03)
    # BESS 2h at ~1 cycle/day → 0.73 MWh discharged per kW per year
    net, comp = apply_policy(lv, "bess", volume_mwh_per_kw=0.73, rte=0.85)
    assert comp["capacity_price"] == 100.0
    assert comp["discharge_comp"] == pytest.approx(0.28 * 0.73 * 1000)        # 204.4 ¥/kW/yr
    assert comp["sysopfee"] == pytest.approx(-0.03 * (0.73/0.85) * 1000)      # −25.76 ¥/kW/yr
    assert net == pytest.approx(sum(v for k, v in comp.items() if isinstance(v, (int, float))))

def test_lever_not_applicable_zeroed():
    lv = PolicyLevers(capacity_price_yuan_kw_yr=100.0)        # a BESS-only lever
    net, comp = apply_policy(lv, "wind", volume_mwh_per_kw=3.0, rte=1.0)
    assert comp["capacity_price"] == 0.0
    assert "capacity_price" in comp["not_applicable"]

def test_annuitize_value():
    assert abs(annuitize(1.0, 0.08, 15) - 8.5595) < 1e-3
    assert annuitize(2.0, 0.0, 10) == 20.0                    # zero-hurdle rule

def test_jensen_extrinsic_nonnegative_bess():
    idx = pd.date_range("2025-11-01", periods=400*24, freq="h")
    s = pd.Series(300 + 150*np.sin(2*np.pi*(idx.hour-6)/24), index=idx)
    c = build_expected_curve(s, date(2026, 11, 30))
    model = BESSDispatchOption(BESSParams())
    int_rev, _ = model.intrinsic(c)
    rng = np.random.default_rng(9)
    paths = c.hourly[:24*365] + rng.normal(0, 30, (50, 24*365))  # mean-zero noise
    vs = compute_value_stack(model, c, paths, PolicyLevers(), capture_rate=None)
    assert vs.extrinsic_yuan_kw >= -1e-6
    assert vs.capture_adj_yuan_kw is None                       # missing capture → None, no crash

def test_coal_stub_raises():
    import pytest
    with pytest.raises(NotImplementedError):
        ASSET_MODELS["coal"]().intrinsic(None)

def test_greeks_signs_and_magnitude():
    # Same 400-day 300 + 150·sin shape as the Jensen test, plus sigma=25 noise:
    # zero noise makes every PCA innovation degenerate (inno_scale ≈ 0), which
    # would force vega = 0 and break the long-volatility assert below.
    rng = np.random.default_rng(3)
    idx = pd.date_range("2025-11-01", periods=400*24, freq="h")
    s = pd.Series(300 + 150*np.sin(2*np.pi*(idx.hour-6)/24) + rng.normal(0, 25, len(idx)), index=idx)
    c = build_expected_curve(s, date(2026, 11, 30))
    cfg = PCASimConfig(n_paths=32, seed=42)
    sp = fit_pca_v2(s, c, cfg)
    model = BESSDispatchOption(BESSParams())
    g = compute_greeks(model, c, sp, cfg, PolicyLevers(), capture_rate=None, prices=s)

    assert set(g) == {"delta", "vega", "theta"}
    assert all(np.isfinite(v) for v in g.values())

    assert g["delta"] < 0    # parallel shift: RTE drag (charge MWh = discharge/rte > discharge)
    # Analytic delta: the engine's own intrinsic discharge volume, normalised to
    # per-kW per day (÷ unit kW ÷ n_days), annualised ×365, earning +10 on each
    # discharged MWh and paying +10 on each charged MWh (= discharge/rte):
    #   analytic = mean_daily_discharge_mwh_per_kw × 365 × 10 × (1 − 1/rte)
    # The value stack spans curve.n_days (400d), so the ×365 annualisation
    # understates |delta| by ~35/400 (~0.12 ¥/kW) — absorbed by the 0.5 tolerance.
    rte = model.params.rte
    mean_daily_per_kw = model.intrinsic(c)[1] / (model.params.power_mw * 1000.0) / c.n_days
    analytic = mean_daily_per_kw * 365 * 10.0 * (1.0 - 1.0 / rte)
    assert abs(g["delta"] - analytic) < 0.5

    assert g["vega"] > 0     # BESS long volatility: more dispersion → more spread capture
