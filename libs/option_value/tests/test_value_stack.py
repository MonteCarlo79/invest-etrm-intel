# test_value_stack.py
import numpy as np, pandas as pd, pytest
from datetime import date
from libs.option_value.curves import build_expected_curve
from libs.option_value.contracts import BESSParams, PolicyLevers, PCASimConfig
from libs.option_value.asset_models import ASSET_MODELS, BESSDispatchOption
from libs.option_value.value_stack import apply_policy, compute_value_stack, annuitize

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
