# test_build_option.py
from datetime import date
import pytest
from libs.option_value.build_option import dp_perpetual_build, value_permit, value_acquisition
from libs.option_value.contracts import PolicyLevers, PolicyScenario, PermitInputs

def test_dixit_pindyck_matches_academy_lab():
    # lab defaults: sigma=0.25, r=0.05, rn_drift=-0.04, cost=100
    # beta1 = 2.8428, wait multiplier = 1.5427, trigger = 154.27, value(100) = 15.82
    v = dp_perpetual_build(100.0, 100.0, 0.25, 0.05, -0.04)
    assert abs(v - 15.82) < 0.01
    assert dp_perpetual_build(200.0, 100.0, 0.25, 0.05, -0.04) == 100.0  # above trigger → exercise

def _inputs(cost):
    return PermitInputs(permit_cost_yuan=cost, validity_years=2.0,
                        capex_yuan_kwh_today=600.0, capex_decline_rate=0.0,
                        power_mw=0.1, duration_h=2.0, hurdle=0.0, life_years=1)

def test_permit_no_scenarios_immediate_or_never():
    # energy 50 ¥/kW/yr, no levers → annual ¥ = 50×100 kW = 5 000; capex = 600×200 kWh = 120 000
    r = value_permit([], PolicyLevers(), 50.0, "bess", 0.85, _inputs(10.0))
    assert r.v_permit_yuan == -10.0 and not r.go          # NPV negative → never build → worth 0 − cost

def test_permit_two_state_hand_computed():
    # scenario doubles capacity_price to 5 000 000 ¥/kW/yr at p=1.0 effective immediately:
    # NPV(state) = (50 + 5e6)×100 − 120 000 > 0 → exercise → root = that NPV
    sc = PolicyScenario("cap_x", {"capacity_price_yuan_kw_yr": 5_000_000.0}, date(2026,1,1), 1.0)
    r = value_permit([sc], PolicyLevers(), 50.0, "bess", 0.85, _inputs(0.0))
    assert r.go and r.breakeven_permit_yuan == pytest.approx((50+5_000_000)*100 - 120_000)

def test_probability_sum_rejected():
    sc1 = PolicyScenario("a", {}, date(2026,1,1), 0.7)
    sc2 = PolicyScenario("b", {}, date(2026,1,1), 0.7)   # same date, overlapping → sum 1.4
    with pytest.raises(ValueError):
        value_permit([sc1, sc2], PolicyLevers(), 50.0, "bess", 0.85, _inputs(0.0))

def test_acquisition_actuals_anchor_replaces_model():
    r = value_acquisition(ask_yuan=1.0, energy_value_yuan_kw=50.0, levers=PolicyLevers(),
                          power_mw=1.0, duration_h=2.0, asset_type="bess", rte=0.85,
                          uplift_rows=[], actuals_anchor_yuan_kw=80.0, hurdle=0.0, life_years=1)
    assert r.fair_npv_yuan == 80.0 * 1000                  # anchor wins over model 50
    assert r.go
