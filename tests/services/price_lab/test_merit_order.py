import math
from services.bess_map.price_lab.merit_order import build_stack, marginal_price

_SEGS = [
    {"fuel": "coal", "capacity_mw": 10000, "heat_rate_kj_kwh": 9500, "vom_yuan_mwh": 12, "label": "coal_low_eff"},
    {"fuel": "coal", "capacity_mw": 30000, "heat_rate_kj_kwh": 8200, "vom_yuan_mwh": 12, "label": "coal_hi_eff"},
    {"fuel": "gas",  "capacity_mw": 5000,  "heat_rate_kj_kwh": 6400, "vom_yuan_mwh": 8,  "label": "ccgt"},
]

def test_stack_sorted_by_variable_cost():
    stack = build_stack(_SEGS, coal_price_yuan_t=850.0, gas_price_yuan_m3=3.1)
    vcs = [s["vc_yuan_mwh"] for s in stack]
    assert vcs == sorted(vcs)
    # hi-eff coal: kg/kWh = 8200/29307 LHV → vc = 850/1000 × (8200/29307) × 1000 + 12
    hi = [s for s in stack if s["label"] == "coal_hi_eff"][0]
    assert abs(hi["vc_yuan_mwh"] - (850 / 1000 * 8200 / 29307 * 1000 + 12)) < 0.01

def test_renewable_first_in_stack():
    stack = build_stack(_SEGS, 850.0, 3.1, renewable_mw=8000)
    assert stack[0]["label"] == "renewable" and stack[0]["vc_yuan_mwh"] == 0.0

def test_marginal_price_walks_the_stack():
    stack = build_stack(_SEGS, 850.0, 3.1, renewable_mw=5000)
    p1 = marginal_price(stack, 3000)    # inside renewable
    assert p1 == 0.0
    p2 = marginal_price(stack, 30000)   # past renewable, inside cheapest thermal
    cheapest = stack[1]
    assert p2 == cheapest["vc_yuan_mwh"]
    p3 = marginal_price(stack, 44000)   # into second thermal
    assert p3 == stack[2]["vc_yuan_mwh"]

def test_import_block_placed_by_price():
    imp = [{"label": "锦苏直流", "price_yuan_mwh": 300.0, "capacity_mw": 2000}]
    stack = build_stack(_SEGS, 850.0, 3.1, import_blocks=imp)
    assert any(s["is_import"] for s in stack)
    imp_seg = [s for s in stack if s["is_import"]][0]
    assert imp_seg["vc_yuan_mwh"] == 300.0 and imp_seg["capacity_mw"] == 2000

def test_demand_above_stack_returns_nan():
    stack = build_stack(_SEGS, 850.0, 3.1)
    assert math.isnan(marginal_price(stack, 999999))
