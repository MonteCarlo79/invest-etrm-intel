# tests/bess_map/test_capacity_stack.py
"""Capacity payment formula + seed-table tests (approved formula:
rate × 1000/D × min(D, base)/base × coefficient)."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "services" / "bess_map"))
from capacity_stack import (  # noqa: E402
    CAPACITY_PRICE_SEED, DEFAULT_RATE, DEFAULT_BASE_HOURS,
    capacity_payment_per_mwh_yr, capacity_stack_map, seed_rows,
)

f = capacity_payment_per_mwh_yr


def test_duration_independent_below_base():
    """D ≤ base: per-MWh payment is the same for 2h and 4h units."""
    assert f(165, 6, 1.0, 2.0) == pytest.approx(f(165, 6, 1.0, 4.0))
    assert f(165, 6, 1.0, 4.0) == pytest.approx(165 * 1000 / 6)  # 27,500


def test_capped_above_base():
    """D > base: payment falls as 1/D (pro-rating caps at base_hours)."""
    assert f(165, 6, 1.0, 8.0) == pytest.approx(165 * 1000 / 8)  # 20,625


def test_coefficient_applied():
    """甘肃 330/6h/0.89 at 4h: 330×250×(4/6)×0.89."""
    assert f(330, 6, 0.89, 4.0) == pytest.approx(330 * 250 * (4 / 6) * 0.89)


def test_hubei_10h_basis():
    assert f(165, 10, 1.0, 4.0) == pytest.approx(165 * 250 * 0.4)  # 16,500


def test_liaoning_370():
    assert f(370, 6, 1.0, 2.0) == pytest.approx(370 * 1000 / 6)


def test_seed_rows_valid():
    rows = seed_rows()
    provs = {r[0] for r in rows}
    assert {"甘肃", "吉林", "青海", "陕西", "湖北", "辽宁", "宁夏", "山东"} <= provs
    assert all(r[4] in {"formal", "default", "draft", "none", "legacy"} for r in rows)


def test_stack_map_excludes_non_stackable():
    rows = [
        ("甘肃", 330, 6, 0.89, "formal"),
        ("宁夏", 165, 6, 1.0, "draft"),     # excluded
        ("山西", 165, 6, 1.0, "none"),      # excluded
        ("山东", 0, 6, 1.0, "legacy"),      # excluded
        ("蒙西", 0, 6, 1.0, "legacy"),      # excluded (user: keep old scheme)
    ]
    m = capacity_stack_map(rows, 4.0)
    assert set(m) == {"甘肃"}
    assert m["甘肃"] == pytest.approx(f(330, 6, 0.89, 4.0))


def test_seed_uses_db_province_names():
    """Seed keys must match spot_prices_hourly province names, or excluded
    provinces silently fall through to the DEFAULT stack (蒙西/河北南网 bug)."""
    provs = {r[0] for r in seed_rows()}
    assert "蒙西" in provs and "内蒙古" not in provs
    assert "河北南网" in provs and "河北" not in provs


def test_stack_map_duration_sensitivity():
    """Sensitivity only appears above base_hours (equal below, by design)."""
    rows = [("湖北", 165, 10, 1.0, "formal")]
    assert capacity_stack_map(rows, 8.0)["湖北"] > capacity_stack_map(rows, 12.0)["湖北"]
    assert capacity_stack_map(rows, 2.0)["湖北"] == capacity_stack_map(rows, 4.0)["湖北"]


def test_ancillary_per_mwh_yr_power_rated():
    """调频 is power-rated: a 2h unit gets 2× the per-MWh revenue of 4h."""
    from capacity_stack import ancillary_per_mwh_yr
    total, mw = 6.42e7, 500.0   # ¥64.2M/yr over 500 MW fleet
    v2 = ancillary_per_mwh_yr(total, mw, 2.0)
    v4 = ancillary_per_mwh_yr(total, mw, 4.0)
    assert v2 == pytest.approx(2 * v4)
    assert v4 == pytest.approx(total / (mw * 1000) * 250)
    assert ancillary_per_mwh_yr(total, 0.0, 4.0) == 0.0


def test_fr_pool_share_regions():
    from capacity_stack import fr_pool_share, SOUTH_GRID_PROVINCES
    assert fr_pool_share("甘肃") == pytest.approx(0.30)
    assert fr_pool_share("广西") == pytest.approx(0.20)
    assert SOUTH_GRID_PROVINCES == frozenset({"广东", "广西", "云南", "贵州", "海南"})


def test_fr_pool_per_mwh_yr():
    """甘肃: 0.4亿 pool × 30% ÷ 2000 MW → ¥6/kW/yr → ×250 = ¥1,500/MWh(4h)."""
    from capacity_stack import fr_pool_per_mwh_yr
    v = fr_pool_per_mwh_yr(0.4e8, 2000.0, 4.0, "甘肃")
    assert v == pytest.approx(1500.0)
    # 广西 same pool at 20%
    v_gx = fr_pool_per_mwh_yr(0.4e8, 2000.0, 4.0, "广西")
    assert v_gx == pytest.approx(1000.0)


def test_fr_component_precedence():
    from capacity_stack import fr_component
    # actuals beat pool
    v = fr_component("新疆", 4.0, 6.42e7, 1e8, 500.0)
    assert v == pytest.approx(6.42e7 / 500e3 * 250)
    # pool used when no actuals
    v2 = fr_component("甘肃", 4.0, None, 0.4e8, 2000.0)
    assert v2 == pytest.approx(1500.0)
    # zero when neither
    assert fr_component("西藏", 4.0, None, None, 100.0) == 0.0


def test_sysopfee_annual_cost():
    from capacity_stack import sysopfee_annual_cost_per_mwh
    # 0.03 ¥/kWh × 1000 kWh × 0.7 cycles × 365 / 0.85 ≈ ¥9,018/MWh/yr
    v = sysopfee_annual_cost_per_mwh(0.03, 0.7)
    assert v == pytest.approx(0.03 * 1000 * 0.7 * 365 / 0.85)
    assert v == pytest.approx(9017.65, rel=1e-3)
    assert sysopfee_annual_cost_per_mwh(0.0, 0.7) == 0.0
    assert sysopfee_annual_cost_per_mwh(0.03, 0.0) == 0.0


def test_discharge_comp_per_mwh_yr():
    """蒙西/蒙东 0.28 元/kWh at 0.72 cycles: 0.28×1000×0.72×365 ≈ ¥73,584/MWh/yr."""
    from capacity_stack import discharge_comp_per_mwh_yr
    v = discharge_comp_per_mwh_yr(0.28, 0.72)
    assert v == pytest.approx(0.28 * 1000 * 0.72 * 365)
    assert v == pytest.approx(73584.0)
    # 山东 0.0705: ≈ ¥18,532
    assert discharge_comp_per_mwh_yr(0.0705, 0.72) == pytest.approx(18532.2, rel=1e-3)
    assert discharge_comp_per_mwh_yr(0.28, 0.0) == 0.0
    assert discharge_comp_per_mwh_yr(0.0, 0.72) == 0.0


def test_discharge_mode_threshold():
    from capacity_stack import DISCHARGE_MODE_RATE_THRESHOLD
    assert DISCHARGE_MODE_RATE_THRESHOLD == 1.0
    # every per-kW-year rate in the seed is ≥100; every discharge rate < 1
    for prov, (rate, *_rest) in CAPACITY_PRICE_SEED.items():
        if rate > 0:
            assert rate >= 100 or rate < DISCHARGE_MODE_RATE_THRESHOLD


def test_cap_comp_name_map():
    from capacity_stack import CAP_COMP_NAME_MAP
    assert "蒙西" in CAP_COMP_NAME_MAP["内蒙古（蒙东）"]
    assert "蒙东" in CAP_COMP_NAME_MAP["内蒙古（蒙东）"]
    assert CAP_COMP_NAME_MAP["冀南"] == ["河北南网"]
