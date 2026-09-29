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
