"""蒙西 BESS daily ranking — discharge-compensation rate per plant (2026-11-11 list)."""
from datetime import date

import pytest

from services.hermes.mengxi_ranking_report import _resolve_comp_rate

_280 = ["华腾南园储能电站", "旭纯哈乐储能电站", "旭纯哈乐储能电站二", "烨荣晟储能电站",
        "明阳把栅储能电站", "蒙能翁贡储能电站", "国能托清储能电站", "景敖乌珠日储能电站",
        "盛德二登图储能电站", "荣鑫地房子储能电站"]


@pytest.mark.parametrize("plant", _280)
def test_listed_280_plants_get_280_even_if_seen_before_cutoff(plant):
    # first_seen before the 2026-01-01 new-asset cutoff would otherwise default to 350
    assert _resolve_comp_rate(plant, date(2025, 6, 1), date(2026, 11, 10)) == 280.0


def test_unlisted_pre_cutoff_plant_keeps_350():
    assert _resolve_comp_rate("景蓝乌尔图储能电站", date(2025, 6, 1), date(2026, 11, 10)) == 350.0


def test_zero_comp_plant_unchanged():
    assert _resolve_comp_rate("大航都林储能电站", date(2025, 6, 1), date(2026, 11, 10)) == 0.0
