from datetime import date

import pytest

from libs.positions.conventions import (
    POSITION_COLUMNS, PROVINCE_ALIASES, Position, canonical_province,
    positions_frame, signed_mwh, tou_of_hour,
)

PROBE_NAMES = [
    "inner_mongolia_mengxi", "guangxi", "jiangsu", "anhui", "shandong", "gansu",
    "冀南", "浙江", "山东", "安徽", "江苏", "福建", "上海", "广东",
    "陕西", "湖北", "山西", "贵州", "广西", "海南", "黑龙江", "吉林", "辽宁", "蒙西",
    "云南", "甘肃", "河南", "新疆", "河北南网", "宁夏", "豫北", "豫西", "豫南", "豫中东",
    "湖南", "四川", "江西", "青海", "海南礼记", "海南那悦", "蒙东",
]


def test_signed_mwh():
    assert signed_mwh("buy", 5) == 5
    assert signed_mwh("sell", 5) == -5
    with pytest.raises(ValueError):
        signed_mwh("hold", 1)


def test_canonical_province():
    assert canonical_province("inner_mongolia_mengxi") == "蒙西"
    assert canonical_province("Inner Mongolia (Mengxi)") == "蒙西"
    assert canonical_province("河北南网") == "冀南"
    assert canonical_province("guangxi") == "广西"
    assert canonical_province("豫北") == "豫北"
    with pytest.raises(ValueError, match="火星"):
        canonical_province("火星")


def test_all_probe_names_map():
    for n in PROBE_NAMES:
        assert canonical_province(n) in PROVINCE_ALIASES


def test_aliases_unique():
    seen = {}
    for k, al in PROVINCE_ALIASES.items():
        assert k in al
        for a in al:
            assert seen.setdefault(a, k) == k


def test_tou():
    assert tou_of_hour(18) == "peak"
    assert tou_of_hour(12) == "offpeak"
    assert tou_of_hour(3) == "flat"


def test_positions_frame():
    df = positions_frame([])
    assert df.empty and list(df.columns) == POSITION_COLUMNS
    p = Position(1, "冀南", "buy", 10.0, date(2026, 12, 1))
    df = positions_frame([p])
    assert len(df) == 1 and df.iloc[0]["tou_block"] == "base"
