"""Tests for services/settlement_ingest/parser_gansu.py"""
import pytest

from services.settlement_ingest.parser_gansu import (
    is_gansu_bill, parse_gansu_discharge_text, _row_amount, _row_volume_kwh,
)

JAN = """
国网甘肃省电力公司
2026-01-01
上网电量 1190371 上网均价 0.408930 元/千瓦 结算金额 486778.88元
本期电费明细
单位：千瓦时、元/千瓦时、元
类别 电量 电价 电费
一、购电费
（一）电能量电费 1190371 448515.11
（二）辅助服务交易 0 38263.77
（三）市场运营费用 0
二、机制电费
（一）机制电费 0 0
"""

MAY_DETACHED = """
国网甘肃省电力公司
0.446270
1369728千瓦
上网电量 上网均价 元/千瓦 结算金额 611268.63元
本期电费明细
类别 电量 电价 电费
一、购电费
（一）电能量电费 1369728 272953.65
（二）系统运行费用 338314.98
（三）市场运营费用 0
二、机制电费
"""

JUN_WITH_DEDUCTIONS = """
国网甘肃省电力公司
1410347千瓦
上网电量 上网均价 元/千瓦 结算金额 802633.54元
本期电费明细
类别 电量 电价 电费
一、购电费
（一）电能量电费 1410347 430687.06
（二）系统运行费用 424087.34
（七）两个细则费用 -88646.4
（八）清算 0 36505.54
二、机制电费
"""

DETAIL_SECTION_TRAP = (
    JAN.replace("二、机制电费", """二、机制电费
辅助服务交易 338314.98
""")
)


def test_is_gansu_bill():
    assert is_gansu_bill(JAN)
    assert not is_gansu_bill("内蒙古电力 上网电费结算单 成分明细")


def test_jan_two_rows_exact_total():
    items = parse_gansu_discharge_text(JAN)
    assert len(items) == 2
    en = next(i for i in items if i["category"] == "discharge_energy")
    fr = next(i for i in items if i["category"] == "frequency")
    assert en["volume_mwh"] == pytest.approx(1190.371)
    assert en["amount_cny"] == pytest.approx(448515.11)
    assert fr["amount_cny"] == pytest.approx(38263.77)
    assert sum(i["amount_cny"] for i in items) == pytest.approx(486778.88)


def test_may_detached_volume_label():
    items = parse_gansu_discharge_text(MAY_DETACHED)
    assert len(items) == 2
    en = next(i for i in items if i["category"] == "discharge_energy")
    assert en["volume_mwh"] == pytest.approx(1369.728)
    so = next(i for i in items if i["category"] == "system_operation")
    assert so["amount_cny"] == pytest.approx(338314.98)


def test_jun_negative_and_clearing_rows():
    items = parse_gansu_discharge_text(JUN_WITH_DEDUCTIONS)
    assert len(items) == 4
    amounts = sorted(i["amount_cny"] for i in items)
    assert amounts == pytest.approx([-88646.4, 36505.54, 424087.34, 430687.06])
    assert sum(i["amount_cny"] for i in items) == pytest.approx(802633.54)


def test_detail_section_after_marker_ignored():
    items = parse_gansu_discharge_text(DETAIL_SECTION_TRAP)
    # 辅助服务交易 beyond the marker must not produce a third row
    assert len(items) == 2
    assert sum(i["amount_cny"] for i in items) == pytest.approx(486778.88)


def test_row_amount_does_not_split_amount():
    # "系统运行费用 338314.98" must not split into 33831 + 4.98
    assert _row_amount("（二）系统运行费用 338314.98", "系统运行费用") == pytest.approx(338314.98)
    # negative single-decimal amount keeps its sign (dash must not be eaten)
    assert _row_amount("（七）两个细则费用 -88646.4", "两个细则费用") == pytest.approx(-88646.4)


def test_row_volume_kwh():
    assert _row_volume_kwh("（一）电能量电费 1190371 448515.11", "电能量电费") == pytest.approx(1190371)
    assert _row_volume_kwh("（二）系统运行费用 338314.98", "系统运行费用") is None


def test_missing_total_returns_empty():
    assert parse_gansu_discharge_text("国网甘肃省电力公司\n没有结算字段") == []
