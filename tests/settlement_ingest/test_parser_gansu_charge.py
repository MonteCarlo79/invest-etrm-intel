"""Tests for the Gansu charge bill parser (下网电费结算单)."""
import pytest

from services.settlement_ingest.parser_gansu import (
    is_gansu_charge_bill, parse_gansu_charge_text, _row_amount,
)

MARCH = """
账单周期 户号 6267000785070
2026-03-01 户名 民勤景肃新能源有限公司
本期电量 856579千瓦时 本期电费 171452.39元 欠费金额 0元
账单概况 单位：千瓦时、千伏安（千瓦）、元
费用组成 计收数量 电费
①市场化购电电费 856579 93991.60
②输配电量电费 856579 34396.91
③输配容（需）量电费 0 0.00
④上网环节线损费用 856579 590.99
⑤系统运行费 856579 3969.81
⑥功率因数调整电费 / 35428.82
⑦政府性基金及附加 856579 3074.26
⑧代理服务费 / /
合计 ¥171452.39
备注：
"""

REFUND = """
账单周期 户号 6267000785070
2026-05-01 户名 民勤景肃新能源有限公司
本期电量 0千瓦时 本期电费 -610866.63元 欠费金额 0元
费用组成 计收数量 电费
①市场化购电电费 0 0.00
②输配电量电费 0 -341762.65
③输配容（需）量电费 0 0.00
④上网环节线损费用 0 0.00
⑤系统运行费 0 0.00
⑥功率因数调整电费 / -228980.98
⑦政府性基金及附加 0 -40123.00
合计 ¥-610866.63
备注：
"""

JAN_TRAP = """
账单周期 户号 6267000785070
本期电量 1551995千瓦时 本期电费 179574.29元
费用组成 计收数量 电费
①市场化购电电费 1551995 179574.29
②输配电量电费 1551995 0.00
合计 ¥179574.29
备注：
时段 峰 平 谷
上期 51634 158000 1142286
本期 138208 270088 1143699
"""


def test_is_gansu_charge_bill():
    assert is_gansu_charge_bill(MARCH)
    assert not is_gansu_charge_bill("内蒙古 下网结算单 电能电费元")


def test_march_rows_and_total():
    items = parse_gansu_charge_text(MARCH)
    assert len(items) == 6
    en = next(i for i in items if i["category"] == "charge_energy")
    assert en["volume_mwh"] == pytest.approx(856.579)
    assert en["amount_cny"] == pytest.approx(-93991.60)
    bf = next(i for i in items if i["category"] == "basic_fee")
    assert bf["amount_cny"] == pytest.approx(-35428.82)  # "/" volume cell
    assert sum(i["amount_cny"] for i in items) == pytest.approx(-171452.39)


def test_refund_bill_positive_pnl():
    items = parse_gansu_charge_text(REFUND)
    # printed -610,866.63 → P&L +610,866.63 (refund to the station)
    assert sum(i["amount_cny"] for i in items) == pytest.approx(610866.63)
    tr = next(i for i in items if i["category"] == "transmission")
    assert tr["amount_cny"] == pytest.approx(341762.65)


def test_valley_table_not_leaked():
    items = parse_gansu_charge_text(JAN_TRAP)
    # the 峰平谷 panel numbers (1142286/1143699) must not appear anywhere
    amounts = [i["amount_cny"] for i in items]
    assert amounts == [pytest.approx(-179574.29)]
    assert not any(abs(a) > 1000000 for a in amounts)


def test_slash_volume_cell_amount():
    assert _row_amount("⑥功率因数调整电费 / 35428.82", "功率因数调整电费") == pytest.approx(35428.82)
    assert _row_amount("⑥功率因数调整电费 / -228980.98", "功率因数调整电费") == pytest.approx(-228980.98)
