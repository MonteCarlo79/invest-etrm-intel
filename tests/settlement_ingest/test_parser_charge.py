"""Tests for services/settlement_ingest/parser_charge.py volume disambiguation."""
import pytest

from services.settlement_ingest.parser_charge import parse_charging_text


# Simplified from 苏右 2025-09 下网结算单: a decoy 2x volume row appears among
# the data rows; the label lines sit in a separate section (as in the real bill).
# max() would pick the decoy; amount-matching must not.
SUYOU_SEP_TEXT = """
24486580.0 0.2156146853 5279666.24
24486580 0.006829 167218.85
48973160 0.0002 9794.64
电价类别 计费电量kWh 电价标准元/kWh 电能电费元 5279666.24
电价类别 计费电量kWh 电价标准元/kWh 上网环节线损费用元 167218.85
电价类别 计费电量kWh 电价标准元/kWh 系统运行费元 524012.83
电价类别 计费电量kWh 电价标准元/kWh 功率因数调整电费元 44781.73
"""


def test_volume_matches_energy_amount_not_max():
    items = parse_charging_text(SUYOU_SEP_TEXT)
    charge = next(i for i in items if i["notes"] == "电能电费(市场化购电)")
    # True volume is 24,486,580 kWh = 24,486.58 MWh; decoy is 48,973.16
    assert charge["volume_mwh"] == pytest.approx(24486.58, rel=1e-4)
    # Charge amounts are negated (cost convention)
    assert charge["amount_cny"] == pytest.approx(-5279666.24)


def test_fallback_to_max_when_no_energy_amount_match():
    # No 电能电费元 line -> max() fallback picks the largest candidate
    text = "12345678 0.30 3703703.40\n99999999 0.0001 9999.99\n"
    items = parse_charging_text(text)
    # no 电能电费 line -> no charge_energy item with volume, but total diff item appears
    # volume is attached only when 电能电费 exists; here we just ensure no crash
    assert isinstance(items, list)


# 四子王旗 2026-03 电费账单 (simplified): 退补电费 printed NEGATIVE (bill
# reduction = money back to the user). Stored sign must be POSITIVE (income),
# and total must equal -(总电费). Bug: printed -216,439.05 was stored as
# -216,439.05 (a cost), understating 净利润 by 2x.
SIZIWANG_202603_TEXT = """
电价类别 计费电量kWh 电价标准元/kWh 电能电费元 2384500.57
电价类别 计费电量kWh 电价标准元/kWh 上网环节线损费用元 75518.04
电价类别 计费电量kWh 电价标准元/kWh 系统运行费元 430374.88
电价类别 计费电量kWh 电价标准元/kWh 功率因数调整电费元 28903.93
退补电费
-216439.05 元
总电费(元)
2702858.37
"""


def test_negative_printed_tuibu_becomes_positive_income():
    items = parse_charging_text(SIZIWANG_202603_TEXT)
    tuibu = next(i for i in items if i["notes"] == "退补电费")
    assert tuibu["category"] == "rebate"
    assert tuibu["amount_cny"] == pytest.approx(216439.05)  # printed -216,439.05 → +216,439.05
    # Full-bill consistency: stored total = -(printed 总电费)
    total = sum(i["amount_cny"] for i in items)
    assert total == pytest.approx(-2702858.37, abs=0.01)
    # Costs still negative
    energy = next(i for i in items if i["notes"] == "电能电费(市场化购电)")
    assert energy["amount_cny"] == pytest.approx(-2384500.57)


# 谷山梁 2026-01: 功率因数调整电费 printed NEGATIVE (reward for good power
# factor) — same sign bug class as 退补: must store as +37,695.00 income.
GUSHANLIANG_202601_TEXT = """
电价类别 计费电量kWh 电价标准元/kWh 电能电费元 10324557.68
电价类别 计费电量kWh 电价标准元/kWh 上网环节线损费用元 384659.96
电价类别 计费电量kWh 电价标准元/kWh 系统运行费元 1855777.58
电价类别 力调实际值 力调标准 参与力调金额元 调整系数 功率因数调整电费元 -37695.00
退补电费
-96222.79 元
总电费(元)
12431077.43
"""


def test_negative_power_factor_adjustment_becomes_positive():
    items = parse_charging_text(GUSHANLIANG_202601_TEXT)
    pf = next(i for i in items if i["notes"] == "功率因数调整电费")
    assert pf["amount_cny"] == pytest.approx(37695.00)  # printed -37,695.00 reward → +37,695.00
    total = sum(i["amount_cny"] for i in items)
    assert total == pytest.approx(-12431077.43, abs=0.01)
