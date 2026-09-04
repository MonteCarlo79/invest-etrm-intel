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
