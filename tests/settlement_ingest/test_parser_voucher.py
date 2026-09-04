"""Tests for parser_voucher.py (发电侧结算凭证)."""
import pytest

from services.settlement_ingest.parser_voucher import parse_generation_voucher_text

# Simplified from 乌海 2026-01 发电侧结算凭证
JAN_TEXT = """
乌海市远鸿富景新能源科技有限公司交易结算凭证（2026年01月）
编号：DC047300001001-2026-01-01
机组名称：储_富景五虎山#1期 机组编码：DC047300001001
电能量市场价格及费用信息 单位：兆瓦时、元/兆瓦时、元
月度上网电量 12559.32 月累计上网电量 12556.89
合约电量 0 差错电量 0.000000 电能电费 4213848.61
合约均价 0 现货市场月度加权均价 335.581
发行费用合计 4213848.61 发行费用均价 335.52
"""

# kWh-denominated variant (unit guard)
KWH_TEXT = """
某公司交易结算凭证（2026年01月）
电能量市场价格及费用信息 单位：千瓦时、元/千瓦时、元
月度上网电量 12559320 月累计上网电量 12559320
电能电费 4213848.61
现货市场月度加权均价 0.335581
"""


def test_mwh_voucher():
    items = parse_generation_voucher_text(JAN_TEXT)
    assert len(items) == 1
    it = items[0]
    assert it["category"] == "discharge_energy"
    assert it["volume_mwh"] == pytest.approx(12559.32)
    assert it["amount_cny"] == pytest.approx(4213848.61)
    assert it["price_cny_kwh"] == pytest.approx(0.335581)
    assert "发电侧结算凭证" in it["notes"]


def test_kwh_voucher_unit_guard():
    items = parse_generation_voucher_text(KWH_TEXT)
    assert items[0]["volume_mwh"] == pytest.approx(12559.32)
    assert items[0]["price_cny_kwh"] == pytest.approx(0.335581)


def test_missing_fields_returns_empty():
    assert parse_generation_voucher_text("nothing here") == []
