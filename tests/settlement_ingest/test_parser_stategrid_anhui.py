"""Tests for the 安徽 (Dingyuan) format family: 国网安徽发电侧电费账单 +
安徽电力交易中心(统推)交易结算单."""
import pathlib

import pytest

from services.settlement_ingest.parser_stategrid import (
    is_ah_discharge,
    is_ah_sdtc,
    parse_ah_discharge,
    parse_ah_discharge_total,
    parse_ah_sdtc,
)

FIX = pathlib.Path(__file__).parent / "fixtures"


def _load(name):
    return (FIX / name).read_text(encoding="utf-8")


def _by_notes(items, needle):
    return [it for it in items if needle in (it["notes"] or "")]


class TestAhDischarge:
    """国网安徽 发电侧上网电费账单 2026-02: 电能量交易电费 total +
    日前/实时 decomposition + fee rows on page 2."""

    @pytest.fixture(scope="class")
    def items(self):
        return parse_ah_discharge(_load("ah_discharge_202602.txt"))

    def test_detection(self):
        assert is_ah_discharge(_load("ah_discharge_202602.txt")) is True
        assert is_ah_discharge(_load("stategrid_discharge_202604.txt")) is False

    def test_energy_rows(self, items):
        da = _by_notes(items, "日前交易")[0]
        assert da["category"] == "discharge_energy"
        assert da["volume_mwh"] == pytest.approx(9570.777)
        assert da["price_cny_kwh"] == pytest.approx(0.37463)
        assert da["amount_cny"] == pytest.approx(3585463.46)
        rt = _by_notes(items, "实时交易")[0]
        assert rt["volume_mwh"] == pytest.approx(457.043)
        assert rt["amount_cny"] == pytest.approx(228306.67)
        # settled volume = 日前 + 实时 = 上网电量 10,027,820 kWh
        assert da["volume_mwh"] + rt["volume_mwh"] == pytest.approx(10027.820, abs=0.001)

    def test_fee_rows(self, items):
        assert _by_notes(items, "两个细则费用")[0]["amount_cny"] == pytest.approx(205585.75)
        assert _by_notes(items, "两个细则费用")[0]["category"] == "frequency"
        assert _by_notes(items, "其他费用")[0]["amount_cny"] == pytest.approx(958826.00)
        assert _by_notes(items, "其他费用")[0]["category"] == "other"

    def test_totals_dropped(self, items):
        assert not _by_notes(items, "电能量交易电费")
        assert not _by_notes(items, "合计")

    def test_sum_equals_printed_total(self, items):
        total = parse_ah_discharge_total(_load("ah_discharge_202602.txt"))
        assert total == pytest.approx(4978181.88)
        assert sum(it["amount_cny"] for it in items) == pytest.approx(total, abs=0.01)


class TestAhDischarge202609Layout:
    """2026-09+ layout: p1 费用组成 summary + p2 账单详情 detail (小计： rows,
    (一)-prefixed sub-rows with amounts, 现货 suffix dropped from value lines).

    Regression: the old detector missed this layout → vision fallback stored
    page-1 summary + page-2 detail (every fee 2x) with 市场运营费用 negated."""

    @pytest.fixture(scope="class")
    def items(self):
        return parse_ah_discharge(_load("ah_discharge_202609.txt"))

    def test_detection(self):
        assert is_ah_discharge(_load("ah_discharge_202609.txt")) is True
        assert is_ah_discharge(_load("stategrid_discharge_202604.txt")) is False

    def test_exactly_five_items_no_duplicates(self, items):
        assert len(items) == 5

    def test_energy_rows(self, items):
        da = _by_notes(items, "日前交易")[0]
        assert da["category"] == "discharge_energy"
        assert da["volume_mwh"] == pytest.approx(5343.402)
        assert da["price_cny_kwh"] == pytest.approx(0.4033)
        assert da["amount_cny"] == pytest.approx(2154980.79)
        rt = _by_notes(items, "实时交易")[0]
        assert rt["volume_mwh"] == pytest.approx(3147.608)
        assert rt["price_cny_kwh"] == pytest.approx(0.40493)
        assert rt["amount_cny"] == pytest.approx(1274567.68)

    def test_fee_rows_once_positive(self, items):
        mo = _by_notes(items, "市场运营费用")
        assert len(mo) == 1 and mo[0]["amount_cny"] == pytest.approx(238198.99)
        assert mo[0]["category"] == "other"
        ts = _by_notes(items, "两个细则费用")
        assert len(ts) == 1 and ts[0]["amount_cny"] == pytest.approx(152103.16)
        ot = _by_notes(items, "其他费用")
        assert len(ot) == 1 and ot[0]["amount_cny"] == pytest.approx(1606770.00)

    def test_summary_page_not_double_counted(self, items):
        total = parse_ah_discharge_total(_load("ah_discharge_202609.txt"))
        assert total == pytest.approx(5426620.62)
        assert sum(it["amount_cny"] for it in items) == pytest.approx(total, abs=0.01)


class TestAhSdtc:
    """安徽电力交易中心(统推) 2026-02: 售电侧 from summary, 购电侧 from detail 合计."""

    @pytest.fixture(scope="class")
    def parsed(self):
        return parse_ah_sdtc(_load("ah_sdtc_202602.txt"))

    def test_detection(self):
        assert is_ah_sdtc(_load("ah_sdtc_202602.txt")) is True
        assert is_ah_sdtc(_load("ah_discharge_202602.txt")) is False

    def test_sell_side(self, parsed):
        sell = parsed["售电侧"]
        assert sell["category"] == "discharge_energy"
        assert sell["volume_mwh"] == pytest.approx(10027.820)
        assert sell["amount_cny"] == pytest.approx(3601657.94)
        assert sell["price_cny_kwh"] == pytest.approx(3601657.94 / 10027.820 / 1000, rel=1e-5)

    def test_buy_side(self, parsed):
        buy = parsed["购电侧"]
        assert buy["category"] == "charge_energy"
        assert buy["volume_mwh"] == pytest.approx(11871.204)
        assert buy["amount_cny"] == pytest.approx(-1376523.94)  # printed positive cost → stored negative
