"""Tests for the 2025 滨州 format family: format A (地方电厂结算单),
format B variants (rich 2025 labels, no-账单概况 layout, wrapped labels,
negative RT volume), format C (交易中心结算单), charge wrap-tail."""
import pathlib

import pytest

from services.settlement_ingest.parser_stategrid import (
    is_local_discharge,
    is_sdtc_settlement,
    is_stategrid_charge,
    is_stategrid_discharge,
    parse_local_discharge,
    parse_local_discharge_total,
    parse_sdtc_settlement,
    parse_stategrid_charge,
    parse_stategrid_charge_total,
    parse_stategrid_discharge,
    parse_stategrid_discharge_total,
)

FIX = pathlib.Path(__file__).parent / "fixtures"


def _load(name):
    return (FIX / name).read_text(encoding="utf-8")


def _by_notes(items, needle):
    return [it for it in items if needle in (it["notes"] or "")]


class TestLocalDischargeFormatA:
    """山东省地方电厂市场化结算单 (2025-01): energy block + fee rows."""

    @pytest.fixture(scope="class")
    def items(self):
        return parse_local_discharge(_load("stategrid_discharge_local_202501.txt"))

    def test_detection(self):
        assert is_local_discharge(_load("stategrid_discharge_local_202501.txt")) is True
        assert is_local_discharge(_load("stategrid_discharge_202604.txt")) is False

    def test_energy_rows_with_volumes(self, items):
        da = _by_notes(items, "日前偏差电费")[0]
        assert da["category"] == "discharge_energy"
        assert da["volume_mwh"] == pytest.approx(4462.961)
        assert da["price_cny_kwh"] == pytest.approx(0.3926)
        assert da["amount_cny"] == pytest.approx(1752362.19)
        rt = _by_notes(items, "实时偏差电费")[0]
        assert rt["category"] == "discharge_energy"
        assert rt["volume_mwh"] == pytest.approx(-424.822)  # RT reduction — negative vol is real
        assert rt["amount_cny"] == pytest.approx(-187670.50)

    def test_fee_categories(self, items):
        assert _by_notes(items, "退补费用")[0]["category"] == "rebate"
        assert _by_notes(items, "预测偏差费用")[0]["category"] == "penalty"
        assert _by_notes(items, "优发优购曲线匹配偏差费")[0]["category"] == "penalty"
        assert _by_notes(items, "阻塞费用")[0]["category"] == "other"
        assert _by_notes(items, "跨月偏差退补费")[0]["category"] == "rebate"
        assert _by_notes(items, "容量补偿费")[0]["category"] == "capacity_compensation"

    def test_subtotals_dropped(self, items):
        assert not _by_notes(items, "小计")
        assert not _by_notes(items, "合计")

    def test_sum_equals_printed_total(self, items):
        total = parse_local_discharge_total(_load("stategrid_discharge_local_202501.txt"))
        assert total == pytest.approx(1863559.70)
        assert sum(it["amount_cny"] for it in items) == pytest.approx(total, abs=0.01)


class TestRichLabels2025:
    """2025-05 发电侧: 日前偏差 row + rich label set."""

    @pytest.fixture(scope="class")
    def items(self):
        return parse_stategrid_discharge(_load("stategrid_discharge_202505.txt"))

    def test_dayahead_and_rt_are_energy(self, items):
        da = _by_notes(items, "日前偏差电费")[0]
        assert da["category"] == "discharge_energy"
        assert da["volume_mwh"] == pytest.approx(4898.616)
        assert da["amount_cny"] == pytest.approx(2224977.80)
        rt = _by_notes(items, "实时偏差电费")[0]
        assert rt["category"] == "discharge_energy"
        assert rt["volume_mwh"] == pytest.approx(-312.128)
        assert rt["price_cny_kwh"] == pytest.approx(0.3329)
        assert rt["amount_cny"] == pytest.approx(-103910.89)
        # settled volume = DA + RT ≈ 上网电量 4,586,490 kWh
        assert da["volume_mwh"] + rt["volume_mwh"] == pytest.approx(4586.488, abs=0.01)

    def test_new_labels(self, items):
        assert _by_notes(items, "阻塞返还电费")[0]["category"] == "rebate"
        assert _by_notes(items, "居民农业新增损益-预测偏差-考核费用")[0]["category"] == "penalty"
        assert _by_notes(items, "爬坡辅助服务补偿及分摊")[0]["category"] == "frequency"
        assert _by_notes(items, "退补费用")[0]["category"] == "rebate"

    def test_sum_equals_printed_total(self, items):
        total = parse_stategrid_discharge_total(_load("stategrid_discharge_202505.txt"))
        assert total == pytest.approx(2186290.89)
        assert sum(it["amount_cny"] for it in items) == pytest.approx(total, abs=0.01)


class TestNoGaikuoVariant:
    """2025-03: 电费明细 table WITHOUT the 账单概况 heading + wrapped label."""

    @pytest.fixture(scope="class")
    def items(self):
        return parse_stategrid_discharge(_load("stategrid_discharge_202503.txt"))

    def test_detection(self):
        assert is_stategrid_discharge(_load("stategrid_discharge_202503.txt")) is True

    def test_wrapped_label_row(self, items):
        rows = _by_notes(items, "居民农业新增损益-预测偏差-考核费用")
        assert len(rows) == 1
        assert rows[0]["category"] == "penalty"
        assert rows[0]["amount_cny"] == pytest.approx(-42055.38)

    def test_sum_equals_printed_total(self, items):
        total = parse_stategrid_discharge_total(_load("stategrid_discharge_202503.txt"))
        assert total == pytest.approx(2068995.70)
        assert sum(it["amount_cny"] for it in items) == pytest.approx(total, abs=0.01)


class TestSdtcSettlement:
    """山东电力交易中心 2025-04: both sides' 合计 rows, watermark noise."""

    @pytest.fixture(scope="class")
    def parsed(self):
        return parse_sdtc_settlement(_load("sdtc_202504.txt"))

    def test_detection(self):
        assert is_sdtc_settlement(_load("sdtc_202504.txt")) is True
        assert is_sdtc_settlement(_load("stategrid_discharge_202604.txt")) is False

    def test_sell_side(self, parsed):
        sell = parsed["售电侧"]
        assert sell["category"] == "discharge_energy"
        assert sell["volume_mwh"] == pytest.approx(4535.379)
        assert sell["amount_cny"] == pytest.approx(2236438.28)
        assert sell["price_cny_kwh"] == pytest.approx(0.493110, abs=1e-6)

    def test_buy_side(self, parsed):
        buy = parsed["购电侧"]
        assert buy["category"] == "charge_energy"
        assert buy["volume_mwh"] == pytest.approx(5444.877)
        assert buy["amount_cny"] == pytest.approx(-266047.23)  # printed negative — kept as printed

    def test_gross_matches_daxie(self, parsed):
        # 大写金额 2,502,485.51 = 售电侧电费 + |购电侧电费|
        gross = parsed["售电侧"]["amount_cny"] + abs(parsed["购电侧"]["amount_cny"])
        assert gross == pytest.approx(2502485.51, abs=0.01)


class TestChargeWrapTail:
    """2025-05 下网: wrapped 优发优购 label ("…返还 / 85716.63 / 费用、阻塞费用")."""

    @pytest.fixture(scope="class")
    def items(self):
        return parse_stategrid_charge(_load("stategrid_charge_202505.txt"))

    def test_wrapped_label_full(self, items):
        # amount sits on the label line ("…返还 173817.89 88.16%。"), so the row
        # is keyed by the label prefix; category + amount are what matter
        rows = _by_notes(items, "优发优购曲线匹配偏差、发电侧返还")
        assert len(rows) == 1
        assert rows[0]["category"] == "penalty"
        assert rows[0]["amount_cny"] == pytest.approx(-173817.89)

    def test_negative_printed_energy_is_credit(self, items):
        # 直接交易电费 printed -149,873.02 (net credit month) → stored positive
        row = _by_notes(items, "直接交易电费")[0]
        assert row["amount_cny"] == pytest.approx(149873.02)

    def test_sum_equals_printed_total(self, items):
        total = parse_stategrid_charge_total(_load("stategrid_charge_202505.txt"))
        assert total == pytest.approx(241660.49)
        assert sum(it["amount_cny"] for it in items) == pytest.approx(-total, abs=0.01)
