"""Tests for the State Grid (网上国网) settlement parser — 山东 滨州 fixtures.

Every parse is asserted against the bill's own printed total (合计/结算金额)
and against the independent arithmetic: components must decompose the total.
"""
import pathlib

import pytest

from services.settlement_ingest.parser_stategrid import (
    is_stategrid_charge,
    is_stategrid_discharge,
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


class TestDetection:
    def test_discharge_detected(self):
        assert is_stategrid_discharge(_load("stategrid_discharge_202604.txt")) is True

    def test_charge_detected(self):
        assert is_stategrid_charge(_load("stategrid_charge_202604.txt")) is True

    def test_cross_not_detected(self):
        assert is_stategrid_charge(_load("stategrid_discharge_202604.txt")) is False
        assert is_stategrid_discharge(_load("stategrid_charge_202604.txt")) is False


class TestDischargeBill:
    @pytest.fixture(scope="class")
    def items(self):
        return parse_stategrid_discharge(_load("stategrid_discharge_202604.txt"))

    def test_total_row_dropped_components_kept(self, items):
        # 上网电费 (2,460,586.29) and 合计 must NOT appear as items
        assert not _by_notes(items, "上网电费")
        assert not _by_notes(items, "合计")
        assert len(items) == 6

    def test_rt_deviation_is_discharge_energy(self, items):
        row = _by_notes(items, "实时偏差电费")[0]
        assert row["category"] == "discharge_energy"
        assert row["volume_mwh"] == pytest.approx(4727.184)
        assert row["price_cny_kwh"] == pytest.approx(0.449004)
        assert row["amount_cny"] == pytest.approx(2122525.83)

    def test_component_categories(self, items):
        assert _by_notes(items, "两个细则费用")[0]["category"] == "frequency"
        assert _by_notes(items, "调频服务补偿费用")[0]["category"] == "frequency"
        assert _by_notes(items, "容量补偿费")[0]["category"] == "capacity_compensation"
        assert _by_notes(items, "跨月偏差退补费")[0]["category"] == "rebate"
        assert _by_notes(items, "运行成本补偿费用")[0]["category"] == "other"

    def test_sum_equals_printed_total(self, items):
        total = parse_stategrid_discharge_total(_load("stategrid_discharge_202604.txt"))
        assert total == pytest.approx(2460586.29)
        assert sum(it["amount_cny"] for it in items) == pytest.approx(total, abs=0.01)


class TestChargeBill:
    @pytest.fixture(scope="class")
    def items(self):
        return parse_stategrid_charge(_load("stategrid_charge_202604.txt"))

    def test_subtotals_dropped_leaves_kept(self, items):
        # 工商业电费/①市场化购电电费/③输配电费/④系统运行费/⑤政府性基金及附加/合计 dropped
        for gone in ("工商业电费", "市场化购电电费", "输配电费 ", "系统运行费 ", "合计"):
            assert not _by_notes(items, gone), f"subtotal leaked: {gone}"

    def test_group2_and_group5_are_leaves(self, items):
        # ②上网环节线损费用 and ⑤政府性基金及附加 are the leaf rows themselves
        assert _by_notes(items, "上网环节线损费用")[0]["amount_cny"] == pytest.approx(-17093.75)
        assert _by_notes(items, "政府性基金及附加")[0]["amount_cny"] == pytest.approx(-20943.27)

    def test_wrapped_label_row(self, items):
        rows = _by_notes(items, "新能源可持续发展价格结算机制差价结算费用")
        assert len(rows) == 1
        assert rows[0]["amount_cny"] == pytest.approx(-146422.72)
        assert rows[0]["category"] == "other"

    def test_sign_and_categories(self, items):
        assert _by_notes(items, "直接交易电费")[0] == {
            "category": "charge_energy", "volume_mwh": None, "price_cny_kwh": None,
            "amount_cny": pytest.approx(-17452.62), "notes": "充电结算: 直接交易电费"}
        assert _by_notes(items, "电量电费")[0]["category"] == "transmission"
        assert _by_notes(items, "煤电容量电费")[0]["category"] == "coal_capacity_charge"
        assert _by_notes(items, "电价交叉补贴新增损益")[0]["category"] == "subsidy"

    def test_zero_rows_skipped(self, items):
        assert not _by_notes(items, "功率因数调整电费")  # 0.00 → dropped

    def test_sum_equals_printed_total(self, items):
        total = parse_stategrid_charge_total(_load("stategrid_charge_202604.txt"))
        assert total == pytest.approx(382899.41)
        assert sum(it["amount_cny"] for it in items) == pytest.approx(-total, abs=0.01)
