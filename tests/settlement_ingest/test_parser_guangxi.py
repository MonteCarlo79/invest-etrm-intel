"""Tests for the deterministic Guangxi settlement parser.

Fixtures are real extracted texts from 灵山 (B-5) bills. The strongest
assertions are cross-checks against the bills' own printed subtotals:
per-计量点小计 for charge bills, 合计 for discharge statements.
"""
import pathlib

import pytest

from services.settlement_ingest.parser_guangxi import (
    charge_page_subtotals,
    is_guangxi_charge_bill,
    is_guangxi_discharge_bill,
    is_guangxi_grid_duplicate,
    parse_guangxi_charge_text,
    parse_guangxi_discharge_text,
)

FIX = pathlib.Path(__file__).parent / "fixtures"


def _load(name: str) -> str:
    return (FIX / name).read_text(encoding="utf-8")


def _by_notes(items, needle):
    return [it for it in items if needle in (it["notes"] or "")]


class TestChargeBillDetection:
    def test_detects_charge_bill(self):
        assert is_guangxi_charge_bill(_load("guangxi_charge_202602.txt")) is True

    def test_rejects_discharge_statement(self):
        assert is_guangxi_charge_bill(_load("guangxi_discharge_202602.txt")) is False

    def test_grid_duplicate_detected(self):
        text = _load("guangxi_grid_dup_202504.txt")
        assert is_guangxi_grid_duplicate(text) is True
        assert is_guangxi_charge_bill(text) is False
        assert is_guangxi_discharge_bill(text) is False


class TestChargeBill202602:
    @pytest.fixture(scope="class")
    def items(self):
        return parse_guangxi_charge_text(_load("guangxi_charge_202602.txt"))

    def test_energy_row_exact(self, items):
        # vision read this row as -126701.63; the bill prints 126704.66
        row = _by_notes(items, "上网环节线损")[0]
        assert row["amount_cny"] == pytest.approx(-126704.66)
        assert row["category"] == "system_operation"
        assert row["volume_mwh"] == pytest.approx(7068.6)
        assert row["price_cny_kwh"] == pytest.approx(0.017925)

    def test_basic_fee_not_1000x(self, items):
        # 计费容量 100000 kVA × 21.4 元 = 2,140,000 — vision read 1000 × 21.4 = 21,400,000
        rows = _by_notes(items, "基本电费")
        amounts = sorted(r["amount_cny"] for r in rows)
        assert amounts == [pytest.approx(-2140000.00), pytest.approx(-2140000.00)]
        assert all(r["category"] == "basic_fee" for r in rows)

    def test_wrapped_label_row(self, items):
        # (7)新能源机制电价差价结算电费 — label wraps mid-word onto the numbers line
        rows = _by_notes(items, "新能源机制电价差价")
        assert len(rows) == 2
        assert rows[0]["amount_cny"] == pytest.approx(-455755.05)

    def test_column_block_13_to_16(self, items):
        # (16)退补电费 printed negative on both metering points → credit (+) here
        rows = _by_notes(items, "退补电费")
        assert len(rows) == 2
        by_mp = {r["notes"].split("计量点")[1][:4]: r["amount_cny"] for r in rows}
        assert by_mp["8255"] == pytest.approx(11280.09)
        assert by_mp["5589"] == pytest.approx(5070890.02)

    def test_two_metering_points(self, items):
        assert _by_notes(items, "计量点8255") and _by_notes(items, "计量点5589")

    def test_matches_printed_page_subtotals(self, items):
        # 计量点电费小计: MP1 5,371,851.26 (implied), MP2 418,750.51 (printed)
        subs = charge_page_subtotals(_load("guangxi_charge_202602.txt"))
        assert subs == [pytest.approx(5371851.26), pytest.approx(418750.51)]
        for mp, subtotal in (("8255", -5371851.26), ("5589", -418750.51)):
            total = sum(it["amount_cny"] for it in _by_notes(items, f"计量点{mp}"))
            assert total == pytest.approx(subtotal, abs=0.02), f"计量点{mp} parse does not match printed 小计"

    def test_matches_printed_bill_total(self, items):
        total = sum(it["amount_cny"] for it in items)
        assert total == pytest.approx(-5790601.77, abs=0.02)  # 电费合计 header

    def test_zero_rows_skipped(self, items):
        # (13)(14)(15) are all 0.00 on both pages — must not appear
        assert not _by_notes(items, "市场化分摊总费用")
        assert not _by_notes(items, "交易考核")


class TestChargeBill202504:
    """2025 row numbering: energy rows to (12), blocks (13)(14)/(15)-18 —
    exercises number-shifted block rows, interleaved wrapped rows, and a
    NEGATIVE 计量点小计 (net credit month on one metering point)."""

    @pytest.fixture(scope="class")
    def items(self):
        return parse_guangxi_charge_text(_load("guangxi_charge_202504.txt"))

    def test_tou_split_rows(self, items):
        row = _by_notes(items, "电能电费(峰)")[0]
        assert row["amount_cny"] == pytest.approx(-199508.43)
        assert row["category"] == "charge_energy"
        assert row["volume_mwh"] == pytest.approx(349.8)

    def test_basic_fee_full_amount(self, items):
        # (13)基本电费 in the 2025 numbering — 100000 kVA × 21.4 = 2,140,000
        rows = _by_notes(items, "基本电费")
        assert sorted(r["amount_cny"] for r in rows) == [pytest.approx(-2140000.0), pytest.approx(-2140000.0)]

    def test_interleaved_wrapped_row(self, items):
        # "220千伏以下 (10)交易电量分摊居民农业价" + numbers on next line, negative price
        rows = _by_notes(items, "交易电量分摊居民农业价")
        assert len(rows) == 2
        assert rows[0]["amount_cny"] == pytest.approx(74016.32)  # printed -74016.32 → credit

    def test_matches_printed_page_subtotals(self, items):
        subs = charge_page_subtotals(_load("guangxi_charge_202504.txt"))
        assert subs == [pytest.approx(-57378.19), pytest.approx(5024009.03)]
        for mp, expected in (("5589", 57378.19), ("8255", -5024009.03)):
            total = sum(it["amount_cny"] for it in _by_notes(items, f"计量点{mp}"))
            assert total == pytest.approx(expected, abs=0.02), f"计量点{mp} mismatch"

    def test_matches_printed_bill_total(self, items):
        assert sum(it["amount_cny"] for it in items) == pytest.approx(-4966630.84, abs=0.02)


class TestChargeBill202606:
    """2026-06: (11)其他费用合计 numbered >10, negative-price rows, 退补 credits."""

    @pytest.fixture(scope="class")
    def items(self):
        return parse_guangxi_charge_text(_load("guangxi_charge_202606.txt"))

    def test_energy_rows_numbered_above_10_kept(self, items):
        rows = _by_notes(items, "其他费用合计")
        assert len(rows) == 2  # (11) on both metering points
        assert rows[0]["category"] == "other"

    def test_matches_printed_page_subtotals(self, items):
        subs = charge_page_subtotals(_load("guangxi_charge_202606.txt"))
        assert subs == [pytest.approx(504030.24), pytest.approx(5459904.28)]
        for mp, expected in (("5589", -504030.24), ("8255", -5459904.28)):
            total = sum(it["amount_cny"] for it in _by_notes(items, f"计量点{mp}"))
            assert total == pytest.approx(expected, abs=0.02), f"计量点{mp} mismatch"

    def test_matches_printed_bill_total(self, items):
        assert sum(it["amount_cny"] for it in items) == pytest.approx(-5963934.52, abs=0.02)


class TestChargeBill202406:
    """2024-06: commissioning partial month — prorated 基本电费, and a
    2-COLUMN shared block (14)(15) whose 退补 credit (¥2.27M) the parser
    originally dropped."""

    @pytest.fixture(scope="class")
    def items(self):
        return parse_guangxi_charge_text(_load("guangxi_charge_202406.txt"))

    def test_two_column_block_parsed(self, items):
        rows = _by_notes(items, "退补电费")
        assert len(rows) == 1
        assert rows[0]["amount_cny"] == pytest.approx(2268006.64)  # printed -2268006.64 → credit

    def test_prorated_basic_fee(self, items):
        # 63,333.33 kVA × 21.4 = 1,355,333.26 (19/30 days of a 100 MVA month)
        rows = _by_notes(items, "基本电费")
        assert sorted(r["amount_cny"] for r in rows) == [pytest.approx(-2140000.0), pytest.approx(-1355333.26)]

    def test_matches_printed_page_subtotals(self, items):
        subs = charge_page_subtotals(_load("guangxi_charge_202406.txt"))
        assert subs == [pytest.approx(1629271.00), pytest.approx(338489.86)]
        for mp, expected in (("8255", -1629271.00), ("5589", -338489.86)):
            total = sum(it["amount_cny"] for it in _by_notes(items, f"计量点{mp}"))
            assert total == pytest.approx(expected, abs=0.02), f"计量点{mp} mismatch"

    def test_matches_printed_bill_total(self, items):
        assert sum(it["amount_cny"] for it in items) == pytest.approx(-1967760.86, abs=0.02)


class TestDischargeStatement:
    def test_detects_discharge(self):
        assert is_guangxi_discharge_bill(_load("guangxi_discharge_202602.txt")) is True
        assert is_guangxi_discharge_bill(_load("guangxi_charge_202602.txt")) is False

    def test_top_level_rows_only(self):
        # 1.1/1.2/2.1..2.9 decompose rows 1/2 — must not be ingested (double-count)
        items = parse_guangxi_discharge_text(_load("guangxi_discharge_202602.txt"))
        assert len(items) == 2
        energy, rebate = items
        assert energy["category"] == "discharge_energy"
        assert energy["volume_mwh"] == pytest.approx(12203.4)
        assert energy["price_cny_kwh"] == pytest.approx(0.326954)
        assert energy["amount_cny"] == pytest.approx(3989952.37)
        assert rebate["category"] == "rebate"
        assert rebate["amount_cny"] == pytest.approx(-56013.72)
        assert sum(it["amount_cny"] for it in items) == pytest.approx(3933938.65)  # printed 合计

    def test_april_with_adjustment_section(self):
        items = parse_guangxi_discharge_text(_load("guangxi_discharge_202504.txt"))
        assert len(items) == 3
        cats = [it["category"] for it in items]
        assert cats == ["discharge_energy", "rebate", "other"]  # 结算调整项目 → other
        assert sum(it["amount_cny"] for it in items) == pytest.approx(4362352.85)  # printed 合计
