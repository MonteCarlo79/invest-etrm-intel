"""Tests for the W-B-2 (悦盛昌渠, 零碳46) settlement parsers."""
import pathlib

import pytest

from services.settlement_ingest.parser_wb2 import (
    is_wb2_charge_bill,
    is_wb2_discharge_bill,
    is_wb2_voucher,
    parse_wb2_charge,
    parse_wb2_charge_total,
    parse_wb2_discharge,
    parse_wb2_discharge_total,
)

FIX = pathlib.Path(__file__).parent / "fixtures"


def _load(name):
    return (FIX / name).read_text(encoding="utf-8")


def _by_notes(items, needle):
    return [it for it in items if needle in (it["notes"] or "")]


class TestWb2Discharge:
    @pytest.fixture(scope="class")
    def items(self):
        return parse_wb2_discharge(_load("wb2_discharge_202503.txt"))

    def test_detection(self):
        assert is_wb2_discharge_bill(_load("wb2_discharge_202503.txt")) is True
        assert is_wb2_discharge_bill(_load("wb2_charge_202503.txt")) is False

    def test_volumes_are_mwh_not_divided(self, items):
        # 现货交易 23,133.000 千千瓦时 = 23,133 MWh (vision had stored 23.133)
        row = _by_notes(items, "现货交易")[0]
        assert row["category"] == "discharge_energy"
        assert row["volume_mwh"] == pytest.approx(23133.0)
        assert row["price_cny_kwh"] == pytest.approx(0.10732)
        assert row["amount_cny"] == pytest.approx(2482702.09)

    def test_dated_labels(self, items):
        # "2024年12月保量保价 86,369.580 283.50 24,485,775.93" — year must not leak into vol
        rows = _by_notes(items, "保量保价")
        assert len(rows) == 6
        amounts = sorted(r["amount_cny"] for r in rows)
        assert amounts[0] == pytest.approx(662361.07)   # 2024-09 保量保价
        assert amounts[-1] == pytest.approx(26509795.88)  # 2025-01 保量保价
        vols = sorted(r["volume_mwh"] for r in rows)
        assert vols[0] == pytest.approx(2098.47)
        assert vols[-1] == pytest.approx(94813.29)

    def test_fee_rows(self, items):
        assert _by_notes(items, "市场平衡类费用")[0]["amount_cny"] == pytest.approx(642527.97)
        assert _by_notes(items, "不平衡资金")[0]["amount_cny"] == pytest.approx(131.91)
        freq = sum(i["amount_cny"] for i in _by_notes(items, "调频电费"))
        assert freq == pytest.approx(-65972.21 - 51291.87 - 49162.48 - 77654.41 - 32233.18 - 2944.39)

    def test_sum_equals_printed_total(self, items):
        total = parse_wb2_discharge_total(_load("wb2_discharge_202503.txt"))
        assert total == pytest.approx(107090449.12)
        assert sum(it["amount_cny"] for it in items) == pytest.approx(total, abs=0.01)
        vol = sum(it["volume_mwh"] or 0 for it in items if it["category"] == "discharge_energy")
        assert vol == pytest.approx(384586.62, abs=0.01)


class TestWb2Charge:
    @pytest.fixture(scope="class")
    def march(self):
        return parse_wb2_charge(_load("wb2_charge_202503.txt"))

    @pytest.fixture(scope="class")
    def july(self):
        return parse_wb2_charge(_load("wb2_charge_202507.txt"))

    def test_detection(self):
        assert is_wb2_charge_bill(_load("wb2_charge_202503.txt")) is True
        assert is_wb2_charge_bill(_load("wb2_discharge_202503.txt")) is False
        assert is_wb2_voucher(_load("wb2_charge_202503.txt")) is False

    def test_march_rows(self, march):
        assert _by_notes(march, "需量电费")[0]["amount_cny"] == pytest.approx(-45864.0)
        assert _by_notes(march, "力率电费")[0]["amount_cny"] == pytest.approx(-133480.83)
        assert _by_notes(march, "力率电费")[0]["category"] == "basic_fee"
        e = _by_notes(march, "电网代购购电电费")[0]
        assert e["category"] == "charge_energy"
        assert e["volume_mwh"] == pytest.approx(142.56)
        assert e["amount_cny"] == pytest.approx(-38895.35)

    def test_july_rows(self, july):
        assert _by_notes(july, "需量电费")[0]["amount_cny"] == pytest.approx(-48360.0)
        assert _by_notes(july, "力率电费")[0]["amount_cny"] == pytest.approx(-70555.08)
        assert _by_notes(july, "绿电费用")[0]["amount_cny"] == pytest.approx(-21.35)

    def test_sums_reconcile(self, march, july):
        assert sum(i["amount_cny"] for i in march) == pytest.approx(-231958.45, abs=0.01)
        assert sum(i["amount_cny"] for i in july) == pytest.approx(-120741.29, abs=0.01)

    def test_totals_match_bills(self, march, july):
        assert parse_wb2_charge_total(_load("wb2_charge_202503.txt")) == pytest.approx(231958.45)
        assert parse_wb2_charge_total(_load("wb2_charge_202507.txt")) == pytest.approx(120741.29)
