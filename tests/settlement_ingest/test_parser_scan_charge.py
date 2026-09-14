"""Tests for the vision+verify scan parser's pure core
(build_items_from_page_records) — 分次结算 rules and the identity gate."""
import pytest

from services.settlement_ingest.parser_scan_charge import build_items_from_page_records

# Hand-verified page records from the 定远 2026-02 scanned sheets
FEB_P1 = {"format": "电费计算明细", "billing_month": "202602", "settlement_kind": "第1期",
          "total_cny": 1858632.83, "active_kwh": 5171106, "market_energy_cny": 1858751.51,
          "transmission_cny": 46.96, "peak_surcharge_cny": 0, "line_loss_cny": 0.00,
          "system_operation_cny": 24.66, "govt_fund_cny": 9.71, "power_factor_adj_cny": -200.01}
FEB_P2 = {"format": "电费计算明细", "billing_month": "202602", "settlement_kind": "终期",
          "total_cny": -482089.64, "active_kwh": 6700098, "market_energy_cny": -482169.51,
          "transmission_cny": 44.39, "peak_surcharge_cny": 0, "line_loss_cny": 3.20,
          "system_operation_cny": 24.07, "govt_fund_cny": 9.46, "power_factor_adj_cny": -1.25}
APR_MERGED = {"format": "电费计算明细", "billing_month": "202604", "settlement_kind": "合并",
              "total_cny": 1313169.10, "active_kwh": 8948874, "market_energy_cny": 1312976.96,
              "transmission_cny": 98.75, "peak_surcharge_cny": 0, "line_loss_cny": 4.30,
              "system_operation_cny": 70.27, "govt_fund_cny": 20.32, "power_factor_adj_cny": -1.50}
APR_ADVANCE = {"format": "电费计算明细", "billing_month": "202604", "settlement_kind": "第1期",
               "total_cny": 1535087.57, "active_kwh": 4455336, "market_energy_cny": 1534996.51,
               "transmission_cny": 47.81, "peak_surcharge_cny": 0, "line_loss_cny": 0.00,
               "system_operation_cny": 33.54, "govt_fund_cny": 9.71, "power_factor_adj_cny": 0.00}


def _by_notes(items, needle):
    return [it for it in items if needle in (it["notes"] or "")]


class TestAdvancePlusFinal:
    def test_items_and_signs(self):
        items = build_items_from_page_records([FEB_P1, FEB_P2])
        assert items is not None
        e1 = _by_notes(items, "市场化交易电费 [第1期]")[0]
        assert e1["category"] == "charge_energy"
        assert e1["amount_cny"] == pytest.approx(-1858751.51)  # printed positive cost → negative
        assert e1["volume_mwh"] == pytest.approx(5171.106)
        e2 = _by_notes(items, "市场化交易电费 [终期]")[0]
        assert e2["amount_cny"] == pytest.approx(482169.51)  # printed negative → stored credit
        pf = _by_notes(items, "力调电费 [第1期]")[0]
        assert pf["category"] == "basic_fee"
        assert pf["amount_cny"] == pytest.approx(200.01)  # printed -200.01 → +200.01

    def test_month_total(self):
        items = build_items_from_page_records([FEB_P1, FEB_P2])
        total = sum(it["amount_cny"] for it in items)
        assert total == pytest.approx(-(1858632.83) - (-482089.64), abs=0.01)
        vol = sum(it["volume_mwh"] or 0 for it in items if it["category"] == "charge_energy")
        assert vol == pytest.approx(11871.204)


class TestMergedSupersedesAdvance:
    def test_only_merged_used(self):
        items = build_items_from_page_records([APR_ADVANCE, APR_MERGED])
        assert items is not None
        assert not _by_notes(items, "[第1期]"), "advance sheet must be excluded when 合并 exists"
        assert _by_notes(items, "[合并]")
        total = sum(it["amount_cny"] for it in items)
        assert total == pytest.approx(-1313169.10, abs=0.01)


class TestRefusals:
    def test_reconciliation_failure_refused(self):
        bad = dict(FEB_P1, market_energy_cny=FEB_P1["market_energy_cny"] + 500.0)
        assert build_items_from_page_records([bad, FEB_P2]) is None

    def test_wrong_format_refused(self):
        assert build_items_from_page_records([{"format": None}]) is None

    def test_mixed_months_refused(self):
        other = dict(FEB_P1, billing_month="202603",
                     total_cny=FEB_P1["total_cny"])
        assert build_items_from_page_records([FEB_P1, other]) is None

    def test_empty_refused(self):
        assert build_items_from_page_records([]) is None

    def test_tolerance_boundary(self):
        # 0.005% drift passes; 0.05% fails
        ok = dict(FEB_P1, market_energy_cny=FEB_P1["market_energy_cny"] + 50.0)
        assert build_items_from_page_records([ok]) is not None
        bad = dict(FEB_P1, market_energy_cny=FEB_P1["market_energy_cny"] + 500.0)
        assert build_items_from_page_records([bad]) is None
