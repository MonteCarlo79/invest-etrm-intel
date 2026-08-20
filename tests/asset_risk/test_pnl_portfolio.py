"""Tests for the Realised P&L portfolio view helpers in tab_pnl.py."""
import pandas as pd
import pytest

from apps.asset_risk.tab_pnl import _portfolio_matrix, _asset_summary


def _sample_items():
    """Two assets x two months of settlement items."""
    rows = [
        # asset A 2026-01: discharge 100, charge -60, capcomp 10
        {"asset": "A", "settlement_month": "2026-01-01", "category": "discharge_energy", "amount_cny": 100.0, "volume_mwh": 10.0},
        {"asset": "A", "settlement_month": "2026-01-01", "category": "charge_energy", "amount_cny": -60.0, "volume_mwh": 11.0},
        {"asset": "A", "settlement_month": "2026-01-01", "category": "capacity_compensation", "amount_cny": 10.0, "volume_mwh": 0.0},
        # asset A 2026-02: discharge 120, charge -70
        {"asset": "A", "settlement_month": "2026-02-01", "category": "discharge_energy", "amount_cny": 120.0, "volume_mwh": 12.0},
        {"asset": "A", "settlement_month": "2026-02-01", "category": "charge_energy", "amount_cny": -70.0, "volume_mwh": 13.0},
        # asset B 2026-01 only: discharge 50, charge -80 (loss month)
        {"asset": "B", "settlement_month": "2026-01-01", "category": "discharge_energy", "amount_cny": 50.0, "volume_mwh": 5.0},
        {"asset": "B", "settlement_month": "2026-01-01", "category": "charge_energy", "amount_cny": -80.0, "volume_mwh": 6.0},
    ]
    return pd.DataFrame(rows)


class TestPortfolioMatrix:
    def test_shape_and_labels(self):
        mat = _portfolio_matrix(_sample_items())
        assert "组合合计" in mat.columns
        assert "资产合计" in mat.index
        assert list(mat.index[:-1]) == ["2026-01", "2026-02"]

    def test_cell_values(self):
        mat = _portfolio_matrix(_sample_items())
        # A 2026-01: 100 - 60 + 10 = 50
        assert mat.loc["2026-01", "A"] == pytest.approx(50.0)
        # B 2026-01: 50 - 80 = -30
        assert mat.loc["2026-01", "B"] == pytest.approx(-30.0)
        # B has no 2026-02 -> 0 fill
        assert mat.loc["2026-02", "B"] == 0.0

    def test_totals(self):
        mat = _portfolio_matrix(_sample_items())
        # 组合合计 2026-01 = 50 + (-30) = 20
        assert mat.loc["2026-01", "组合合计"] == pytest.approx(20.0)
        # 资产合计 A = 50 + 50 = 100
        assert mat.loc["资产合计", "A"] == pytest.approx(100.0)
        # grand total = 20 + 50 = 70
        assert mat.loc["资产合计", "组合合计"] == pytest.approx(70.0)


class TestAssetSummary:
    def test_per_asset_values(self):
        s = _asset_summary(_sample_items()).set_index("asset")
        assert s.loc["A", "net_profit"] == pytest.approx(100.0)
        assert s.loc["A", "discharge_mwh"] == pytest.approx(22.0)
        assert s.loc["A", "charge_mwh"] == pytest.approx(24.0)
        # A arb_income = (100 + 120) + (-60 - 70) = 90; spread = 90 / 22
        assert s.loc["A", "arb_income"] == pytest.approx(90.0)
        assert s.loc["A", "arb_spread"] == pytest.approx(90.0 / 22.0)

    def test_zero_discharge_spread_is_none(self):
        df = pd.DataFrame([
            {"asset": "C", "settlement_month": "2026-01-01", "category": "charge_energy",
             "amount_cny": -50.0, "volume_mwh": 5.0},
        ])
        s = _asset_summary(df).set_index("asset")
        assert s.loc["C", "arb_spread"] is None or pd.isna(s.loc["C", "arb_spread"])


class TestCyclesPerDay:
    def test_cycles_uses_capacity_and_calendar_days(self):
        """cycles_per_day = charge_mwh / (capacity x duration) / days-in-months-present."""
        rows = []
        # Asset D: 100MW x 4h = 400 MWh/cycle; charge 23,600 MWh across Jan+Feb 2026 (59 days)
        for month, vol in [("2026-01-01", 12000.0), ("2026-02-01", 11600.0)]:
            rows.append({"asset": "D", "capacity_mw": 100.0, "bess_duration_h": 4.0,
                         "settlement_month": month, "category": "charge_energy",
                         "amount_cny": -1.0, "volume_mwh": vol})
        s = _asset_summary(pd.DataFrame(rows)).set_index("asset")
        assert s.loc["D", "cycles_per_day"] == pytest.approx(23600.0 / 400.0 / 59.0, rel=1e-3)

    def test_cycles_none_without_capacity(self):
        """No capacity columns -> cycles_per_day is None, other fields unaffected."""
        s = _asset_summary(_sample_items()).set_index("asset")
        assert s.loc["A", "cycles_per_day"] is None or pd.isna(s.loc["A", "cycles_per_day"])
        assert s.loc["A", "net_profit"] == pytest.approx(100.0)
