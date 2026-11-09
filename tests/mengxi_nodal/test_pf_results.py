"""Tests for services.mengxi_nodal.pf_results aggregation.

Contract (517305ba + fdbde460): nodes rank by ¥ per MWh discharged, and a
node needs MIN_COVERAGE_DAYS covered days in the window to rank at all.
"""
from datetime import date, timedelta

import pandas as pd
import pytest

from services.mengxi_nodal.pf_results import MIN_COVERAGE_DAYS, aggregate_pf


def _node(name: str, start: date, days: int, rev: float, mwh: float) -> list:
    return [(name, (start + timedelta(days=i)).isoformat(), rev, mwh)
            for i in range(days)]


def _rows():
    aug1, sep1 = date(2026, 8, 1), date(2026, 9, 1)
    rows = (
        # n1: 31 Aug days at 200 ¥/MWh + 5 Sep days at 400 ¥/MWh
        _node("n1", aug1, 31, 20000.0, 100.0) + _node("n1", sep1, 5, 40000.0, 100.0)
        # n2: 31 Aug days at 300 ¥/MWh, no September rows
        + _node("n2", aug1, 31, 30000.0, 100.0)
        # n3: one day short of the coverage floor, very high ¥/MWh
        + _node("n3", aug1, MIN_COVERAGE_DAYS - 1, 90000.0, 100.0)
    )
    return pd.DataFrame(rows, columns=["node_name", "data_date",
                                       "revenue_cny", "discharge_mwh"])


class TestAggregatePf:
    def test_totals_ranked_by_rev_per_mwh(self):
        totals, _ = aggregate_pf(_rows(), power_mw=100)
        # n2 = 300 ¥/MWh; n1 = (620k + 200k) / 3600 MWh ≈ 227.8 ¥/MWh
        assert list(totals["node_name"]) == ["n2", "n1"]
        assert totals.iloc[0]["rev_per_mwh"] == pytest.approx(300.0)
        assert totals.iloc[1]["rev_per_mwh"] == pytest.approx(820000.0 / 3600.0)
        assert list(totals["rank"]) == [1, 2]

    def test_coverage_floor_excludes_short_nodes(self):
        totals, monthly = aggregate_pf(_rows(), power_mw=100)
        assert "n3" not in set(totals["node_name"])
        assert "n3" in monthly.index  # monthly breakdown is not floored

    def test_rev_per_mw_scales_by_power(self):
        totals, _ = aggregate_pf(_rows(), power_mw=100)
        n2 = totals[totals["node_name"] == "n2"].iloc[0]
        assert n2["total_profit_cny"] == pytest.approx(930000.0)
        assert n2["rev_per_mw"] == pytest.approx(9300.0)

    def test_monthly_breakdown_per_node(self):
        _, monthly = aggregate_pf(_rows(), power_mw=100)
        assert monthly.loc["n1", "2026-08"] == pytest.approx(200.0)
        assert monthly.loc["n1", "2026-09"] == pytest.approx(400.0)
        assert monthly.loc["n2", "2026-08"] == pytest.approx(300.0)

    def test_missing_months_are_zero_filled(self):
        _, monthly = aggregate_pf(_rows(), power_mw=100)
        assert monthly.loc["n2", "2026-09"] == pytest.approx(0.0)

    def test_empty_frame(self):
        totals, monthly = aggregate_pf(
            pd.DataFrame(columns=["node_name", "data_date",
                                  "revenue_cny", "discharge_mwh"]), power_mw=100)
        assert totals.empty and monthly.empty
