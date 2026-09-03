"""Tests for services.mengxi_nodal.pf_results aggregation."""
import pandas as pd
import pytest

from services.mengxi_nodal.pf_results import aggregate_pf


def _rows():
    return pd.DataFrame([
        # node, data_date, revenue_cny
        ("n1", "2026-08-01", 10000.0),
        ("n1", "2026-08-02", 20000.0),
        ("n1", "2026-09-01", 30000.0),
        ("n2", "2026-08-01", 50000.0),
        ("n2", "2026-08-02", 10000.0),
        ("n3", "2026-09-01", 5000.0),
    ], columns=["node_name", "data_date", "revenue_cny"])


class TestAggregatePf:
    def test_totals_ranked_by_rev_per_mw(self):
        totals, _ = aggregate_pf(_rows(), power_mw=100)
        assert list(totals["node_name"]) == ["n1", "n2", "n3"]  # 600/600/50 per MW... n1=600, n2=600, n3=50
        assert totals.iloc[0]["rev_per_mw"] == pytest.approx(600.0)
        assert totals.iloc[0]["rank"] == 1

    def test_rev_per_mw_scales_by_power(self):
        totals, _ = aggregate_pf(_rows(), power_mw=100)
        # n2 total 60000 over 100MW -> 600 per MW
        n2 = totals[totals["node_name"] == "n2"].iloc[0]
        assert n2["total_profit_cny"] == pytest.approx(60000.0)
        assert n2["rev_per_mw"] == pytest.approx(600.0)

    def test_monthly_breakdown_per_node(self):
        _, monthly = aggregate_pf(_rows(), power_mw=100)
        # monthly df: index=node, columns=YYYY-MM
        assert monthly.loc["n1", "2026-08"] == pytest.approx(300.0)
        assert monthly.loc["n1", "2026-09"] == pytest.approx(300.0)
        assert monthly.loc["n2", "2026-08"] == pytest.approx(600.0)
        assert monthly.loc["n3", "2026-09"] == pytest.approx(50.0)

    def test_missing_months_are_zero_filled(self):
        _, monthly = aggregate_pf(_rows(), power_mw=100)
        assert monthly.loc["n2", "2026-09"] == pytest.approx(0.0)
