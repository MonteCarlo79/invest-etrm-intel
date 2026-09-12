"""Tests for services.interconnector.model — S5 forecast model."""
from datetime import date

import pytest

from services.interconnector import model


class TestMonthlyAgreementVolume:
    def test_sums_matching_pairs_divided_by_12(self):
        ag = [dict(send_prov="青海", recv_prov="上海", annual_gwh=4000.0),
              dict(send_prov="青海", recv_prov="江苏", annual_gwh=1200.0),
              dict(send_prov="山西", recv_prov="上海", annual_gwh=5000.0)]
        assert model.monthly_agreement_volume(ag, "青海") == pytest.approx((4000+1200)/12, rel=1e-2)
        assert model.monthly_agreement_volume(ag, "青海", "上海") == pytest.approx(4000/12, rel=1e-2)
        assert model.monthly_agreement_volume(ag, "甘肃") == 0.0


class TestOpportunisticVolume:
    def test_zero_when_spread_negative_or_missing(self):
        assert model.opportunistic_volume(-10, 500) == 0.0
        assert model.opportunistic_volume(None, 500) == 0.0

    def test_scales_to_historical_cap_at_100_spread(self):
        assert model.opportunistic_volume(50, 500) == pytest.approx(250.0)
        assert model.opportunistic_volume(100, 500) == pytest.approx(500.0)
        assert model.opportunistic_volume(200, 500) == pytest.approx(500.0)   # capped

    def test_sensitivity_scales(self):
        assert model.opportunistic_volume(50, 500, sensitivity=2.0) == pytest.approx(500.0)
        assert model.opportunistic_volume(50, 500, sensitivity=0.0) == 0.0


class TestExporterForecast:
    def test_baseline_is_max_of_agreement_and_history(self):
        out = model.exporter_forecast(333.3, 500.0, 100.0, 250.0, None, None)
        assert out["baseline"] == 500.0 and out["total"] == pytest.approx(850.0)
        out2 = model.exporter_forecast(333.3, 100.0, 100.0, 250.0, None, None)
        assert out2["baseline"] == pytest.approx(333.3) and out2["total"] == pytest.approx(683.3)

    def test_no_double_count_agreement_and_history(self):
        out = model.exporter_forecast(333.3, 300.0, 0.0, 0.0, None, None)
        assert out["total"] == pytest.approx(333.3)

    def test_channel_cap_binds_first_then_history(self):
        out = model.exporter_forecast(333.3, 0.0, 100.0, 500.0, channel_cap=800.0, hist_max=900.0)
        assert out["total"] == 800.0 and out["capped_by"] == "channel"
        out2 = model.exporter_forecast(333.3, 0.0, 100.0, 500.0, channel_cap=None, hist_max=700.0)
        assert out2["total"] == pytest.approx(840.0) and out2["capped_by"] == "history"


class TestImporterStack:
    def test_fills_cheapest_first_and_reports_marginal(self):
        cands = [dict(send="新疆", cost=300.0, volume=400.0),
                 dict(send="甘肃", cost=250.0, volume=200.0),
                 dict(send="山西", cost=320.0, volume=500.0)]
        out = model.importer_stack(cands, demand_gwh=550.0)
        alloc = {a["send"]: a["allocated"] for a in out["stack"]}
        assert alloc == {"甘肃": 200.0, "新疆": 350.0, "山西": 0.0}
        assert out["marginal_price"] == 300.0
        assert out["inflow"] == 550.0 and out["unfilled"] == 0.0

    def test_unfilled_demand_and_none_cost_last(self):
        cands = [dict(send="青海", cost=None, volume=300.0),
                 dict(send="甘肃", cost=250.0, volume=100.0)]
        out = model.importer_stack(cands, demand_gwh=500.0)
        assert out["unfilled"] == 100.0
        assert out["stack"][-1]["send"] == "青海"  # None cost sorts last
        assert out["marginal_price"] is None or out["marginal_price"] == 250.0


class TestSpreadRegression:
    def test_insufficient_history(self):
        assert model.spread_regression([(10, 100), (20, 200)])["ok"] is False

    def test_recovers_linear_relation(self):
        pts = [(float(s), 50.0 + 2.0*s) for s in (10, 20, 30, 40, 50)]
        out = model.spread_regression(pts)
        assert out["ok"] and out["slope"] == pytest.approx(2.0, abs=0.01)
        assert out["intercept"] == pytest.approx(50.0, abs=0.5)
        assert out["r2"] == pytest.approx(1.0)
        assert out["forecast"](60) == pytest.approx(170.0)

    def test_forecast_floors_at_zero(self):
        pts = [(float(s), 10.0 - 0.5*s) for s in (0, 5, 10, 15, 20)]
        out = model.spread_regression(pts)
        assert out["forecast"](100) == 0.0


class TestMarketRenewable:
    def test_mechanism_share_deducted(self):
        assert model.market_renewable(1000.0, 62.5) == pytest.approx(375.0)
        assert model.market_renewable(1000.0, 8.0) == pytest.approx(920.0)
        assert model.market_renewable(1000.0, 0.0) == pytest.approx(1000.0)

    def test_none_share_means_no_deduction(self):
        assert model.market_renewable(1000.0, None) == pytest.approx(1000.0)
