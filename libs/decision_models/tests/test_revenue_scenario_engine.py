"""
libs/decision_models/tests/test_revenue_scenario_engine.py

Unit tests for the revenue_scenario_engine model asset.
No DB or external dependencies — pure logic tests.
"""
import dataclasses
from datetime import date

import pytest

import libs.decision_models.revenue_scenario_engine  # registers model
from libs.decision_models.runners.local import run


TRADE_DATE = date(2026, 4, 1)
FLAT_PRICE = [50.0] * 96          # 50 yuan/MWh flat all day
DISCHARGE_PROFILE = [100.0] * 48 + [0.0] * 48    # discharge first half
CHARGE_PROFILE = [-100.0] * 48 + [0.0] * 48       # charge first half (negative = charge)


def _run_engine(scenario_dispatch, compensation=350.0):
    return run("revenue_scenario_engine", {
        "asset_code": "suyou",
        "trade_date": TRADE_DATE,
        "actual_price": FLAT_PRICE,
        "scenario_dispatch": scenario_dispatch,
        "compensation_yuan_per_mwh": compensation,
    })


class TestRevenueScenarioEngine:
    def test_discharge_revenue(self):
        result = _run_engine({"cleared_actual": DISCHARGE_PROFILE})
        scenario = result["scenarios"][0]
        # 48 intervals * 0.25h * 100MW * 50 yuan/MWh = 60,000
        assert abs(scenario["market_revenue_yuan"] - 60_000.0) < 1.0

    def test_compensation_revenue(self):
        result = _run_engine({"cleared_actual": DISCHARGE_PROFILE}, compensation=350.0)
        scenario = result["scenarios"][0]
        discharge_mwh = 48 * 0.25 * 100.0  # 1200 MWh
        expected_comp = discharge_mwh * 350.0
        assert abs(scenario["compensation_revenue_yuan"] - expected_comp) < 1.0

    def test_attribution_ladder_partial(self):
        result = _run_engine({
            "perfect_foresight_unrestricted": DISCHARGE_PROFILE,
            "cleared_actual": CHARGE_PROFILE,
        })
        # ladder requires pf_grid_feasible between the two — should be None
        assert result["grid_restriction_loss"] is None

    def test_attribution_ladder_full(self):
        pf_unres = [100.0] * 96
        pf_grid = [90.0] * 96
        actual = [80.0] * 96
        result = _run_engine({
            "perfect_foresight_unrestricted": pf_unres,
            "perfect_foresight_grid_feasible": pf_grid,
            "cleared_actual": actual,
        })
        # All revenues use flat 50 yuan price
        assert result["grid_restriction_loss"] is not None
        assert result["grid_restriction_loss"] > 0

    def test_empty_scenarios(self):
        result = _run_engine({})
        assert result["scenarios"] == []
