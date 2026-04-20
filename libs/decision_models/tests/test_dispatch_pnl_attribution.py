"""
libs/decision_models/tests/test_dispatch_pnl_attribution.py

Unit tests for dispatch_pnl_attribution model.

No DB dependencies. All tests call run() with in-memory inputs.

Run:
    cd bess-platform
    pytest libs/decision_models/tests/test_dispatch_pnl_attribution.py -v
"""
from __future__ import annotations

from datetime import date

import pytest

import libs.decision_models.dispatch_pnl_attribution  # noqa: F401

from libs.decision_models.runners.local import run

_DATE = date(2026, 4, 1)
_ASSET = "suyou"

_FULL_INPUT = {
    "asset_code": _ASSET,
    "trade_date": _DATE,
    "pf_unrestricted_pnl": 100_000.0,
    "pf_grid_feasible_pnl": 90_000.0,
    "tt_forecast_optimal_pnl": 80_000.0,
    "tt_strategy_pnl": 75_000.0,
    "nominated_pnl": 70_000.0,
    "cleared_actual_pnl": 65_000.0,
}


class TestAttributionLadder:
    def test_full_ladder_losses(self):
        result = run("dispatch_pnl_attribution", _FULL_INPUT)
        assert result["grid_restriction_loss"] == pytest.approx(10_000.0)
        assert result["forecast_error_loss"] == pytest.approx(10_000.0)
        assert result["strategy_error_loss"] == pytest.approx(5_000.0)
        assert result["nomination_loss"] == pytest.approx(5_000.0)
        assert result["execution_clearing_loss"] == pytest.approx(5_000.0)

    def test_realisation_gaps(self):
        result = run("dispatch_pnl_attribution", _FULL_INPUT)
        assert result["realisation_gap_vs_pf"] == pytest.approx(35_000.0)
        assert result["realisation_gap_vs_pf_grid"] == pytest.approx(25_000.0)

    def test_scenarios_available_complete(self):
        result = run("dispatch_pnl_attribution", _FULL_INPUT)
        assert len(result["scenarios_available"]) == 6

    def test_echo_input_values(self):
        result = run("dispatch_pnl_attribution", _FULL_INPUT)
        assert result["asset_code"] == _ASSET
        assert result["pf_unrestricted_pnl"] == pytest.approx(100_000.0)
        assert result["cleared_actual_pnl"] == pytest.approx(65_000.0)


class TestPartialLadder:
    def test_missing_middle_scenario_yields_none_losses(self):
        inp = dict(_FULL_INPUT)
        inp["tt_forecast_optimal_pnl"] = None
        result = run("dispatch_pnl_attribution", inp)
        assert result["forecast_error_loss"] is None
        assert result["strategy_error_loss"] is None
        # Other losses unaffected
        assert result["grid_restriction_loss"] == pytest.approx(10_000.0)
        assert result["nomination_loss"] == pytest.approx(5_000.0)

    def test_only_pf_and_actual_provided(self):
        result = run("dispatch_pnl_attribution", {
            "asset_code": _ASSET,
            "trade_date": _DATE,
            "pf_unrestricted_pnl": 100_000.0,
            "cleared_actual_pnl": 65_000.0,
        })
        assert result["realisation_gap_vs_pf"] == pytest.approx(35_000.0)
        assert result["grid_restriction_loss"] is None
        assert result["scenarios_available"] == ["pf_unrestricted_pnl", "cleared_actual_pnl"]

    def test_no_scenarios_provided(self):
        result = run("dispatch_pnl_attribution", {
            "asset_code": _ASSET,
            "trade_date": _DATE,
        })
        assert result["realisation_gap_vs_pf"] is None
        assert result["scenarios_available"] == []


class TestEdgeCases:
    def test_zero_pnl_values(self):
        result = run("dispatch_pnl_attribution", {
            "asset_code": _ASSET,
            "trade_date": _DATE,
            "pf_unrestricted_pnl": 0.0,
            "cleared_actual_pnl": 0.0,
        })
        assert result["realisation_gap_vs_pf"] == pytest.approx(0.0)

    def test_negative_loss_allowed(self):
        """Cleared actual can exceed PF grid feasible (e.g. compensation uplift)."""
        result = run("dispatch_pnl_attribution", {
            "asset_code": _ASSET,
            "trade_date": _DATE,
            "pf_grid_feasible_pnl": 50_000.0,
            "cleared_actual_pnl": 55_000.0,
        })
        assert result["realisation_gap_vs_pf_grid"] == pytest.approx(-5_000.0)

    def test_output_is_serialisable(self):
        import json
        result = run("dispatch_pnl_attribution", _FULL_INPUT)
        json.dumps(result, default=str)  # must not raise
