"""
services/monitoring/tests/test_run_daily_attribution.py

Unit tests for run_daily_attribution.py covering:
  B4  — Attribution identity check (pure function, no DB)
"""
from __future__ import annotations

from datetime import date

import pytest

from services.monitoring.run_daily_attribution import (
    _IDENTITY_TOLERANCE,
    _check_attribution_identity,
)

_DATE = date(2026, 4, 18)
_ASSET = "suyou"


def _result(
    grid=0.0,
    forecast=0.0,
    strategy=0.0,
    nomination=0.0,
    execution=0.0,
    gap=None,
):
    """Build a minimal attribution result dict."""
    total = grid + forecast + strategy + nomination + execution
    return {
        "grid_restriction_loss": grid,
        "forecast_error_loss": forecast,
        "strategy_error_loss": strategy,
        "nomination_loss": nomination,
        "execution_clearing_loss": execution,
        "realisation_gap_vs_pf_grid": gap if gap is not None else total,
    }


class TestCheckAttributionIdentity:

    def test_exact_match_passes(self):
        result = _result(grid=100.0, forecast=200.0, strategy=50.0, nomination=30.0, execution=20.0)
        assert _check_attribution_identity(result, _DATE, _ASSET) is None

    def test_within_tolerance_passes(self):
        result = _result(
            grid=100.0, forecast=200.0, strategy=50.0, nomination=30.0, execution=20.0,
            gap=400.0 + _IDENTITY_TOLERANCE * 0.5,  # half a Yuan off
        )
        assert _check_attribution_identity(result, _DATE, _ASSET) is None

    def test_at_tolerance_boundary_passes(self):
        result = _result(
            grid=100.0, forecast=200.0, strategy=50.0, nomination=30.0, execution=20.0,
            gap=400.0 + _IDENTITY_TOLERANCE,  # exactly at tolerance — passes (not >)
        )
        assert _check_attribution_identity(result, _DATE, _ASSET) is None

    def test_just_above_tolerance_fails(self):
        result = _result(
            grid=100.0, forecast=200.0, strategy=50.0, nomination=30.0, execution=20.0,
            gap=400.0 + _IDENTITY_TOLERANCE + 0.01,
        )
        err = _check_attribution_identity(result, _DATE, _ASSET)
        assert err is not None
        assert _ASSET in err
        assert str(_DATE) in err
        assert "discrepancy" in err.lower()

    def test_large_discrepancy_fails(self):
        result = _result(
            grid=100.0, forecast=200.0, strategy=50.0, nomination=30.0, execution=20.0,
            gap=500.0,  # 100 Yuan off
        )
        err = _check_attribution_identity(result, _DATE, _ASSET)
        assert err is not None

    def test_missing_loss_field_skips_check(self):
        result = {
            "grid_restriction_loss": 100.0,
            "forecast_error_loss": None,  # partial ladder
            "strategy_error_loss": 50.0,
            "nomination_loss": 30.0,
            "execution_clearing_loss": 20.0,
            "realisation_gap_vs_pf_grid": 200.0,
        }
        assert _check_attribution_identity(result, _DATE, _ASSET) is None

    def test_missing_gap_field_skips_check(self):
        result = {
            "grid_restriction_loss": 100.0,
            "forecast_error_loss": 200.0,
            "strategy_error_loss": 50.0,
            "nomination_loss": 30.0,
            "execution_clearing_loss": 20.0,
            "realisation_gap_vs_pf_grid": None,
        }
        assert _check_attribution_identity(result, _DATE, _ASSET) is None

    def test_all_zeros_passes(self):
        result = _result(grid=0.0, forecast=0.0, strategy=0.0, nomination=0.0, execution=0.0, gap=0.0)
        assert _check_attribution_identity(result, _DATE, _ASSET) is None

    def test_negative_values_still_checked(self):
        # Losses can be negative (e.g. execution outperforms benchmark)
        result = _result(
            grid=-50.0, forecast=200.0, strategy=50.0, nomination=30.0, execution=20.0,
            gap=250.0,  # correct: -50+200+50+30+20=250
        )
        assert _check_attribution_identity(result, _DATE, _ASSET) is None

    def test_negative_values_fail_on_mismatch(self):
        result = _result(
            grid=-50.0, forecast=200.0, strategy=50.0, nomination=30.0, execution=20.0,
            gap=300.0,  # wrong: should be 250
        )
        err = _check_attribution_identity(result, _DATE, _ASSET)
        assert err is not None
