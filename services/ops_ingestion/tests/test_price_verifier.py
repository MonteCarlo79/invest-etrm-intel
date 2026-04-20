"""
tests/test_price_verifier.py

Unit tests for services/ops_ingestion/inner_mongolia/price_verifier.py

Tests do not require a DB connection — they exercise the calculation functions
and level assignment directly.

Covers:
  - MAE calculation
  - Pearson r calculation
  - Level assignment: 'high' when MAE<5 and n>=80
  - Level assignment: 'medium' when 5<=MAE<20 and n>=80
  - Level assignment: 'low' when MAE>=20 and n>=80
  - Level assignment: 'unverified' when n<80
  - price_verification_notes is non-empty
  - Graceful empty DB (unverified with reason)
  - verify_prices_no_db always returns 'unverified'
"""
from __future__ import annotations

import sys
import os
import math

import pytest

sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')))

from inner_mongolia.price_verifier import (
    _mean_absolute_error,
    _pearson_r,
    _compute_level,
    _build_notes,
    _unverified,
    verify_prices_no_db,
    PriceVerificationResult,
    PRICE_VERIFY_HIGH_MAE,
    PRICE_VERIFY_MEDIUM_MAE,
    PRICE_VERIFY_MIN_N,
)


# ---------------------------------------------------------------------------
# _mean_absolute_error
# ---------------------------------------------------------------------------

class TestMAE:
    def test_perfect_match(self):
        a = [100.0, 200.0, 300.0]
        b = [100.0, 200.0, 300.0]
        assert _mean_absolute_error(a, b) == pytest.approx(0.0)

    def test_constant_error(self):
        a = [100.0, 200.0, 300.0]
        b = [110.0, 210.0, 310.0]
        assert _mean_absolute_error(a, b) == pytest.approx(10.0)

    def test_mixed_sign_errors(self):
        a = [100.0, 200.0]
        b = [110.0, 190.0]
        assert _mean_absolute_error(a, b) == pytest.approx(10.0)

    def test_single_element(self):
        assert _mean_absolute_error([150.0], [145.0]) == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# _pearson_r
# ---------------------------------------------------------------------------

class TestPearsonR:
    def test_perfect_positive_correlation(self):
        a = [1.0, 2.0, 3.0, 4.0, 5.0]
        b = [2.0, 4.0, 6.0, 8.0, 10.0]
        assert _pearson_r(a, b) == pytest.approx(1.0, abs=1e-10)

    def test_perfect_negative_correlation(self):
        a = [1.0, 2.0, 3.0]
        b = [3.0, 2.0, 1.0]
        assert _pearson_r(a, b) == pytest.approx(-1.0, abs=1e-10)

    def test_no_correlation_constant(self):
        # One series is constant → zero variance → None
        a = [1.0, 2.0, 3.0]
        b = [5.0, 5.0, 5.0]
        assert _pearson_r(a, b) is None

    def test_fewer_than_2_returns_none(self):
        assert _pearson_r([1.0], [2.0]) is None

    def test_high_r_real_data(self):
        # Slightly noisy but highly correlated
        a = [100.0, 110.0, 120.0, 130.0, 140.0]
        b = [101.0, 109.0, 121.0, 129.0, 141.0]
        r = _pearson_r(a, b)
        assert r is not None
        assert r > 0.99

    def test_result_clamped_to_unit_interval(self):
        # Ensure no floating point escape
        a = [1.0, 2.0, 3.0]
        b = [1.0, 2.0, 3.0]
        r = _pearson_r(a, b)
        assert -1.0 <= r <= 1.0


# ---------------------------------------------------------------------------
# _compute_level
# ---------------------------------------------------------------------------

class TestComputeLevel:
    def test_high_level(self):
        assert _compute_level(96, PRICE_VERIFY_HIGH_MAE - 0.1) == 'high'

    def test_medium_level(self):
        assert _compute_level(96, PRICE_VERIFY_HIGH_MAE + 1.0) == 'medium'

    def test_low_level(self):
        assert _compute_level(96, PRICE_VERIFY_MEDIUM_MAE + 1.0) == 'low'

    def test_unverified_too_few_intervals(self):
        assert _compute_level(PRICE_VERIFY_MIN_N - 1, 1.0) == 'unverified'

    def test_unverified_zero_intervals(self):
        assert _compute_level(0, 0.0) == 'unverified'

    def test_boundary_high_threshold(self):
        # Exactly at HIGH_MAE threshold — NOT high (boundary is strict <)
        assert _compute_level(96, PRICE_VERIFY_HIGH_MAE) == 'medium'

    def test_boundary_medium_threshold(self):
        # Exactly at MEDIUM_MAE threshold — NOT medium (boundary is strict <)
        assert _compute_level(96, PRICE_VERIFY_MEDIUM_MAE) == 'low'

    def test_boundary_min_n(self):
        # Exactly at MIN_N — should be verified (not 'unverified')
        assert _compute_level(PRICE_VERIFY_MIN_N, 1.0) == 'high'


# ---------------------------------------------------------------------------
# _build_notes
# ---------------------------------------------------------------------------

class TestBuildNotes:
    def test_notes_non_empty(self):
        notes = _build_notes(96, 96, 1.5, 0.999, 'high')
        assert len(notes) > 0

    def test_notes_contains_n(self):
        notes = _build_notes(80, 96, 2.0, 0.995, 'high')
        assert '80/96' in notes

    def test_notes_contains_mae(self):
        notes = _build_notes(96, 96, 3.75, 0.99, 'high')
        assert 'MAE' in notes
        assert '3.8' in notes   # rounded to 1 decimal

    def test_notes_contains_r(self):
        notes = _build_notes(96, 96, 1.0, 0.9991, 'high')
        assert 'r=' in notes

    def test_notes_contains_level(self):
        notes = _build_notes(96, 96, 1.0, 0.999, 'high')
        assert "'high'" in notes

    def test_notes_without_r(self):
        notes = _build_notes(96, 96, 1.0, None, 'high')
        assert 'r=' not in notes


# ---------------------------------------------------------------------------
# verify_prices_no_db
# ---------------------------------------------------------------------------

class TestVerifyPricesNoDb:
    def test_always_unverified(self):
        result = verify_prices_no_db([])
        assert result.price_verification_level == 'unverified'

    def test_notes_explain_reason(self):
        result = verify_prices_no_db([{'interval_start': 'x', 'nodal_price_excel': 100.0}])
        assert 'verify-prices' in result.price_verification_notes.lower() or \
               'not requested' in result.price_verification_notes.lower()

    def test_n_is_zero(self):
        result = verify_prices_no_db([])
        assert result.price_match_n == 0

    def test_mae_is_none(self):
        result = verify_prices_no_db([])
        assert result.price_match_mae is None

    def test_r_is_none(self):
        result = verify_prices_no_db([])
        assert result.price_match_r is None


# ---------------------------------------------------------------------------
# _unverified helper
# ---------------------------------------------------------------------------

class TestUnverifiedHelper:
    def test_level_is_unverified(self):
        result = _unverified("test reason")
        assert result.price_verification_level == 'unverified'

    def test_reason_in_notes(self):
        result = _unverified("no data available")
        assert "no data available" in result.price_verification_notes

    def test_n_is_zero(self):
        result = _unverified("x")
        assert result.price_match_n == 0
