"""
services/monitoring/tests/test_realization_monitor.py

Unit tests for realization_monitor.py covering:
  B1  — DATA_ABSENT status for insufficient data
  B2  — INDETERMINATE status for non-positive pf_grid_feasible_pnl
  B5  — Structured MONITORING_ALERT log events
"""
from __future__ import annotations

import logging

import pytest

from services.monitoring.realization_monitor import (
    _MIN_DAYS_FOR_RATIO,
    _build_narrative,
    _classify_status,
)


# ---------------------------------------------------------------------------
# TestClassifyStatus — B1 + B2
# ---------------------------------------------------------------------------

class TestClassifyStatus:
    """_classify_status priority: DATA_ABSENT > INDETERMINATE > ratio thresholds."""

    # --- B1: DATA_ABSENT ---

    def test_zero_days_is_data_absent(self):
        assert _classify_status(ratio=None, days_in_window=0) == "DATA_ABSENT"

    def test_insufficient_days_is_data_absent(self):
        assert _classify_status(ratio=0.8, days_in_window=_MIN_DAYS_FOR_RATIO - 1) == "DATA_ABSENT"

    def test_exact_min_days_not_data_absent(self):
        result = _classify_status(ratio=0.8, days_in_window=_MIN_DAYS_FOR_RATIO)
        assert result != "DATA_ABSENT"

    def test_data_absent_takes_priority_over_good_ratio(self):
        # Even a perfect ratio must return DATA_ABSENT when days < threshold
        assert _classify_status(ratio=1.0, days_in_window=2) == "DATA_ABSENT"

    # --- B2: INDETERMINATE ---

    def test_non_positive_pf_grid_is_indeterminate(self):
        assert _classify_status(ratio=None, days_in_window=10, pf_grid_positive=False) == "INDETERMINATE"

    def test_ratio_none_with_sufficient_days_is_indeterminate(self):
        # ratio=None but days OK → INDETERMINATE (benchmark absent)
        assert _classify_status(ratio=None, days_in_window=10) == "INDETERMINATE"

    def test_indeterminate_takes_priority_over_ratio(self):
        # pf_grid_positive=False overrides whatever ratio would say
        assert _classify_status(ratio=0.9, days_in_window=20, pf_grid_positive=False) == "INDETERMINATE"

    # --- Ratio-based statuses ---

    def test_ratio_above_0_70_is_normal(self):
        assert _classify_status(ratio=0.75, days_in_window=10) == "NORMAL"

    def test_ratio_at_0_70_boundary_is_normal(self):
        assert _classify_status(ratio=0.70, days_in_window=10) == "NORMAL"

    def test_ratio_just_below_0_70_is_warn(self):
        assert _classify_status(ratio=0.699, days_in_window=10) == "WARN"

    def test_ratio_at_0_50_boundary_is_warn(self):
        assert _classify_status(ratio=0.50, days_in_window=10) == "WARN"

    def test_ratio_below_0_50_is_alert(self):
        assert _classify_status(ratio=0.45, days_in_window=10) == "ALERT"

    def test_ratio_at_0_30_boundary_is_alert(self):
        assert _classify_status(ratio=0.30, days_in_window=10) == "ALERT"

    def test_ratio_below_0_30_is_critical(self):
        assert _classify_status(ratio=0.20, days_in_window=10) == "CRITICAL"

    def test_ratio_zero_is_critical(self):
        assert _classify_status(ratio=0.0, days_in_window=10) == "CRITICAL"

    def test_negative_ratio_is_critical(self):
        # Rare but possible if cleared_actual_pnl < 0 and pf_grid is positive
        assert _classify_status(ratio=-0.1, days_in_window=10) == "CRITICAL"


# ---------------------------------------------------------------------------
# TestBuildNarrative — DATA_ABSENT and INDETERMINATE variants
# ---------------------------------------------------------------------------

class TestBuildNarrative:
    """_build_narrative must explain the data/market condition for non-ratio statuses."""

    def test_zero_days_narrative_mentions_no_rows(self):
        narrative = _build_narrative("ASSET_X", None, "DATA_ABSENT", 0, None, None, None)
        assert "DATA_ABSENT" in narrative
        assert "No attribution rows" in narrative

    def test_insufficient_days_narrative_mentions_minimum(self):
        narrative = _build_narrative("ASSET_X", None, "DATA_ABSENT", 3, None, None, None)
        assert "DATA_ABSENT" in narrative
        assert "3 day(s)" in narrative
        assert str(_MIN_DAYS_FOR_RATIO) in narrative

    def test_indeterminate_narrative_explains_benchmark(self):
        narrative = _build_narrative(
            "ASSET_X", None, "INDETERMINATE", 10, None, 50_000.0, -1_000.0
        )
        assert "INDETERMINATE" in narrative
        assert "non-positive" in narrative or "unavailable" in narrative

    def test_critical_narrative_does_not_mention_data_missing(self):
        # B1 regression: "or data is missing" phrase was removed from CRITICAL
        narrative = _build_narrative(
            "ASSET_X", 0.15, "CRITICAL", 10, "avg_grid_restriction_loss", 80_000.0, 500_000.0
        )
        assert "data is missing" not in narrative.lower()
        assert "CRITICAL" in narrative


# ---------------------------------------------------------------------------
# TestStructuredLogEvents — B5
# ---------------------------------------------------------------------------

class TestStructuredLogEvents:
    """MONITORING_ALERT must fire for ALERT and CRITICAL statuses only."""

    def _run_with_mock_engine(self, rows_data, caplog):
        """Helper: call compute_realization_status with a mock engine."""
        from datetime import date
        from unittest.mock import MagicMock

        from services.monitoring.realization_monitor import compute_realization_status

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = rows_data
        mock_engine = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        with caplog.at_level(logging.INFO, logger="services.monitoring.realization_monitor"):
            return compute_realization_status("TEST_ASSET", date(2026, 4, 18), mock_engine)

    def _make_rows(self, actual, pf_grid, n=10):
        """Produce n identical attribution rows."""
        return [
            (actual, pf_grid, 0.0, 0.0, 0.0, 0.0, 0.0)
            for _ in range(n)
        ]

    def test_alert_status_emits_monitoring_alert(self, caplog):
        rows = self._make_rows(actual=35_000, pf_grid=100_000)  # ratio=0.35 → ALERT
        self._run_with_mock_engine(rows, caplog)
        assert any("MONITORING_ALERT" in r.message for r in caplog.records)

    def test_critical_status_emits_monitoring_alert(self, caplog):
        rows = self._make_rows(actual=10_000, pf_grid=100_000)  # ratio=0.10 → CRITICAL
        self._run_with_mock_engine(rows, caplog)
        assert any("MONITORING_ALERT" in r.message for r in caplog.records)

    def test_normal_status_does_not_emit_monitoring_alert(self, caplog):
        rows = self._make_rows(actual=80_000, pf_grid=100_000)  # ratio=0.80 → NORMAL
        self._run_with_mock_engine(rows, caplog)
        assert not any("MONITORING_ALERT" in r.message for r in caplog.records)

    def test_warn_status_does_not_emit_monitoring_alert(self, caplog):
        rows = self._make_rows(actual=55_000, pf_grid=100_000)  # ratio=0.55 → WARN
        self._run_with_mock_engine(rows, caplog)
        assert not any("MONITORING_ALERT" in r.message for r in caplog.records)

    def test_monitoring_alert_contains_asset_and_status(self, caplog):
        rows = self._make_rows(actual=10_000, pf_grid=100_000)
        self._run_with_mock_engine(rows, caplog)
        alert_messages = [r.message for r in caplog.records if "MONITORING_ALERT" in r.message]
        assert alert_messages
        assert "TEST_ASSET" in alert_messages[0]
        assert "CRITICAL" in alert_messages[0]
