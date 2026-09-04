"""
services/monitoring/tests/test_idempotency.py

B6: Idempotency verification tests.

Unit tests (no DB): inspect SQL source for ON CONFLICT clauses.
Integration tests (requires PGURL): run upsert twice, assert no duplicate rows.
"""
from __future__ import annotations

import inspect
from datetime import date

import pytest


# ---------------------------------------------------------------------------
# Unit: verify ON CONFLICT is present in upsert SQL source
# ---------------------------------------------------------------------------

class TestUpsertSqlContainsOnConflict:
    """Parse the module source to confirm ON CONFLICT ... DO UPDATE is present."""

    def test_realization_upsert_has_on_conflict(self):
        import services.monitoring.realization_monitor as mod
        src = inspect.getsource(mod.upsert_realization_status)
        assert "ON CONFLICT" in src
        assert "DO UPDATE" in src

    def test_fragility_upsert_has_on_conflict(self):
        import services.monitoring.fragility_monitor as mod
        src = inspect.getsource(mod.upsert_fragility_status)
        assert "ON CONFLICT" in src
        assert "DO UPDATE" in src

    def test_attribution_upsert_has_on_conflict(self):
        import services.monitoring.run_daily_attribution as mod
        src = inspect.getsource(mod._upsert_attribution)
        assert "ON CONFLICT" in src
        assert "DO UPDATE" in src

    def test_realization_conflict_key_includes_asset_and_date(self):
        import services.monitoring.realization_monitor as mod
        src = inspect.getsource(mod.upsert_realization_status)
        # Conflict key must include asset_code, snapshot_date, lookback_days
        assert "asset_code" in src
        assert "snapshot_date" in src
        assert "lookback_days" in src

    def test_fragility_conflict_key_includes_asset_and_date(self):
        import services.monitoring.fragility_monitor as mod
        src = inspect.getsource(mod.upsert_fragility_status)
        assert "asset_code" in src
        assert "snapshot_date" in src


# ---------------------------------------------------------------------------
# Integration: double-upsert produces no duplicate rows
# ---------------------------------------------------------------------------

_SNAP = date(2026, 1, 15)
_ASSET = "test_idempotency_asset"


@pytest.mark.integration
class TestRealizationUpsertIdempotency:
    """Upsert the same realization row twice; row count must remain 1."""

    @pytest.fixture(autouse=True)
    def cleanup(self, db_engine):
        from sqlalchemy import text
        yield
        with db_engine.begin() as conn:
            conn.execute(text(
                "DELETE FROM monitoring.asset_realization_status "
                "WHERE asset_code = :a AND snapshot_date = :s"
            ), {"a": _ASSET, "s": _SNAP})

    def _row(self, status="NORMAL", ratio=0.80):
        return {
            "asset_code": _ASSET,
            "snapshot_date": _SNAP,
            "lookback_days": 30,
            "days_in_window": 10,
            "avg_cleared_actual_pnl": 80_000.0,
            "avg_pf_grid_feasible_pnl": 100_000.0,
            "realization_ratio": ratio,
            "avg_grid_restriction_loss": 5_000.0,
            "avg_forecast_error_loss": 8_000.0,
            "avg_strategy_error_loss": 3_000.0,
            "avg_nomination_loss": 2_000.0,
            "avg_execution_clearing_loss": 2_000.0,
            "dominant_loss_bucket": "avg_forecast_error_loss",
            "status_level": status,
            "narrative": "test narrative",
        }

    def test_double_upsert_no_duplicate(self, db_engine):
        from sqlalchemy import text
        from services.monitoring.realization_monitor import upsert_realization_status

        row = self._row()
        upsert_realization_status(db_engine, [row])
        upsert_realization_status(db_engine, [row])

        with db_engine.begin() as conn:
            count = conn.execute(text(
                "SELECT COUNT(*) FROM monitoring.asset_realization_status "
                "WHERE asset_code = :a AND snapshot_date = :s AND lookback_days = 30"
            ), {"a": _ASSET, "s": _SNAP}).scalar()
        assert count == 1

    def test_second_upsert_updates_values(self, db_engine):
        from sqlalchemy import text
        from services.monitoring.realization_monitor import upsert_realization_status

        upsert_realization_status(db_engine, [self._row(status="NORMAL", ratio=0.80)])
        upsert_realization_status(db_engine, [self._row(status="WARN", ratio=0.60)])

        with db_engine.begin() as conn:
            row = conn.execute(text(
                "SELECT status_level, realization_ratio "
                "FROM monitoring.asset_realization_status "
                "WHERE asset_code = :a AND snapshot_date = :s AND lookback_days = 30"
            ), {"a": _ASSET, "s": _SNAP}).fetchone()
        assert row[0] == "WARN"
        assert abs(float(row[1]) - 0.60) < 0.001


@pytest.mark.integration
class TestFragilityUpsertIdempotency:
    """Upsert the same fragility row twice; row count must remain 1."""

    @pytest.fixture(autouse=True)
    def cleanup(self, db_engine):
        from sqlalchemy import text
        yield
        with db_engine.begin() as conn:
            conn.execute(text(
                "DELETE FROM monitoring.asset_fragility_status "
                "WHERE asset_code = :a AND snapshot_date = :s"
            ), {"a": _ASSET, "s": _SNAP})

    def _row(self, level="LOW", score=0.10):
        return {
            "asset_code": _ASSET,
            "snapshot_date": _SNAP,
            "realization_score": 0.0,
            "trend_score": 0.2,
            "composite_score": score,
            "fragility_level": level,
            "realization_ratio": 0.80,
            "realization_status_level": "NORMAL",
            "days_in_window": 10,
            "recent_ratio": 0.82,
            "prior_ratio": 0.80,
            "ratio_delta": 0.02,
            "dominant_factor": "realization_score",
            "narrative": "test fragility narrative",
        }

    def test_double_upsert_no_duplicate(self, db_engine):
        from sqlalchemy import text
        from services.monitoring.fragility_monitor import upsert_fragility_status

        row = self._row()
        upsert_fragility_status(db_engine, [row])
        upsert_fragility_status(db_engine, [row])

        with db_engine.begin() as conn:
            count = conn.execute(text(
                "SELECT COUNT(*) FROM monitoring.asset_fragility_status "
                "WHERE asset_code = :a AND snapshot_date = :s"
            ), {"a": _ASSET, "s": _SNAP}).scalar()
        assert count == 1

    def test_second_upsert_updates_fragility_level(self, db_engine):
        from sqlalchemy import text
        from services.monitoring.fragility_monitor import upsert_fragility_status

        upsert_fragility_status(db_engine, [self._row(level="LOW", score=0.10)])
        upsert_fragility_status(db_engine, [self._row(level="HIGH", score=0.60)])

        with db_engine.begin() as conn:
            row = conn.execute(text(
                "SELECT fragility_level, composite_score "
                "FROM monitoring.asset_fragility_status "
                "WHERE asset_code = :a AND snapshot_date = :s"
            ), {"a": _ASSET, "s": _SNAP}).fetchone()
        assert row[0] == "HIGH"
        assert abs(float(row[1]) - 0.60) < 0.001
