"""
Smoke tests for the unified data-ingestion framework.

Run:
  pytest services/data_ingestion/tests/test_smoke.py -v

Requires: PGURL env var pointing to the RDS marketdata database
          (either directly or via SSH tunnel at 127.0.0.1:15432).
"""
import os
import sys
from pathlib import Path
from datetime import date, timedelta

import pytest
from sqlalchemy import text

# Make repo root importable
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))


# ---------------------------------------------------------------------------
# Connectivity
# ---------------------------------------------------------------------------

def test_db_connection(engine):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).scalar()
    assert result == 1


def test_ops_schema_exists(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name = 'ops'
        """)).scalar()
    assert result == "ops", "ops schema not found — run db/ddl/ops/ingestion_control.sql first"


# ---------------------------------------------------------------------------
# Control tables present
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table", [
    "ops.ingestion_job_runs",
    "ops.ingestion_dataset_status",
    "ops.ingestion_expected_freshness",
    "ops.ingestion_gap_queue",
])
def test_control_table_exists(engine, table):
    schema, tbl = table.split(".")
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = :schema AND table_name = :tbl
        """), {"schema": schema, "tbl": tbl}).scalar()
    assert result == tbl, f"Table {table} not found"


def test_freshness_seed_rows(engine):
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM ops.ingestion_expected_freshness")
        ).scalar()
    assert count >= 11, f"Expected at least 11 freshness seed rows, got {count}"


# ---------------------------------------------------------------------------
# Dry-run: control table write roundtrip
# ---------------------------------------------------------------------------

def test_dry_run_roundtrip(engine):
    """
    Simulates a dry_run collector run: start_run → finish_run(skipped).
    Verifies the row lands in ops.ingestion_job_runs with status='skipped'.
    """
    from services.data_ingestion.shared.control import start_run, finish_run

    today = date.today()
    run_id = start_run(
        collector="_smoke_test",
        mode="daily",
        start_date=today - timedelta(days=1),
        end_date=today - timedelta(days=1),
        dry_run=True,
    )
    assert isinstance(run_id, int) and run_id > 0

    finish_run(run_id, "skipped")

    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT status, dry_run FROM ops.ingestion_job_runs WHERE id = :id
        """), {"id": run_id}).fetchone()

    assert row is not None
    assert row.status == "skipped"
    assert row.dry_run is True

    # Cleanup
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM ops.ingestion_job_runs WHERE id = :id"),
                     {"id": run_id})


# ---------------------------------------------------------------------------
# Freshness monitor (read-only probe)
# ---------------------------------------------------------------------------

def test_freshness_monitor_no_crash(engine):
    """Freshness monitor should run without raising for known tables."""
    from services.data_ingestion.freshness_monitor import check_freshness
    gaps = check_freshness(engine, date.today())
    assert isinstance(gaps, list)


# ---------------------------------------------------------------------------
# RunContext parsing
# ---------------------------------------------------------------------------

def test_runcontext_daily_defaults():
    from services.data_ingestion.shared.context import RunContext
    ctx = RunContext.from_env_and_args("_test", argv=["--mode", "daily", "--lookback-days", "3"])
    assert ctx.mode == "daily"
    assert ctx.lookback_days == 3
    assert ctx.dry_run is False
    delta = (ctx.end_date - ctx.start_date).days
    assert delta == 2  # 3-day window → delta = lookback-1


def test_runcontext_reconcile_requires_start_date():
    from services.data_ingestion.shared.context import RunContext
    with pytest.raises(SystemExit):
        RunContext.from_env_and_args("_test", argv=["--mode", "reconcile"])


# ---------------------------------------------------------------------------
# column_to_matrix_all — DB connection precedence (no live DB required)
# ---------------------------------------------------------------------------

def test_column_to_matrix_db_precedence(monkeypatch):
    """_db_engine() respects DB_DSN > PGURL > discrete vars without connecting to DB."""
    import services.data_ingestion.column_to_matrix_all as cma

    # --- Test 1: DB_DSN beats PGURL ---
    monkeypatch.setenv("DB_DSN",  "postgresql://u:p@dsn-host:5432/testdb")
    monkeypatch.setenv("PGURL",   "postgresql://u:p@pgurl-host:5432/testdb")
    cma._DB_MODE_LOGGED = False
    engine = cma._db_engine()
    assert engine.url.host == "dsn-host", (
        f"Expected dsn-host, got {engine.url.host}. DB_DSN should take precedence."
    )

    # --- Test 2: PGURL used when DB_DSN absent ---
    monkeypatch.delenv("DB_DSN", raising=False)
    cma._DB_MODE_LOGGED = False
    engine = cma._db_engine()
    assert engine.url.host == "pgurl-host", (
        f"Expected pgurl-host, got {engine.url.host}. PGURL should be used when DB_DSN absent."
    )

    # --- Test 3: discrete vars used as last resort ---
    monkeypatch.delenv("PGURL", raising=False)
    # Patch DB_DEFAULTS directly (module-level dict is mutable)
    original = cma.DB_DEFAULTS.copy()
    try:
        cma.DB_DEFAULTS.update({"host": "discrete-host", "port": "5432",
                                 "user": "discrete-user", "password": "x",
                                 "name": "testdb"})
        cma._DB_MODE_LOGGED = False
        engine = cma._db_engine()
        assert engine.url.host == "discrete-host", (
            f"Expected discrete-host, got {engine.url.host}. Discrete vars should be used as fallback."
        )
    finally:
        cma.DB_DEFAULTS.update(original)
