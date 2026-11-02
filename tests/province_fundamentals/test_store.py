# tests/province_fundamentals/test_store.py
"""Store-layer tests for marketdata.province_fundamentals_monthly.

All DB access is mocked (fake-conn pattern from tests/hermes/test_data_patrol.py);
no real DB is touched, no live data/ fixtures are read.
"""
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import psycopg2
import psycopg2.extras
import pytest

from services.province_fundamentals import store
from services.province_fundamentals.models import FundRow


def _make_cursor():
    cur = MagicMock()
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    return cur


def _make_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.__enter__ = lambda s: s
    conn.__exit__ = MagicMock(return_value=False)
    return conn


@pytest.fixture
def fake_db(monkeypatch):
    """Patch psycopg2.connect -> MagicMock conn; spy on execute_values.

    Returns (cursor, ev_calls); ev_calls collects (cursor, sql, batch_rows).
    """
    cur = _make_cursor()
    conn = _make_conn(cur)
    monkeypatch.setattr(psycopg2, "connect", lambda url: conn)
    ev_calls = []

    def _spy(c, sql, batch, **kwargs):
        ev_calls.append((c, sql, [tuple(r) for r in batch]))

    monkeypatch.setattr(psycopg2.extras, "execute_values", _spy)
    return cur, ev_calls


# ── write_rows: 口径分离与冲突规则 (task brief Step 1) ────────────────────────

def test_same_key_different_scope_metrics_coexist(fake_db):
    """安徽 2025-01: 省调 cap_total=6831.49 与全口径 cap_total_full=12635.64 必须共存 —
    metric is part of the natural key, so different scopes never collide."""
    cur, ev_calls = fake_db
    cur.fetchone.side_effect = [None, None]  # no confirmed value for either key
    rows = [
        FundRow(province="安徽", month=date(2025, 1, 1), period="month",
                section="capacity", metric="cap_total", value=6831.49,
                unit="万千瓦", source="安徽2025年1月月报数据.xlsx"),
        FundRow(province="安徽", month=date(2025, 1, 1), period="month",
                section="capacity", metric="cap_total_full", value=12635.64,
                unit="万千瓦", source="安徽2025年1月月报数据.xlsx"),
    ]
    result = store.write_rows(rows, pg_url="postgresql://test")
    assert result == {"written": 2, "conflicts": 0, "skipped": 0}
    # pre-check is scoped per metric: each row's lookup carries its own metric
    precheck_params = [c.args[1] for c in cur.execute.call_args_list]
    assert [p[4] for p in precheck_params] == ["cap_total", "cap_total_full"]
    # one batched insert (execute_values, never row-by-row) with both rows confirmed
    assert len(ev_calls) == 1
    batch = ev_calls[0][2]
    assert [t[4] for t in batch] == ["cap_total", "cap_total_full"]
    assert [t[5] for t in batch] == [6831.49, 12635.64]
    assert all(t[8] == "confirmed" for t in batch)


def test_restatement_within_half_pct_overwrites(fake_db):
    """同键同 source 族: 6831.49 -> 6831.52 (0.0004%, within 0.5%) -> written+1, 无 conflict."""
    cur, ev_calls = fake_db
    cur.fetchone.side_effect = [(Decimal("6831.49"),)]  # existing confirmed value
    rows = [FundRow(province="安徽", month=date(2025, 1, 1), period="month",
                    section="capacity", metric="cap_total", value=6831.52,
                    unit="万千瓦", source="安徽2025年1月月报数据.xlsx")]
    result = store.write_rows(rows, pg_url="postgresql://test")
    assert result == {"written": 1, "conflicts": 0, "skipped": 0}
    assert len(ev_calls) == 1
    batch = ev_calls[0][2]
    assert batch[0][5] == 6831.52
    assert batch[0][8] == "confirmed"


def test_restatement_beyond_half_pct_marks_conflict(fake_db):
    """6831.49 -> 6900.0 (~1.0%, beyond 0.5%) -> 旧行保留, 新行 status='conflict', conflicts+1."""
    cur, ev_calls = fake_db
    cur.fetchone.side_effect = [(Decimal("6831.49"),)]
    rows = [FundRow(province="安徽", month=date(2025, 1, 1), period="month",
                    section="capacity", metric="cap_total", value=6900.0,
                    unit="万千瓦", source="安徽2025年2月月报数据.xlsx")]
    result = store.write_rows(rows, pg_url="postgresql://test")
    assert result == {"written": 0, "conflicts": 1, "skipped": 0}
    # written as an INSERT carrying status='conflict' on the source-scoped key —
    # the old confirmed row (different source) is never overwritten
    assert len(ev_calls) == 1
    sql, batch = ev_calls[0][1], ev_calls[0][2]
    assert "INSERT INTO marketdata.province_fundamentals_monthly" in sql
    assert "ON CONFLICT (province, month, period, section, metric, COALESCE(source, ''))" in sql
    assert batch[0][5] == 6900.0
    assert batch[0][8] == "conflict"


def test_same_source_restatement_beyond_half_pct_keeps_confirmed_row(fake_db):
    """Same natural key AND same source as the existing confirmed row (corrected 月报
    re-ingested under the same filename): the confirmed row must survive. The conflict
    row carries a '#conflict'-suffixed source -> new unique key, never an in-place flip."""
    cur, ev_calls = fake_db
    cur.fetchone.side_effect = [(Decimal("6831.49"),)]
    rows = [FundRow(province="安徽", month=date(2025, 1, 1), period="month",
                    section="capacity", metric="cap_total", value=6900.0,
                    unit="万千瓦", source="安徽2025年1月月报数据.xlsx")]  # same source as confirmed row
    result = store.write_rows(rows, pg_url="postgresql://test")
    assert result == {"written": 0, "conflicts": 1, "skipped": 0}
    batch = ev_calls[0][2]
    assert batch[0][5] == 6900.0
    assert batch[0][7] == "安徽2025年1月月报数据.xlsx#conflict"  # disambiguated -> old row survives
    assert batch[0][8] == "conflict"


def test_duplicate_key_within_one_call_last_wins(fake_db):
    """Two rows sharing (province, month, period, section, metric, source) in one call
    must not both enter the batch — PG rejects ON CONFLICT affecting a row twice and
    rolls back everything. Dedupe to a single write, last value wins."""
    cur, ev_calls = fake_db
    cur.fetchone.side_effect = [None, None]  # pre-dedupe code would pre-check twice
    rows = [
        FundRow(province="安徽", month=date(2025, 1, 1), period="month",
                section="capacity", metric="cap_total", value=6831.49,
                unit="万千瓦", source="dup.xlsx"),
        FundRow(province="安徽", month=date(2025, 1, 1), period="month",
                section="capacity", metric="cap_total", value=6832.0,
                unit="万千瓦", source="dup.xlsx"),
    ]
    result = store.write_rows(rows, pg_url="postgresql://test")
    assert result == {"written": 1, "conflicts": 0, "skipped": 0}
    assert len(ev_calls) == 1
    batch = ev_calls[0][2]
    assert len(batch) == 1
    assert batch[0][5] == 6832.0  # last wins


# ── write_rows: skip / default_status / empty ─────────────────────────────────

def test_equal_value_skips(fake_db):
    """Exact restatement of the confirmed value is a no-op (idempotent re-runs)."""
    cur, ev_calls = fake_db
    cur.fetchone.side_effect = [(Decimal("6831.49"),)]
    rows = [FundRow(province="安徽", month=date(2025, 1, 1), period="month",
                    section="capacity", metric="cap_total", value=6831.49,
                    unit="万千瓦", source="安徽2025年1月月报数据.xlsx")]
    result = store.write_rows(rows, pg_url="postgresql://test")
    assert result == {"written": 0, "conflicts": 0, "skipped": 1}
    assert ev_calls == []  # nothing written at all


def test_write_rows_default_status_override(fake_db):
    """Intake-style callers can land rows as draft (review -> confirmed later)."""
    cur, ev_calls = fake_db
    cur.fetchone.side_effect = [None]
    rows = [FundRow(province="安徽", month=date(2025, 1, 1), period="year",
                    section="demand", metric="demand_total", value=3214.5,
                    unit="亿千瓦时", source="hermes-record-fact-20261102")]
    result = store.write_rows(rows, pg_url="postgresql://test", default_status="draft")
    assert result == {"written": 1, "conflicts": 0, "skipped": 0}
    assert ev_calls[0][2][0][8] == "draft"


def test_write_rows_empty_returns_zero_counts():
    assert store.write_rows([], pg_url="postgresql://test") == {
        "written": 0, "conflicts": 0, "skipped": 0}


# ── read_series: confirmed-only, DISTINCT-ON latest, period included ──────────

def test_read_series_confirmed_distinct_on_latest(fake_db):
    cur, _ = fake_db
    cur.fetchall.return_value = [
        ("安徽", date(2026, 1, 1), "year", "capacity", "cap_total",
         Decimal("12635.64"), "万千瓦", "2025交接表.xlsx", datetime(2026, 1, 20, 8, 0)),
        ("安徽", date(2026, 1, 1), "month", "capacity", "cap_total",
         Decimal("12600.0"), "万千瓦", "安徽2026年1月月报.xlsx", datetime(2026, 2, 10, 8, 0)),
    ]
    out = store.read_series("安徽", "capacity", ["cap_total"],
                            pg_url="postgresql://test", months=18)
    sql, params = cur.execute.call_args.args
    assert "DISTINCT ON (month, period, metric)" in sql
    assert "status = 'confirmed'" in sql
    assert "metric = ANY" in sql
    assert params == ["安徽", "capacity", 18, ["cap_total"]]
    # year row and January month row share month=2026-01-01 but period separates them
    assert [r["period"] for r in out] == ["year", "month"]
    assert out[0]["value"] == 12635.64
    assert isinstance(out[0]["value"], float)
    assert out[0]["unit"] == "万千瓦"
    assert out[1]["source"] == "安徽2026年1月月报.xlsx"


def test_read_series_without_metrics_filter(fake_db):
    cur, _ = fake_db
    cur.fetchall.return_value = []
    out = store.read_series("山东", "demand", pg_url="postgresql://test")
    sql, params = cur.execute.call_args.args
    assert "metric = ANY" not in sql
    assert params == ["山东", "demand", 24]  # default months=24
    assert out == []


# ── DDL file sanity ────────────────────────────────────────────────────────────

def test_ddl_file_defines_table_and_expression_unique_index():
    ddl = (Path(__file__).resolve().parents[2] / "db" / "ddl" / "marketdata"
           / "province_fundamentals_monthly.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS marketdata.province_fundamentals_monthly" in ddl
    assert "period" in ddl
    assert "CREATE UNIQUE INDEX IF NOT EXISTS" in ddl
    assert "(province, month, period, section, metric, COALESCE(source, ''))" in ddl
