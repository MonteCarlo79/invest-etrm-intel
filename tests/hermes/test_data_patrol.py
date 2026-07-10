# tests/hermes/test_data_patrol.py
from datetime import date, timedelta
from unittest.mock import MagicMock, patch, call
import psycopg2

from services.hermes.data_patrol import SourceStatus, PatrolReport, _days_behind


def _make_cursor(rows):
    """Return a mock cursor whose fetchall() returns rows."""
    cur = MagicMock()
    cur.fetchall.return_value = rows
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    return cur


def _make_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.__enter__ = lambda s: s
    conn.__exit__ = MagicMock(return_value=False)
    return conn


def test_source_status_defaults():
    s = SourceStatus(name="test", table="t", last_date=None, days_behind=999, status="missing", group="auto")
    assert s.reminder_text == ""
    assert s.fill_table == ""


def test_days_behind_fresh():
    from datetime import datetime, timezone, timedelta
    _BJ = timezone(timedelta(hours=8))
    today = datetime.now(_BJ).date()
    assert _days_behind(today, today=today) == 0


def test_days_behind_stale():
    from datetime import timedelta, datetime, timezone
    _BJ = timezone(timedelta(hours=8))
    today = datetime.now(_BJ).date()
    three_days_ago = today - timedelta(days=3)
    assert _days_behind(three_days_ago, today=today) == 3


def test_days_behind_none():
    assert _days_behind(None) == 9999


def test_classify_daily():
    from services.hermes.data_patrol import _classify_daily, _MISSING_SENTINEL
    assert _classify_daily(_MISSING_SENTINEL) == "missing"
    assert _classify_daily(3) == "stale"
    assert _classify_daily(0) == "fresh"
    assert _classify_daily(2) == "fresh"
    assert _classify_daily(9) == "stale"


def test_patrol_report_counts():
    s_fresh = SourceStatus(name="a", table="t", last_date=date.today(), days_behind=0, status="fresh", group="auto")
    s_stale = SourceStatus(name="b", table="t", last_date=None, days_behind=9999, status="missing", group="manual")
    r = PatrolReport(sources=[s_fresh, s_stale], kb_summaries=[])
    assert r.count_by_status("fresh") == 1
    assert r.count_by_status("missing") == 1
    assert r.has_alerts is True


def test_check_auto_pipelines_fresh(monkeypatch):
    today = date.today()
    # Return today's date for every query
    cur = _make_cursor([(today,)])
    conn = _make_conn(cur)
    monkeypatch.setattr("psycopg2.connect", lambda url: conn)

    from services.hermes.data_patrol import check_auto_pipelines
    results = check_auto_pipelines("postgresql://test")
    assert len(results) > 0
    assert all(s.status == "fresh" for s in results)


def test_check_auto_pipelines_stale(monkeypatch):
    stale_date = date.today() - timedelta(days=5)
    cur = _make_cursor([(stale_date,)])
    conn = _make_conn(cur)
    monkeypatch.setattr("psycopg2.connect", lambda url: conn)

    from services.hermes.data_patrol import check_auto_pipelines
    results = check_auto_pipelines("postgresql://test")
    assert any(s.status == "stale" for s in results)


def test_check_auto_pipelines_db_error(monkeypatch):
    monkeypatch.setattr("psycopg2.connect", MagicMock(side_effect=psycopg2.OperationalError("down")))
    from services.hermes.data_patrol import check_auto_pipelines
    results = check_auto_pipelines("postgresql://test")
    # DB error → all missing (graceful degradation)
    assert all(s.status == "missing" for s in results)


def test_check_manual_uploads_stale(monkeypatch):
    stale = date.today() - timedelta(days=4)
    cur = _make_cursor([(stale,)])
    conn = _make_conn(cur)
    monkeypatch.setattr("psycopg2.connect", lambda url: conn)

    from services.hermes.data_patrol import check_manual_uploads
    results = check_manual_uploads("postgresql://test")
    assert len(results) == 1
    assert results[0].status == "stale"
    assert "pdf" in results[0].reminder_text.lower()


def test_check_monthly_data_missing_returns_fill_table(monkeypatch):
    import services.hermes.data_patrol as dp
    from unittest.mock import MagicMock
    from datetime import datetime as real_dt, timezone, timedelta

    _BJ = timezone(timedelta(hours=8))
    # Fix "today" to 15th so should_flag=True (past 10th)
    fixed_now = real_dt(2026, 7, 15, 10, 0, tzinfo=_BJ)
    monkeypatch.setattr(dp, "datetime", MagicMock(now=MagicMock(return_value=fixed_now)))

    cur = _make_cursor([])
    conn = _make_conn(cur)
    monkeypatch.setattr("psycopg2.connect", lambda url: conn)

    results = dp.check_monthly_data("postgresql://test")
    fill_targets = [s for s in results if s.fill_table]
    assert len(fill_targets) > 0


def test_check_kb_activity_returns_summaries(monkeypatch):
    today = date.today()
    # cursor returns (last_date, count_7d, count_30d) per query
    cur = MagicMock()
    cur.fetchall.return_value = [(today, 5, 12)]
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    conn = _make_conn(cur)
    monkeypatch.setattr("psycopg2.connect", lambda url: conn)

    from services.hermes.data_patrol import check_kb_activity
    results = check_kb_activity("postgresql://test")
    assert len(results) > 0
    assert all(isinstance(r.count_7d, int) for r in results)
