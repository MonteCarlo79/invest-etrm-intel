# tests/hermes/test_data_patrol.py
from datetime import date
from services.hermes.data_patrol import SourceStatus, PatrolReport, _days_behind


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
