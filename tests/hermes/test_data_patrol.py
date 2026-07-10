# tests/hermes/test_data_patrol.py
from datetime import date
from services.hermes.data_patrol import SourceStatus, PatrolReport, _days_behind


def test_source_status_defaults():
    s = SourceStatus(name="test", table="t", last_date=None, days_behind=999, status="missing", group="auto")
    assert s.reminder_text == ""
    assert s.fill_table == ""


def test_days_behind_fresh():
    today = date.today()
    assert _days_behind(today) == 0


def test_days_behind_stale():
    from datetime import timedelta
    yesterday = date.today() - timedelta(days=3)
    assert _days_behind(yesterday) == 3


def test_days_behind_none():
    assert _days_behind(None) == 9999


def test_patrol_report_counts():
    s_fresh = SourceStatus(name="a", table="t", last_date=date.today(), days_behind=0, status="fresh", group="auto")
    s_stale = SourceStatus(name="b", table="t", last_date=None, days_behind=9999, status="missing", group="manual")
    r = PatrolReport(sources=[s_fresh, s_stale], kb_summaries=[])
    assert r.count_by_status("fresh") == 1
    assert r.count_by_status("missing") == 1
    assert r.has_alerts is True
