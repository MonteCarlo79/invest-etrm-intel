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


def test_build_summary_card_green_when_all_fresh():
    from services.hermes.data_patrol import build_summary_card, PatrolReport
    s = SourceStatus(name="x", table="t", last_date=date.today(), days_behind=0, status="fresh", group="auto")
    r = PatrolReport(sources=[s], kb_summaries=[])
    card = build_summary_card(r)
    assert card["header"]["template"] == "green"
    assert "elements" in card


def test_build_summary_card_orange_when_stale():
    from services.hermes.data_patrol import build_summary_card, PatrolReport
    s = SourceStatus(name="x", table="t", last_date=date.today() - timedelta(days=5),
                     days_behind=5, status="stale", group="auto")
    r = PatrolReport(sources=[s], kb_summaries=[])
    card = build_summary_card(r)
    assert card["header"]["template"] == "orange"


def test_build_detail_card_contains_fill_button():
    from services.hermes.data_patrol import build_detail_card, PatrolReport
    s = SourceStatus(name="容量补偿 2026-06", table="province_cap_comp",
                     last_date=None, days_behind=30, status="missing", group="monthly",
                     fill_table="province_cap_comp", fill_month="2026-06")
    r = PatrolReport(sources=[s], kb_summaries=[])
    card = build_detail_card(r)
    card_str = str(card)
    assert "填入数据" in card_str


def test_run_patrol_returns_report(monkeypatch):
    import services.hermes.data_patrol as dp
    monkeypatch.setattr(dp, "check_auto_pipelines", lambda pg: [])
    monkeypatch.setattr(dp, "check_manual_uploads", lambda pg: [])
    monkeypatch.setattr(dp, "check_monthly_data", lambda pg: [])
    monkeypatch.setattr(dp, "check_kb_activity", lambda pg: [])
    result = dp.run_patrol("postgresql://test", feishu=None, owner_open_id="", api_key="")
    assert isinstance(result, dp.PatrolReport)


def test_extract_from_file_for_gap_txt(monkeypatch):
    import anthropic as _ant
    mock_msg = MagicMock()
    mock_msg.content = [MagicMock(text='{"cap_comp_yuan_kw": 165.0, "peak_duration_hours": 6.0}')]
    mock_client = MagicMock()
    mock_client.messages.create.return_value = mock_msg
    monkeypatch.setattr(_ant, "Anthropic", lambda api_key: mock_client)

    from services.hermes.capcomp_manual_etl import extract_from_file_for_gap
    txt_bytes = "广东省2026年容量补偿标准165元/kW，峰值6小时".encode("utf-8")
    result = extract_from_file_for_gap(
        file_bytes=txt_bytes,
        filename="test.txt",
        fill_table="province_cap_comp",
        province="广东",
        month="2026-06",
        api_key="test-key",
    )
    assert result["extracted"] is True
    assert result["values"]["cap_comp_yuan_kw"] == 165.0


# ── check_ingest_ops (portal Data Operations replacement) ────────────────────

def _ops_cursor(ops_rows, job_rows):
    """Cursor mock: first execute() (ops_log) returns ops_rows, second (jobs) job_rows."""
    cur = MagicMock()
    cur.fetchall.side_effect = [ops_rows, job_rows]
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    return cur


def test_check_ingest_ops_aggregates_lingfeng(monkeypatch):
    import services.hermes.data_patrol as dp
    ops_rows = [
        ("lingfeng_ingest", "江西", "2026-09-11→2026-09-12", "success", ""),
        ("lingfeng_ingest", "广州", "2026-09-11→2026-09-12", "failed", "Failed chunks: [...]"),
        ("lingfeng_ingest", "冀北", "2026-09-11→2026-09-12", "failed", "Failed chunks: [...]"),
        ("capture", "all", "2026-09-11→2026-09-12", "success", ""),
    ]
    cur = _ops_cursor(ops_rows, [("inner_mongolia",)])
    monkeypatch.setattr("psycopg2.connect", lambda *a, **k: _make_conn(cur))
    s = dp.check_ingest_ops("postgresql://test")
    assert s.available is True
    assert s.lf_total == 3
    assert s.lf_success == 1
    assert [m for m, _ in s.lf_failed] == ["广州", "冀北"]
    assert s.lf_date_range == "2026-09-11→2026-09-12"
    assert s.capture_status == "success"
    assert s.running_jobs == ["inner_mongolia"]
    assert s.has_failures is True


def test_check_ingest_ops_empty(monkeypatch):
    import services.hermes.data_patrol as dp
    cur = _ops_cursor([], [])
    monkeypatch.setattr("psycopg2.connect", lambda *a, **k: _make_conn(cur))
    s = dp.check_ingest_ops("postgresql://test")
    assert s.available is False
    assert s.has_failures is False


def test_check_ingest_ops_db_down(monkeypatch):
    import services.hermes.data_patrol as dp
    monkeypatch.setattr("psycopg2.connect", MagicMock(side_effect=Exception("no db")))
    s = dp.check_ingest_ops("postgresql://test")
    assert s.available is False


def test_summary_card_shows_lingfeng_line():
    import services.hermes.data_patrol as dp
    ops = dp.IngestOpsSummary(
        available=True, lf_total=29, lf_success=27,
        lf_failed=[("广州", "x"), ("冀北", "y")], lf_date_range="2026-09-11→2026-09-12",
        capture_status="success", capture_date_range="2026-09-11→2026-09-12",
        running_jobs=["inner_mongolia"],
    )
    r = dp.PatrolReport(sources=[], kb_summaries=[], ingest_ops=ops)
    card_str = str(dp.build_summary_card(r))
    assert "27/29 成功" in card_str
    assert "广州" in card_str and "冀北" in card_str
    assert "inner_mongolia" in card_str
    assert r.has_alerts is True


def test_summary_card_no_alerts_when_all_success():
    import services.hermes.data_patrol as dp
    ops = dp.IngestOpsSummary(
        available=True, lf_total=29, lf_success=29, lf_failed=[],
        capture_status="success",
    )
    r = dp.PatrolReport(sources=[], kb_summaries=[], ingest_ops=ops)
    card_str = str(dp.build_summary_card(r))
    assert "29/29 成功" in card_str
    assert r.has_alerts is False


def test_detail_card_lists_failed_markets():
    import services.hermes.data_patrol as dp
    ops = dp.IngestOpsSummary(
        available=True, lf_total=3, lf_success=1,
        lf_failed=[("广州", "Failed chunks: [(2026-09-11, 2026-09-12)]")],
        lf_date_range="2026-09-11→2026-09-12",
        capture_status="success", capture_date_range="2026-09-11→2026-09-12",
    )
    r = dp.PatrolReport(sources=[], kb_summaries=[], ingest_ops=ops)
    card_str = str(dp.build_detail_card(r))
    assert "采集进度" in card_str
    assert "1/3 成功" in card_str
    assert "✗ 广州: Failed chunks" in card_str
    assert "Capture  success" in card_str


# ── KB province institutional freshness (Group F) ────────────────────────────

def _conn_fetchone_seq(seq):
    """Mock conn whose cursor fetchone() returns items from seq in order."""
    cur = MagicMock()
    cur.fetchone.side_effect = seq
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    return _make_conn(cur), cur


def test_kb_province_freshness_missing_stale_fresh(monkeypatch):
    import services.hermes.data_patrol as dp
    today = date(2026, 10, 15)
    conn, cur = _conn_fetchone_seq([
        (None,),                                  # 安徽 — never any official doc
        (date(2026, 3, 1),),                      # 山东 — 228 days old → stale
        (date(2026, 10, 10),),                    # 广东 — 5 days old → fresh, no row
    ])
    monkeypatch.setattr("psycopg2.connect", lambda url: conn)
    rows = dp.check_kb_province_freshness(
        "pg", provinces=["安徽", "山东", "广东"], today=today)
    by_name = {s.name: s for s in rows}
    assert set(by_name) == {"安徽 制度文档", "山东 制度文档"}       # fresh province absent
    assert by_name["安徽 制度文档"].status == "missing"
    assert by_name["山东 制度文档"].status == "stale"
    assert by_name["山东 制度文档"].days_behind == 228
    assert all(s.group == "kb_province" for s in rows)
    # read-only, active+done docs only, province column OR title fallback
    sql, params = cur.execute.call_args[0][0], cur.execute.call_args[0][1]
    assert "active" in sql and "ingest_status = 'done'" in sql
    assert "title LIKE" in sql and "province =" in sql
    assert "INSERT" not in sql and "UPDATE" not in sql and "DELETE" not in sql
    assert params[-2] == "广东" and params[-1] == "%广东%"


def test_kb_province_freshness_db_down_returns_empty(monkeypatch):
    import services.hermes.data_patrol as dp
    monkeypatch.setattr("psycopg2.connect",
                        MagicMock(side_effect=psycopg2.OperationalError("down")))
    assert dp.check_kb_province_freshness("pg", provinces=["安徽"]) == []


def test_summary_card_kb_province_line(monkeypatch):
    import services.hermes.data_patrol as dp
    rows = [
        dp.SourceStatus(name="安徽 制度文档", table="staging.spot_knowledge_docs",
                        last_date=None, days_behind=9999, status="missing", group="kb_province"),
        dp.SourceStatus(name="山东 制度文档", table="staging.spot_knowledge_docs",
                        last_date=date(2026, 3, 1), days_behind=228, status="stale", group="kb_province"),
    ]
    r = dp.PatrolReport(sources=rows, kb_summaries=[], ingest_ops=None)
    card_str = str(dp.build_summary_card(r))
    assert "KB省份覆盖" in card_str
    assert "1 省缺制度文档" in card_str and "1 省超90天" in card_str
    assert r.has_alerts is True


def test_detail_card_kb_province_section_capped():
    import services.hermes.data_patrol as dp
    rows = [
        dp.SourceStatus(name=f"省{i:02d} 制度文档", table="staging.spot_knowledge_docs",
                        last_date=date(2026, 1, 1), days_behind=100 + i,
                        status="stale", group="kb_province")
        for i in range(15)
    ]
    r = dp.PatrolReport(sources=rows, kb_summaries=[], ingest_ops=None)
    card_str = str(dp.build_detail_card(r))
    assert "KB 省份制度覆盖" in card_str
    shown = sum(1 for i in range(15) if f"省{i:02d}" in card_str)
    assert shown == 12                       # capped
    assert "另 3 项" in card_str
