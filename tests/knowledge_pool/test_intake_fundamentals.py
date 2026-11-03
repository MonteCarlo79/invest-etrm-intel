# tests/knowledge_pool/test_intake_fundamentals.py
"""fundamentals_stat intake route — registry-first parse + draft writer.

Spec: docs/superpowers/specs/2026-11-02-market-fundamentals-tab-design.md (write path 2).
- Registered workbook formats (安徽月报数据 / 装机-月 / 交接表) are parsed by the
  deterministic parser registry BEFORE any vision call (数值零误读).
- Vision-proposed fundamentals_stat rows land as status='draft' rows in
  marketdata.province_fundamentals_monthly; a confirmed/superseded row under the
  same natural key is never touched (_write_rate_draft EXISTS skip-guard semantics).

All DB access mocked (fake-conn pattern from tests/province_fundamentals/test_store.py);
no real DB is touched.
"""
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import psycopg2
import psycopg2.extras
import pytest

from services.knowledge_pool import intake_extract as ie
from services.knowledge_pool import intake_routes as ir

PF_FIXTURES = (Path(__file__).resolve().parents[1]
               / "province_fundamentals" / "fixtures")
ANHUI_FIXTURE = "安徽电力市场信息披露月报数据-截至2026年5月.xlsx"


# ── fake-DB fixture (same pattern as tests/province_fundamentals/test_store.py) ──

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
    """Patch psycopg2.connect + execute_values; pin the writer's pg_url.

    Returns (cursor, ev_calls); ev_calls collects (cursor, sql, batch_rows).
    fetchone side_effect order: per-row EXISTS skip-guard checks first, then
    store.write_rows' per-row current-confirmed lookups for the survivors.
    """
    cur = _make_cursor()
    conn = _make_conn(cur)
    monkeypatch.setattr(psycopg2, "connect", lambda url, **kw: conn)
    monkeypatch.setattr(ir, "_pg_url", lambda: "postgresql://test")
    ev_calls = []

    def _spy(c, sql, batch, **kwargs):
        ev_calls.append((c, sql, [tuple(r) for r in batch]))

    monkeypatch.setattr(psycopg2.extras, "execute_values", _spy)
    return cur, ev_calls


# ── fake vision client (same shape as tests/knowledge_pool/test_intake_extract.py) ──

def _tool_resp(payload, stop_reason="end_turn"):
    block = type("B", (), {"type": "tool_use", "name": "submit_extraction",
                           "input": payload})()
    return type("R", (), {"content": [block], "stop_reason": stop_reason})()


class _FakeClient:
    def __init__(self, resp): self._resp = resp; self.calls = []
    @property
    def messages(self): return self
    def create(self, **kwargs): self.calls.append(kwargs); return self._resp


_CANNED_FUND = {
    "title": "安徽2026年6月电力基本面",
    "province": "安徽",
    "category": "market_intel",
    "summary": "安徽6月全社会用电量同比+8%，装机6831万千瓦。",
    "pages": [{"page_no": 1, "text": "第一页转录"}],
    "routes": [
        {"type": "fundamentals_stat", "province": "安徽",
         "content": "6月用电量288.5亿千瓦时，同比+8%",
         "structured": {"rows": [
             {"month": "2026-06-01", "period": "month", "section": "demand",
              "metric": "demand_total", "value": 288.5, "unit": "亿千瓦时"},
             {"month": "2026-06-01", "period": "month", "section": "demand",
              "metric": "demand_yoy", "value": 8.0, "unit": "%"},
         ]}},
    ],
}


# ── 1. route type registration + prompt guidance ───────────────────────────────

def test_route_type_registered_everywhere():
    assert "fundamentals_stat" in ie.ROUTE_TYPES
    props = ie._EXTRACTION_TOOL["input_schema"]["properties"]
    enum = props["routes"]["items"]["properties"]["type"]["enum"]
    assert "fundamentals_stat" in enum


def test_prompt_guides_fundamentals_routing():
    """Model must be told: 装机/发电量/利用小时/用电 → fundamentals_stat, with the
    section/metric/period vocabulary the writer and the long table expect."""
    tmpl = ie._PROMPT_TMPL
    for token in ("fundamentals_stat",
                  "capacity", "generation", "util_hours", "demand",
                  "cap_total", "gen_total", "hours_total", "demand_total",
                  "period", "month"):
        assert token in tmpl, token
    # division of labour with the existing storage-pipeline route
    assert "pipeline_stat" in tmpl


def test_extract_batch_keeps_fundamentals_route():
    client = _FakeClient(_tool_resp(_CANNED_FUND))
    pages = [ie.Page(filename="IMG_1.jpg", data=b"\xff\xd8img1", kind="image")]
    prop = ie.extract_batch(pages, api_key="k", _client=client)
    assert [r.type for r in prop.routes] == ["fundamentals_stat"]
    rows = prop.routes[0].structured["rows"]
    assert rows[0]["section"] == "demand" and rows[0]["metric"] == "demand_total"


# ── 2. write_fundamentals_draft — draft writes + skip-guard ────────────────────

def test_draft_rows_written_with_draft_status(fake_db):
    cur, ev_calls = fake_db
    cur.fetchone.side_effect = [(False,), (False,), None, None]  # EXISTS ×2 → pre-check ×2
    rows = [
        {"month": "2026-06-01", "period": "month", "section": "demand",
         "metric": "demand_total", "value": 288.5, "unit": "亿千瓦时"},
        {"month": "2026-06-01", "period": "month", "section": "capacity",
         "metric": "cap_total", "value": 6831.49, "unit": "万千瓦",
         "source": "安徽2026年6月月报.xlsx"},
    ]
    out = ir.write_fundamentals_draft(province="安徽", rows=rows, source="intake:test")
    assert out == "written 2, skipped 0"
    assert len(ev_calls) == 1
    sql, batch = ev_calls[0][1], ev_calls[0][2]
    assert "INSERT INTO marketdata.province_fundamentals_monthly" in sql
    assert all(t[8] == "draft" for t in batch)                    # status col
    assert all(t[0] == "安徽" and t[1] == date(2026, 6, 1) for t in batch)
    by_metric = {t[4]: t for t in batch}
    # source fallback: row without source uses the writer arg; row source wins otherwise
    assert by_metric["demand_total"][7] == "intake:test"
    assert by_metric["cap_total"][7] == "安徽2026年6月月报.xlsx"
    assert by_metric["demand_total"][6] == "亿千瓦时"


def test_confirmed_row_never_overwritten_by_draft(fake_db):
    """Step-1 acceptance: skip-guard — a confirmed row under the same natural key
    suppresses the draft write for that row (source-agnostic EXISTS, mirroring
    _write_rate_draft); other rows in the batch still land."""
    cur, ev_calls = fake_db
    cur.fetchone.side_effect = [(True,), (False,), None]
    rows = [
        {"month": "2026-06-01", "period": "month", "section": "demand",
         "metric": "demand_total", "value": 999.9, "unit": "亿千瓦时"},   # confirmed twin → skip
        {"month": "2026-06-01", "period": "month", "section": "demand",
         "metric": "demand_yoy", "value": 8.0, "unit": "%"},              # new key → draft
    ]
    out = ir.write_fundamentals_draft(province="安徽", rows=rows, source="intake:test")
    assert out == "written 1, skipped 1"
    batch = ev_calls[0][2]
    assert len(batch) == 1 and batch[0][4] == "demand_yoy"
    # guard SQL: natural key + status filter, and NO source predicate — a confirmed
    # row from ANY source blocks the draft (not just same-source twins)
    exists_sql = cur.execute.call_args_list[0].args[0]
    assert "province_fundamentals_monthly" in exists_sql
    assert "status IN ('confirmed','superseded')" in exists_sql
    assert "source" not in exists_sql


def test_all_confirmed_means_no_insert(fake_db):
    cur, ev_calls = fake_db
    cur.fetchone.side_effect = [(True,)]
    rows = [{"month": "2026-06-01", "period": "month", "section": "demand",
             "metric": "demand_total", "value": 288.5, "unit": "亿千瓦时"}]
    out = ir.write_fundamentals_draft(province="安徽", rows=rows, source="intake:test")
    assert out == "written 0, skipped 1"
    assert ev_calls == []


def test_malformed_rows_counted_not_written(fake_db):
    cur, ev_calls = fake_db
    cur.fetchone.side_effect = [(False,), None]
    rows = [
        {"month": "2026-06-01", "section": "demand", "metric": "demand_total",
         "value": 288.5, "unit": "亿千瓦时"},                       # ok (period defaults)
        {"month": "2026-06-01", "section": "demand", "value": 8.0,
         "unit": "%"},                                               # missing metric
        {"section": "demand", "metric": "demand_yoy", "value": 8.0,
         "unit": "%"},                                               # missing month
    ]
    out = ir.write_fundamentals_draft(province="安徽", rows=rows, source="x")
    assert "written 1" in out and "malformed 2" in out
    assert ev_calls[0][2][0][2] == "month"                           # period default


def test_month_and_period_normalization(fake_db):
    cur, ev_calls = fake_db
    cur.fetchone.side_effect = [(False,), (False,), None, None]
    rows = [
        {"month": "2026-06", "period": "month", "section": "demand",   # bare YYYY-MM
         "metric": "demand_total", "value": 288.5, "unit": "亿千瓦时"},
        {"month": "2026-01-01", "period": "year", "section": "demand",  # annual row
         "metric": "demand_total", "value": 3214.5, "unit": "亿千瓦时"},
    ]
    out = ir.write_fundamentals_draft(province="安徽", rows=rows, source="x")
    assert out == "written 2, skipped 0"
    batch = sorted(ev_calls[0][2], key=lambda t: t[2])
    assert batch[0][1:3] == (date(2026, 6, 1), "month")
    assert batch[1][1:3] == (date(2026, 1, 1), "year")


def test_empty_rows_no_db_touch(monkeypatch):
    monkeypatch.setattr(ir, "_pg_url", lambda: "postgresql://test")
    def _boom(url, **kw):
        raise AssertionError("no connection expected for empty rows")
    monkeypatch.setattr(psycopg2, "connect", _boom)
    assert ir.write_fundamentals_draft(province="安徽", rows=[], source="x") == \
        "written 0, skipped 0"


def test_vision_proposal_rows_flow_into_writer(fake_db):
    """End-to-end (mocked DB): vision proposal → fundamentals_stat route → drafts."""
    cur, ev_calls = fake_db
    cur.fetchone.side_effect = [(False,), (False,), None, None]
    client = _FakeClient(_tool_resp(_CANNED_FUND))
    prop = ie.extract_batch([ie.Page(filename="a.jpg", data=b"x", kind="image")],
                            api_key="k", _client=client)
    route = prop.routes[0]
    out = ir.write_fundamentals_draft(province=route.province,
                                      rows=route.structured["rows"],
                                      source=f"intake:{prop.title[:60]}")
    assert out == "written 2, skipped 0"
    assert all(t[8] == "draft" for t in ev_calls[0][2])


# ── 3. registry-first workbook parsing ─────────────────────────────────────────

def test_registry_parse_hits_anhui_fixture():
    data = (PF_FIXTURES / ANHUI_FIXTURE).read_bytes()
    fmt, rows = ie.try_registry_parse(ANHUI_FIXTURE, data)
    assert fmt == "format_a"
    assert rows, "fixture must parse non-empty"
    jan = [r for r in rows if r["month"] == "2025-01-01" and r["metric"] == "cap_total"]
    assert len(jan) == 1
    assert jan[0]["value"] == pytest.approx(6831.49)
    assert jan[0]["province"] == "安徽"          # province sniffed from the filename
    assert jan[0]["unit"] == "万千瓦"
    assert jan[0]["period"] == "month"
    assert jan[0]["section"] == "capacity"
    assert jan[0]["source"].endswith(".xlsx")


def test_registry_parse_unknown_workbook_returns_empty():
    """未命中注册格式 → (None, []) — caller falls back to the vision path."""
    import io
    import openpyxl
    wb = openpyxl.Workbook()
    wb.active.title = "sheet1"
    wb.active.append(["x"])
    buf = io.BytesIO()
    wb.save(buf)
    fmt, rows = ie.try_registry_parse("安徽-乱七八糟.xlsx", buf.getvalue())
    assert fmt is None
    assert rows == []


def test_registry_parse_garbage_bytes_returns_empty():
    fmt, rows = ie.try_registry_parse("安徽月报.xlsx", b"not a workbook")
    assert fmt is None
    assert rows == []
