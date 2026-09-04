# Data Patrol Agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a scheduled + on-demand data patrol agent in Hermes that checks all platform data sources for staleness/gaps, delivers a tiered Feishu card, sends upload reminders for manual items, and lets users fill monthly data gaps (capcomp / FR / installed / sysopfee) by typing values or uploading a file for AI extraction — plus a gap heatmap in the bess-map Cap Comp + FR tab.

**Architecture:** Single new module `services/hermes/data_patrol.py` following the existing screener pattern (`news_screener.py`, `capcomp_screener.py`). Wired into `services/hermes/app.py` for scheduling (00:35 UTC) and command handling (`/datacheck`, `/巡视`). Two new HTTP endpoints. `capcomp_manual_etl.py` extended with DOCX/PPT/TXT extraction + a unified gap-fill extractor. `apps/bess-map/app.py` gets a Data Gaps expander at the bottom of the Cap Comp + FR Market tab.

**Tech Stack:** Python 3.11, psycopg2, FastAPI, APScheduler, anthropic SDK (claude-haiku-4-5-20251001), pdfplumber, pandas, python-docx, python-pptx, Streamlit + Plotly (bess-map), pytest + unittest.mock

---

## File Map

| Action | File | Responsibility |
|--------|------|---------------|
| **Create** | `services/hermes/data_patrol.py` | All check functions, PatrolReport dataclass, Feishu card builders, `run_patrol()` entry point |
| **Modify** | `services/hermes/app.py` | Add `_pending_gap_fill` session dict (line ~29), import data_patrol, scheduler job (after line 711), `/datacheck` command handler, `POST /hermes/patrol` + `POST /hermes/patrol/fill` + `GET /hermes/patrol/status` endpoints, file-upload gap-fill handler |
| **Modify** | `services/hermes/capcomp_manual_etl.py` | Extend `_text_from_bytes()` with DOCX/PPT/TXT; add `extract_from_file_for_gap()` dispatcher |
| **Create** | `tests/hermes/test_data_patrol.py` | Unit tests for all check functions and card builders |
| **Modify** | `apps/bess-map/app.py` | Add `st.expander("📋 Data Gaps")` at bottom of `tab_aux` block (before line 2853) |

---

## Task 1: Core data structures and DB check helpers

**Files:**
- Create: `services/hermes/data_patrol.py`
- Create: `tests/hermes/test_data_patrol.py`

- [ ] **Step 1.1: Write failing tests for SourceStatus and PatrolReport dataclasses**

```python
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
```

- [ ] **Step 1.2: Run test to verify it fails**

```
cd C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform
python -m pytest tests/hermes/test_data_patrol.py -v 2>&1 | head -20
```
Expected: `ModuleNotFoundError: No module named 'services.hermes.data_patrol'`

- [ ] **Step 1.3: Create `services/hermes/data_patrol.py` with data structures**

```python
# services/hermes/data_patrol.py
"""
Data Patrol Agent
=================
Checks all platform data sources for staleness/gaps and delivers
a tiered Feishu report. Follows the pattern of news_screener.py.

Entry point:
    run_patrol(pg_url, feishu, owner_open_id, api_key) -> PatrolReport
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Literal, Optional

logger = logging.getLogger(__name__)

_BJ = timezone(timedelta(hours=8))
_DAILY_STALE_DAYS = 2       # flag auto/manual daily data if > 2 days behind
_MONTHLY_FLAG_DAY = 10      # flag missing monthly data after 10th of following month


@dataclass
class SourceStatus:
    name: str
    table: str
    last_date: Optional[date]
    days_behind: int
    status: Literal["fresh", "stale", "missing"]
    group: Literal["auto", "manual", "monthly"]
    reminder_text: str = ""   # non-empty → send separate upload reminder
    fill_table: str = ""      # non-empty → show 填入数据 button in detail card
    fill_province: str = ""
    fill_month: str = ""      # YYYY-MM


@dataclass
class KBSummary:
    name: str
    table: str
    last_ingested: Optional[date]
    count_7d: int
    count_30d: int


@dataclass
class PatrolReport:
    sources: list[SourceStatus]
    kb_summaries: list[KBSummary]
    generated_at: datetime = field(default_factory=lambda: datetime.now(_BJ))

    def count_by_status(self, status: str) -> int:
        return sum(1 for s in self.sources if s.status == status)

    def by_group(self, group: str) -> list[SourceStatus]:
        return [s for s in self.sources if s.group == group]

    @property
    def has_alerts(self) -> bool:
        return any(s.status in ("stale", "missing") for s in self.sources)


def _days_behind(last_date: Optional[date]) -> int:
    if last_date is None:
        return 9999
    today = datetime.now(_BJ).date()
    return max(0, (today - last_date).days)


def _classify_daily(days: int) -> Literal["fresh", "stale", "missing"]:
    if days == 9999:
        return "missing"
    if days > _DAILY_STALE_DAYS:
        return "stale"
    return "fresh"
```

- [ ] **Step 1.4: Run tests to verify they pass**

```
python -m pytest tests/hermes/test_data_patrol.py -v
```
Expected: 5 tests PASS.

- [ ] **Step 1.5: Commit**

```bash
git add services/hermes/data_patrol.py tests/hermes/test_data_patrol.py
git commit -m "feat(patrol): add PatrolReport dataclasses and _days_behind helper"
```

---

## Task 2: Group A — Auto pipeline checks

**Files:**
- Modify: `services/hermes/data_patrol.py`
- Modify: `tests/hermes/test_data_patrol.py`

- [ ] **Step 2.1: Write failing tests for check_auto_pipelines**

Add to `tests/hermes/test_data_patrol.py`:

```python
from unittest.mock import MagicMock, patch, call
from datetime import date, timedelta
import psycopg2


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
```

- [ ] **Step 2.2: Run to verify failure**

```
python -m pytest tests/hermes/test_data_patrol.py::test_check_auto_pipelines_fresh -v
```
Expected: `AttributeError: module 'services.hermes.data_patrol' has no attribute 'check_auto_pipelines'`

- [ ] **Step 2.3: Implement `check_auto_pipelines` in `data_patrol.py`**

Append after the `_classify_daily` function:

```python
# ── Group A: Auto pipeline freshness checks ───────────────────────────────────

_AUTO_SOURCES = [
    # (display_name, table, date_col, extra_where)
    ("LingFeng 基本面 (29省)", "marketdata.spot_fundamentals_hourly", "datetime::date", ""),
    ("LingFeng 现货价格 (29省)", "marketdata.spot_prices_hourly", "datetime::date", ""),
    ("Canon 日内出清", "marketdata.md_id_cleared_energy", "data_date", ""),
    ("Canon 日前出清", "marketdata.md_da_cleared_energy", "data_date", ""),
    ("Canon RT节点电价", "marketdata.md_rt_nodal_price", "data_date", ""),
    ("BESS捕获率日数据", "marketdata.bess_capture_daily", "trade_date", ""),
    ("GB Elexon结算", "intl_market.gb_elexon_sp", "settlement_date", ""),
    ("GB风电预测", "intl_market.gb_wind_forecast", "start_time::date", ""),
]

_MENGXI_HIST_TABLES = [
    "hist_mengxi_provincerealtimeclearprice_15min",
    "hist_mengxi_newenergyreal_15min",
    "hist_mengxi_windpowerreal_15min",
    "hist_mengxi_solarpowerreal_15min",
    "hist_mengxi_loadregulationreal_15min",
    "hist_mengxi_biddingspacereal_15min",
]

_FENGXING_TABLES = [
    ("marketdata.md_shanxi_nodal_price_96", "data_date"),
]


def check_auto_pipelines(pg_url: str) -> list[SourceStatus]:
    """Query max date for each auto-scheduled data source."""
    import psycopg2
    results: list[SourceStatus] = []

    def _query_max(cur, table: str, date_col: str, extra: str = "") -> Optional[date]:
        try:
            where = f"WHERE {extra}" if extra else ""
            cur.execute(f"SELECT MAX({date_col}) FROM {table} {where}")
            row = cur.fetchall()
            val = row[0][0] if row else None
            if val is None:
                return None
            return val.date() if hasattr(val, "date") else val
        except Exception as exc:
            logger.warning("patrol query failed for %s: %s", table, exc)
            return None

    try:
        conn = psycopg2.connect(pg_url)
        with conn:
            with conn.cursor() as cur:
                # Standard auto sources
                for name, table, date_col, extra in _AUTO_SOURCES:
                    last = _query_max(cur, table, date_col, extra)
                    days = _days_behind(last)
                    results.append(SourceStatus(
                        name=name, table=table, last_date=last,
                        days_behind=days, status=_classify_daily(days), group="auto",
                    ))

                # Mengxi hist tables (report as group — show worst)
                mengxi_dates = []
                for tbl in _MENGXI_HIST_TABLES:
                    d = _query_max(cur, f"public.{tbl}", "time::date")
                    mengxi_dates.append(d)
                mengxi_last = max((d for d in mengxi_dates if d), default=None)
                days = _days_behind(mengxi_last)
                results.append(SourceStatus(
                    name="蒙西 hist_* 实时数据", table="public.hist_mengxi_*",
                    last_date=mengxi_last, days_behind=days,
                    status=_classify_daily(days), group="auto",
                ))

                # Fengxing nodal tables
                for table, date_col in _FENGXING_TABLES:
                    last = _query_max(cur, table, date_col)
                    days = _days_behind(last)
                    results.append(SourceStatus(
                        name=f"丰行节点电价 ({table.split('.')[-1]})",
                        table=table, last_date=last, days_behind=days,
                        status=_classify_daily(days), group="auto",
                    ))
        conn.close()
    except Exception as exc:
        logger.error("check_auto_pipelines: DB unavailable: %s", exc)
        # Return all missing on total DB failure
        for name, table, _, _ in _AUTO_SOURCES:
            results.append(SourceStatus(
                name=name, table=table, last_date=None,
                days_behind=9999, status="missing", group="auto",
            ))
    return results
```

- [ ] **Step 2.4: Run tests**

```
python -m pytest tests/hermes/test_data_patrol.py -k "auto_pipeline" -v
```
Expected: 3 tests PASS.

- [ ] **Step 2.5: Commit**

```bash
git add services/hermes/data_patrol.py tests/hermes/test_data_patrol.py
git commit -m "feat(patrol): add check_auto_pipelines for Group A sources"
```

---

## Task 3: Group B + C — Manual upload and monthly data checks

**Files:**
- Modify: `services/hermes/data_patrol.py`
- Modify: `tests/hermes/test_data_patrol.py`

- [ ] **Step 3.1: Write failing tests**

Add to `tests/hermes/test_data_patrol.py`:

```python
def test_check_manual_uploads_stale(monkeypatch):
    from datetime import timedelta
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
    # Return no rows — means no data for current month
    cur = _make_cursor([])
    conn = _make_conn(cur)
    monkeypatch.setattr("psycopg2.connect", lambda url: conn)

    from services.hermes.data_patrol import check_monthly_data
    results = check_monthly_data("postgresql://test")
    fill_targets = [s for s in results if s.fill_table]
    assert len(fill_targets) > 0
```

- [ ] **Step 3.2: Run to verify failure**

```
python -m pytest tests/hermes/test_data_patrol.py -k "manual_uploads or monthly_data" -v
```
Expected: `AttributeError`

- [ ] **Step 3.3: Implement `check_manual_uploads` and `check_monthly_data`**

Append to `services/hermes/data_patrol.py`:

```python
# ── Group B: Manual upload checks ────────────────────────────────────────────

def check_manual_uploads(pg_url: str) -> list[SourceStatus]:
    """Check manually-uploaded daily data sources."""
    import psycopg2
    results: list[SourceStatus] = []
    try:
        conn = psycopg2.connect(pg_url)
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(report_date) FROM spot_daily")
                row = cur.fetchall()
                last = row[0][0] if row and row[0][0] else None
                if last and hasattr(last, "date"):
                    last = last.date()
                days = _days_behind(last)
                today = datetime.now(_BJ).date()
                reminder = (
                    f"请上传 电力现货市场价格与运行日报-{today.strftime('%Y%m%d')}.pdf"
                    if days > _DAILY_STALE_DAYS else ""
                )
                results.append(SourceStatus(
                    name="现货日报 PDF",
                    table="spot_daily",
                    last_date=last,
                    days_behind=days,
                    status=_classify_daily(days),
                    group="manual",
                    reminder_text=reminder,
                ))
        conn.close()
    except Exception as exc:
        logger.error("check_manual_uploads: %s", exc)
        results.append(SourceStatus(
            name="现货日报 PDF", table="spot_daily",
            last_date=None, days_behind=9999, status="missing", group="manual",
            reminder_text="请上传 电力现货市场价格与运行日报.pdf",
        ))
    return results


# ── Group C: Monthly data checks ─────────────────────────────────────────────

_MONTHLY_FILL_SOURCES = [
    # (display_name, table, year_month_col, fill_table)
    ("容量补偿", "marketdata.province_cap_comp", "EXTRACT(year FROM effective_date)::int * 100 + EXTRACT(month FROM effective_date)::int", "province_cap_comp"),
    ("调频市场", "marketdata.province_fr_market", "EXTRACT(year FROM effective_date)::int * 100 + EXTRACT(month FROM effective_date)::int", "province_fr_market"),
    ("储能装机容量", "province_installed_monthly", "year_month", "province_installed_monthly"),
    ("系统运行费", "province_sysopfee_monthly", "year_month", "province_sysopfee_monthly"),
]

_EXCHANGE_REPORT_PROVINCES = 29  # expected province folders


def check_monthly_data(pg_url: str) -> list[SourceStatus]:
    """Check monthly data tables for current-month coverage."""
    import psycopg2
    results: list[SourceStatus] = []
    now = datetime.now(_BJ)
    today = now.date()
    # Only flag if we're past the 10th of the month following the target
    # Target month = previous calendar month
    if today.month == 1:
        target_year, target_month = today.year - 1, 12
    else:
        target_year, target_month = today.year, today.month - 1
    flag_date = date(today.year, today.month, _MONTHLY_FLAG_DAY)
    should_flag = today >= flag_date

    try:
        conn = psycopg2.connect(pg_url)
        with conn:
            with conn.cursor() as cur:
                for name, table, ym_col, fill_table in _MONTHLY_FILL_SOURCES:
                    try:
                        target_ym = target_year * 100 + target_month
                        cur.execute(
                            f"SELECT COUNT(*) FROM {table} WHERE {ym_col} = %s",
                            (target_ym,)
                        )
                        row = cur.fetchall()
                        count = row[0][0] if row else 0
                        if count == 0 and should_flag:
                            status = "missing"
                        else:
                            status = "fresh"
                        results.append(SourceStatus(
                            name=f"{name} ({target_year}-{target_month:02d})",
                            table=table,
                            last_date=None if count == 0 else date(target_year, target_month, 1),
                            days_behind=0 if status == "fresh" else 30,
                            status=status,
                            group="monthly",
                            fill_table=fill_table if status == "missing" else "",
                            fill_month=f"{target_year}-{target_month:02d}" if status == "missing" else "",
                        ))
                    except Exception as exc:
                        logger.warning("monthly check failed for %s: %s", table, exc)

                # Exchange monthly reports — check via DB
                try:
                    target_ym_str = f"{target_year}-{target_month:02d}"
                    cur.execute(
                        "SELECT COUNT(DISTINCT province) FROM staging.exchange_monthly_reports "
                        "WHERE TO_CHAR(report_month, 'YYYY-MM') = %s",
                        (target_ym_str,)
                    )
                    row = cur.fetchall()
                    found = row[0][0] if row else 0
                    missing = _EXCHANGE_REPORT_PROVINCES - found
                    status = "missing" if (missing > 0 and should_flag) else "fresh"
                    results.append(SourceStatus(
                        name=f"交易所月报 ({target_ym_str}, {found}/{_EXCHANGE_REPORT_PROVINCES}省)",
                        table="staging.exchange_monthly_reports",
                        last_date=None if found == 0 else date(target_year, target_month, 1),
                        days_behind=0 if status == "fresh" else 30,
                        status=status,
                        group="monthly",
                    ))
                except Exception as exc:
                    logger.warning("exchange reports check failed: %s", exc)
        conn.close()
    except Exception as exc:
        logger.error("check_monthly_data: DB unavailable: %s", exc)
    return results
```

- [ ] **Step 3.4: Run tests**

```
python -m pytest tests/hermes/test_data_patrol.py -k "manual_uploads or monthly_data" -v
```
Expected: 2 tests PASS.

- [ ] **Step 3.5: Commit**

```bash
git add services/hermes/data_patrol.py tests/hermes/test_data_patrol.py
git commit -m "feat(patrol): add check_manual_uploads and check_monthly_data"
```

---

## Task 4: Group D — KB activity summary

**Files:**
- Modify: `services/hermes/data_patrol.py`
- Modify: `tests/hermes/test_data_patrol.py`

- [ ] **Step 4.1: Write failing test**

Add to `tests/hermes/test_data_patrol.py`:

```python
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
```

- [ ] **Step 4.2: Run to verify failure**

```
python -m pytest tests/hermes/test_data_patrol.py::test_check_kb_activity_returns_summaries -v
```
Expected: `AttributeError`

- [ ] **Step 4.3: Implement `check_kb_activity`**

Append to `services/hermes/data_patrol.py`:

```python
# ── Group D: KB activity ──────────────────────────────────────────────────────

_KB_TABLES = [
    ("Spot KB", "staging.spot_knowledge_docs", "created_at"),
    ("GB KB", "intl_market.gb_knowledge_docs", "created_at"),
    ("AU KB", "intl_market.au_knowledge_docs", "created_at"),
    ("PH KB", "intl_market.ph_knowledge_docs", "created_at"),
    ("PO KB", "intl_market.po_knowledge_docs", "created_at"),
]


def check_kb_activity(pg_url: str) -> list[KBSummary]:
    """Count KB docs ingested in last 7 and 30 days."""
    import psycopg2
    results: list[KBSummary] = []
    try:
        conn = psycopg2.connect(pg_url)
        with conn:
            with conn.cursor() as cur:
                for name, table, ts_col in _KB_TABLES:
                    try:
                        cur.execute(f"""
                            SELECT
                                MAX({ts_col})::date,
                                COUNT(*) FILTER (WHERE {ts_col} >= NOW() - INTERVAL '7 days'),
                                COUNT(*) FILTER (WHERE {ts_col} >= NOW() - INTERVAL '30 days')
                            FROM {table}
                        """)
                        row = cur.fetchall()
                        if row and row[0][0] is not None:
                            last_raw = row[0][0]
                            last = last_raw.date() if hasattr(last_raw, "date") else last_raw
                            results.append(KBSummary(
                                name=name, table=table, last_ingested=last,
                                count_7d=int(row[0][1] or 0),
                                count_30d=int(row[0][2] or 0),
                            ))
                        else:
                            results.append(KBSummary(
                                name=name, table=table, last_ingested=None,
                                count_7d=0, count_30d=0,
                            ))
                    except Exception as exc:
                        logger.warning("KB activity check failed for %s: %s", table, exc)
        conn.close()
    except Exception as exc:
        logger.error("check_kb_activity: DB unavailable: %s", exc)
    return results
```

- [ ] **Step 4.4: Run test**

```
python -m pytest tests/hermes/test_data_patrol.py::test_check_kb_activity_returns_summaries -v
```
Expected: PASS.

- [ ] **Step 4.5: Commit**

```bash
git add services/hermes/data_patrol.py tests/hermes/test_data_patrol.py
git commit -m "feat(patrol): add check_kb_activity for Group D"
```

---

## Task 5: Feishu card builders and `run_patrol` entry point

**Files:**
- Modify: `services/hermes/data_patrol.py`
- Modify: `tests/hermes/test_data_patrol.py`

- [ ] **Step 5.1: Write failing tests for card builders**

Add to `tests/hermes/test_data_patrol.py`:

```python
def test_build_summary_card_green_when_all_fresh():
    from services.hermes.data_patrol import build_summary_card, SourceStatus, KBSummary, PatrolReport
    s = SourceStatus(name="x", table="t", last_date=date.today(), days_behind=0, status="fresh", group="auto")
    r = PatrolReport(sources=[s], kb_summaries=[])
    card = build_summary_card(r)
    assert card["header"]["template"] == "green"
    assert "elements" in card


def test_build_summary_card_orange_when_stale():
    from services.hermes.data_patrol import build_summary_card, SourceStatus, KBSummary, PatrolReport
    from datetime import timedelta
    s = SourceStatus(name="x", table="t", last_date=date.today() - timedelta(days=5),
                     days_behind=5, status="stale", group="auto")
    r = PatrolReport(sources=[s], kb_summaries=[])
    card = build_summary_card(r)
    assert card["header"]["template"] == "orange"


def test_build_detail_card_contains_fill_button():
    from services.hermes.data_patrol import build_detail_card, SourceStatus, KBSummary, PatrolReport
    s = SourceStatus(name="容量补偿 2026-06", table="province_cap_comp",
                     last_date=None, days_behind=30, status="missing", group="monthly",
                     fill_table="province_cap_comp", fill_month="2026-06")
    r = PatrolReport(sources=[s], kb_summaries=[])
    card = build_detail_card(r)
    card_str = str(card)
    assert "填入数据" in card_str


def test_run_patrol_returns_report(monkeypatch):
    from services.hermes.data_patrol import run_patrol
    monkeypatch.setattr("services.hermes.data_patrol.check_auto_pipelines", lambda pg: [])
    monkeypatch.setattr("services.hermes.data_patrol.check_manual_uploads", lambda pg: [])
    monkeypatch.setattr("services.hermes.data_patrol.check_monthly_data", lambda pg: [])
    monkeypatch.setattr("services.hermes.data_patrol.check_kb_activity", lambda pg: [])
    from services.hermes.data_patrol import PatrolReport
    result = run_patrol("postgresql://test", feishu=None, owner_open_id="", api_key="")
    assert isinstance(result, PatrolReport)
```

- [ ] **Step 5.2: Run to verify failure**

```
python -m pytest tests/hermes/test_data_patrol.py -k "summary_card or detail_card or run_patrol" -v
```
Expected: `AttributeError`

- [ ] **Step 5.3: Implement card builders and `run_patrol`**

Append to `services/hermes/data_patrol.py`:

```python
# ── Feishu card builders ──────────────────────────────────────────────────────

_STATUS_ICON = {"fresh": "✅", "stale": "⚠️", "missing": "🔴"}
_WEEKDAYS_CN = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]


def build_summary_card(report: PatrolReport) -> dict:
    now = report.generated_at
    date_str = f"{now.year}年{now.month}月{now.day}日 {_WEEKDAYS_CN[now.weekday()]}"

    auto_sources = report.by_group("auto")
    manual_sources = report.by_group("manual")
    monthly_sources = report.by_group("monthly")

    auto_fresh = sum(1 for s in auto_sources if s.status == "fresh")
    manual_issues = sum(1 for s in manual_sources if s.status != "fresh")
    monthly_missing = sum(1 for s in monthly_sources if s.status == "missing")

    kb_line = ""
    if report.kb_summaries:
        total_7d = sum(k.count_7d for k in report.kb_summaries)
        now_bj = datetime.now(_BJ)
        if now_bj.weekday() == 0:
            kb_line = f"📊 知识库        本周新增 {total_7d} 篇"
        elif now_bj.day == 1:
            total_30d = sum(k.count_30d for k in report.kb_summaries)
            kb_line = f"📊 知识库        本月新增 {total_30d} 篇"

    lines = [
        f"✅ 自动管道      {auto_fresh}/{len(auto_sources)} 正常",
        f"{'⚠️' if manual_issues else '✅'} 手动上传      {'%d 项需关注' % manual_issues if manual_issues else '正常'}",
        f"{'🔴' if monthly_missing else '✅'} 月度数据      {'%d 项缺失' % monthly_missing if monthly_missing else '正常'}",
    ]
    if kb_line:
        lines.append(kb_line)

    body = "\n".join(lines)
    template = "orange" if report.has_alerts else "green"

    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": template,
            "title": {"content": f"📡 数据巡视报告 — {date_str}", "tag": "plain_text"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": body}},
            {"tag": "hr"},
            {"tag": "action", "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "展开详情 ▼"},
                 "type": "primary", "value": {"act": "patrol_expand"}},
                {"tag": "button", "text": {"tag": "plain_text", "content": "关闭"},
                 "type": "default", "value": {"act": "patrol_close"}},
            ]},
        ],
    }


def _source_row(s: SourceStatus) -> dict:
    """Build one div row for a SourceStatus, with optional 填入数据 button."""
    icon = _STATUS_ICON.get(s.status, "❓")
    last_str = str(s.last_date) if s.last_date else "—"
    behind_str = f" · 落后 {s.days_behind} 天" if s.days_behind not in (0, 9999) else ""
    label = f"{icon} **{s.name}**  最后: {last_str}{behind_str}"
    row: dict = {"tag": "div", "text": {"tag": "lark_md", "content": label}}
    if s.fill_table:
        row["extra"] = {
            "tag": "button",
            "text": {"tag": "plain_text", "content": "填入数据"},
            "type": "danger",
            "value": {
                "act": "patrol_fill_open",
                "fill_table": s.fill_table,
                "fill_province": s.fill_province,
                "fill_month": s.fill_month,
            },
        }
    elif s.reminder_text:
        row["extra"] = {
            "tag": "button",
            "text": {"tag": "plain_text", "content": "上传提醒"},
            "type": "warning",
            "value": {"act": "patrol_remind", "reminder": s.reminder_text},
        }
    return row


def build_detail_card(report: PatrolReport) -> dict:
    now = report.generated_at
    date_str = f"{now.year}年{now.month}月{now.day}日 {_WEEKDAYS_CN[now.weekday()]}"
    elements: list[dict] = []

    def _section(title: str, sources: list[SourceStatus]) -> None:
        if not sources:
            return
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**{title}**"}})
        for s in sources:
            elements.append(_source_row(s))
        elements.append({"tag": "hr"})

    _section("⚡ 自动管道", report.by_group("auto"))
    _section("📤 手动上传", report.by_group("manual"))
    _section("🗓 月度数据", report.by_group("monthly"))

    if report.kb_summaries:
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**📚 知识库活跃度**"}})
        for k in report.kb_summaries:
            last_str = str(k.last_ingested) if k.last_ingested else "—"
            elements.append({"tag": "div", "text": {"tag": "lark_md",
                "content": f"• {k.name}  最后入库: {last_str} · 7天 {k.count_7d}篇 · 30天 {k.count_30d}篇"}})
        elements.append({"tag": "hr"})

    elements.append({"tag": "action", "actions": [
        {"tag": "button", "text": {"tag": "plain_text", "content": "收起 ▲"},
         "type": "default", "value": {"act": "patrol_collapse"}},
    ]})

    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "orange" if report.has_alerts else "green",
            "title": {"content": f"📡 数据巡视详情 — {date_str}", "tag": "plain_text"},
        },
        "elements": elements,
    }


def build_fill_card(fill_table: str, fill_province: str, fill_month: str) -> dict:
    """Interactive card for manually entering a missing monthly value."""
    _TABLE_LABELS = {
        "province_cap_comp":      ("容量补偿", "cap_comp_yuan_kw", "容量补偿标准 (¥/kW·年)", "peak_duration_hours", "年最高净负荷峰值时段 (h)"),
        "province_fr_market":     ("调频市场", "fr_price_yuan_kw_h", "调频容量价格 (¥/kW·h)", "fr_pool_yi_yuan", "全省调频资金池 (亿元/年)"),
        "province_installed_monthly": ("储能装机", "installed_mw", "储能装机 (MW)", None, None),
        "province_sysopfee_monthly":  ("系统运行费", "fee_yuan_kwh", "系统运行费 (¥/kWh)", None, None),
    }
    label, field1, field1_label, field2, field2_label = _TABLE_LABELS.get(
        fill_table, (fill_table, "value", "数值", None, None)
    )
    actions = [
        {"tag": "input", "name": field1,
         "placeholder": {"tag": "plain_text", "content": field1_label}, "width": "fill"},
    ]
    if field2:
        actions.append(
            {"tag": "input", "name": field2,
             "placeholder": {"tag": "plain_text", "content": field2_label}, "width": "fill"}
        )
    actions.append({
        "tag": "button", "text": {"tag": "plain_text", "content": "提交"},
        "type": "primary",
        "value": {"act": "patrol_fill_submit",
                  "fill_table": fill_table, "fill_province": fill_province,
                  "fill_month": fill_month, "field1": field1, "field2": field2 or ""},
    })
    actions.append({
        "tag": "button", "text": {"tag": "plain_text", "content": "发文件给我，AI自动提取"},
        "type": "default",
        "value": {"act": "patrol_fill_file",
                  "fill_table": fill_table, "fill_province": fill_province,
                  "fill_month": fill_month},
    })
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "blue",
            "title": {"content": f"填写缺失数据 — {label} / {fill_province or '(选省份)'} / {fill_month}", "tag": "plain_text"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md",
                "content": f"省份: **{fill_province or '请输入'}**\n月份: **{fill_month}**"}},
            {"tag": "hr"},
            {"tag": "action", "actions": actions},
        ],
    }


# ── Entry point ───────────────────────────────────────────────────────────────

# Cache last patrol result in memory so /hermes/patrol/status can return it
_last_report: Optional[PatrolReport] = None


def run_patrol(
    pg_url: str,
    feishu,
    owner_open_id: str,
    api_key: str = "",
) -> PatrolReport:
    """
    Run all data checks, build and send the summary Feishu card,
    and send separate upload reminder messages for stale manual items.
    Returns the PatrolReport for HTTP callers.
    """
    global _last_report

    sources: list[SourceStatus] = []
    sources.extend(check_auto_pipelines(pg_url))
    sources.extend(check_manual_uploads(pg_url))
    sources.extend(check_monthly_data(pg_url))
    kb = check_kb_activity(pg_url)

    report = PatrolReport(sources=sources, kb_summaries=kb)
    _last_report = report

    if feishu and owner_open_id:
        try:
            card = build_summary_card(report)
            feishu.send_card(open_id=owner_open_id, card=card)
        except Exception as exc:
            logger.error("patrol: failed to send summary card: %s", exc)
            try:
                feishu.send_text(open_id=owner_open_id,
                                 text=f"📡 数据巡视完成，{'存在异常' if report.has_alerts else '一切正常'}。")
            except Exception:
                pass

        # Send separate upload reminders for stale manual items
        for s in sources:
            if s.reminder_text and s.status != "fresh":
                try:
                    feishu.send_text(open_id=owner_open_id, text=f"📤 数据缺失提醒：\n{s.reminder_text}")
                except Exception as exc:
                    logger.warning("patrol: reminder send failed: %s", exc)

    return report


def get_last_report() -> Optional[PatrolReport]:
    return _last_report
```

- [ ] **Step 5.4: Run all tests**

```
python -m pytest tests/hermes/test_data_patrol.py -v
```
Expected: all tests PASS.

- [ ] **Step 5.5: Commit**

```bash
git add services/hermes/data_patrol.py tests/hermes/test_data_patrol.py
git commit -m "feat(patrol): add Feishu card builders and run_patrol entry point"
```

---

## Task 6: Wire patrol into `app.py` — scheduler, commands, endpoints

**Files:**
- Modify: `services/hermes/app.py` (lines ~29, ~57, ~711, ~905, ~2504, ~1186)

- [ ] **Step 6.1: Add `_pending_gap_fill` session dict and import**

In `services/hermes/app.py`, after line 29 (`_pending_capcomp_ingest: dict[str, int] = {}`):

```python
# sender_id → {fill_table, fill_province, fill_month} — pending gap fill via file upload
_pending_gap_fill: dict[str, dict] = {}
```

After the existing imports block (around line 57, after the `from services.hermes.capcomp_screener import ...` line), add:

```python
from services.hermes.data_patrol import (
    run_patrol as _run_patrol,
    build_detail_card as _patrol_detail_card,
    build_fill_card as _patrol_fill_card,
    get_last_report as _patrol_last_report,
)
```

- [ ] **Step 6.2: Add scheduler job**

In `services/hermes/app.py`, after line 711 (the closing `})` of the capcomp screener job), insert:

```python
        # Data patrol: daily 00:35 UTC (08:35 Beijing) — after health check
        scheduler.add_job(
            _run_patrol,
            "cron",
            hour=0, minute=35,
            kwargs={
                "pg_url":          _mengxi_pg_url,
                "feishu":          feishu,
                "owner_open_id":   os.environ.get("FEISHU_OWNER_OPEN_ID", ""),
                "api_key":         os.environ.get("ANTHROPIC_API_KEY", ""),
            },
        )
```

- [ ] **Step 6.3: Add HTTP endpoints**

In `services/hermes/app.py`, after the `@app.post("/hermes/news-screener/backfill")` block (around line 905), add:

```python
    @app.post("/hermes/patrol")
    async def _patrol_trigger(background_tasks: BackgroundTasks):
        """Trigger a patrol run immediately. Returns last cached report if one exists."""
        pg_url = os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", "")
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        owner_open_id = os.environ.get("FEISHU_OWNER_OPEN_ID", "")
        background_tasks.add_task(_run_patrol, pg_url, feishu, owner_open_id, api_key)
        return {"status": "started"}

    @app.get("/hermes/patrol/status")
    async def _patrol_status():
        report = _patrol_last_report()
        if report is None:
            return {"status": "no_report"}
        return {
            "status": "ok",
            "generated_at": report.generated_at.isoformat(),
            "has_alerts": report.has_alerts,
            "fresh": report.count_by_status("fresh"),
            "stale": report.count_by_status("stale"),
            "missing": report.count_by_status("missing"),
        }

    @app.post("/hermes/patrol/fill")
    async def _patrol_fill(request: Request):
        """Upsert a manually entered gap fill row. Body: {fill_table, fill_province, fill_month, field1_name, field1_value, field2_name?, field2_value?}"""
        body = await request.json()
        pg_url = os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", "")
        try:
            _upsert_gap_fill(body, pg_url)
            return {"status": "ok"}
        except Exception as exc:
            logger.error("patrol/fill failed: %s", exc)
            return {"status": "error", "detail": str(exc)}
```

- [ ] **Step 6.4: Add `_upsert_gap_fill` helper** (add near other ETL helpers in `app.py`, around line 730):

```python
def _upsert_gap_fill(body: dict, pg_url: str) -> None:
    """Upsert one manually-entered gap fill row into the appropriate table."""
    import psycopg2
    fill_table = body.get("fill_table", "")
    province   = body.get("fill_province", "")
    fill_month = body.get("fill_month", "")      # YYYY-MM
    if not fill_table or not fill_month:
        raise ValueError("fill_table and fill_month are required")

    year, month = map(int, fill_month.split("-"))
    effective_date = f"{year}-{month:02d}-01"

    conn = psycopg2.connect(pg_url)
    with conn:
        with conn.cursor() as cur:
            if fill_table == "province_cap_comp":
                from services.hermes.capcomp_etl import upsert_cap_rows
                upsert_cap_rows([{
                    "province": province,
                    "cap_comp_yuan_kw": float(body.get("cap_comp_yuan_kw", 0)),
                    "peak_duration_hours": float(body["peak_duration_hours"]) if body.get("peak_duration_hours") else None,
                    "effective_year": year,
                    "source": "manual_patrol_fill",
                }], pg_url=pg_url, source="manual_patrol_fill")
            elif fill_table == "province_fr_market":
                from services.hermes.capcomp_etl import upsert_fr_rows
                upsert_fr_rows([{
                    "province": province,
                    "fr_price_yuan_kw_h": float(body.get("fr_price_yuan_kw_h", 0)),
                    "fr_pool_yi_yuan": float(body["fr_pool_yi_yuan"]) if body.get("fr_pool_yi_yuan") else None,
                    "effective_year": year,
                    "source": "manual_patrol_fill",
                }], pg_url=pg_url, source="manual_patrol_fill")
            elif fill_table == "province_installed_monthly":
                cur.execute("""
                    INSERT INTO province_installed_monthly (province, year_month, installed_mw, source_file)
                    VALUES (%s, %s, %s, 'manual_patrol_fill')
                    ON CONFLICT (province, year_month) DO UPDATE
                      SET installed_mw = EXCLUDED.installed_mw,
                          source_file  = EXCLUDED.source_file
                """, (province, year * 100 + month, float(body.get("installed_mw", 0))))
            elif fill_table == "province_sysopfee_monthly":
                cur.execute("""
                    INSERT INTO province_sysopfee_monthly (province, year_month, fee_yuan_kwh, source_file)
                    VALUES (%s, %s, %s, 'manual_patrol_fill')
                    ON CONFLICT (province, year_month) DO UPDATE
                      SET fee_yuan_kwh = EXCLUDED.fee_yuan_kwh,
                          source_file  = EXCLUDED.source_file
                """, (province, year * 100 + month, float(body.get("fee_yuan_kwh", 0))))
            else:
                raise ValueError(f"Unsupported fill_table: {fill_table}")
    conn.close()
```

- [ ] **Step 6.5: Add `/datacheck` and `/巡视` command handlers**

In `services/hermes/app.py`, around line 2504 (near the `# ── /news command` block), add:

```python
    # ── /datacheck / /巡视 command — trigger data patrol ─────────────────────
    if text_lower in ("/datacheck", "/巡视", "datacheck", "巡视"):
        pg_url = os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", "")
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if msg.source == "feishu" and feishu:
            feishu.send_text(open_id=msg.sender_id, text="📡 正在巡视数据源，约15秒…")
        def _run():
            _run_patrol(
                pg_url=pg_url,
                feishu=feishu,
                owner_open_id=msg.sender_id,
                api_key=api_key,
            )
        import threading as _t
        _t.Thread(target=_run, daemon=True).start()
        return
```

- [ ] **Step 6.6: Add Feishu card callback handlers for patrol actions**

In `services/hermes/app.py`, inside the `@app.post("/hermes/inbound/feishu-card")` handler (around line 1186), add patrol action handling alongside existing `done_task`, `route`, `confirm` handlers:

```python
            # Patrol card interactions
            if act == "patrol_expand":
                report = _patrol_last_report()
                if report:
                    new_card = _patrol_detail_card(report)
                    try:
                        feishu.update_card(message_id=event_message_id, card=new_card)
                    except Exception as exc:
                        logger.warning("patrol expand card update failed: %s", exc)
                return Response(content="{}", media_type="application/json")

            if act == "patrol_collapse":
                report = _patrol_last_report()
                if report:
                    from services.hermes.data_patrol import build_summary_card as _patrol_summary
                    new_card = _patrol_summary(report)
                    try:
                        feishu.update_card(message_id=event_message_id, card=new_card)
                    except Exception as exc:
                        logger.warning("patrol collapse card update failed: %s", exc)
                return Response(content="{}", media_type="application/json")

            if act == "patrol_remind":
                reminder = action_value.get("reminder", "")
                if reminder and feishu and sender_open_id:
                    feishu.send_text(open_id=sender_open_id, text=f"📤 数据缺失提醒：\n{reminder}")
                return Response(content="{}", media_type="application/json")

            if act == "patrol_fill_open":
                fill_table   = action_value.get("fill_table", "")
                fill_province = action_value.get("fill_province", "")
                fill_month   = action_value.get("fill_month", "")
                fill_card = _patrol_fill_card(fill_table, fill_province, fill_month)
                if feishu and sender_open_id:
                    feishu.send_card(open_id=sender_open_id, card=fill_card)
                return Response(content="{}", media_type="application/json")

            if act == "patrol_fill_submit":
                pg_url = os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", "")
                try:
                    _upsert_gap_fill(action_value, pg_url)
                    if feishu and sender_open_id:
                        feishu.send_text(open_id=sender_open_id, text="✅ 数据已提交并写入数据库。")
                except Exception as exc:
                    if feishu and sender_open_id:
                        feishu.send_text(open_id=sender_open_id, text=f"⚠️ 提交失败：{exc}")
                return Response(content="{}", media_type="application/json")

            if act == "patrol_fill_file":
                fill_table   = action_value.get("fill_table", "")
                fill_province = action_value.get("fill_province", "")
                fill_month   = action_value.get("fill_month", "")
                if sender_open_id:
                    _pending_gap_fill[sender_open_id] = {
                        "fill_table": fill_table,
                        "fill_province": fill_province,
                        "fill_month": fill_month,
                    }
                    if feishu:
                        feishu.send_text(
                            open_id=sender_open_id,
                            text=(
                                f"📎 好的，请发送文件（PDF、Excel、JPG、PNG、DOCX、PPT、TXT 均可）。\n"
                                f"AI将自动提取 **{fill_table}** / **{fill_province}** / **{fill_month}** 的数据，"
                                f"并显示供你确认。"
                            ),
                        )
                return Response(content="{}", media_type="application/json")
```

- [ ] **Step 6.7: Add gap-fill file upload handler in Feishu file receive section**

In `services/hermes/app.py`, in the Feishu file receive block (around line 1758, just before the `if sender_id in _pending_capcomp_ingest:` check), add:

```python
    # Gap fill via file upload (patrol)
    if sender_id in _pending_gap_fill:
        ctx = _pending_gap_fill.pop(sender_id)
        pg_url = os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", "")
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        try:
            from services.hermes.capcomp_manual_etl import extract_from_file_for_gap
            result = extract_from_file_for_gap(
                file_bytes=file_bytes,
                filename=filename,
                fill_table=ctx["fill_table"],
                province=ctx["fill_province"],
                month=ctx["fill_month"],
                api_key=api_key,
            )
            if result.get("extracted"):
                # Build confirmation card
                confirm_card = {
                    "config": {"wide_screen_mode": True},
                    "header": {"template": "blue",
                               "title": {"content": "AI提取结果 — 请确认", "tag": "plain_text"}},
                    "elements": [
                        {"tag": "div", "text": {"tag": "lark_md",
                            "content": f"从文件 **{filename}** 提取到以下数据：\n\n```\n{result['summary']}\n```"}},
                        {"tag": "hr"},
                        {"tag": "action", "actions": [
                            {"tag": "button", "text": {"tag": "plain_text", "content": "✅ 确认提交"},
                             "type": "primary",
                             "value": {"act": "patrol_fill_submit", **ctx, **result["values"]}},
                            {"tag": "button", "text": {"tag": "plain_text", "content": "❌ 取消"},
                             "type": "default",
                             "value": {"act": "patrol_close"}},
                        ]},
                    ],
                }
                feishu.send_card(open_id=sender_id, card=confirm_card)
            else:
                feishu.send_text(open_id=sender_id,
                                 text=f"⚠️ AI未能从文件中提取到有效数据。请手动输入，或换一个文件。\n错误：{result.get('error','')}")
        except Exception as exc:
            logger.error("Gap fill file extraction failed: %s", exc, exc_info=True)
            feishu.send_text(open_id=sender_id, text=f"⚠️ 文件处理失败：{exc}")
        return  # do not fall through to other handlers
```

- [ ] **Step 6.8: Test the wired-up endpoints manually**

```bash
# Start hermes locally (or run in container), then:
curl -s -X POST http://localhost:8000/hermes/patrol | python -m json.tool
# Expected: {"status": "started"}

curl -s http://localhost:8000/hermes/patrol/status | python -m json.tool
# Expected: {"status": "ok", "generated_at": "...", "has_alerts": ..., ...}
```

- [ ] **Step 6.9: Commit**

```bash
git add services/hermes/app.py
git commit -m "feat(patrol): wire patrol into app.py scheduler, commands, and endpoints"
```

---

## Task 7: Extend `capcomp_manual_etl.py` with DOCX/PPT/TXT + gap fill extractor

**Files:**
- Modify: `services/hermes/capcomp_manual_etl.py` (lines 77–90, append at end)

- [ ] **Step 7.1: Extend `_text_from_bytes` to handle DOCX, PPTX, TXT**

In `services/hermes/capcomp_manual_etl.py`, replace lines 77–90 (the `_text_from_bytes` function):

```python
def _text_from_bytes(filename: str, file_bytes: bytes) -> str:
    """Dispatch to the right extractor based on file extension."""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext == "pdf":
        return _text_from_pdf(file_bytes)
    if ext in ("xlsx", "xls", "xlsm"):
        return _text_from_excel(file_bytes)
    if ext in ("docx", "doc"):
        try:
            import docx as _docx
            doc = _docx.Document(io.BytesIO(file_bytes))
            parts = [p.text for p in doc.paragraphs if p.text.strip()]
            for table in doc.tables:
                for row in table.rows:
                    parts.append(" | ".join(cell.text.strip() for cell in row.cells))
            return "\n".join(parts)
        except Exception as exc:
            logger.warning("DOCX text extraction failed: %s", exc)
            return ""
    if ext in ("pptx", "ppt"):
        try:
            from pptx import Presentation
            prs = Presentation(io.BytesIO(file_bytes))
            parts = []
            for slide in prs.slides:
                for shape in slide.shapes:
                    if hasattr(shape, "text") and shape.text.strip():
                        parts.append(shape.text.strip())
            return "\n".join(parts)
        except Exception as exc:
            logger.warning("PPTX text extraction failed: %s", exc)
            return ""
    # TXT / CSV / other plain text
    for enc in ("utf-8", "gbk", "gb2312"):
        try:
            return file_bytes.decode(enc)
        except Exception:
            pass
    return file_bytes.decode("utf-8", errors="replace")
```

- [ ] **Step 7.2: Add `extract_from_file_for_gap` dispatcher**

Append to the end of `services/hermes/capcomp_manual_etl.py`:

```python
# ── Gap fill dispatcher ───────────────────────────────────────────────────────

_GAP_FILL_PROMPTS = {
    "province_cap_comp": (
        "从以下文本中提取指定省份的储能容量补偿标准（元/kW·年）和年最高净负荷峰值时段（小时）。"
        "以JSON回答: {{\"cap_comp_yuan_kw\": <数值>, \"peak_duration_hours\": <数值或null>}}"
    ),
    "province_fr_market": (
        "从以下文本中提取指定省份的调频容量价格（元/kW·h）和全省调频资金池（亿元/年）。"
        "以JSON回答: {{\"fr_price_yuan_kw_h\": <数值>, \"fr_pool_yi_yuan\": <数值或null>}}"
    ),
    "province_installed_monthly": (
        "从以下文本中提取指定省份的储能装机容量（MW）。"
        "以JSON回答: {{\"installed_mw\": <数值>}}"
    ),
    "province_sysopfee_monthly": (
        "从以下文本中提取指定省份的系统运行费（元/kWh）。"
        "以JSON回答: {{\"fee_yuan_kwh\": <数值>}}"
    ),
}


def extract_from_file_for_gap(
    file_bytes: bytes,
    filename: str,
    fill_table: str,
    province: str,
    month: str,
    api_key: str,
) -> dict:
    """
    Extract the relevant value(s) from a file for a specific gap fill target.
    Supports PDF, Excel, JPG/PNG (vision), DOCX, PPTX, TXT.
    Returns:
        {"extracted": True, "values": {...field: value}, "summary": str}
        {"extracted": False, "error": str}
    """
    import anthropic as _ant

    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    prompt_template = _GAP_FILL_PROMPTS.get(fill_table)
    if not prompt_template:
        return {"extracted": False, "error": f"Unsupported fill_table: {fill_table}"}

    client = _ant.Anthropic(api_key=api_key)

    # Image path: use Claude vision
    if ext in ("jpg", "jpeg", "png", "webp", "gif"):
        _mime = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
                 "webp": "image/webp", "gif": "image/gif"}
        import base64 as _b64
        b64 = _b64.standard_b64encode(file_bytes).decode()
        vision_prompt = (
            f"图片中包含电力市场数据。省份：{province}，月份：{month}。\n"
            f"请先转录图片中的文字，然后{prompt_template}"
        )
        try:
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=300,
                messages=[{"role": "user", "content": [
                    {"type": "image", "source": {"type": "base64",
                     "media_type": _mime.get(ext, "image/jpeg"), "data": b64}},
                    {"type": "text", "text": vision_prompt},
                ]}],
            )
            text = resp.content[0].text.strip()
        except Exception as exc:
            return {"extracted": False, "error": str(exc)}
    else:
        # Text-based extraction
        raw_text = _text_from_bytes(filename, file_bytes)
        if not raw_text.strip():
            return {"extracted": False, "error": "无法从文件中提取文本"}
        full_prompt = (
            f"省份：{province}，月份/年份：{month}。\n"
            f"{prompt_template}\n\n文本内容：\n{raw_text[:6000]}"
        )
        try:
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=300,
                messages=[{"role": "user", "content": full_prompt}],
            )
            text = resp.content[0].text.strip()
        except Exception as exc:
            return {"extracted": False, "error": str(exc)}

    # Parse JSON from response
    import json as _json, re as _re
    match = _re.search(r'\{[^{}]+\}', text)
    if not match:
        return {"extracted": False, "error": f"AI未能返回JSON: {text[:200]}"}
    try:
        values = _json.loads(match.group())
        # Add context fields
        values["fill_table"] = fill_table
        values["fill_province"] = province
        values["fill_month"] = month
        summary = "\n".join(f"{k}: {v}" for k, v in values.items()
                            if k not in ("fill_table", "fill_province", "fill_month"))
        return {"extracted": True, "values": values, "summary": summary}
    except Exception as exc:
        return {"extracted": False, "error": f"JSON解析失败: {exc} — {text[:200]}"}
```

- [ ] **Step 7.3: Write a test for `extract_from_file_for_gap` with mocked Claude**

Add to `tests/hermes/test_data_patrol.py`:

```python
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
```

- [ ] **Step 7.4: Run test**

```
python -m pytest tests/hermes/test_data_patrol.py::test_extract_from_file_for_gap_txt -v
```
Expected: PASS.

- [ ] **Step 7.5: Commit**

```bash
git add services/hermes/capcomp_manual_etl.py tests/hermes/test_data_patrol.py
git commit -m "feat(patrol): extend capcomp_manual_etl with DOCX/PPT/TXT + extract_from_file_for_gap"
```

---

## Task 8: bess-map — Data Gaps expander in Cap Comp + FR tab

**Files:**
- Modify: `apps/bess-map/app.py` (insert before line 2853, which is `# ── Tab 7`)

- [ ] **Step 8.1: Add gap heatmap helper function**

In `apps/bess-map/app.py`, after the `load_installed_capacity` function (around line 1625), add:

```python
@st.cache_data(ttl=300, show_spinner=False)
def load_monthly_gaps(_eng_key):
    """
    Return three DataFrames (capcomp, fr_market, installed) with
    province × YYYY-MM coverage for the last 12 months.
    Each df has columns: province, year_month (str), has_data (bool).
    """
    import pandas as pd
    from datetime import date
    from dateutil.relativedelta import relativedelta

    eng = _get_engine(_eng_key)
    today = date.today()
    months = [(today - relativedelta(months=i)).strftime("%Y-%m") for i in range(12)]

    results = {}
    queries = {
        "capcomp": (
            "SELECT province, TO_CHAR(effective_date, 'YYYY-MM') AS ym FROM marketdata.province_cap_comp",
            "province_cap_comp",
        ),
        "fr_market": (
            "SELECT province, TO_CHAR(effective_date, 'YYYY-MM') AS ym FROM marketdata.province_fr_market",
            "province_fr_market",
        ),
        "installed": (
            "SELECT province, TO_CHAR(TO_DATE(year_month::text, 'YYYYMM'), 'YYYY-MM') AS ym FROM province_installed_monthly",
            "province_installed_monthly",
        ),
    }
    for key, (sql, _) in queries.items():
        try:
            with eng.connect() as conn:
                df = pd.read_sql(sql, conn)
            df["has_data"] = True
            # Build full province × month grid
            provinces = sorted(df["province"].unique())
            grid = pd.MultiIndex.from_product([provinces, months], names=["province", "year_month"])
            full = pd.DataFrame(index=grid).reset_index()
            full = full.merge(df, on=["province", "year_month"], how="left")
            full["has_data"] = full["has_data"].fillna(False)
            results[key] = full
        except Exception:
            results[key] = pd.DataFrame(columns=["province", "year_month", "has_data"])
    return results["capcomp"], results["fr_market"], results["installed"]
```

- [ ] **Step 8.2: Add the Data Gaps expander at bottom of `tab_aux`**

In `apps/bess-map/app.py`, insert the following block immediately before line 2853 (`# ── Tab 7: Dispatch & Economics`):

```python
    # ── Data Gaps expander ────────────────────────────────────────────────────
    with st.expander("📋 Data Gaps — 容量补偿 / 调频市场 / 装机容量", expanded=False):
        import plotly.express as px
        import pandas as pd

        _hermes_url_fill = os.environ.get("HERMES_URL", "").rstrip("/")

        _gap_cc, _gap_fr, _gap_inst = load_monthly_gaps(_ENG_KEY)

        _gap_tabs = st.tabs(["容量补偿", "调频市场", "装机容量"])
        _gap_data = [
            (_gap_tabs[0], _gap_cc, "province_cap_comp",        "cap_comp_yuan_kw",   "容量补偿标准 (¥/kW·年)"),
            (_gap_tabs[1], _gap_fr, "province_fr_market",       "fr_price_yuan_kw_h", "调频容量价格 (¥/kW·h)"),
            (_gap_tabs[2], _gap_inst, "province_installed_monthly", "installed_mw",   "储能装机 (MW)"),
        ]

        for _tab, _df, _fill_table, _field1, _field1_label in _gap_data:
            with _tab:
                if _df.empty:
                    st.info("暂无数据")
                    continue

                # Pivot for heatmap: provinces × months
                _pivot = _df.pivot(index="province", columns="year_month", values="has_data").fillna(False)
                _z = _pivot.values.astype(int)
                _fig = px.imshow(
                    _z,
                    x=list(_pivot.columns),
                    y=list(_pivot.index),
                    color_continuous_scale=[[0, "#ef4444"], [1, "#22c55e"]],
                    zmin=0, zmax=1,
                    aspect="auto",
                    labels={"color": "有数据"},
                )
                _fig.update_layout(
                    height=max(250, len(_pivot.index) * 22),
                    margin=dict(t=10, b=40, l=140, r=10),
                    coloraxis_showscale=False,
                )
                _fig.update_xaxes(tickangle=-45)
                st.plotly_chart(_fig, use_container_width=True, key=f"gap_{_fill_table}")

                # Manual fill form
                _missing_rows = _df[~_df["has_data"]]
                if not _missing_rows.empty and _hermes_url_fill:
                    st.caption(f"🔴 {len(_missing_rows)} 条数据缺失。可在下方手动填写：")
                    with st.form(key=f"fill_form_{_fill_table}"):
                        _col1, _col2, _col3 = st.columns(3)
                        _provs_missing = sorted(_missing_rows["province"].unique())
                        _months_missing = sorted(_missing_rows["year_month"].unique(), reverse=True)
                        with _col1:
                            _sel_prov = st.selectbox("省份", _provs_missing, key=f"fp_{_fill_table}")
                        with _col2:
                            _sel_month = st.selectbox("月份", _months_missing, key=f"fm_{_fill_table}")
                        with _col3:
                            _val1 = st.number_input(_field1_label, min_value=0.0, step=0.01,
                                                     key=f"fv1_{_fill_table}")
                        _submitted = st.form_submit_button("提交")
                        if _submitted:
                            import requests as _rq
                            try:
                                _resp = _rq.post(
                                    f"{_hermes_url_fill}/hermes/patrol/fill",
                                    json={
                                        "fill_table":    _fill_table,
                                        "fill_province": _sel_prov,
                                        "fill_month":    _sel_month,
                                        _field1:         _val1,
                                    },
                                    timeout=10,
                                )
                                if _resp.status_code == 200 and _resp.json().get("status") == "ok":
                                    st.success(f"✅ {_sel_prov} {_sel_month} 数据已提交。")
                                    st.cache_data.clear()
                                    st.rerun()
                                else:
                                    st.warning(f"提交失败：{_resp.text[:100]}")
                            except Exception as _fe:
                                st.warning(f"无法连接到 Hermes：{_fe}")
                elif _missing_rows.empty:
                    st.success("✅ 所有数据均已覆盖（近12个月）")
                else:
                    st.caption("请通过 Feishu 的 `/datacheck` 命令填写缺失数据，或配置 HERMES_URL。")
```

- [ ] **Step 8.3: Verify the bess-map app starts without errors**

```bash
cd C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform
python -c "import ast; ast.parse(open('apps/bess-map/app.py').read()); print('syntax OK')"
```
Expected: `syntax OK`

- [ ] **Step 8.4: Commit**

```bash
git add apps/bess-map/app.py
git commit -m "feat(bess-map): add Data Gaps heatmap expander to Cap Comp + FR tab"
```

---

## Task 9: Run full test suite and verify

- [ ] **Step 9.1: Run all data patrol tests**

```
python -m pytest tests/hermes/test_data_patrol.py -v
```
Expected: all tests PASS with no errors.

- [ ] **Step 9.2: Run full hermes test suite**

```
python -m pytest tests/hermes/ -v
```
Expected: all previously passing tests still PASS.

- [ ] **Step 9.3: Syntax-check all modified files**

```bash
python -c "import ast; [ast.parse(open(f).read()) for f in ['services/hermes/data_patrol.py','services/hermes/capcomp_manual_etl.py','apps/bess-map/app.py']]; print('all OK')"
```
Expected: `all OK`

- [ ] **Step 9.4: Final commit**

```bash
git add -A
git commit -m "feat(patrol): data patrol agent complete — patrol module, hermes wiring, bess-map gap heatmap"
```

---

## Self-Review Notes

**Spec coverage check:**
- ✅ Scheduled at 08:35 Beijing — Task 6.2
- ✅ On-demand `/datacheck` + `/巡视` — Task 6.5
- ✅ Summary card (tiered) — Task 5.3 `build_summary_card`
- ✅ Expand/collapse detail card — Task 5.3 `build_detail_card`, Task 6.6
- ✅ All Group A sources — Task 2.3 `check_auto_pipelines`
- ✅ Group B spot daily PDF — Task 3.3 `check_manual_uploads`
- ✅ Group C monthly data with fill buttons — Task 3.3 `check_monthly_data`, Task 5.3 `build_fill_card`
- ✅ Group D KB activity counts (Mon / 1st) — Task 4.3 `check_kb_activity`
- ✅ Upload reminder separate message — Task 5.3 `run_patrol`
- ✅ Inline fill: manual typing — Task 6.6 `patrol_fill_submit`
- ✅ Inline fill: file upload → AI extract → confirm — Task 6.6 `patrol_fill_file`, Task 6.7, Task 7.2
- ✅ File types: PDF, Excel, JPG, PNG, DOCX, PPT, TXT — Task 7.1 + 7.2
- ✅ HTTP endpoints `/patrol`, `/patrol/status`, `/patrol/fill` — Task 6.3
- ✅ bess-map gap heatmap with fill form — Task 8
- ✅ Exchange monthly reports — Task 3.3 `check_monthly_data`
