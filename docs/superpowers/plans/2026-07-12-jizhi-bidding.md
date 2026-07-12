# 机制竞价 Intelligence System — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a structured 136号文 机制竞价 intelligence system: DB tables, AI extraction from uploaded documents, a Streamlit tab with historical results + upcoming calendar + upload, nightly Hermes internet scan, and Feishu file routing + `/机制竞价` command.

**Architecture:** A new `services/knowledge_pool/jizhi_extractor.py` holds DB DDL + Claude tool-use extraction + upsert logic. Hermes gains a nightly scan job (`_run_jizhi_scan`) that uses the existing `internet_agent`, a Feishu file handler, and an on-demand endpoint. The spot-market app gets a 14th tab with three sub-tabs: 历史结果, 即将竞价, 上传&录入.

**Tech Stack:** Python, psycopg2, anthropic SDK (claude-haiku tool-use), APScheduler (already in Hermes), Streamlit, plotly, Feishu Open API (already in Hermes)

---

## File Map

| File | Change |
|---|---|
| `services/knowledge_pool/jizhi_extractor.py` | New — `ensure_tables`, `extract_bids`, `extract_upcoming`, `save_bids`, `save_upcoming` |
| `services/knowledge_pool/tests/test_jizhi_extractor.py` | New — unit tests (mocked anthropic + psycopg2) |
| `services/hermes/app.py` | Add module-level shims + `_run_jizhi_scan` (before `logger=`), APScheduler job after `kb_digest_nightly`, `_handle_jizhi_file` helper, `/机制竞价` command, `POST /hermes/jizhi/scan` endpoint |
| `services/hermes/tests/test_jizhi_scan.py` | New — unit tests for `_run_jizhi_scan` |
| `apps/spot-market/app.py` | Add `tab_jizhi` to `st.tabs()` at line 1633; add tab body between `tab_library` (line 4628) and `tab_mgmt` (line 4633) |

---

## Task 1: `jizhi_extractor.py` — ensure_tables + extract_bids

**Files:**
- Create: `services/knowledge_pool/jizhi_extractor.py`
- Create: `services/knowledge_pool/tests/__init__.py`
- Create: `services/knowledge_pool/tests/test_jizhi_extractor.py`

- [ ] **Step 1.1: Create `services/knowledge_pool/tests/__init__.py`** (empty file)

```bash
# Just touch the file
```

- [ ] **Step 1.2: Write failing tests for `ensure_tables` and `extract_bids`**

Create `services/knowledge_pool/tests/test_jizhi_extractor.py`:

```python
# services/knowledge_pool/tests/test_jizhi_extractor.py
"""Unit tests for jizhi_extractor — all DB and API calls mocked."""
from __future__ import annotations
from unittest.mock import MagicMock, patch, call
import pytest


class TestEnsureTables:
    def test_executes_ddl_and_commits(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("psycopg2.connect", return_value=mock_conn):
            from services.knowledge_pool.jizhi_extractor import ensure_tables
            ensure_tables("postgresql://fake/db")

        mock_cur.execute.assert_called_once()
        ddl_arg = mock_cur.execute.call_args[0][0]
        assert "jizhi_bids" in ddl_arg
        assert "jizhi_bid_winners" in ddl_arg
        assert "jizhi_upcoming" in ddl_arg
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()


class TestExtractBids:
    def _mock_response(self, bids: list) -> MagicMock:
        block = MagicMock()
        block.type = "tool_use"
        block.name = "save_bid_results"
        block.input = {"bids": bids}
        resp = MagicMock()
        resp.content = [block]
        return resp

    def test_happy_path_returns_list_of_dicts(self):
        bids = [
            {"province": "广东", "year": 2025, "batch": "存量",
             "tech_type": "光伏", "cleared_price": 0.35, "cleared_volume_gwh": 100.0}
        ]
        with patch("anthropic.Anthropic") as mock_cls:
            mock_cls.return_value.messages.create.return_value = self._mock_response(bids)
            from services.knowledge_pool.jizhi_extractor import extract_bids
            result = extract_bids("some document text about 机制竞价", "test-key")

        assert len(result) == 1
        assert result[0]["province"] == "广东"
        assert result[0]["cleared_price"] == 0.35

    def test_empty_api_key_returns_empty_list(self):
        from services.knowledge_pool.jizhi_extractor import extract_bids
        assert extract_bids("text", "") == []

    def test_empty_text_returns_empty_list(self):
        from services.knowledge_pool.jizhi_extractor import extract_bids
        assert extract_bids("   ", "key") == []

    def test_api_failure_returns_empty_list(self):
        with patch("anthropic.Anthropic") as mock_cls:
            mock_cls.return_value.messages.create.side_effect = Exception("API down")
            from services.knowledge_pool.jizhi_extractor import extract_bids
            result = extract_bids("text", "key")
        assert result == []

    def test_no_tool_use_block_returns_empty_list(self):
        resp = MagicMock()
        resp.content = [MagicMock(type="text")]
        with patch("anthropic.Anthropic") as mock_cls:
            mock_cls.return_value.messages.create.return_value = resp
            from services.knowledge_pool.jizhi_extractor import extract_bids
            result = extract_bids("text", "key")
        assert result == []
```

- [ ] **Step 1.3: Run tests to confirm they fail**

```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform
py -m pytest services/knowledge_pool/tests/test_jizhi_extractor.py::TestEnsureTables services/knowledge_pool/tests/test_jizhi_extractor.py::TestExtractBids -v 2>&1 | head -30
```

Expected: `ImportError` — module doesn't exist yet.

- [ ] **Step 1.4: Create `services/knowledge_pool/jizhi_extractor.py` with `ensure_tables` and `extract_bids`**

```python
# services/knowledge_pool/jizhi_extractor.py
"""
Structured extraction and persistence for 136号文 机制竞价 bid data.

Tables managed:
  staging.jizhi_bids         — completed bid results (province × year × batch × tech_type)
  staging.jizhi_bid_winners  — 中标清单 (optional sub-table)
  staging.jizhi_upcoming     — upcoming bid calendar

Public API:
  ensure_tables(pg_url)
  extract_bids(text, api_key) -> list[dict]
  extract_upcoming(text, api_key) -> list[dict]
  save_bids(records, source_doc_id, pg_url) -> int
  save_upcoming(records, pg_url) -> int
"""
from __future__ import annotations
import logging

import psycopg2

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS staging.jizhi_bids (
    id                  SERIAL PRIMARY KEY,
    province            TEXT NOT NULL,
    year                INT  NOT NULL,
    batch               TEXT NOT NULL,
    tech_type           TEXT NOT NULL,
    price_floor         NUMERIC,
    price_cap           NUMERIC,
    mechanism_type      TEXT,
    mechanism_value     NUMERIC,
    supply_demand_ratio NUMERIC,
    cleared_price       NUMERIC,
    cleared_volume_gwh  NUMERIC,
    bid_date            DATE,
    verified            BOOLEAN NOT NULL DEFAULT FALSE,
    source_doc_id       INT,
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (province, year, batch, tech_type)
);
CREATE TABLE IF NOT EXISTS staging.jizhi_bid_winners (
    id            SERIAL PRIMARY KEY,
    bid_id        INT NOT NULL REFERENCES staging.jizhi_bids(id) ON DELETE CASCADE,
    project_name  TEXT NOT NULL,
    operator      TEXT,
    capacity_mw   NUMERIC,
    cleared_price NUMERIC,
    tech_type     TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_jizhi_winners_bid
    ON staging.jizhi_bid_winners(bid_id);
CREATE TABLE IF NOT EXISTS staging.jizhi_upcoming (
    id                   SERIAL PRIMARY KEY,
    province             TEXT NOT NULL,
    year                 INT  NOT NULL,
    batch                TEXT NOT NULL,
    tech_type            TEXT NOT NULL,
    price_floor          NUMERIC,
    price_cap            NUMERIC,
    target_volume_gwh    NUMERIC,
    supply_demand_ratio  NUMERIC,
    bid_open_date        DATE,
    bid_close_date       DATE,
    source_url           TEXT,
    announcement_date    DATE,
    verified             BOOLEAN NOT NULL DEFAULT FALSE,
    notes                TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (province, year, batch, tech_type, bid_open_date)
);
"""

_BIDS_TOOL = {
    "name": "save_bid_results",
    "description": "Save extracted 机制竞价 completed bid results from the document.",
    "input_schema": {
        "type": "object",
        "properties": {
            "bids": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "province":            {"type": "string",  "description": "Province name in Chinese, e.g. 广东"},
                        "year":                {"type": "integer", "description": "Year of the bidding, e.g. 2025"},
                        "batch":               {"type": "string",  "description": "One of: 存量, 增量_2025-12, 增量_2026-12, 增量_2027-12"},
                        "tech_type":           {"type": "string",  "description": "One of: 陆风, 海风, 光伏, 水电"},
                        "price_floor":         {"type": "number",  "description": "Minimum bid price in 元/kWh"},
                        "price_cap":           {"type": "number",  "description": "Maximum bid price in 元/kWh"},
                        "mechanism_type":      {"type": "string",  "description": "One of: 电量, 比例, 小时数"},
                        "mechanism_value":     {"type": "number",  "description": "Value in GWh (电量), % (比例), or hours (小时数)"},
                        "supply_demand_ratio": {"type": "number",  "description": "Supply-demand ratio, e.g. 1.35"},
                        "cleared_price":       {"type": "number",  "description": "Final cleared price in 元/kWh"},
                        "cleared_volume_gwh":  {"type": "number",  "description": "Total cleared volume in GWh"},
                        "bid_date":            {"type": "string",  "description": "Bid date as YYYY-MM-DD"},
                        "notes":               {"type": "string"},
                    },
                    "required": ["province", "year", "batch", "tech_type"],
                },
            }
        },
        "required": ["bids"],
    },
}

_BIDS_PROMPT = """\
Extract ALL 机制竞价 completed bid results from the document below.

Normalisation rules:
- batch: 存量 = grid-connected before 2025-05-31 \
; 增量_2025-12 = before 2025-12-31; 增量_2026-12 = before 2026-12-31; 增量_2027-12 = before 2027-12-31
- tech_type: 陆风 / 海风 / 光伏 / 水电  (map 风电/wind → 陆风 unless specifically 海风)
- prices in 元/kWh  (divide by 1000 if document uses 元/MWh)
- cleared_volume_gwh in GWh  (divide by 1000 if document uses TWh)
- bid_date as YYYY-MM-DD

Document:
{text}"""


def ensure_tables(pg_url: str) -> None:
    """Create jizhi_bids, jizhi_bid_winners, jizhi_upcoming if they don't exist."""
    conn = psycopg2.connect(pg_url)
    try:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()
        logger.info("[jizhi] tables ensured")
    finally:
        conn.close()


def extract_bids(text: str, api_key: str) -> list[dict]:
    """Extract structured bid results from document text via Claude tool-use.

    Returns list of dicts with keys matching staging.jizhi_bids columns
    (excluding id, source_doc_id, created_at).
    Returns [] on failure or empty input.
    """
    if not api_key or not text.strip():
        return []
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=4096,
            tools=[_BIDS_TOOL],
            tool_choice={"type": "tool", "name": "save_bid_results"},
            messages=[{
                "role": "user",
                "content": _BIDS_PROMPT.format(text=text[:15000]),
            }],
        )
        for block in response.content:
            if block.type == "tool_use" and block.name == "save_bid_results":
                return block.input.get("bids", [])
    except Exception as exc:
        logger.error("[jizhi] extract_bids failed: %s", exc)
    return []
```

- [ ] **Step 1.5: Run the tests — expect them to pass**

```bash
py -m pytest services/knowledge_pool/tests/test_jizhi_extractor.py::TestEnsureTables services/knowledge_pool/tests/test_jizhi_extractor.py::TestExtractBids -v
```

Expected: 6 passed

- [ ] **Step 1.6: Commit**

```bash
git add services/knowledge_pool/jizhi_extractor.py \
        services/knowledge_pool/tests/__init__.py \
        services/knowledge_pool/tests/test_jizhi_extractor.py
git commit -m "feat: jizhi_extractor ensure_tables + extract_bids"
```

---

## Task 2: `jizhi_extractor.py` — extract_upcoming + save_bids + save_upcoming

**Files:**
- Modify: `services/knowledge_pool/jizhi_extractor.py`
- Modify: `services/knowledge_pool/tests/test_jizhi_extractor.py`

- [ ] **Step 2.1: Add failing tests for `extract_upcoming`, `save_bids`, `save_upcoming`**

Append to `services/knowledge_pool/tests/test_jizhi_extractor.py`:

```python

class TestExtractUpcoming:
    def _mock_response(self, upcoming: list) -> MagicMock:
        block = MagicMock()
        block.type = "tool_use"
        block.name = "save_upcoming_bids"
        block.input = {"upcoming": upcoming}
        resp = MagicMock()
        resp.content = [block]
        return resp

    def test_returns_upcoming_records(self):
        upcoming = [
            {"province": "山东", "year": 2026, "batch": "增量_2026-12",
             "tech_type": "陆风", "bid_open_date": "2026-03-01", "price_cap": 0.40}
        ]
        with patch("anthropic.Anthropic") as mock_cls:
            mock_cls.return_value.messages.create.return_value = self._mock_response(upcoming)
            from services.knowledge_pool.jizhi_extractor import extract_upcoming
            result = extract_upcoming("announcement text", "key")
        assert len(result) == 1
        assert result[0]["province"] == "山东"
        assert result[0]["bid_open_date"] == "2026-03-01"

    def test_empty_api_key_returns_empty(self):
        from services.knowledge_pool.jizhi_extractor import extract_upcoming
        assert extract_upcoming("text", "") == []

    def test_api_failure_returns_empty(self):
        with patch("anthropic.Anthropic") as mock_cls:
            mock_cls.return_value.messages.create.side_effect = RuntimeError("boom")
            from services.knowledge_pool.jizhi_extractor import extract_upcoming
            result = extract_upcoming("text", "key")
        assert result == []


class TestSaveBids:
    def _mock_conn(self, fetchone_return=(1,)):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = fetchone_return
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return mock_conn, mock_cur

    def test_inserts_record_and_returns_count(self):
        mock_conn, mock_cur = self._mock_conn(fetchone_return=(42,))
        records = [{"province": "广东", "year": 2025, "batch": "存量",
                    "tech_type": "光伏", "cleared_price": 0.35, "cleared_volume_gwh": 100.0}]
        with patch("psycopg2.connect", return_value=mock_conn):
            from services.knowledge_pool.jizhi_extractor import save_bids
            count = save_bids(records, source_doc_id=5, pg_url="postgresql://fake/db")
        assert count == 1
        mock_conn.commit.assert_called_once()

    def test_no_conflict_upsert_not_counted(self):
        mock_conn, mock_cur = self._mock_conn(fetchone_return=None)
        records = [{"province": "广东", "year": 2025, "batch": "存量", "tech_type": "光伏"}]
        with patch("psycopg2.connect", return_value=mock_conn):
            from services.knowledge_pool.jizhi_extractor import save_bids
            count = save_bids(records, source_doc_id=None, pg_url="postgresql://fake/db")
        assert count == 0

    def test_empty_records_returns_zero(self):
        from services.knowledge_pool.jizhi_extractor import save_bids
        assert save_bids([], None, "postgresql://fake/db") == 0

    def test_db_failure_returns_zero_and_rollbacks(self):
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            side_effect=Exception("DB error")
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        records = [{"province": "广东", "year": 2025, "batch": "存量", "tech_type": "光伏"}]
        with patch("psycopg2.connect", return_value=mock_conn):
            from services.knowledge_pool.jizhi_extractor import save_bids
            count = save_bids(records, None, "postgresql://fake/db")
        assert count == 0
        mock_conn.rollback.assert_called_once()


class TestSaveUpcoming:
    def test_returns_zero_on_empty(self):
        from services.knowledge_pool.jizhi_extractor import save_upcoming
        assert save_upcoming([], "postgresql://fake/db") == 0

    def test_inserts_and_returns_count(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (1,)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        records = [{"province": "浙江", "year": 2026, "batch": "增量_2026-12",
                    "tech_type": "海风", "bid_open_date": "2026-02-01"}]
        with patch("psycopg2.connect", return_value=mock_conn):
            from services.knowledge_pool.jizhi_extractor import save_upcoming
            count = save_upcoming(records, "postgresql://fake/db")
        assert count == 1
        mock_conn.commit.assert_called_once()
```

- [ ] **Step 2.2: Run new tests to confirm they fail**

```bash
py -m pytest services/knowledge_pool/tests/test_jizhi_extractor.py::TestExtractUpcoming services/knowledge_pool/tests/test_jizhi_extractor.py::TestSaveBids services/knowledge_pool/tests/test_jizhi_extractor.py::TestSaveUpcoming -v 2>&1 | head -20
```

Expected: `ImportError` or `AttributeError` — functions not yet defined.

- [ ] **Step 2.3: Append `extract_upcoming`, `save_bids`, `save_upcoming` to `jizhi_extractor.py`**

Append after `extract_bids`:

```python

_UPCOMING_TOOL = {
    "name": "save_upcoming_bids",
    "description": "Save upcoming 机制竞价 bid announcements from a notice or web page.",
    "input_schema": {
        "type": "object",
        "properties": {
            "upcoming": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "province":            {"type": "string"},
                        "year":                {"type": "integer"},
                        "batch":               {"type": "string",  "description": "存量 / 增量_2025-12 / 增量_2026-12 / 增量_2027-12"},
                        "tech_type":           {"type": "string",  "description": "陆风 / 海风 / 光伏 / 水电"},
                        "price_floor":         {"type": "number",  "description": "元/kWh"},
                        "price_cap":           {"type": "number",  "description": "元/kWh"},
                        "target_volume_gwh":   {"type": "number",  "description": "Target volume in GWh"},
                        "supply_demand_ratio": {"type": "number"},
                        "bid_open_date":       {"type": "string",  "description": "YYYY-MM-DD"},
                        "bid_close_date":      {"type": "string",  "description": "YYYY-MM-DD"},
                        "source_url":          {"type": "string"},
                        "announcement_date":   {"type": "string",  "description": "YYYY-MM-DD"},
                        "notes":               {"type": "string"},
                    },
                    "required": ["province", "year", "batch", "tech_type"],
                },
            }
        },
        "required": ["upcoming"],
    },
}

_UPCOMING_PROMPT = """\
Extract upcoming 机制竞价 bid announcements from the text below.
Focus on: province, bidding dates, price range (元/kWh), target volume (GWh), supply-demand ratio.
batch values: 存量 / 增量_2025-12 / 增量_2026-12 / 增量_2027-12
tech_type values: 陆风 / 海风 / 光伏 / 水电
dates as YYYY-MM-DD

Text:
{text}"""


def extract_upcoming(text: str, api_key: str) -> list[dict]:
    """Extract upcoming bid announcements from text via Claude tool-use.

    Returns list of dicts matching staging.jizhi_upcoming columns.
    Returns [] on failure or empty input.
    """
    if not api_key or not text.strip():
        return []
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2048,
            tools=[_UPCOMING_TOOL],
            tool_choice={"type": "tool", "name": "save_upcoming_bids"},
            messages=[{
                "role": "user",
                "content": _UPCOMING_PROMPT.format(text=text[:12000]),
            }],
        )
        for block in response.content:
            if block.type == "tool_use" and block.name == "save_upcoming_bids":
                return block.input.get("upcoming", [])
    except Exception as exc:
        logger.error("[jizhi] extract_upcoming failed: %s", exc)
    return []


def save_bids(records: list[dict], source_doc_id: int | None, pg_url: str) -> int:
    """Upsert bid records to staging.jizhi_bids.

    Verified rows (verified=TRUE) are never overwritten.
    Returns count of rows actually inserted or updated.
    """
    if not records or not pg_url:
        return 0
    conn = psycopg2.connect(pg_url)
    count = 0
    try:
        with conn.cursor() as cur:
            for r in records:
                cur.execute(
                    """
                    INSERT INTO staging.jizhi_bids
                        (province, year, batch, tech_type,
                         price_floor, price_cap, mechanism_type, mechanism_value,
                         supply_demand_ratio, cleared_price, cleared_volume_gwh,
                         bid_date, verified, source_doc_id, notes)
                    VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s,FALSE,%s,%s)
                    ON CONFLICT (province, year, batch, tech_type) DO UPDATE SET
                        price_floor         = EXCLUDED.price_floor,
                        price_cap           = EXCLUDED.price_cap,
                        mechanism_type      = EXCLUDED.mechanism_type,
                        mechanism_value     = EXCLUDED.mechanism_value,
                        supply_demand_ratio = EXCLUDED.supply_demand_ratio,
                        cleared_price       = EXCLUDED.cleared_price,
                        cleared_volume_gwh  = EXCLUDED.cleared_volume_gwh,
                        bid_date            = EXCLUDED.bid_date,
                        source_doc_id       = EXCLUDED.source_doc_id,
                        notes               = EXCLUDED.notes
                    WHERE staging.jizhi_bids.verified = FALSE
                    RETURNING id
                    """,
                    (
                        r.get("province"), r.get("year"), r.get("batch"), r.get("tech_type"),
                        r.get("price_floor"), r.get("price_cap"),
                        r.get("mechanism_type"), r.get("mechanism_value"),
                        r.get("supply_demand_ratio"), r.get("cleared_price"),
                        r.get("cleared_volume_gwh"), r.get("bid_date") or None,
                        source_doc_id, r.get("notes"),
                    ),
                )
                if cur.fetchone():
                    count += 1
        conn.commit()
    except Exception as exc:
        conn.rollback()
        logger.error("[jizhi] save_bids failed: %s", exc)
    finally:
        conn.close()
    return count


def save_upcoming(records: list[dict], pg_url: str) -> int:
    """Upsert upcoming bid records to staging.jizhi_upcoming.

    Verified rows are never overwritten.
    Returns count of rows inserted or updated.
    """
    if not records or not pg_url:
        return 0
    conn = psycopg2.connect(pg_url)
    count = 0
    try:
        with conn.cursor() as cur:
            for r in records:
                cur.execute(
                    """
                    INSERT INTO staging.jizhi_upcoming
                        (province, year, batch, tech_type,
                         price_floor, price_cap, target_volume_gwh, supply_demand_ratio,
                         bid_open_date, bid_close_date, source_url, announcement_date, notes)
                    VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s,%s)
                    ON CONFLICT (province, year, batch, tech_type, bid_open_date) DO UPDATE SET
                        price_floor         = EXCLUDED.price_floor,
                        price_cap           = EXCLUDED.price_cap,
                        target_volume_gwh   = EXCLUDED.target_volume_gwh,
                        supply_demand_ratio = EXCLUDED.supply_demand_ratio,
                        bid_close_date      = EXCLUDED.bid_close_date,
                        source_url          = EXCLUDED.source_url,
                        notes               = EXCLUDED.notes
                    WHERE staging.jizhi_upcoming.verified = FALSE
                    RETURNING id
                    """,
                    (
                        r.get("province"), r.get("year"), r.get("batch"), r.get("tech_type"),
                        r.get("price_floor"), r.get("price_cap"),
                        r.get("target_volume_gwh"), r.get("supply_demand_ratio"),
                        r.get("bid_open_date") or None, r.get("bid_close_date") or None,
                        r.get("source_url"), r.get("announcement_date") or None,
                        r.get("notes"),
                    ),
                )
                if cur.fetchone():
                    count += 1
        conn.commit()
    except Exception as exc:
        conn.rollback()
        logger.error("[jizhi] save_upcoming failed: %s", exc)
    finally:
        conn.close()
    return count
```

- [ ] **Step 2.4: Run all tests**

```bash
py -m pytest services/knowledge_pool/tests/test_jizhi_extractor.py -v
```

Expected: All tests pass (TestEnsureTables: 1, TestExtractBids: 5, TestExtractUpcoming: 3, TestSaveBids: 4, TestSaveUpcoming: 2) = **15 passed**

- [ ] **Step 2.5: Commit**

```bash
git add services/knowledge_pool/jizhi_extractor.py \
        services/knowledge_pool/tests/test_jizhi_extractor.py
git commit -m "feat: jizhi_extractor extract_upcoming + save_bids + save_upcoming"
```

---

## Task 3: Hermes — `_run_jizhi_scan` + APScheduler job + endpoint

**Files:**
- Modify: `services/hermes/app.py`
- Create: `services/hermes/tests/test_jizhi_scan.py`

Context: `services/hermes/app.py` has module-level shims before `logger = logging.getLogger(...)` (currently at line ~142). The `kb_digest_nightly` APScheduler job is at line ~695. The `POST /hermes/knowledge/digest` endpoint is at line ~1038.

- [ ] **Step 3.1: Write failing tests**

Create `services/hermes/tests/test_jizhi_scan.py`:

```python
# services/hermes/tests/test_jizhi_scan.py
"""Unit tests for _run_jizhi_scan helper in services/hermes/app.py."""
from __future__ import annotations
from unittest.mock import MagicMock, patch
import pytest


class TestRunJizhiScan:

    def test_returns_counts_on_success(self):
        mock_feishu = MagicMock()
        with patch("services.hermes.app._jizhi_run_internet_query", return_value="some results text"), \
             patch("services.hermes.app._jizhi_extract_upcoming",
                   return_value=[{"province": "广东", "year": 2026, "batch": "增量_2026-12",
                                   "tech_type": "陆风", "bid_open_date": "2026-03-01"}]), \
             patch("services.hermes.app._jizhi_save_upcoming", return_value=1), \
             patch.dict("os.environ", {"PGURL": "postgresql://fake/db",
                                       "FEISHU_OWNER_OPEN_ID": "test_id"}):
            from services.hermes.app import _run_jizhi_scan
            result = _run_jizhi_scan("test-key", feishu=mock_feishu)

        assert result["new_upcoming"] >= 1
        assert isinstance(result["provinces"], list)

    def test_empty_api_key_returns_zeros(self):
        with patch("services.hermes.app._jizhi_run_internet_query") as mock_search:
            from services.hermes.app import _run_jizhi_scan
            result = _run_jizhi_scan("", feishu=None)

        mock_search.assert_not_called()
        assert result == {"new_upcoming": 0, "provinces": []}

    def test_internet_query_failure_doesnt_crash(self):
        with patch("services.hermes.app._jizhi_run_internet_query",
                   side_effect=RuntimeError("network error")), \
             patch("services.hermes.app._jizhi_extract_upcoming", return_value=[]), \
             patch("services.hermes.app._jizhi_save_upcoming", return_value=0), \
             patch.dict("os.environ", {"PGURL": "postgresql://fake/db"}):
            from services.hermes.app import _run_jizhi_scan
            result = _run_jizhi_scan("key", feishu=None)

        assert result["new_upcoming"] == 0

    def test_new_results_trigger_feishu_notification(self):
        mock_feishu = MagicMock()
        with patch("services.hermes.app._jizhi_run_internet_query", return_value="text"), \
             patch("services.hermes.app._jizhi_extract_upcoming",
                   return_value=[{"province": "山东", "year": 2026,
                                   "batch": "增量_2026-12", "tech_type": "光伏"}]), \
             patch("services.hermes.app._jizhi_save_upcoming", return_value=2), \
             patch.dict("os.environ", {"PGURL": "postgresql://fake/db",
                                       "FEISHU_OWNER_OPEN_ID": "ou_test123"}):
            from services.hermes.app import _run_jizhi_scan
            _run_jizhi_scan("key", feishu=mock_feishu)

        mock_feishu.send_card.assert_called_once()
        call_kwargs = mock_feishu.send_card.call_args
        assert call_kwargs[1]["open_id"] == "ou_test123"

    def test_no_new_results_no_feishu_notification(self):
        mock_feishu = MagicMock()
        with patch("services.hermes.app._jizhi_run_internet_query", return_value="text"), \
             patch("services.hermes.app._jizhi_extract_upcoming", return_value=[]), \
             patch("services.hermes.app._jizhi_save_upcoming", return_value=0), \
             patch.dict("os.environ", {"PGURL": "postgresql://fake/db"}):
            from services.hermes.app import _run_jizhi_scan
            _run_jizhi_scan("key", feishu=mock_feishu)

        mock_feishu.send_card.assert_not_called()
```

- [ ] **Step 3.2: Run tests to confirm they fail**

```bash
py -m pytest services/hermes/tests/test_jizhi_scan.py -v 2>&1 | head -20
```

Expected: `AttributeError` — `_jizhi_run_internet_query` not yet in app.py.

- [ ] **Step 3.3: Add module-level shims and `_run_jizhi_scan` to `services/hermes/app.py`**

Find the block added in Task 2 of the kb-digest work (lines ~92–139, ending just before `logger = logging.getLogger(__name__)`). Add the following immediately after `_run_kb_digest` and before the `logger` line:

```python
# ── Jizhi scan pipeline shims (module-level so tests can patch them) ─────────
def _jizhi_run_internet_query(question: str, api_key: str, pg_url: str) -> str:
    from services.hermes.internet_agent import run_internet_query
    return run_internet_query(question=question, api_key=api_key, pg_url=pg_url)


def _jizhi_extract_upcoming(text: str, api_key: str) -> list[dict]:
    from services.knowledge_pool.jizhi_extractor import extract_upcoming
    return extract_upcoming(text=text, api_key=api_key)


def _jizhi_save_upcoming(records: list[dict], pg_url: str) -> int:
    from services.knowledge_pool.jizhi_extractor import save_upcoming
    return save_upcoming(records=records, pg_url=pg_url)


def _run_jizhi_scan(api_key: str, feishu=None) -> dict:
    """
    Nightly internet scan for 机制竞价 bid announcements.

    Runs 3 broad web searches (via internet_agent) covering all key provinces,
    extracts structured upcoming bid records, upserts to staging.jizhi_upcoming.
    If new records found, sends a Feishu card notification.

    Returns {"new_upcoming": int, "provinces": list[str]}.
    """
    import datetime as _dt
    _log = logging.getLogger(__name__)

    if not api_key:
        _log.warning("[jizhi_scan] skipped — ANTHROPIC_API_KEY not set")
        return {"new_upcoming": 0, "provinces": []}

    pg_url = os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", "")
    year = _dt.datetime.now().year

    # Three broad queries — avoids per-province timeout
    _search_queries = [
        f"各省新能源机制竞价{year}年公告 价格区间 陆风 海风 光伏 申报量 供需比",
        f"广东 山东 浙江 江苏 湖南 机制竞价{year} 竞价结果 中标",
        f"四川 广西 福建 贵州 云南 内蒙古 新疆 机制竞价{year} 公告",
    ]

    total_new = 0
    new_provinces: list[str] = []

    for query in _search_queries:
        try:
            result_text = _jizhi_run_internet_query(
                question=query, api_key=api_key, pg_url=pg_url
            )
            records = _jizhi_extract_upcoming(result_text, api_key)
            if records:
                n = _jizhi_save_upcoming(records, pg_url)
                total_new += n
                for r in records:
                    prov = r.get("province", "")
                    if prov and prov not in new_provinces:
                        new_provinces.append(prov)
        except Exception as exc:
            _log.error("[jizhi_scan] query failed — %s | %s", query[:40], exc)

    if total_new > 0 and feishu:
        _owner = os.environ.get("FEISHU_OWNER_OPEN_ID", "")
        if _owner:
            _provinces_str = "、".join(new_provinces[:8])
            try:
                feishu.send_card(open_id=_owner, card={
                    "header": {
                        "title": {"tag": "plain_text",
                                  "content": f"⚡ 机制竞价新公告 ({total_new}条)"},
                        "template": "orange",
                    },
                    "elements": [{
                        "tag": "markdown",
                        "content": (
                            f"**省份：** {_provinces_str}\n"
                            f"**新增：** {total_new} 条即将竞价记录\n\n"
                            "在 Spot Markets → **机制竞价** → 即将竞价 查看详情"
                        ),
                    }],
                })
            except Exception as exc:
                _log.error("[jizhi_scan] feishu notify failed: %s", exc)

    _log.info("[jizhi_scan] new_upcoming=%d provinces=%s", total_new, new_provinces)
    return {"new_upcoming": total_new, "provinces": new_provinces}
```

- [ ] **Step 3.4: Add APScheduler job inside `create_app()`**

Find the `kb_digest_nightly` job block (around line 695). Add the jizhi scan job **immediately after** its closing `)`:

```python
        # 机制竞价 scan: 10:07 UTC (18:07 Beijing) — search for new provincial notices
        scheduler.add_job(
            lambda: _run_jizhi_scan(
                os.environ.get("ANTHROPIC_API_KEY", ""),
                feishu=feishu,
            ),
            "cron",
            hour=10, minute=7,
            id="jizhi_scan_nightly",
            max_instances=1,
            misfire_grace_time=3600,
        )
```

Note: `feishu` is captured by the lambda closure — it is defined earlier in `create_app()`.

- [ ] **Step 3.5: Add `POST /hermes/jizhi/scan` endpoint inside `create_app()`**

Find `@app.post("/hermes/knowledge/digest")` (line ~1038). Add the jizhi scan endpoint **immediately before** it:

```python
    @app.post("/hermes/jizhi/scan")
    async def run_jizhi_scan_endpoint(background: BackgroundTasks):
        """Trigger 机制竞价 internet scan on demand.

        Returns immediately with {"status": "started"}.
        """
        _api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not _api_key:
            return Response(content="ANTHROPIC_API_KEY not set", status_code=503)
        background.add_task(_run_jizhi_scan, _api_key)
        return {"status": "started"}
```

- [ ] **Step 3.6: Verify the routes are registered**

```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform
py -c "
from services.hermes.app import create_app
app = create_app()
routes = [r.path for r in app.routes]
assert '/hermes/jizhi/scan' in routes, f'missing, got: {routes}'
print('route registered ok')
"
```

Expected: `route registered ok`

- [ ] **Step 3.7: Run tests**

```bash
py -m pytest services/hermes/tests/test_jizhi_scan.py services/hermes/tests/test_kb_digest.py -v
```

Expected: All pass (test_jizhi_scan: 5, test_kb_digest: 5)

- [ ] **Step 3.8: Commit**

```bash
git add services/hermes/app.py services/hermes/tests/test_jizhi_scan.py
git commit -m "feat: hermes _run_jizhi_scan + APScheduler job + POST /hermes/jizhi/scan"
```

---

## Task 4: Hermes — Feishu file routing + `/机制竞价` command

**Files:**
- Modify: `services/hermes/app.py`

Context: `_handle_file_message` is at line ~1878. It downloads the file from Feishu and decides what to do with it. Text command routing is in `_handle_message` at line ~2437, using `_re.match(...)` guards. The last slash command before the LLM fallback is around line 3237–3620.

- [ ] **Step 4.1: Add `_handle_jizhi_file` helper near `_handle_file_message`**

Read `_handle_file_message` (lines 1878–~1960) to understand where the file bytes are available. Then add `_handle_jizhi_file` as a new function **just before** `_handle_file_message`:

```python
def _handle_jizhi_file(
    file_bytes: bytes,
    filename: str,
    sender_id: str,
    feishu,
    api_key: str,
) -> None:
    """Ingest a 机制竞价 file to KB + extract structured bid data."""
    _log = logging.getLogger(__name__)
    pg_url = os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", "")

    # Stage 1: store in knowledge base
    doc_id = None
    try:
        from services.knowledge_pool.knowledge_docs import register_and_ingest
        doc_id, _, _ = register_and_ingest(
            file_bytes=file_bytes,
            filename=filename,
            category_override="policy_doc",
            app="shared",
            api_key=api_key,
        )
    except Exception as exc:
        _log.error("[jizhi_file] kb ingest failed: %s", exc)

    # Stage 2: extract + save structured bids
    bids: list[dict] = []
    saved = 0
    try:
        from services.knowledge_pool.knowledge_docs import _extract_pages
        from services.knowledge_pool.jizhi_extractor import (
            extract_bids, save_bids, ensure_tables,
        )
        ensure_tables(pg_url)
        pages = _extract_pages(file_bytes, filename, api_key)
        full_text = "\n\n".join(text for _, text in pages)
        bids = extract_bids(full_text, api_key)
        saved = save_bids(bids, source_doc_id=doc_id, pg_url=pg_url)
    except Exception as exc:
        _log.error("[jizhi_file] extraction failed: %s", exc)

    # Reply with summary card
    if not feishu or not sender_id:
        return
    if bids:
        provinces = list({b.get("province", "") for b in bids if b.get("province")})
        feishu.send_card(open_id=sender_id, card={
            "header": {
                "title": {"tag": "plain_text", "content": "✅ 机制竞价提取完成"},
                "template": "green",
            },
            "elements": [{
                "tag": "markdown",
                "content": (
                    f"**文件：** {filename}\n"
                    f"**提取记录：** {len(bids)} 条\n"
                    f"**已保存：** {saved} 条\n"
                    f"**涉及省份：** {'、'.join(provinces)}\n\n"
                    "_数据未标记为已验证，请在 Spot Markets → 机制竞价 中核实_"
                ),
            }],
        })
    else:
        feishu.send_text(
            open_id=sender_id,
            text=f"📄 {filename} 已存入知识库，未能提取机制竞价结构化数据。",
        )
```

- [ ] **Step 4.2: Add file-routing check inside `_handle_file_message`**

Read `_handle_file_message` body to find where `filename` is available and where other routing decisions happen (look for `if agent._pending_kb_ingest` or similar state checks). Add this check **early in the function body, after the file bytes are obtained**:

```python
    # Route 机制竞价 files to dedicated handler
    _filename_lower = filename.lower()
    if any(_kw in _filename_lower for _kw in ("机制竞价", "136", "jizhi")):
        _handle_jizhi_file(
            file_bytes=file_bytes,
            filename=filename,
            sender_id=sender_id,
            feishu=feishu,
            api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
        )
        return True
```

This must go BEFORE any state-machine check (like `_pending_kb_ingest`) so the explicit filename match takes priority.

- [ ] **Step 4.3: Add `/机制竞价` command in `_handle_message`**

Find the last slash-command block before the LLM fallback (the `/model` command at ~line 3629 or `/save` at ~line 3620). Add the jizhi command block **after the last slash command and before the LLM fallback at line ~3674**:

```python
        if _re.match(r'^/?机制竞价$', msg.text.strip()):
            _pg = os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", "")

            def _send_jizhi_card() -> None:
                try:
                    import psycopg2 as _pg2
                    conn = _pg2.connect(_pg)
                    cur = conn.cursor()
                    cur.execute("""
                        SELECT province, year, batch, tech_type,
                               price_floor, price_cap, bid_open_date, bid_close_date
                        FROM staging.jizhi_upcoming
                        WHERE bid_open_date >= CURRENT_DATE
                          AND bid_open_date <= CURRENT_DATE + INTERVAL '90 days'
                        ORDER BY bid_open_date ASC
                        LIMIT 8
                    """)
                    upcoming = cur.fetchall()
                    cur.execute("""
                        SELECT province, year, batch, tech_type,
                               cleared_price, cleared_volume_gwh
                        FROM staging.jizhi_bids
                        ORDER BY bid_date DESC NULLS LAST, created_at DESC
                        LIMIT 5
                    """)
                    recent = cur.fetchall()
                    conn.close()

                    up_lines = "\n".join(
                        f"• {r[0]} {r[1]} {r[2]} {r[3]}: "
                        + (f"¥{r[4]}–{r[5]}/kWh " if r[4] else "价格待定 ")
                        + (str(r[6]) if r[6] else "日期待定")
                        for r in upcoming
                    ) or "（暂无近期公告）"
                    rec_lines = "\n".join(
                        f"• {r[0]} {r[1]} {r[2]} {r[3]}: "
                        + (f"¥{r[4]}/kWh" if r[4] else "价格未知")
                        + (f", {r[5]} GWh" if r[5] else "")
                        for r in recent
                    ) or "（暂无历史记录）"

                    card = {
                        "header": {
                            "title": {"tag": "plain_text", "content": "⚡ 机制竞价信息"},
                            "template": "blue",
                        },
                        "elements": [
                            {"tag": "markdown",
                             "content": f"**📅 即将竞价（90天内）**\n{up_lines}"},
                            {"tag": "hr"},
                            {"tag": "markdown",
                             "content": f"**📊 最近结果**\n{rec_lines}"},
                        ],
                    }
                    if msg.source == "feishu" and feishu:
                        feishu.send_card(open_id=msg.sender_id, card=card)
                    elif msg.source == "telegram" and telegram:
                        telegram.send_text(
                            chat_id=msg.sender_id,
                            text=f"即将竞价：\n{up_lines}\n\n最近结果：\n{rec_lines}",
                        )
                except Exception as exc:
                    logger.error("[jizhi_cmd] failed: %s", exc)

            import threading as _threading_jz
            _threading_jz.Thread(target=_send_jizhi_card, daemon=True).start()
            return True
```

- [ ] **Step 4.4: Import check**

```bash
py -c "from services.hermes.app import create_app; print('ok')"
```

Expected: `ok`

- [ ] **Step 4.5: Commit**

```bash
git add services/hermes/app.py
git commit -m "feat: hermes feishu 机制竞价 file routing + /机制竞价 command"
```

---

## Task 5: Spot-market — 机制竞价 tab (历史结果 + 即将竞价)

**Files:**
- Modify: `apps/spot-market/app.py`

Context:
- `st.tabs()` call at lines 1633–1638 — unpack 13 variables
- New tab `tab_jizhi` inserted between `tab_library` and `tab_mgmt`
- Tab bodies: `with tab_library:` at line 4628, `with tab_mgmt:` at line 4633
- DB connection: use `psycopg2.connect(os.environ.get("PGURL", "..."))` pattern (see line 720)
- Plotly already imported at top of file as `go` (confirm with grep if needed)

- [ ] **Step 5.1: Add `tab_jizhi` to `st.tabs()` at line 1633**

Change:
```python
tab_overview, tab_spread, tab_heatmap, tab_intraday, tab_province, tab_dist, tab_geo, \
tab_interprov, tab_fundamentals, tab_agent, tab_news, tab_library, tab_mgmt = st.tabs([
    _t("tab_overview"), _t("tab_spread"), _t("tab_heatmap"), _t("tab_intraday"),
    _t("tab_province"), _t("tab_dist"), _t("tab_geo"),
    _t("tab_interprov"), _t("tab_fundamentals"), _t("tab_agent"), _t("tab_news"), "Library", _t("tab_mgmt"),
])
```

To:
```python
tab_overview, tab_spread, tab_heatmap, tab_intraday, tab_province, tab_dist, tab_geo, \
tab_interprov, tab_fundamentals, tab_agent, tab_news, tab_library, tab_jizhi, tab_mgmt = st.tabs([
    _t("tab_overview"), _t("tab_spread"), _t("tab_heatmap"), _t("tab_intraday"),
    _t("tab_province"), _t("tab_dist"), _t("tab_geo"),
    _t("tab_interprov"), _t("tab_fundamentals"), _t("tab_agent"), _t("tab_news"),
    "Library", "机制竞价", _t("tab_mgmt"),
])
```

- [ ] **Step 5.2: Add tab body between `tab_library` and `tab_mgmt` (after line 4631)**

Insert the following block after `with tab_library:` section (after the `render_library_tab(...)` call, before `# ── Tab 9: Data Management`):

```python
# ── 机制竞价 ──────────────────────────────────────────────────────────────────
with tab_jizhi:
    import psycopg2 as _jz_pg
    import pandas as _jz_pd
    import plotly.graph_objects as _jz_go
    from datetime import date as _jz_date

    _jz_pg_url = (
        os.environ.get("PGURL")
        or os.environ.get("DATABASE_URL")
        or "postgresql://postgres:root@127.0.0.1:5433/marketdata"
    )

    @st.cache_data(ttl=300, show_spinner=False)
    def _load_jizhi_bids(_pg: str) -> _jz_pd.DataFrame:
        try:
            conn = _jz_pg.connect(_pg)
            df = _jz_pd.read_sql("""
                SELECT id, province, year, batch, tech_type,
                       price_floor, price_cap, mechanism_type, mechanism_value,
                       supply_demand_ratio, cleared_price, cleared_volume_gwh,
                       bid_date, verified, notes, source_doc_id
                FROM staging.jizhi_bids
                ORDER BY year DESC, province, batch, tech_type
            """, conn)
            conn.close()
            return df
        except Exception:
            return _jz_pd.DataFrame()

    @st.cache_data(ttl=300, show_spinner=False)
    def _load_jizhi_upcoming(_pg: str) -> _jz_pd.DataFrame:
        try:
            conn = _jz_pg.connect(_pg)
            df = _jz_pd.read_sql("""
                SELECT id, province, year, batch, tech_type,
                       price_floor, price_cap, target_volume_gwh,
                       supply_demand_ratio, bid_open_date, bid_close_date,
                       source_url, announcement_date, verified, notes, created_at
                FROM staging.jizhi_upcoming
                ORDER BY bid_open_date ASC NULLS LAST
            """, conn)
            conn.close()
            return df
        except Exception:
            return _jz_pd.DataFrame()

    @st.cache_data(ttl=300, show_spinner=False)
    def _load_jizhi_winners(_bid_id: int, _pg: str) -> _jz_pd.DataFrame:
        try:
            conn = _jz_pg.connect(_pg)
            df = _jz_pd.read_sql("""
                SELECT project_name, operator, capacity_mw, cleared_price, tech_type
                FROM staging.jizhi_bid_winners
                WHERE bid_id = %s
                ORDER BY capacity_mw DESC NULLS LAST
            """, conn, params=[_bid_id])
            conn.close()
            return df
        except Exception:
            return _jz_pd.DataFrame()

    _jz_tab_results, _jz_tab_upcoming, _jz_tab_upload = st.tabs(
        ["📊 历史结果", "📅 即将竞价", "📂 上传 & 录入"]
    )

    # ── Sub-tab 1: 历史结果 ────────────────────────────────────────────────────
    with _jz_tab_results:
        _jz_bids_df = _load_jizhi_bids(_jz_pg_url)

        if _jz_bids_df.empty:
            st.info("暂无历史竞价数据。请在「上传 & 录入」标签中上传竞价文件。")
        else:
            # Filter row
            _jz_col_prov, _jz_col_year, _jz_col_tech = st.columns([2, 2, 2])
            with _jz_col_prov:
                _jz_provs = st.multiselect(
                    "省份", sorted(_jz_bids_df["province"].unique()), key="jz_prov_filter"
                )
            with _jz_col_year:
                _jz_years = st.multiselect(
                    "年份", sorted(_jz_bids_df["year"].unique(), reverse=True), key="jz_year_filter"
                )
            with _jz_col_tech:
                _jz_techs = st.multiselect(
                    "技术类型", sorted(_jz_bids_df["tech_type"].unique()), key="jz_tech_filter"
                )

            _jz_filtered = _jz_bids_df.copy()
            if _jz_provs:
                _jz_filtered = _jz_filtered[_jz_filtered["province"].isin(_jz_provs)]
            if _jz_years:
                _jz_filtered = _jz_filtered[_jz_filtered["year"].isin(_jz_years)]
            if _jz_techs:
                _jz_filtered = _jz_filtered[_jz_filtered["tech_type"].isin(_jz_techs)]

            # Add ⚠️ badge for unverified rows
            _jz_display = _jz_filtered.copy()
            _jz_display["verified"] = _jz_display["verified"].apply(
                lambda v: "✅" if v else "⚠️"
            )
            _jz_display = _jz_display.rename(columns={
                "province": "省份", "year": "年份", "batch": "批次",
                "tech_type": "技术", "price_floor": "价格下限", "price_cap": "价格上限",
                "mechanism_type": "机制类型", "mechanism_value": "机制量",
                "supply_demand_ratio": "供需比", "cleared_price": "中标价格",
                "cleared_volume_gwh": "中标量(GWh)", "bid_date": "竞价日期", "verified": "验证",
            })
            _jz_show_cols = [
                "省份", "年份", "批次", "技术", "价格下限", "价格上限",
                "机制类型", "机制量", "供需比", "中标价格", "中标量(GWh)", "竞价日期", "验证"
            ]
            st.dataframe(
                _jz_display[[c for c in _jz_show_cols if c in _jz_display.columns]],
                use_container_width=True, hide_index=True,
            )

            # Winner list selector
            if not _jz_filtered.empty:
                with st.expander("🏆 查看中标清单 (选择竞价记录)"):
                    _jz_opts = {
                        f"{r['province']} {r['year']} {r['batch']} {r['tech_type']}": int(r["id"])
                        for _, r in _jz_filtered.iterrows()
                    }
                    _jz_sel = st.selectbox("选择竞价记录", list(_jz_opts.keys()), key="jz_winner_sel")
                    if _jz_sel:
                        _jz_bid_id = _jz_opts[_jz_sel]
                        _jz_winners_df = _load_jizhi_winners(_jz_bid_id, _jz_pg_url)
                        if _jz_winners_df.empty:
                            st.info("该竞价暂无中标清单数据。")
                        else:
                            st.dataframe(_jz_winners_df, use_container_width=True, hide_index=True)

            # Charts
            if len(_jz_filtered) >= 2:
                st.divider()
                _jz_chart_col1, _jz_chart_col2 = st.columns(2)
                with _jz_chart_col1:
                    _jz_avg = (
                        _jz_filtered.dropna(subset=["cleared_price"])
                        .groupby(["province", "tech_type"])["cleared_price"]
                        .mean()
                        .reset_index()
                    )
                    if not _jz_avg.empty:
                        _jz_fig_bar = _jz_go.Figure()
                        for _tech in _jz_avg["tech_type"].unique():
                            _sub = _jz_avg[_jz_avg["tech_type"] == _tech]
                            _jz_fig_bar.add_trace(_jz_go.Bar(
                                x=_sub["province"], y=_sub["cleared_price"], name=_tech
                            ))
                        _jz_fig_bar.update_layout(
                            title="各省平均中标价格 (元/kWh)",
                            barmode="group", height=320,
                            margin=dict(l=0, r=0, t=36, b=0),
                            legend=dict(orientation="h", y=-0.2),
                        )
                        st.plotly_chart(_jz_fig_bar, use_container_width=True)

                with _jz_chart_col2:
                    _jz_sd = _jz_filtered.dropna(subset=["supply_demand_ratio", "year"])
                    if not _jz_sd.empty:
                        _jz_fig_sd = _jz_go.Figure()
                        for _prov in _jz_sd["province"].unique():
                            _sub = _jz_sd[_jz_sd["province"] == _prov].sort_values("year")
                            _jz_fig_sd.add_trace(_jz_go.Scatter(
                                x=_sub["year"], y=_sub["supply_demand_ratio"],
                                name=_prov, mode="lines+markers"
                            ))
                        _jz_fig_sd.update_layout(
                            title="供需比趋势",
                            height=320, margin=dict(l=0, r=0, t=36, b=0),
                            legend=dict(orientation="h", y=-0.2),
                        )
                        st.plotly_chart(_jz_fig_sd, use_container_width=True)

    # ── Sub-tab 2: 即将竞价 ────────────────────────────────────────────────────
    with _jz_tab_upcoming:
        _jz_up_df = _load_jizhi_upcoming(_jz_pg_url)

        # Last scan timestamp
        _jz_last_scan = (
            str(_jz_up_df["created_at"].max())[:16]
            if not _jz_up_df.empty and "created_at" in _jz_up_df.columns
            else "—"
        )
        st.caption(f"数据最后更新：{_jz_last_scan}  ·  每晚 18:07 (北京时间) 自动扫描")

        if _jz_up_df.empty:
            st.info("暂无即将竞价信息。数据将由 Hermes 每晚自动扫描更新。")
        else:
            _jz_up_col_prov, _jz_up_col_tech = st.columns(2)
            with _jz_up_col_prov:
                _jz_up_provs = st.multiselect(
                    "省份", sorted(_jz_up_df["province"].unique()), key="jz_up_prov"
                )
            with _jz_up_col_tech:
                _jz_up_techs = st.multiselect(
                    "技术类型", sorted(_jz_up_df["tech_type"].unique()), key="jz_up_tech"
                )

            _jz_up_filtered = _jz_up_df.copy()
            if _jz_up_provs:
                _jz_up_filtered = _jz_up_filtered[
                    _jz_up_filtered["province"].isin(_jz_up_provs)
                ]
            if _jz_up_techs:
                _jz_up_filtered = _jz_up_filtered[
                    _jz_up_filtered["tech_type"].isin(_jz_up_techs)
                ]

            # Compute days-until column
            _today = _jz_date.today()
            def _days_until(d):
                if _jz_pd.isna(d):
                    return None
                return (d.date() if hasattr(d, "date") else d) - _today

            _jz_up_filtered = _jz_up_filtered.copy()
            _jz_up_filtered["距今"] = _jz_up_filtered["bid_open_date"].apply(
                lambda d: f"{_days_until(d).days}天" if _days_until(d) is not None else "—"
            )

            _jz_up_display = _jz_up_filtered.rename(columns={
                "province": "省份", "year": "年份", "batch": "批次",
                "tech_type": "技术", "price_floor": "价格下限", "price_cap": "价格上限",
                "target_volume_gwh": "目标量(GWh)", "supply_demand_ratio": "供需比",
                "bid_open_date": "开始日期", "bid_close_date": "截止日期",
                "verified": "已验证",
            })
            _jz_up_show = [
                "省份", "年份", "批次", "技术", "价格下限", "价格上限",
                "目标量(GWh)", "供需比", "开始日期", "截止日期", "距今", "已验证"
            ]
            st.dataframe(
                _jz_up_display[[c for c in _jz_up_show if c in _jz_up_display.columns]],
                use_container_width=True, hide_index=True,
            )
```

- [ ] **Step 5.3: Syntax check**

```bash
py -c "import ast; ast.parse(open('apps/spot-market/app.py').read()); print('syntax ok')"
```

Expected: `syntax ok`

- [ ] **Step 5.4: Commit**

```bash
git add apps/spot-market/app.py
git commit -m "feat: spot-market 机制竞价 tab (历史结果 + 即将竞价)"
```

---

## Task 6: Spot-market — 上传 & 录入 sub-tab

**Files:**
- Modify: `apps/spot-market/app.py`

Context: The `_jz_tab_upload` variable was created in Task 5. This task fills its body. It goes in the same `with tab_jizhi:` block, after the `with _jz_tab_upcoming:` block. The upload pattern mirrors the existing "📂 Upload Files" sub-tab in `tab_agent` (line ~3892 in the original file) which calls `register_and_ingest()`.

- [ ] **Step 6.1: Add `with _jz_tab_upload:` body**

Add the following block after the `with _jz_tab_upcoming:` block (still inside `with tab_jizhi:`):

```python
    # ── Sub-tab 3: 上传 & 录入 ─────────────────────────────────────────────────
    with _jz_tab_upload:
        st.markdown("上传竞价结果文件（PPT/PDF/Excel）→ AI 自动提取结构化数据 → 预览确认 → 保存")

        _jz_up_file = st.file_uploader(
            "选择文件",
            type=["pdf", "pptx", "ppt", "xlsx", "xls", "docx", "doc", "txt", "jpg", "jpeg", "png"],
            key="jz_upload_file",
        )
        _jz_up_url = st.text_input("或输入 URL", placeholder="https://...", key="jz_upload_url")

        if _jz_up_file is not None or (_jz_up_url and st.button("获取 URL", key="jz_fetch_url")):
            _jz_api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            if not _jz_api_key:
                st.error("ANTHROPIC_API_KEY 未配置，无法运行 AI 提取。")
            else:
                with st.spinner("正在提取竞价数据…"):
                    try:
                        from services.knowledge_pool.knowledge_docs import (
                            register_and_ingest, register_url, _extract_pages,
                        )
                        from services.knowledge_pool.jizhi_extractor import (
                            extract_bids, save_bids, ensure_tables,
                        )
                        ensure_tables(_jz_pg_url)

                        if _jz_up_file is not None:
                            _jz_fbytes = _jz_up_file.read()
                            _jz_fname  = _jz_up_file.name
                            _jz_doc_id, _, _ = register_and_ingest(
                                file_bytes=_jz_fbytes, filename=_jz_fname,
                                category_override="policy_doc", app="shared",
                                api_key=_jz_api_key,
                            )
                            _jz_pages = _extract_pages(_jz_fbytes, _jz_fname, _jz_api_key)
                        else:
                            _jz_doc_id, _, _ = register_url(_jz_up_url, api_key=_jz_api_key)
                            _jz_pages = _extract_pages(
                                b"", f"url_{_jz_up_url[-20:]}.txt", _jz_api_key
                            )

                        _jz_full_text = "\n\n".join(t for _, t in _jz_pages)
                        _jz_extracted = extract_bids(_jz_full_text, _jz_api_key)
                    except Exception as _e:
                        st.error(f"提取失败：{_e}")
                        _jz_extracted = []
                        _jz_doc_id = None

                if _jz_extracted:
                    st.success(f"提取到 {len(_jz_extracted)} 条竞价记录，请确认后保存：")
                    _jz_preview_df = _jz_pd.DataFrame(_jz_extracted)
                    _jz_edited = st.data_editor(
                        _jz_preview_df, use_container_width=True,
                        num_rows="dynamic", key="jz_preview_editor",
                    )
                    if st.button("💾 保存到数据库", key="jz_save_btn"):
                        _jz_n = save_bids(
                            _jz_edited.to_dict("records"),
                            source_doc_id=_jz_doc_id,
                            pg_url=_jz_pg_url,
                        )
                        st.success(f"已保存 {_jz_n} 条记录（已存在且已验证的记录不会被覆盖）")
                        _load_jizhi_bids.clear()
                else:
                    st.warning("未能从文件中提取结构化竞价数据。文件已存入知识库。")

        st.divider()
        with st.expander("✏️ 手动录入单条记录"):
            _jz_m_col1, _jz_m_col2 = st.columns(2)
            with _jz_m_col1:
                _jz_m_prov  = st.text_input("省份", placeholder="广东", key="jz_m_prov")
                _jz_m_year  = st.number_input("年份", min_value=2020, max_value=2035,
                                               value=2025, step=1, key="jz_m_year")
                _jz_m_batch = st.selectbox(
                    "批次", ["存量", "增量_2025-12", "增量_2026-12", "增量_2027-12"],
                    key="jz_m_batch"
                )
                _jz_m_tech  = st.selectbox(
                    "技术类型", ["陆风", "海风", "光伏", "水电"], key="jz_m_tech"
                )
                _jz_m_pfloor = st.number_input("价格下限 (元/kWh)", min_value=0.0,
                                                step=0.001, format="%.4f", key="jz_m_pfloor")
                _jz_m_pcap   = st.number_input("价格上限 (元/kWh)", min_value=0.0,
                                                step=0.001, format="%.4f", key="jz_m_pcap")
            with _jz_m_col2:
                _jz_m_mtype  = st.selectbox("机制类型", ["小时数", "电量", "比例"], key="jz_m_mtype")
                _jz_m_mval   = st.number_input("机制量 (小时/GWh/%)", min_value=0.0,
                                                step=1.0, key="jz_m_mval")
                _jz_m_sdr    = st.number_input("供需比", min_value=0.0, step=0.01,
                                                format="%.2f", key="jz_m_sdr")
                _jz_m_cprice = st.number_input("中标价格 (元/kWh)", min_value=0.0,
                                                step=0.001, format="%.4f", key="jz_m_cprice")
                _jz_m_cvol   = st.number_input("中标量 (GWh)", min_value=0.0,
                                                step=1.0, key="jz_m_cvol")
                _jz_m_date   = st.date_input("竞价日期", key="jz_m_date")
                _jz_m_notes  = st.text_input("备注", key="jz_m_notes")

            if st.button("保存手动记录", key="jz_m_save"):
                if not _jz_m_prov:
                    st.error("省份不能为空。")
                else:
                    from services.knowledge_pool.jizhi_extractor import save_bids, ensure_tables
                    ensure_tables(_jz_pg_url)
                    _jz_manual_rec = [{
                        "province": _jz_m_prov, "year": int(_jz_m_year),
                        "batch": _jz_m_batch, "tech_type": _jz_m_tech,
                        "price_floor": _jz_m_pfloor or None, "price_cap": _jz_m_pcap or None,
                        "mechanism_type": _jz_m_mtype, "mechanism_value": _jz_m_mval or None,
                        "supply_demand_ratio": _jz_m_sdr or None,
                        "cleared_price": _jz_m_cprice or None,
                        "cleared_volume_gwh": _jz_m_cvol or None,
                        "bid_date": str(_jz_m_date), "notes": _jz_m_notes or None,
                    }]
                    _jz_n2 = save_bids(_jz_manual_rec, source_doc_id=None, pg_url=_jz_pg_url)
                    if _jz_n2:
                        st.success("已保存。（数据标记为未验证）")
                        _load_jizhi_bids.clear()
                    else:
                        st.info("记录已存在且已验证，未覆盖。")
```

- [ ] **Step 6.2: Syntax check**

```bash
py -c "import ast; ast.parse(open('apps/spot-market/app.py').read()); print('syntax ok')"
```

Expected: `syntax ok`

- [ ] **Step 6.3: Commit**

```bash
git add apps/spot-market/app.py
git commit -m "feat: spot-market 机制竞价 上传&录入 sub-tab"
```

---

## Task 7: Deploy

**Files:** none — build + push + ECS update

- [ ] **Step 7.1: Run all unit tests**

```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform
py -m pytest services/knowledge_pool/tests/test_jizhi_extractor.py \
             services/hermes/tests/test_jizhi_scan.py \
             services/hermes/tests/test_kb_digest.py -v
```

Expected: All pass (15 + 5 + 5 = **25 passed**)

- [ ] **Step 7.2: Build and push Hermes image**

```powershell
/c/WINDOWS/System32/WindowsPowerShell/v1.0/powershell -File scripts/deploy_hermes.ps1
```

If the PowerShell script fails at task-def registration (known issue), run the Python fallback:

```bash
py scripts/update_hermes_taskdef.py
```

- [ ] **Step 7.3: Build and push spot-markets image**

```bash
ECR="319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets"
docker build -t "bess-spot-markets:v54" -f apps/spot-market/Dockerfile .
docker tag "bess-spot-markets:v54" "$ECR:v54"
docker push "$ECR:v54"
IMAGE_TAG=v54 py scripts/update_spot_markets_taskdef.py
```

- [ ] **Step 7.4: Verify Hermes endpoint**

```bash
curl -sk -X POST https://pjh-etrm.ai/hermes/jizhi/scan | cat
```

Note: `pjh-etrm.ai` blocks POST via CloudFront (returns 405). Verify via ECS deployment status instead:

```bash
aws ecs describe-services \
  --cluster bess-platform-cluster \
  --services bess-platform-hermes-svc \
  --region ap-southeast-1 \
  --query 'services[0].deployments[*].{status:status,running:runningCount,taskDef:taskDefinition}' \
  --output json
```

Expected: single PRIMARY deployment with `runningCount: 1`.

- [ ] **Step 7.5: Backfill — upload the existing 2025 PPT**

Once the spot-markets app is deployed, upload the existing PPT via the new tab:

File path: `C:\Users\dipeng.chen\OneDrive\Envision Energy\136号文件\2025年机制竞价结果回顾与分析.pptx`

Go to Spot Markets → 机制竞价 → 上传 & 录入 → upload file → review extracted rows → Save to DB.

---

## Self-Review

**Spec coverage:**
- ✅ DB tables (`ensure_tables`): Task 1
- ✅ `extract_bids`: Task 1
- ✅ `extract_upcoming`: Task 2
- ✅ `save_bids` / `save_upcoming`: Task 2
- ✅ `_run_jizhi_scan` helper + module-level shims: Task 3
- ✅ APScheduler job at 10:07 UTC: Task 3
- ✅ `POST /hermes/jizhi/scan`: Task 3
- ✅ Feishu file routing (`_handle_jizhi_file`): Task 4
- ✅ `/机制竞价` Feishu command: Task 4
- ✅ Streamlit 历史结果 sub-tab (table + charts + winner list): Task 5
- ✅ Streamlit 即将竞价 sub-tab (countdown + filters): Task 5
- ✅ Streamlit 上传&录入 sub-tab (file upload + URL + manual entry): Task 6
- ✅ Deploy: Task 7

**No placeholders:** All code blocks contain complete working code.

**Type consistency:**
- `extract_bids(text: str, api_key: str) -> list[dict]` — consistent across Tasks 1, 4, 6
- `extract_upcoming(text: str, api_key: str) -> list[dict]` — consistent across Tasks 2, 3
- `save_bids(records, source_doc_id, pg_url) -> int` — consistent across Tasks 2, 4, 6
- `save_upcoming(records, pg_url) -> int` — consistent across Tasks 2, 3
- `ensure_tables(pg_url)` — consistent across Tasks 1, 4, 6
- `_run_jizhi_scan(api_key, feishu=None) -> dict` — consistent across Tasks 3, 7
- `_jz_pg_url` DB connection variable — consistent across Tasks 5, 6
- `_load_jizhi_bids`, `_load_jizhi_upcoming`, `_load_jizhi_winners` cache loaders — consistent between Task 5 definition and Task 6 usage (`.clear()` calls)
