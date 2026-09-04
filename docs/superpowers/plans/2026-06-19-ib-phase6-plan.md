# IB Platform Phase 6 — Advisor App Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Pre-Trade and Daily Briefing advisor to the portfolio app, backed by tag-based KB insight retrieval, a daily briefing generator, and a trade outcome monitor.

**Architecture:** Two new tabs are added to `apps/portfolio/app.py`. The Pre-Trade tab uses `inject_memory` to surface relevant KB insights by tag and drives a Sonnet chat. The Daily Briefing tab calls `generate_daily_briefing` on demand and lets users extract insights from session notes. A `trade_monitor.py` APScheduler service runs at 18:00 ET to extract lessons from the day's closed trades.

**Tech Stack:** Python 3.13, Streamlit, Anthropic (Haiku + Sonnet), APScheduler, psycopg2

---

## Context

- Repo: `C:\Users\dipeng.chen\OneDrive\ETRM\ib-platform`
- Run tests: `cd C:/Users/dipeng.chen/OneDrive/ETRM/ib-platform && /c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/ -q`
- Design spec: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\docs\superpowers\specs\2026-06-19-ib-phase6-design.md`
- Phase 5 left 372 tests passing. All tasks must keep the suite green.
- `services/knowledge/expert_memory.py` already exists with `_write_insight`, `digest_kb_docs`, `extract_from_trade_outcome`. Phase 6 extends it.
- `apps/portfolio/app.py` has 7 tabs (indexes 0–6). Phase 6 adds tabs 7 and 8.
- Pattern for APScheduler services: see `services/knowledge/ingest.py` — `conn=None/try/finally`, `build_scheduler()` returns `BlockingScheduler`.
- Pattern for Streamlit tabs: single `render(conn, ...)` function, `import streamlit as st` inside `render()` only.

---

### Task 1: DB Migration — add `tags` column to `kb_insights`

**Files:**
- Create: `db/migrations/003_kb_insights_tags.sql`

- [ ] **Step 1: Write the migration file**

```sql
-- db/migrations/003_kb_insights_tags.sql
-- Adds tags TEXT[] column to trading.kb_insights for tag-based insight retrieval.
-- Run once: psql $PGURL -f db/migrations/003_kb_insights_tags.sql

ALTER TABLE trading.kb_insights
    ADD COLUMN IF NOT EXISTS tags TEXT[] NOT NULL DEFAULT '{}';

CREATE INDEX IF NOT EXISTS idx_kb_insights_tags
    ON trading.kb_insights USING GIN(tags);
```

- [ ] **Step 2: Commit**

```bash
git add db/migrations/003_kb_insights_tags.sql
git commit -m "feat(db): add tags column to kb_insights"
```

---

### Task 2: Extend `_write_insight` with `tags` + update Haiku prompt

**Files:**
- Modify: `services/knowledge/expert_memory.py`
- Modify: `tests/services/knowledge/test_expert_memory.py`

This task changes the Haiku JSON schema to emit `tags`, adds `tags` to `_write_insight`, and updates all callers. Existing tests must still pass.

- [ ] **Step 1: Write the failing test** (verifies tags flow through digest and trade outcome)

Add this class to the bottom of `tests/services/knowledge/test_expert_memory.py`:

```python
def _mock_haiku_response_with_tags(insight_text="rates elevated", insight_type="macro_risk",
                                    confidence="high", tags=None):
    if tags is None:
        tags = ["rates", "macro"]
    mock_client = MagicMock()
    mock_msg = MagicMock()
    mock_msg.content[0].text = json.dumps({
        "insights": [{"insight": insight_text, "type": insight_type,
                      "confidence": confidence, "tags": tags}]
    })
    mock_client.messages.create.return_value = mock_msg
    return mock_client


class TestWriteInsightTags:
    def test_digest_passes_tags_from_haiku_to_write_insight(self):
        mock_client = _mock_haiku_response_with_tags(tags=["rates", "macro"])
        conn = MagicMock()

        with patch("services.knowledge.expert_memory._fetch_undigested", return_value=[_SAMPLE_DOC]), \
             patch("services.knowledge.expert_memory._write_insight") as mock_write, \
             patch("anthropic.Anthropic", return_value=mock_client):
            from services.knowledge.expert_memory import digest_kb_docs
            digest_kb_docs(conn, api_key="test_key")

        call_kwargs = mock_write.call_args[1]
        assert call_kwargs["tags"] == ["rates", "macro"]

    def test_extract_from_trade_outcome_passes_tags(self):
        mock_client = _mock_haiku_response_with_tags(
            insight_text="delta dominated", insight_type="trade_outcome",
            confidence="high", tags=["SPY", "equities"]
        )
        conn = MagicMock()

        with patch("services.knowledge.expert_memory._write_insight") as mock_write, \
             patch("anthropic.Anthropic", return_value=mock_client):
            from services.knowledge.expert_memory import extract_from_trade_outcome
            extract_from_trade_outcome(
                conn, trade_id=1, signal_source="vix_model",
                expected_direction="long", actual_pnl=100.0,
                pnl_explain={"delta_pct": 100, "gamma_pct": 0, "vega_pct": 0, "theta_pct": 0},
                market_context={}, api_key="test_key",
            )

        call_kwargs = mock_write.call_args[1]
        assert call_kwargs["tags"] == ["SPY", "equities"]
```

- [ ] **Step 2: Run to verify it fails**

```
pytest tests/services/knowledge/test_expert_memory.py::TestWriteInsightTags -v
```
Expected: FAIL — `_write_insight()` got unexpected keyword argument `'tags'`

- [ ] **Step 3: Update `_SYSTEM_PROMPT` in `expert_memory.py`**

Replace the last two lines of `_SYSTEM_PROMPT`:

Old:
```python
'Respond ONLY with valid JSON, no markdown:\n'
'{"insights": [{"insight": "...", "type": "...", "confidence": "high|medium|low"}]}'
```

New:
```python
'Respond ONLY with valid JSON, no markdown:\n'
'{"insights": [{"insight": "...", "type": "...", "confidence": "high|medium|low", "tags": ["SPY", "rates"]}]}\n'
'Tags: ticker symbols mentioned, asset classes (equities, rates, fx, vol, macro, credit), strategy types (spread, options, fixed_income).'
```

- [ ] **Step 4: Update `_write_insight` to accept `tags`**

Replace the existing `_write_insight` function body:

```python
def _write_insight(
    conn,
    *,
    insight: str,
    insight_type: str,
    confidence: str,
    tags: list = (),
    source_doc_url: str | None = None,
    source_trade_id: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO trading.kb_insights
                (insight_text, insight_type, confidence, tags, source_doc_url, source_trade_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (insight, insight_type, confidence, list(tags), source_doc_url, source_trade_id),
        )
    conn.commit()
```

- [ ] **Step 5: Update `digest_kb_docs` caller**

In `digest_kb_docs`, find the `_write_insight(...)` call and add `tags=item.get("tags", [])`:

```python
_write_insight(
    conn,
    insight=item.get("insight", ""),
    insight_type=item.get("type", "market_regime"),
    confidence=item.get("confidence", "medium"),
    tags=item.get("tags", []),
    source_doc_url=doc["url"],
)
```

- [ ] **Step 6: Update `extract_from_trade_outcome` caller**

In `extract_from_trade_outcome`, find the `_write_insight(...)` call and add `tags=item.get("tags", [])`:

```python
_write_insight(
    conn,
    insight=item.get("insight", ""),
    insight_type="trade_outcome",
    confidence=item.get("confidence", "medium"),
    tags=item.get("tags", []),
    source_doc_url=None,
    source_trade_id=str(trade_id),
)
```

- [ ] **Step 7: Run all tests to verify green**

```
pytest tests/services/knowledge/test_expert_memory.py -v
```
Expected: All tests pass (existing 8 + new 2 = 10)

- [ ] **Step 8: Commit**

```bash
git add services/knowledge/expert_memory.py tests/services/knowledge/test_expert_memory.py
git commit -m "feat(knowledge): add tags to _write_insight and Haiku extraction prompt"
```

---

### Task 3: Channel 1 — `inject_memory` and `extract_insights`

**Files:**
- Modify: `services/knowledge/expert_memory.py`
- Modify: `tests/services/knowledge/test_expert_memory.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/services/knowledge/test_expert_memory.py`:

```python
class TestInjectMemory:
    def test_returns_insights_with_matching_tags(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = [
            ("rates will stay elevated", "macro_risk", "high"),
            ("vol term structure inverted", "vol_signal", "medium"),
        ]
        cur.description = [("insight_text",), ("insight_type",), ("confidence",)]

        from services.knowledge.expert_memory import inject_memory
        result = inject_memory(conn, tags=["rates", "macro"])

        assert len(result) == 2
        assert result[0]["insight_text"] == "rates will stay elevated"
        assert result[0]["insight_type"] == "macro_risk"

    def test_empty_tags_returns_empty_list_without_db_call(self):
        conn = MagicMock()

        from services.knowledge.expert_memory import inject_memory
        result = inject_memory(conn, tags=[])

        assert result == []
        conn.cursor.assert_not_called()

    def test_db_error_returns_empty_list(self):
        conn = MagicMock()
        conn.cursor.side_effect = Exception("DB error")

        from services.knowledge.expert_memory import inject_memory
        result = inject_memory(conn, tags=["rates"])

        assert result == []


class TestExtractInsights:
    def test_writes_insights_with_tags_and_returns_count(self):
        mock_client = MagicMock()
        mock_msg = MagicMock()
        mock_msg.content[0].text = json.dumps({
            "insights": [
                {"insight": "SPY vol elevated pre-CPI", "type": "vol_signal",
                 "confidence": "high", "tags": ["SPY", "vol", "equities"]}
            ]
        })
        mock_client.messages.create.return_value = mock_msg
        conn = MagicMock()

        with patch("services.knowledge.expert_memory._write_insight") as mock_write, \
             patch("anthropic.Anthropic", return_value=mock_client):
            from services.knowledge.expert_memory import extract_insights
            count = extract_insights(conn, "Sold SPY straddle. IV was elevated.", api_key="key")

        assert count == 1
        call_kwargs = mock_write.call_args[1]
        assert call_kwargs["tags"] == ["SPY", "vol", "equities"]
        assert call_kwargs["source_doc_url"] is None
        assert call_kwargs["source_trade_id"] is None

    def test_api_error_returns_zero(self):
        conn = MagicMock()

        with patch("anthropic.Anthropic") as mock_cls:
            mock_cls.return_value.messages.create.side_effect = Exception("API err")
            from services.knowledge.expert_memory import extract_insights
            count = extract_insights(conn, "some notes", api_key="key")

        assert count == 0
```

- [ ] **Step 2: Run to verify failure**

```
pytest tests/services/knowledge/test_expert_memory.py::TestInjectMemory tests/services/knowledge/test_expert_memory.py::TestExtractInsights -v
```
Expected: FAIL — `cannot import name 'inject_memory'`

- [ ] **Step 3: Implement `inject_memory` and `extract_insights` in `expert_memory.py`**

Add to the bottom of `services/knowledge/expert_memory.py`:

```python
def inject_memory(conn, tags: list, top_k: int = 5) -> list[dict]:
    """Return up to top_k active insights whose tags overlap with the query tags.
    Returns [] if tags is empty or on any error. Never raises."""
    if not tags:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT insight_text, insight_type, confidence
                FROM trading.kb_insights
                WHERE active = TRUE AND tags && %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (list(tags), top_k),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception:
        logger.exception("inject_memory failed")
        return []


def extract_insights(conn, session_text: str, api_key: str) -> int:
    """Extract durable insights from a free-form trading session note.
    Returns count of insights written. Never raises."""
    try:
        client = anthropic.Anthropic(api_key=api_key)
        insights = _call_haiku(client, session_text)
    except Exception:
        logger.exception("extract_insights failed")
        return 0

    written = 0
    for item in insights:
        try:
            _write_insight(
                conn,
                insight=item.get("insight", ""),
                insight_type=item.get("type", "market_regime"),
                confidence=item.get("confidence", "medium"),
                tags=item.get("tags", []),
                source_doc_url=None,
                source_trade_id=None,
            )
            written += 1
        except Exception:
            logger.exception("Failed to write insight from session note")
    return written
```

- [ ] **Step 4: Run tests to verify green**

```
pytest tests/services/knowledge/test_expert_memory.py -v
```
Expected: All tests pass (10 existing + 5 new = 15)

- [ ] **Step 5: Commit**

```bash
git add services/knowledge/expert_memory.py tests/services/knowledge/test_expert_memory.py
git commit -m "feat(knowledge): add inject_memory and extract_insights (Channel 1)"
```

---

### Task 4: `daily_briefing.py`

**Files:**
- Create: `services/knowledge/daily_briefing.py`
- Create: `tests/services/knowledge/test_daily_briefing.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/services/knowledge/test_daily_briefing.py`:

```python
from __future__ import annotations
import json
from unittest.mock import MagicMock, patch

import pytest


class TestGenerateDailyBriefing:
    def _make_client(self, text="• Macro content"):
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content[0].text = text
        mock_client.messages.create.return_value = mock_response
        return mock_client

    def test_returns_all_5_sections(self):
        mock_client = self._make_client()
        conn = MagicMock()

        with patch("services.knowledge.daily_briefing._fetch_insights_for_section", return_value=[]), \
             patch("services.knowledge.daily_briefing._fetch_docs_for_section", return_value=[]), \
             patch("services.knowledge.daily_briefing._upsert_briefing_section"), \
             patch("anthropic.Anthropic", return_value=mock_client):
            from services.knowledge.daily_briefing import generate_daily_briefing
            result = generate_daily_briefing(conn, api_key="key")

        assert set(result.keys()) == {"macro", "rates", "vol", "equity", "fx"}

    def test_writes_5_sections_to_db(self):
        mock_client = self._make_client()
        conn = MagicMock()

        with patch("services.knowledge.daily_briefing._fetch_insights_for_section", return_value=[]), \
             patch("services.knowledge.daily_briefing._fetch_docs_for_section", return_value=[]), \
             patch("services.knowledge.daily_briefing._upsert_briefing_section") as mock_upsert, \
             patch("anthropic.Anthropic", return_value=mock_client):
            from services.knowledge.daily_briefing import generate_daily_briefing
            generate_daily_briefing(conn, api_key="key")

        assert mock_upsert.call_count == 5

    def test_failed_section_returns_no_data_without_aborting_others(self):
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("API error on first call")
            mock_response = MagicMock()
            mock_response.content[0].text = "• Some content"
            return mock_response

        mock_client = MagicMock()
        mock_client.messages.create.side_effect = side_effect
        conn = MagicMock()

        with patch("services.knowledge.daily_briefing._fetch_insights_for_section", return_value=[]), \
             patch("services.knowledge.daily_briefing._fetch_docs_for_section", return_value=[]), \
             patch("services.knowledge.daily_briefing._upsert_briefing_section"), \
             patch("anthropic.Anthropic", return_value=mock_client):
            from services.knowledge.daily_briefing import generate_daily_briefing
            result = generate_daily_briefing(conn, api_key="key")

        # First section (macro) failed — must fall back to "No data available."
        assert result["macro"] == "No data available."
        # Remaining 4 sections must have content
        assert result["rates"] != "No data available."
        assert len(result) == 5
```

- [ ] **Step 2: Run to verify failure**

```
pytest tests/services/knowledge/test_daily_briefing.py -v
```
Expected: FAIL — `No module named 'services.knowledge.daily_briefing'`

- [ ] **Step 3: Implement `services/knowledge/daily_briefing.py`**

```python
from __future__ import annotations
import logging
from datetime import date

import anthropic

logger = logging.getLogger(__name__)

_HAIKU = "claude-haiku-4-5-20251001"

_SECTIONS: dict[str, dict] = {
    "macro":  {"tags": ["macro", "macro_risk"],   "sources": ["fred", "fed_speeches", "news_rss"]},
    "rates":  {"tags": ["rates", "fixed_income"], "sources": ["treasury", "fred", "fed_speeches"]},
    "vol":    {"tags": ["vol", "vol_signal"],      "sources": ["news_rss", "bis"]},
    "equity": {"tags": ["equities", "equity"],     "sources": ["news_rss"]},
    "fx":     {"tags": ["fx"],                     "sources": ["fred", "news_rss"]},
}


def _fetch_insights_for_section(conn, tags: list[str], limit: int = 10) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT insight_text FROM trading.kb_insights
               WHERE active = TRUE AND tags && %s
               ORDER BY created_at DESC LIMIT %s""",
            (tags, limit),
        )
        return [row[0] for row in cur.fetchall()]


def _fetch_docs_for_section(conn, sources: list[str], limit: int = 5) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT title, content FROM trading.kb_docs
               WHERE source = ANY(%s) AND fetched_at > NOW() - INTERVAL '3 days'
               ORDER BY fetched_at DESC LIMIT %s""",
            (sources, limit),
        )
        return [f"{row[0]}: {row[1][:500]}" for row in cur.fetchall()]


def _upsert_briefing_section(conn, section: str, content: str) -> None:
    today = date.today()
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO trading.kb_briefings (briefing_date, market_section, content)
               VALUES (%s, %s, %s)
               ON CONFLICT (briefing_date, market_section)
               DO UPDATE SET content = EXCLUDED.content, generated_at = NOW()""",
            (today, section, content),
        )
    conn.commit()


def generate_daily_briefing(conn, api_key: str) -> dict[str, str]:
    """Generate a 5-section daily briefing from KB insights and docs.
    Writes each section to trading.kb_briefings.
    Returns {"macro": "...", "rates": "...", "vol": "...", "equity": "...", "fx": "..."}.
    Never raises — failed sections return "No data available."
    """
    client = anthropic.Anthropic(api_key=api_key)
    result: dict[str, str] = {}

    for section, cfg in _SECTIONS.items():
        try:
            insights = _fetch_insights_for_section(conn, cfg["tags"])
            docs = _fetch_docs_for_section(conn, cfg["sources"])

            insight_text = "\n".join(f"- {i}" for i in insights) or "No insights available."
            doc_text = "\n".join(docs) or "No recent documents available."

            prompt = (
                f"Write a 3-5 bullet {section} briefing for a professional trader "
                f"based on these insights and documents.\n\n"
                f"Insights:\n{insight_text}\n\nDocuments:\n{doc_text}"
            )
            response = client.messages.create(
                model=_HAIKU,
                max_tokens=512,
                messages=[{"role": "user", "content": prompt}],
            )
            content = response.content[0].text.strip()
        except Exception:
            logger.exception("Failed to generate %s briefing section", section)
            content = "No data available."

        result[section] = content
        try:
            _upsert_briefing_section(conn, section, content)
        except Exception:
            logger.exception("Failed to save %s briefing section", section)

    return result
```

- [ ] **Step 4: Run tests to verify green**

```
pytest tests/services/knowledge/test_daily_briefing.py -v
```
Expected: 3 tests pass

- [ ] **Step 5: Run full suite**

```
pytest tests/ -q
```
Expected: 378 passed (372 + 6 new)

- [ ] **Step 6: Commit**

```bash
git add services/knowledge/daily_briefing.py tests/services/knowledge/test_daily_briefing.py
git commit -m "feat(knowledge): add daily_briefing generator (5-section Haiku briefing)"
```

---

### Task 5: DB helpers + advisor tab tests scaffold

**Files:**
- Modify: `apps/shared/db.py`
- Create: `tests/apps/__init__.py`
- Create: `tests/apps/portfolio/__init__.py`
- Create: `tests/apps/portfolio/test_advisor_tabs.py`

- [ ] **Step 1: Write failing tests for the new DB functions**

Create directory structure and test file:

```bash
mkdir -p tests/apps/portfolio
touch tests/apps/__init__.py tests/apps/portfolio/__init__.py
```

Create `tests/apps/portfolio/test_advisor_tabs.py`:

```python
from __future__ import annotations
from unittest.mock import MagicMock

import pandas as pd


class TestGetKbInsights:
    def test_returns_dataframe_with_tag_filtered_rows(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = [
            ("rates elevated", "macro_risk", "high", ["rates", "macro"], "2026-06-19"),
        ]
        cur.description = [
            ("insight_text",), ("insight_type",), ("confidence",), ("tags",), ("created_at",)
        ]

        from apps.shared.db import get_kb_insights
        df = get_kb_insights(conn, tags=["rates"])

        assert len(df) == 1
        assert df.iloc[0]["insight_text"] == "rates elevated"

    def test_empty_result_returns_empty_dataframe(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = []
        cur.description = [("insight_text",), ("insight_type",), ("confidence",), ("tags",), ("created_at",)]

        from apps.shared.db import get_kb_insights
        df = get_kb_insights(conn, tags=["rates"])

        assert df.empty


class TestGetKbBriefing:
    def test_returns_dataframe_of_briefing_sections(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = [
            ("macro", "• Rates elevated", "2026-06-19 06:30:00"),
        ]
        cur.description = [("market_section",), ("content",), ("generated_at",)]

        from apps.shared.db import get_kb_briefing
        df = get_kb_briefing(conn, "2026-06-19")

        assert len(df) == 1
        assert df.iloc[0]["market_section"] == "macro"
        assert df.iloc[0]["content"] == "• Rates elevated"

    def test_returns_empty_dataframe_when_no_briefing(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = []
        cur.description = [("market_section",), ("content",), ("generated_at",)]

        from apps.shared.db import get_kb_briefing
        df = get_kb_briefing(conn, "2026-06-19")

        assert df.empty
```

- [ ] **Step 2: Run to verify failure**

```
pytest tests/apps/portfolio/test_advisor_tabs.py -v
```
Expected: FAIL — `cannot import name 'get_kb_insights'`

- [ ] **Step 3: Add `get_kb_insights` and `get_kb_briefing` to `apps/shared/db.py`**

Append to the bottom of `apps/shared/db.py`:

```python
def get_kb_insights(conn, tags: list, limit: int = 10) -> pd.DataFrame:
    """Fetch active insights whose tags overlap with the query tags."""
    return _fetch(
        conn,
        """SELECT insight_text, insight_type, confidence, tags, created_at
           FROM trading.kb_insights
           WHERE active = TRUE AND tags && %s
           ORDER BY created_at DESC LIMIT %s""",
        (list(tags), limit),
    )


def get_kb_briefing(conn, date: str) -> pd.DataFrame:
    """Fetch all sections of a briefing for the given ISO date string."""
    return _fetch(
        conn,
        """SELECT market_section, content, generated_at
           FROM trading.kb_briefings
           WHERE briefing_date = %s
           ORDER BY market_section""",
        (date,),
    )
```

- [ ] **Step 4: Run tests to verify green**

```
pytest tests/apps/portfolio/test_advisor_tabs.py -v
```
Expected: 4 tests pass

- [ ] **Step 5: Run full suite**

```
pytest tests/ -q
```
Expected: 382 passed

- [ ] **Step 6: Commit**

```bash
git add apps/shared/db.py tests/apps/__init__.py tests/apps/portfolio/__init__.py tests/apps/portfolio/test_advisor_tabs.py
git commit -m "feat(db): add get_kb_insights and get_kb_briefing helpers"
```

---

### Task 6: `advisor_pretrade.py` — Pre-Trade tab

**Files:**
- Create: `apps/portfolio/tabs/advisor_pretrade.py`

No unit tests for the Streamlit render function itself — the underlying functions (`inject_memory`, `get_news_items`) are already tested. This task is implementation only.

- [ ] **Step 1: Create `apps/portfolio/tabs/advisor_pretrade.py`**

```python
from __future__ import annotations

STRATEGY_TAGS: dict[str, list[str]] = {
    "equity_long":    ["equities"],
    "options_spread": ["options", "vol", "equities"],
    "fixed_income":   ["rates", "fixed_income"],
    "fx":             ["fx"],
    "vol_arb":        ["vol"],
    "other":          [],
}


def render(conn) -> None:
    import os
    import streamlit as st
    import anthropic
    from services.knowledge.expert_memory import inject_memory
    from apps.shared.db import get_news_items

    symbol: str = st.text_input("Symbol", placeholder="e.g. SPY", key="pt_symbol")
    strategy: str = st.selectbox("Strategy", list(STRATEGY_TAGS.keys()), key="pt_strategy")

    # Reset state when symbol or strategy changes
    if (symbol != st.session_state.get("_pt_last_symbol", "")
            or strategy != st.session_state.get("_pt_last_strategy", "")):
        st.session_state["_pt_messages"] = []
        st.session_state["_pt_briefing_done"] = False
        st.session_state["_pt_insights"] = []
        st.session_state["_pt_last_symbol"] = symbol
        st.session_state["_pt_last_strategy"] = strategy

    if st.button("Generate Briefing", disabled=not symbol):
        tags = ([symbol.upper()] + STRATEGY_TAGS[strategy]) if symbol else []
        insights = inject_memory(conn, tags, top_k=5)
        st.session_state["_pt_insights"] = insights
        news_df = get_news_items(conn, min_relevance=0.4, symbols=[symbol], limit=5)
        st.session_state["_pt_news_df"] = news_df
        st.session_state["_pt_briefing_done"] = True

    if st.session_state.get("_pt_briefing_done"):
        insights = st.session_state.get("_pt_insights", [])
        with st.expander(f"KB Insights ({len(insights)} found)", expanded=True):
            if insights:
                for item in insights:
                    st.info(f"[{item['insight_type']}] {item['insight_text']}")
            else:
                st.caption("No KB insights found for this symbol/strategy yet.")

        news_df = st.session_state.get("_pt_news_df")
        if news_df is not None and not news_df.empty:
            with st.expander("Recent News"):
                for _, row in news_df.iterrows():
                    sentiment = row.get("sentiment", "neutral")
                    badge = {"bullish": "🟢", "bearish": "🔴"}.get(sentiment, "⚪")
                    st.markdown(f"• {badge} {row.get('headline', '')}")

        # Chat
        st.divider()
        if "_pt_messages" not in st.session_state:
            st.session_state["_pt_messages"] = []

        for msg in st.session_state["_pt_messages"]:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

        if prompt := st.chat_input("Ask a follow-up question..."):
            st.session_state["_pt_messages"].append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)

            injected = st.session_state.get("_pt_insights", [])
            injected_text = "\n".join(
                f"- [{i['insight_type']}] {i['insight_text']}" for i in injected
            )
            system_prompt = (
                f"You are a pre-trade advisor for {symbol} using {strategy} strategy.\n"
                f"Relevant KB insights:\n{injected_text or 'None available.'}\n"
                "Answer concisely."
            )

            api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            client = anthropic.Anthropic(api_key=api_key)

            def _stream():
                with client.messages.stream(
                    model="claude-sonnet-4-6",
                    max_tokens=512,
                    system=system_prompt,
                    messages=st.session_state["_pt_messages"],
                ) as stream:
                    yield from stream.text_stream

            with st.chat_message("assistant"):
                response = st.write_stream(_stream())
            st.session_state["_pt_messages"].append({"role": "assistant", "content": response})
```

- [ ] **Step 2: Verify the module imports cleanly (no Streamlit required at import time)**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -c "from apps.portfolio.tabs.advisor_pretrade import render, STRATEGY_TAGS; print('OK', list(STRATEGY_TAGS.keys()))"
```
Expected: `OK ['equity_long', 'options_spread', 'fixed_income', 'fx', 'vol_arb', 'other']`

- [ ] **Step 3: Run full suite (no regressions)**

```
pytest tests/ -q
```
Expected: 382 passed

- [ ] **Step 4: Commit**

```bash
git add apps/portfolio/tabs/advisor_pretrade.py
git commit -m "feat(portfolio): add Pre-Trade advisor tab"
```

---

### Task 7: `advisor_daily.py` + portfolio app changes

**Files:**
- Create: `apps/portfolio/tabs/advisor_daily.py`
- Modify: `apps/portfolio/app.py`

- [ ] **Step 1: Create `apps/portfolio/tabs/advisor_daily.py`**

```python
from __future__ import annotations


def render(conn) -> None:
    import os
    from datetime import date
    import streamlit as st
    from services.knowledge.daily_briefing import generate_daily_briefing
    from services.knowledge.expert_memory import extract_insights
    from apps.shared.db import get_kb_briefing

    today = date.today().isoformat()

    st.subheader("Daily Briefing")
    df = get_kb_briefing(conn, today)

    if not df.empty:
        briefing = dict(zip(df["market_section"], df["content"]))
        generated_at = df["generated_at"].max()
        for section in ["macro", "rates", "vol", "equity", "fx"]:
            with st.expander(section.title(), expanded=True):
                st.markdown(briefing.get(section, "No data available."))
        st.caption(f"Generated at {generated_at}")
    else:
        st.info("No briefing for today.")
        if st.button("Generate Today's Briefing"):
            api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            if not api_key:
                st.error("ANTHROPIC_API_KEY is not set. Add it to config/.env.")
                return
            with st.spinner("Generating briefing..."):
                briefing = generate_daily_briefing(conn, api_key)
            for section in ["macro", "rates", "vol", "equity", "fx"]:
                with st.expander(section.title(), expanded=True):
                    st.markdown(briefing.get(section, "No data available."))

    st.divider()
    st.subheader("Extract Insights from Session Notes")
    notes: str = st.text_area(
        "Paste your trading session notes...", height=150, key="session_notes"
    )
    if st.button("Extract Insights") and notes.strip():
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            st.error("ANTHROPIC_API_KEY is not set. Add it to config/.env.")
            return
        n = extract_insights(conn, notes, api_key)
        st.success(f"Extracted {n} insights into KB.")
```

- [ ] **Step 2: Update `apps/portfolio/app.py`**

Replace the entire file:

```python
import os, sys
import streamlit as st
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from apps.shared.db import connect
from apps.portfolio.tabs import (
    positions, pnl, risk, options_book, fixed_income, performance, cashflow,
    advisor_pretrade, advisor_daily,
)

st.set_page_config(page_title="Portfolio", layout="wide", page_icon="📊")

@st.cache_resource
def _get_conn():
    return connect()

conn = _get_conn()
account_id = os.environ.get("ACCOUNT_ID", "paper_default")
st.title("Portfolio Dashboard")
tab_labels = [
    "Positions", "P&L", "Risk", "Options Book", "Fixed Income",
    "Performance", "Cash Flow", "Pre-Trade", "Daily Briefing",
]
tabs = st.tabs(tab_labels)
with tabs[0]: positions.render(conn, account_id)
with tabs[1]: pnl.render(conn)
with tabs[2]: risk.render(conn, account_id)
with tabs[3]: options_book.render(conn, account_id)
with tabs[4]: fixed_income.render(conn, account_id)
with tabs[5]: performance.render(conn)
with tabs[6]: cashflow.render(conn)
with tabs[7]: advisor_pretrade.render(conn)
with tabs[8]: advisor_daily.render(conn)
```

- [ ] **Step 3: Verify both new modules import cleanly**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -c "from apps.portfolio.tabs.advisor_daily import render; from apps.portfolio.tabs.advisor_pretrade import render; print('OK')"
```
Expected: `OK`

- [ ] **Step 4: Run full suite (no regressions)**

```
pytest tests/ -q
```
Expected: 382 passed

- [ ] **Step 5: Commit**

```bash
git add apps/portfolio/tabs/advisor_daily.py apps/portfolio/app.py
git commit -m "feat(portfolio): add Daily Briefing tab and wire up 9-tab portfolio app"
```

---

### Task 8: `trade_monitor.py` APScheduler service

**Files:**
- Create: `services/broker_service/trade_monitor.py`
- Create: `tests/broker_service/test_trade_monitor.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/broker_service/test_trade_monitor.py`:

```python
from __future__ import annotations
from unittest.mock import MagicMock, patch

import pytest
from apscheduler.triggers.cron import CronTrigger


class TestBuildScheduler:
    def test_returns_exactly_one_job(self):
        from services.broker_service.trade_monitor import build_scheduler
        sched = build_scheduler()
        assert len(sched.get_jobs()) == 1

    def test_job_id_is_extract_trade_insights(self):
        from services.broker_service.trade_monitor import build_scheduler
        sched = build_scheduler()
        assert sched.get_jobs()[0].id == "extract_trade_insights"

    def test_job_uses_eastern_timezone(self):
        from services.broker_service.trade_monitor import build_scheduler
        sched = build_scheduler()
        job = sched.get_jobs()[0]
        assert str(job.trigger.timezone) == "America/New_York"

    def test_job_is_cron_trigger_mon_fri_18_00(self):
        from services.broker_service.trade_monitor import build_scheduler
        sched = build_scheduler()
        job = sched.get_jobs()[0]
        assert isinstance(job.trigger, CronTrigger)
        fields = {f.name: str(f) for f in job.trigger.fields}
        assert fields["hour"] == "18"
        assert fields["minute"] == "0"
        assert fields["day_of_week"] == "mon-fri"


class TestJobExtractTradeInsights:
    _GROUPS = [
        {
            "symbol": "SPY", "strategy_id": "vix_model",
            "trade_date": "2026-06-19", "raw_pnl": 250.0, "net_qty": 100,
        }
    ]

    def test_skips_already_processed_trade(self):
        conn = MagicMock()

        with patch("services.broker_service.trade_monitor._get_recent_trade_groups",
                   return_value=self._GROUPS), \
             patch("services.broker_service.trade_monitor._is_already_processed",
                   return_value=True), \
             patch("services.broker_service.trade_monitor.extract_from_trade_outcome") as mock_extract, \
             patch("psycopg2.connect", return_value=conn), \
             patch.dict("os.environ", {"PGURL": "postgresql://mock", "ACCOUNT_ID": "test",
                                       "ANTHROPIC_API_KEY": "key"}):
            from services.broker_service.trade_monitor import job_extract_trade_insights
            job_extract_trade_insights()

        mock_extract.assert_not_called()

    def test_calls_extract_for_new_trade_with_correct_args(self):
        conn = MagicMock()

        with patch("services.broker_service.trade_monitor._get_recent_trade_groups",
                   return_value=self._GROUPS), \
             patch("services.broker_service.trade_monitor._is_already_processed",
                   return_value=False), \
             patch("services.broker_service.trade_monitor.extract_from_trade_outcome") as mock_extract, \
             patch("psycopg2.connect", return_value=conn), \
             patch.dict("os.environ", {"PGURL": "postgresql://mock", "ACCOUNT_ID": "test",
                                       "ANTHROPIC_API_KEY": "key"}):
            from services.broker_service.trade_monitor import job_extract_trade_insights
            job_extract_trade_insights()

        mock_extract.assert_called_once()
        call_kwargs = mock_extract.call_args[1]
        assert call_kwargs["trade_id"] == "SPY:vix_model:2026-06-19"
        assert call_kwargs["expected_direction"] == "long"   # net_qty > 0
        assert call_kwargs["actual_pnl"] == 250.0

    def test_expected_direction_short_when_net_qty_negative(self):
        conn = MagicMock()
        groups = [{**self._GROUPS[0], "net_qty": -50}]

        with patch("services.broker_service.trade_monitor._get_recent_trade_groups",
                   return_value=groups), \
             patch("services.broker_service.trade_monitor._is_already_processed",
                   return_value=False), \
             patch("services.broker_service.trade_monitor.extract_from_trade_outcome") as mock_extract, \
             patch("psycopg2.connect", return_value=conn), \
             patch.dict("os.environ", {"PGURL": "postgresql://mock", "ACCOUNT_ID": "test",
                                       "ANTHROPIC_API_KEY": "key"}):
            from services.broker_service.trade_monitor import job_extract_trade_insights
            job_extract_trade_insights()

        call_kwargs = mock_extract.call_args[1]
        assert call_kwargs["expected_direction"] == "short"
```

- [ ] **Step 2: Run to verify failure**

```
pytest tests/broker_service/test_trade_monitor.py -v
```
Expected: FAIL — `No module named 'services.broker_service.trade_monitor'`

- [ ] **Step 3: Create `services/broker_service/trade_monitor.py`**

```python
from __future__ import annotations
import logging
import os

import psycopg2
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from services.knowledge.expert_memory import extract_from_trade_outcome

logger = logging.getLogger(__name__)


def _get_recent_trade_groups(conn, account_id: str) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                symbol,
                strategy_id,
                ts_fill::date AS trade_date,
                SUM(CASE WHEN side = 'BUY' THEN -fill_price * quantity
                         ELSE fill_price * quantity END) AS raw_pnl,
                SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) AS net_qty
            FROM trading.trades
            WHERE account_id = %s AND ts_fill >= NOW() - INTERVAL '24 hours'
            GROUP BY symbol, strategy_id, ts_fill::date
            """,
            (account_id,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _is_already_processed(conn, trade_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM trading.kb_insights WHERE source_trade_id = %s LIMIT 1",
            (trade_id,),
        )
        return cur.fetchone() is not None


def job_extract_trade_insights() -> None:
    conn = None
    try:
        conn = psycopg2.connect(os.environ["PGURL"])
        account_id = os.environ.get("ACCOUNT_ID", "paper_default")
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")

        groups = _get_recent_trade_groups(conn, account_id)
        for group in groups:
            trade_id = f"{group['symbol']}:{group['strategy_id']}:{group['trade_date']}"
            if _is_already_processed(conn, trade_id):
                continue
            extract_from_trade_outcome(
                conn,
                trade_id=trade_id,
                signal_source=group["strategy_id"] or "unknown",
                expected_direction="long" if (group["net_qty"] or 0) > 0 else "short",
                actual_pnl=float(group["raw_pnl"] or 0),
                pnl_explain={"delta_pct": 100, "gamma_pct": 0, "vega_pct": 0, "theta_pct": 0},
                market_context={"symbol": group["symbol"], "date": str(group["trade_date"])},
                api_key=api_key,
            )
    except Exception:
        logger.exception("job_extract_trade_insights failed")
    finally:
        if conn:
            conn.close()


def build_scheduler() -> BlockingScheduler:
    tz = "America/New_York"
    sched = BlockingScheduler(timezone=tz)
    sched.add_job(
        job_extract_trade_insights,
        CronTrigger(day_of_week="mon-fri", hour=18, minute=0, timezone=tz),
        id="extract_trade_insights",
        name="Extract trade insights",
    )
    return sched


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    build_scheduler().start()
```

- [ ] **Step 4: Run tests to verify green**

```
pytest tests/broker_service/test_trade_monitor.py -v
```
Expected: 7 tests pass

- [ ] **Step 5: Run full suite**

```
pytest tests/ -q
```
Expected: 389 passed (382 + 7)

- [ ] **Step 6: Commit**

```bash
git add services/broker_service/trade_monitor.py tests/broker_service/test_trade_monitor.py
git commit -m "feat(broker_service): add trade_monitor APScheduler service (Channel 5 wiring)"
```

---

## Final check

After all tasks, run the full test suite one last time:

```
cd C:/Users/dipeng.chen/OneDrive/ETRM/ib-platform
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/ -q
```

Expected: **389 passed** (372 baseline + 17 new tests)

### New files summary

| File | Role |
|---|---|
| `db/migrations/003_kb_insights_tags.sql` | Add `tags TEXT[]` column to `kb_insights` |
| `services/knowledge/daily_briefing.py` | 5-section Haiku briefing generator |
| `services/broker_service/trade_monitor.py` | Daily 18:00 ET job to learn from closed trades |
| `apps/portfolio/tabs/advisor_pretrade.py` | Pre-Trade tab (inject_memory + Sonnet chat) |
| `apps/portfolio/tabs/advisor_daily.py` | Daily Briefing tab + session note extractor |

### Modified files summary

| File | Change |
|---|---|
| `services/knowledge/expert_memory.py` | `_write_insight` + tags, Haiku prompt, `inject_memory`, `extract_insights` |
| `apps/portfolio/app.py` | 7 → 9 tabs |
| `apps/shared/db.py` | `get_kb_insights`, `get_kb_briefing` |
| `tests/services/knowledge/test_expert_memory.py` | Tags + Channel 1 tests |
