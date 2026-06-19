# IB Platform — Phase 6 Design
## Advisor App (Portfolio Tabs)

**Date:** 2026-06-19
**Repo:** `C:\Users\dipeng.chen\OneDrive\ETRM\ib-platform`
**Status:** Approved, ready for implementation

---

## Overview

Phase 6 puts the Phase 5 Knowledge Base to work. It adds two new tabs to the existing portfolio app — "Pre-Trade" and "Daily Briefing" — backed by Channel 1 (`extract_insights`, `inject_memory`) in `expert_memory.py`, a `daily_briefing.py` generator, and a `trade_monitor.py` APScheduler service that automatically extracts lessons from closed trades.

Phase 6 is app-layer only. No new standalone Streamlit apps. No new packages required.

---

## Section 1: File Structure

### New files

```
db/migrations/003_kb_insights_tags.sql

services/knowledge/daily_briefing.py
services/broker_service/trade_monitor.py

apps/portfolio/tabs/advisor_pretrade.py
apps/portfolio/tabs/advisor_daily.py

tests/services/knowledge/test_daily_briefing.py
tests/services/broker_service/test_trade_monitor.py
tests/apps/portfolio/test_advisor_tabs.py
```

### Modified files

- `services/knowledge/expert_memory.py` — add `extract_insights`, `inject_memory`; extend Haiku JSON schema to emit `tags`; add `tags` param to `_write_insight`
- `apps/portfolio/app.py` — add two new tabs ("Pre-Trade", "Daily Briefing")
- `apps/shared/db.py` — add `get_kb_insights`, `get_kb_briefing`
- `tests/services/knowledge/test_expert_memory.py` — extend with Channel 1 + tags tests

---

## Section 2: Schema Migration

**`db/migrations/003_kb_insights_tags.sql`**

```sql
ALTER TABLE trading.kb_insights
    ADD COLUMN IF NOT EXISTS tags TEXT[] NOT NULL DEFAULT '{}';

CREATE INDEX IF NOT EXISTS idx_kb_insights_tags
    ON trading.kb_insights USING GIN(tags);
```

Run once: `psql $PGURL -f db/migrations/003_kb_insights_tags.sql`

Existing rows get `tags = '{}'` (empty array default). They will not surface via tag-based retrieval until re-digested — acceptable, as the KB is new.

---

## Section 3: `expert_memory.py` Changes

### Haiku JSON schema extension

Add `"tags"` to the insight JSON schema in `_SYSTEM_PROMPT`:

```
Respond ONLY with valid JSON, no markdown:
{"insights": [{"insight": "...", "type": "...", "confidence": "high|medium|low", "tags": ["SPY", "rates", "vol"]}]}
```

Tags should be: ticker symbols mentioned, asset classes (`equities`, `rates`, `fx`, `vol`, `macro`, `credit`), and strategy types (`spread`, `options`, `fixed_income`). Haiku infers these from context.

### `_write_insight` — add `tags` parameter

Replace the existing 5-column INSERT with a 6-column INSERT that includes `tags`. The full updated signature:

```python
def _write_insight(
    conn,
    *,
    insight: str,
    insight_type: str,
    confidence: str,
    tags: list[str] = (),              # NEW — defaults to empty list
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

`source_session` column (exists in schema) remains NULL for all Phase 6 callers — not populated until Phase 7.

All existing callers (`digest_kb_docs`, `extract_from_trade_outcome`) add `tags=item.get("tags", [])` to pass through the tags Haiku now returns.

### `extract_insights(conn, session_text: str, api_key: str) -> int`

**Channel 1 — manual session note extraction.**

```python
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

Note: `source_session` column exists in the schema but is not populated here — the column is available for Phase 7 if session tracking is needed.

### `inject_memory(conn, tags: list[str], top_k: int = 5) -> list[dict]`

**Channel 1 — retrieve relevant insights by tag overlap.**

```python
def inject_memory(conn, tags: list[str], top_k: int = 5) -> list[dict]:
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
                (tags, top_k),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception:
        logger.exception("inject_memory failed")
        return []
```

---

## Section 4: `services/knowledge/daily_briefing.py`

A callable module — not an APScheduler service. The Daily Briefing tab calls it on demand.

### Section → tags/sources mapping

```python
_SECTIONS: dict[str, dict] = {
    "macro":  {"tags": ["macro", "macro_risk"],      "sources": ["fred", "fed_speeches", "news_rss"]},
    "rates":  {"tags": ["rates", "fixed_income"],    "sources": ["treasury", "fred", "fed_speeches"]},
    "vol":    {"tags": ["vol", "vol_signal"],         "sources": ["news_rss", "bis"]},
    "equity": {"tags": ["equities", "equity"],        "sources": ["news_rss"]},
    "fx":     {"tags": ["fx"],                        "sources": ["fred", "news_rss"]},
}
```

### `generate_daily_briefing(conn, api_key: str) -> dict[str, str]`

```python
def generate_daily_briefing(conn, api_key: str) -> dict[str, str]:
    """Generate a 5-section daily briefing from KB insights and docs.
    Writes each section to trading.kb_briefings.
    Returns {"macro": "...", "rates": "...", "vol": "...", "equity": "...", "fx": "..."}.
    Never raises — failed sections return "No data available."
    """
```

For each section:
1. Fetch up to 10 recent `kb_insights` WHERE `active=TRUE AND tags && ARRAY[<section_tags>]` ORDER BY `created_at DESC`
2. Fetch up to 5 recent `kb_docs` WHERE `source = ANY(ARRAY[<section_sources>]) AND fetched_at > NOW() - INTERVAL '3 days'` ORDER BY `fetched_at DESC`
3. Build prompt: `"Write a 3-5 bullet {section} briefing for a professional trader based on these insights and documents."` followed by insights + doc content snippets (first 500 chars each)
4. Call **Haiku** (`claude-haiku-4-5-20251001`) directly via `anthropic.Anthropic(api_key=api_key).messages.create(...)` — NOT via `_call_haiku` from `expert_memory.py` (which expects insight JSON format). `max_tokens=512`. On any error: log, use `"No data available."`
5. Upsert to `trading.kb_briefings` ON CONFLICT (`briefing_date`, `market_section`) DO UPDATE

Returns the dict of `{section: content}`.

---

## Section 5: `apps/portfolio/tabs/advisor_pretrade.py`

```python
STRATEGY_TAGS: dict[str, list[str]] = {
    "equity_long":    ["equities"],
    "options_spread": ["options", "vol", "equities"],
    "fixed_income":   ["rates", "fixed_income"],
    "fx":             ["fx"],
    "vol_arb":        ["vol"],
    "other":          [],
}
```

### `render(conn)`

```
render(conn)
│
├── st.text_input("Symbol", placeholder="e.g. SPY", key="pt_symbol")
├── st.selectbox("Strategy", list(STRATEGY_TAGS.keys()), key="pt_strategy")
│
├── [Generate Briefing] button
│   ├── tags = [symbol.upper()] + STRATEGY_TAGS[strategy]
│   ├── insights = inject_memory(conn, tags, top_k=5)
│   ├── news_df = get_news_items(conn, min_relevance=0.4, symbols=[symbol], limit=5)
│   └── Display:
│       ├── st.expander("KB Insights (N found)")
│       │   └── per insight: st.info(f"[{insight_type}] {insight_text}")
│       └── st.expander("Recent News")
│           └── per row: "• {headline}" with sentiment badge
│
└── Chat (rendered only after briefing generated, tracked via st.session_state["pt_briefing_done"])
    ├── Display message history from st.session_state["pt_messages"]
    ├── st.chat_input("Ask a follow-up question...")
    └── On submit:
        ├── System prompt: "You are a pre-trade advisor for {symbol} using {strategy}.
        │   Relevant KB insights:\n{injected_text}\nAnswer concisely."
        ├── Append user message to history
        ├── Call claude-sonnet-4-6 with full history
        └── Stream response with st.write_stream; append to history
```

State in `st.session_state`: `pt_messages` (list), `pt_briefing_done` (bool), `pt_insights` (list). All reset when symbol or strategy changes (compare against `pt_last_symbol` / `pt_last_strategy`).

---

## Section 6: `apps/portfolio/tabs/advisor_daily.py`

### `render(conn)`

```
render(conn)
│
├── [Top: Daily Briefing]
│   ├── today = date.today().isoformat()
│   ├── df = get_kb_briefing(conn, today)  →  5 rows (one per section) or empty
│   ├── If not empty:
│   │   ├── For each section in ["macro","rates","vol","equity","fx"]:
│   │   │   └── st.expander(section.title()) → st.markdown(content)
│   │   └── st.caption(f"Generated at {generated_at}")
│   └── If empty:
│       ├── st.info("No briefing for today.")
│       └── [Generate Today's Briefing] button
│           └── generate_daily_briefing(conn, api_key) → display sections
│
└── [Bottom: Session Notes]
    ├── st.subheader("Extract Insights from Session Notes")
    ├── st.text_area("Paste trading session notes...", height=150, key="session_notes")
    ├── [Extract Insights] button
    │   └── n = extract_insights(conn, session_text, api_key)
    └── st.success(f"Extracted {n} insights into KB.")
```

### `apps/portfolio/app.py` change

```python
from apps.portfolio.tabs import (
    positions, pnl, risk, options_book,
    fixed_income, performance, cashflow,
    advisor_pretrade, advisor_daily,          # new
)

tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs([
    "Positions", "P&L", "Risk", "Options Book",
    "Fixed Income", "Performance", "Cash Flow",
    "Pre-Trade", "Daily Briefing",             # new
])
with tab8:
    advisor_pretrade.render(conn)
with tab9:
    advisor_daily.render(conn)
```

---

## Section 7: `services/broker_service/trade_monitor.py`

APScheduler service: `python -m services.broker_service.trade_monitor`

**Job: `job_extract_trade_insights()`** — Mon–Fri 18:00 ET

1. Query `trading.trades` for fills from last 24h:
   ```sql
   SELECT symbol, strategy_id, ts_fill::date AS trade_date,
          SUM(CASE WHEN side='BUY' THEN -fill_price*quantity ELSE fill_price*quantity END) AS raw_pnl,
          SUM(CASE WHEN side='BUY' THEN quantity ELSE -quantity END) AS net_qty
   FROM trading.trades
   WHERE account_id = %s AND ts_fill >= NOW() - INTERVAL '24 hours'
   GROUP BY symbol, strategy_id, ts_fill::date
   ```
2. For each group, build `trade_id = f"{symbol}:{strategy_id}:{trade_date}"`
3. Skip if `kb_insights` row with `source_trade_id = trade_id` already exists
4. Call `extract_from_trade_outcome`:
   - `signal_source = strategy_id`
   - `expected_direction = "long"` if `net_qty > 0` else `"short"`
   - `actual_pnl = raw_pnl`
   - `pnl_explain = {"delta_pct": 100, "gamma_pct": 0, "vega_pct": 0, "theta_pct": 0}` — simplified; Phase 7 enriches with actual greeks
   - `market_context = {"symbol": symbol, "date": str(trade_date)}`
5. `conn = None` / try / finally pattern. `account_id` from `ACCOUNT_ID` env var.

### `build_scheduler() -> BlockingScheduler`

```python
def build_scheduler() -> BlockingScheduler:
    tz = "America/New_York"
    sched = BlockingScheduler(timezone=tz)
    sched.add_job(
        job_extract_trade_insights,
        CronTrigger(day_of_week="mon-fri", hour=18, minute=0, timezone=tz),
        id="extract_trade_insights",
    )
    return sched
```

---

## Section 8: DB Layer + Testing

### `apps/shared/db.py` additions

```python
def get_kb_insights(conn, tags: list[str], limit: int = 10) -> pd.DataFrame:
    """Fetch active insights whose tags overlap with query tags."""
    return _fetch(conn,
        """SELECT insight_text, insight_type, confidence, tags, created_at
           FROM trading.kb_insights
           WHERE active = TRUE AND tags && %s
           ORDER BY created_at DESC LIMIT %s""",
        (tags, limit))


def get_kb_briefing(conn, date: str) -> pd.DataFrame:
    """Fetch all sections of a briefing for the given date (ISO string)."""
    return _fetch(conn,
        """SELECT market_section, content, generated_at
           FROM trading.kb_briefings
           WHERE briefing_date = %s
           ORDER BY market_section""",
        (date,))
```

### Testing scope

| File | What it tests |
|---|---|
| `tests/services/knowledge/test_daily_briefing.py` | `generate_daily_briefing`: mock cursor + Anthropic; verify all 5 sections written to `kb_briefings`; verify error in one Haiku call falls back to `"No data available."` without aborting others |
| `tests/services/broker_service/test_trade_monitor.py` | `build_scheduler()` returns 1 job, id=`"extract_trade_insights"`, Mon–Fri 18:00 ET; `job_extract_trade_insights` mock: dedup skip when `source_trade_id` exists; `extract_from_trade_outcome` called when trade is new |
| `tests/apps/portfolio/test_advisor_tabs.py` | `inject_memory`: mock cursor → 2 insights returned; empty tags → `[]`; `extract_insights`: mock Haiku → `source_session` NOT written (column left NULL), tags written correctly |
| `tests/services/knowledge/test_expert_memory.py` (extended) | `_write_insight` with `tags` param writes correct array; `digest_kb_docs` passes `tags` from Haiku response to `_write_insight`; `extract_from_trade_outcome` passes `tags` |

---

## Key Technical Notes

- **No new packages** — `anthropic`, `psycopg2`, `streamlit`, `apscheduler` already present
- **Model**: Haiku (`claude-haiku-4-5-20251001`) for briefing generation and insight extraction; Sonnet (`claude-sonnet-4-6`) for pre-trade chat only
- **`ACCOUNT_ID` env var** — `trade_monitor.py` reads this to query the correct account's trades
- **Tag overlap operator** — `tags && %s` uses PostgreSQL array overlap; psycopg2 passes Python lists as PG arrays
- **Chat streaming** — `client.messages.stream()` context manager with `st.write_stream`
- **State reset on symbol change** — `st.session_state` keys `pt_last_symbol` / `pt_last_strategy` compared each render; if changed, clear `pt_messages` and `pt_briefing_done`
- **`source_session` column** — exists in schema, left NULL by `extract_insights` in Phase 6; available for Phase 7 session tracking
- **Running tests:** `cd C:/Users/dipeng.chen/OneDrive/ETRM/ib-platform && /c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/ -q`

---

## What Phase 6 Does NOT Include

- Streaming for briefing generation (Haiku called per section, result shown after all sections complete)
- Push notifications or webhooks for trade monitor (APScheduler polling only)
- `extract_from_model_run` / `extract_from_backtest` (Channels 3+4) — Phase 7
- Vector/semantic search for `inject_memory` — tag-based is sufficient for Phase 6
- `source_session` population — Phase 7
