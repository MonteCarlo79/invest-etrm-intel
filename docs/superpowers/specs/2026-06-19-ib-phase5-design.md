# IB Platform — Phase 5 Design
## Knowledge Base Pipeline

**Date:** 2026-06-19
**Repo:** `C:\Users\dipeng.chen\OneDrive\ETRM\ib-platform`
**Status:** Approved, ready for implementation

---

## Overview

Phase 5 adds a long-running APScheduler knowledge ingestion service: 5 document connectors fetch macro/rates/research content daily, store it in `trading.kb_docs`, and a digest job extracts durable trading insights into `trading.kb_insights` via Claude Haiku. A standalone `extract_from_trade_outcome()` function (Channel 5) is also built here, ready for Phase 6 to wire up.

Phase 5 is backend only — no app. The advisor app (`apps/advisor/`) is Phase 6.

---

## Section 1: File Structure

### New files

```
services/knowledge/
├── __init__.py
├── config.py
├── base.py
├── expert_memory.py
├── ingest.py
└── connectors/
    ├── __init__.py
    ├── fred.py
    ├── fed_speeches.py
    ├── treasury.py
    ├── bis.py
    └── news_rss.py

db/migrations/
└── 002_kb_tables.sql

tests/services/knowledge/
├── __init__.py
├── test_connectors.py
├── test_expert_memory.py
└── test_ingest_jobs.py
```

### Modified files

- `requirements.txt` — add `FRED_API_KEY=` note to config/.env (no new packages; `requests` and `feedparser` already present)
- `config/.env` — add `FRED_API_KEY=` (optional; connector returns `[]` silently if missing)

---

## Section 2: `config.py`

```python
FRED_SERIES = {
    "DGS2":    "US 2Y Treasury yield",
    "DGS5":    "US 5Y Treasury yield",
    "DGS10":   "US 10Y Treasury yield",
    "DGS30":   "US 30Y Treasury yield",
    "FEDFUNDS": "Fed Funds effective rate",
    "SOFR":    "SOFR overnight rate",
    "CPIAUCSL": "CPI all items",
    "UNRATE":  "Unemployment rate",
    "GDP":     "Real GDP (quarterly)",
    "T10YIE":  "10Y breakeven inflation",
    "T10Y2Y":  "10Y-2Y Treasury spread",
}

RSS_FEEDS = [
    {"name": "Reuters", "url": "https://feeds.reuters.com/reuters/businessNews"},
    {"name": "CNBC",    "url": "https://feeds.nbcnews.com/nbcnews/public/business"},
    {"name": "FT",      "url": "https://www.ft.com/rss/home"},
]
# Defined here (not imported from services/news/sources.py) to avoid cross-service coupling.
# If the feed list changes, update both places.

INSIGHT_TYPES = [
    "market_regime", "price_driver", "vol_signal", "macro_risk",
    "opportunity", "strategy", "model_insight",
    "strategy_backtest", "trade_outcome",
]

TRADE_OUTCOME_MIN_PNL = 50.0   # |pnl| threshold below which extraction is skipped
DIGEST_STALE_DAYS = 30         # docs older than this with short content are skipped in digest
```

---

## Section 3: `base.py`

### `BaseConnector` ABC

```python
class BaseConnector(ABC):
    source: str  # e.g. "fred", "fed_speeches", "treasury", "bis", "news_rss"

    @abstractmethod
    def fetch(self, lookback_days: int = 7) -> list[dict]:
        """Returns list of doc dicts. Never raises — returns [] on any error."""
        ...
```

Each doc dict:
```python
{
    "source":         str,   # connector name
    "doc_type":       str,   # "rate_series"|"speech"|"minutes"|"yield_curve"|"research_paper"|"news_article"
    "title":          str,
    "url":            str,   # used as conflict key in upsert
    "published_date": date | None,
    "content":        str,   # full text or JSON-serialised data
}
```

### `upsert_doc(conn, doc: dict) -> bool`

```python
def upsert_doc(conn, doc: dict) -> bool:
    """INSERT INTO trading.kb_docs ... ON CONFLICT (url) DO UPDATE
    only when content differs (avoids re-triggering digest on unchanged docs).
    Returns True if row was inserted or content updated, False if unchanged."""
```

`search_vector` is a `GENERATED ALWAYS AS` column in the schema — no manual update needed.

---

## Section 4: Connectors

### `fred.py`

- Reads `FRED_API_KEY` from env. Returns `[]` silently if missing or empty.
- For each series in `FRED_SERIES`: `GET https://api.stlouisfed.org/fred/series/observations?series_id={id}&api_key={key}&sort_order=desc&limit=lookback_days&file_type=json`
- Returns one doc per series: `doc_type="rate_series"`, `url=f"https://fred.stlouisfed.org/series/{id}"`, `content=json.dumps(observations)`

### `fed_speeches.py`

- Two feeds (no API key):
  - `https://www.federalreserve.gov/feeds/speeches.xml`
  - `https://www.federalreserve.gov/feeds/press_monetary.xml`
- Parsed via `feedparser`. Entries newer than `lookback_days` only.
- `doc_type="speech"` for speeches feed, `"minutes"` for press_monetary feed.
- `content = entry.summary` (full text where available from feed).

### `treasury.py`

- No API key. Fetches official Treasury yield curve CSV:
  `https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/{year}/all?type=daily_treasury_yield_curve&field_tdr_date_value={year}`
- Parses CSV; filters rows where date >= `today - lookback_days`.
- One doc per row: `doc_type="yield_curve"`, `url=f"treasury://yield-curve/{row_date}"`, `content=json.dumps(row_dict)`.
- The `treasury://` URL scheme is a synthetic unique key for dedup — Treasury CSV rows have no canonical URL. The `url` column is UNIQUE in `kb_docs`; this format guarantees one row per date.

### `bis.py`

- Two RSS feeds (no API key):
  - `https://www.bis.org/rss/quarterly-review.xml`
  - `https://www.bis.org/rss/working-papers.xml`
- Parsed via `feedparser`. Entries newer than `lookback_days`.
- `doc_type="research_paper"`.
- Returns `[]` on network error (BIS feeds can be slow).

### `news_rss.py`

- Uses the 3 RSS feeds defined in `config.RSS_FEEDS` (Reuters, CNBC, FT). Does NOT import from `services/news/sources.py` — cross-service imports couple two independent services.
- Distinct from news ingest: stores full entry `summary` as a KB doc for advisor reasoning, not for the scored news feed.
- `doc_type="news_article"`.
- Dedup via `upsert_doc` URL conflict key — no sha256 needed here.

---

## Section 5: `expert_memory.py`

Two public functions for Phase 5. Channel 1 (`extract_insights`, `inject_memory`) deferred to Phase 6.

### `digest_kb_docs(conn, api_key: str, batch_size: int = 20) -> int`

**Channel 2 — document digestion.**

1. Fetch up to `batch_size` docs from `trading.kb_docs` where `url NOT IN (SELECT DISTINCT source_doc_url FROM trading.kb_insights WHERE source_doc_url IS NOT NULL)` — undigested docs only.
2. For each doc: call Haiku with extraction prompt (see below). On any error: log, skip, continue.
3. Write each extracted insight to `trading.kb_insights` with `source_doc_url = doc.url`.
4. Returns total count of insights inserted.

**Docs older than `DIGEST_STALE_DAYS` (30) days with short content** (`len(content) < 100`) are skipped silently.

### `extract_from_trade_outcome(conn, trade_id: int | str, signal_source: str, expected_direction: str, actual_pnl: float, pnl_explain: dict, market_context: dict, api_key: str) -> bool`

**Channel 5 — trade outcome learning.**

- Returns `False` immediately if `abs(actual_pnl) < TRADE_OUTCOME_MIN_PNL` (not worth extracting).
- Calls Haiku with prompt:
  ```
  Trade closed. Signal source: {signal_source}. Expected direction: {expected_direction}.
  Actual P&L: ${actual_pnl:.2f}.
  P&L attribution: delta={delta_pct:.0f}%, gamma={gamma_pct:.0f}%, vega={vega_pct:.0f}%, theta={theta_pct:.0f}%.
  Market context at entry: {market_context}.
  Extract 1-2 durable lessons about what the model got right or wrong.
  ```
- On parse error or API failure: logs, returns `False`. Never raises.
- On success: inserts into `trading.kb_insights` with `insight_type="trade_outcome"`, `source_trade_id=str(trade_id)`. Returns `True`.

### System prompt (both functions)

```
You are extracting durable insights for a professional trading knowledge base.
Extract ONLY insights that are: non-obvious, validated by context, durable (relevant for
weeks or months), and domain-specific to markets, instruments, risk, macro, vol, rates, FX,
or specific strategies.

Insight types: market_regime | price_driver | vol_signal | macro_risk |
               opportunity | strategy | model_insight | strategy_backtest | trade_outcome

Respond ONLY with valid JSON, no markdown:
{"insights": [{"insight": "...", "type": "...", "confidence": "high|medium|low"}]}
```

Model: `claude-haiku-4-5-20251001` (cost efficiency).

---

## Section 6: `ingest.py` — APScheduler Service

Started with: `python -m services.knowledge.ingest`

**Two jobs:**

| Job | Schedule | Action |
|---|---|---|
| `job_ingest_docs` | Mon–Fri 06:00 ET | Run each connector; call `upsert_doc` per result. Per-connector exceptions caught individually — one failure does not abort others. |
| `job_digest_docs` | Mon–Fri 06:30 ET | `digest_kb_docs(conn, api_key, batch_size=20)` |

Both jobs follow the `conn = None` / try / `if conn: conn.close()` pattern from Phase 4.

Timezone: `America/New_York`. `BlockingScheduler` with `CronTrigger`.

---

## Section 7: DB Migration

**`db/migrations/002_kb_tables.sql`** — extracts the `trading.kb_docs`, `trading.kb_insights`, and `trading.kb_briefings` DDL already present in `db/schema.sql` into a standalone runnable file. No schema changes.

Run once: `psql $PGURL -f db/migrations/002_kb_tables.sql`

---

## Section 8: Testing Scope

| Test file | What it tests |
|---|---|
| `tests/services/knowledge/test_connectors.py` | Each connector: mock `requests.get` and `feedparser.parse`; verify output dict shape (`source`, `doc_type`, `title`, `url`, `content` present); verify `[]` returned when `FRED_API_KEY` missing; verify `[]` returned on network error (requests raises `ConnectionError`) |
| `tests/services/knowledge/test_expert_memory.py` | `digest_kb_docs`: mock Anthropic client + cursor; verify undigested doc triggers Haiku call and insight is written; verify doc with existing `source_doc_url` in kb_insights is skipped (no API call). `extract_from_trade_outcome`: mock Anthropic + cursor; verify insight written on valid P&L; verify `False` returned without API call when `|pnl| < 50` |
| `tests/services/knowledge/test_ingest_jobs.py` | `build_scheduler()` returns scheduler with exactly 2 jobs; job IDs are `"ingest_docs"` and `"digest_docs"`; both use `America/New_York` timezone |

No tests for `BaseConnector` ABC. `upsert_doc` tested implicitly via mock cursor in connector tests.

---

## Key Technical Notes

- **No new packages required** — `requests` and `feedparser` already in `requirements.txt`
- **`FRED_API_KEY`** — free, register at fred.stlouisfed.org. Add to `config/.env`. If absent, FRED connector silently returns `[]`.
- **`kb_docs.search_vector`** — `GENERATED ALWAYS AS` column; no manual update needed in `upsert_doc`
- **Digestion dedup** — tracked via `kb_insights.source_doc_url` (no schema change)
- **Channel 1 deferred** — `extract_insights` and `inject_memory` are Phase 6 (advisor app)
- **`extract_from_trade_outcome`** — built here, wired up by `trade_monitor.py` in Phase 6
- **Running tests:** `cd C:/Users/dipeng.chen/OneDrive/ETRM/ib-platform && /c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/ -q`

---

## What Phase 5 Does NOT Include

- `apps/advisor/` — Phase 6
- `trade_monitor.py` and `daily_briefing.py` — Phase 6
- `extract_insights` / `inject_memory` (Channel 1) — Phase 6
- `extract_from_model_run` / `extract_from_backtest` (Channels 3+4) — Phase 7 (requires `libs/ml`, `libs/backtest`)
- CBOE and SEC EDGAR connectors — deferred (complex scraping)
