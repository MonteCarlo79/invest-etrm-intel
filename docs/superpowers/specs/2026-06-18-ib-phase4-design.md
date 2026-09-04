# IB Platform — Phase 4 Design
## Market Data Pipeline + News Service

**Date:** 2026-06-18  
**Repo:** `C:\Users\dipeng.chen\OneDrive\ETRM\ib-platform`  
**Status:** Approved, ready for implementation

---

## Overview

Phase 4 adds two independent long-running APScheduler services and a 4-tab Streamlit news app:

1. **`services/market_data/ingest.py`** — ingests bars, VIX term structure, and FX rates from yfinance (primary) with Polygon as an optional pluggable source
2. **`services/news/ingest.py`** — ingests news from RSS feeds + optional Polygon news, scores each item with Claude for relevance and sentiment
3. **`apps/news/`** — 4-tab Streamlit app: Top Stories, By Symbol, Full Feed, Digest

Both services run as separate processes, independently startable/stoppable.

---

## Section 1: Market Data Service

### New files in `services/market_data/`

**`yfinance_feed.py`** — pure functions, no scheduler, no side effects:
- `fetch_bars_1d(symbols: list[str], period: str = "1y") -> list[dict]` — EOD OHLCV via `yf.download`; each dict: `{symbol, ts_date, open, high, low, close, volume, source="yfinance"}`
- `fetch_bars_1h(symbols: list[str], period: str = "5d") -> list[dict]` — 1h bars; each dict: `{symbol, ts, open, high, low, close, volume, source="yfinance"}`
- `fetch_vix_term_structure() -> dict` — fetches `^VIX` (spot) and `^VVIX` via yfinance. Note: yfinance does not expose individual VIX futures months (M1–M8); those are populated by `data_writer.py` on the personal laptop via IB. This function writes only `vix_index`, `vvix`, `contango_pct=NULL`, `roll_yield_annualised=NULL`, `regime=NULL`, `source="yfinance"` — the M1-M8 and regime fields are left for the IB path to fill. The VIX tab in `apps/markets` reads whatever is in the DB regardless of source.
- `fetch_fx_rates(pairs: list[str]) -> list[dict]` — each pair as yfinance ticker (e.g. `"EURUSD=X"`); returns `{pair, ts, spot, source="yfinance"}`

**`polygon_feed.py`** — identical signatures to yfinance equivalents; reads `POLYGON_API_KEY` from env; raises `SkipSource` (a local exception) if key is not set or empty. `ingest.py` catches `SkipSource` and falls back to yfinance silently. Not tested with live API calls — mocked in tests.

**`ingest.py`** — APScheduler long-running service. Started with `python -m services.market_data.ingest`. Runs until killed.

| Job | Schedule (APScheduler cron) | Action |
|---|---|---|
| EOD bars | Mon–Fri 16:10 ET | `fetch_bars_1d` for `WATCHLIST` + distinct symbols from `trading.positions` → upsert `trading.bars_1d` |
| Intraday bars | Mon–Fri every hour 09:30–16:00 ET | `fetch_bars_1h` for held position symbols → upsert `trading.bars_1h` |
| VIX term structure | Mon–Fri 09:35 ET | `fetch_vix_term_structure()` → upsert `trading.vix_term_structure` |
| FX rates | Every 4h | `fetch_fx_rates(["EURUSD=X","GBPUSD=X","USDJPY=X","USDCNY=X"])` → upsert `trading.fx_rates` |

All DB writes use `INSERT ... ON CONFLICT DO UPDATE` (upsert) to be idempotent.

Timezone: all schedules run in US/Eastern. APScheduler configured with `timezone="America/New_York"`.

---

## Section 2: News Ingestion

### New package `services/news/`

**`sources.py`** — registry only, no network calls:

```python
RSS_FEEDS = [
    {"name": "Reuters", "url": "https://feeds.reuters.com/reuters/businessNews"},
    {"name": "CNBC",    "url": "https://feeds.nbcnews.com/nbcnews/public/business"},
    {"name": "FT",      "url": "https://www.ft.com/rss/home"},
]

def fetch_polygon_news(symbols: list[str]) -> list[dict]:
    """Reads POLYGON_API_KEY from env. Returns [] if key not set or empty.
    Each dict: {headline, url, body_text, published_ts, source='polygon'}."""
```

**`ingest.py`** — APScheduler long-running service. Started with `python -m services.news.ingest`. One job: every 15 min, active 06:00–22:00 ET Mon–Fri (covers pre/post-market):

1. Pull each RSS feed via `feedparser`; pull Polygon news if `POLYGON_API_KEY` is set
2. For each item: compute `url_hash = sha256(url).hexdigest()`, skip if already in `trading.news_items`
3. Batch-insert new items (raw fields only: `headline`, `url`, `body_text`, `published_ts`, `source`, `url_hash`); `relevance_score` left as NULL
4. Call `scorer.score_pending(conn)` after each batch

### DB table: `trading.news_items`

```sql
CREATE TABLE IF NOT EXISTS trading.news_items (
    item_id           BIGSERIAL PRIMARY KEY,
    url_hash          TEXT UNIQUE NOT NULL,
    source            TEXT,
    headline          TEXT,
    body_text         TEXT,
    url               TEXT,
    published_ts      TIMESTAMPTZ,
    symbols_mentioned TEXT[],
    relevance_score   NUMERIC(5,4),
    sentiment         TEXT,           -- bullish | bearish | neutral
    ai_summary        TEXT,
    ts_ingested       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_news_published  ON trading.news_items (published_ts DESC);
CREATE INDEX IF NOT EXISTS idx_news_relevance  ON trading.news_items (relevance_score DESC);
```

Migration file: `db/migrations/001_news_items.sql`. Also added to `db/schema.sql`.

---

## Section 3: News Scorer

**`services/news/scorer.py`**

One public function: `score_pending(conn) -> int` — scores all rows where `relevance_score IS NULL`, returns count scored.

**Context assembly (once per batch):**
1. Load held symbols from `trading.positions` (distinct `symbol` values)
2. Parse `WATCHLIST` env var (comma-separated, e.g. `"SPY,QQQ,NVDA"`)
3. Merge into deduplicated `universe: list[str]`

**Per-item Claude call** — model: `claude-haiku-4-5-20251001` (cost efficiency):

```
System: You are a financial news analyst. Respond only with valid JSON, no markdown.

User:
Universe (positions + watchlist): {universe}

Headline: {headline}
Body: {body_text[:500]}

Respond with:
{
  "relevance_score": <float 0.0–1.0>,
  "sentiment": <"bullish"|"bearish"|"neutral">,
  "symbols_mentioned": [<tickers from universe appearing in article>],
  "ai_summary": <string, max 120 chars, one sentence>
}
```

**Batching:** Max 20 items per `score_pending` call. Items with `ts_ingested < NOW() - INTERVAL '48 hours'` that are still unscored receive `relevance_score = 0.0`, `sentiment = "neutral"`, `ai_summary = NULL` without a Claude call (stale news).

**Error handling:** If Claude call fails or returns invalid JSON for a single item, that item gets `relevance_score = 0.0`, `ai_summary = NULL` — never blocks the batch. Error logged but not raised.

---

## Section 4: News App

### `apps/news/` structure

```
apps/news/
├── __init__.py
├── app.py
└── tabs/
    ├── __init__.py
    ├── top_stories.py
    ├── by_symbol.py
    ├── full_feed.py
    └── digest.py
```

**`app.py`** — 4 tabs, `@st.cache_resource` on `connect()`. No `account_id` (news is account-agnostic).

### New DB query functions in `apps/shared/db.py`

- `get_news_items(conn, min_relevance=0.0, symbols=None, limit=100) -> pd.DataFrame` — ordered by `relevance_score DESC, published_ts DESC`; if `symbols` list provided, filters `symbols_mentioned && symbols` (PostgreSQL array overlap)
- `get_news_by_symbol(conn, symbol: str, limit=50) -> pd.DataFrame` — items where `symbol = ANY(symbols_mentioned)`, ordered by `published_ts DESC`

Both use cursor pattern (not `pd.read_sql`), fully mockable.

### Tab designs

**`tabs/top_stories.py`** — `render(conn)`:
- Sidebar slider: relevance threshold (default 0.5, range 0.0–1.0)
- Calls `get_news_items(conn, min_relevance=threshold, limit=50)`
- Each item rendered as: headline (hyperlink), source + published timestamp, sentiment badge (🟢 bullish / 🔴 bearish / ⚪ neutral), AI summary text
- `st.expander` per item to avoid wall-of-text

**`tabs/by_symbol.py`** — `render(conn)`:
- Selectbox populated from `WATCHLIST` env var + distinct held positions from DB
- Calls `get_news_by_symbol(conn, symbol)`
- Timeline table of headlines + sentiment
- Plotly scatter: x=published_ts, y=relevance_score, color=sentiment — shows news flow over time

**`tabs/full_feed.py`** — `render(conn)`:
- Calls `get_news_items(conn, min_relevance=0.0, limit=200)`
- `st.text_input` for keyword search (client-side filter on `headline` column)
- `st.multiselect` for source filter
- Full `st.dataframe`

**`tabs/digest.py`** — `render(conn)`:
- Loads today's top 20 items (`min_relevance > 0.3`, `published_ts >= today`)
- "Generate Daily Briefing" button → single Claude call using `claude-sonnet-4-6`
- Prompt asks for structured briefing: macro themes, key movers, risks, opportunities
- Result displayed as `st.markdown`
- Result cached in `trading.agent_memory` via upsert on `(app_key, category, subject)` unique key: `app_key="news_digest"`, `category="digest"`, `subject=today_date` (ISO string), `content=briefing_text`, `ts_updated=NOW()` — re-clicks within same day reuse cached result without re-calling Claude

All tab files import `streamlit` only inside `render()` — never at module level.

---

## Section 5: Configuration & Dependencies

### `config/.env` additions
```
WATCHLIST=SPY,QQQ,NVDA,AAPL        # comma-separated, used by scorer and news app
ANTHROPIC_API_KEY=sk-ant-...        # scorer + digest tab
POLYGON_API_KEY=                    # optional; leave blank to skip Polygon sources
AV_API_KEY=                         # optional; leave blank to skip Alpha Vantage
```

### `requirements.txt` additions
```
feedparser==6.0.11
anthropic>=0.30.0
```

### DB migration
Run once: `psql $PGURL -f db/migrations/001_news_items.sql`

---

## Testing Scope

| Test file | What it tests |
|---|---|
| `tests/services/market_data/test_yfinance_feed.py` | Mock `yf.download`; verify output dict shape and column mapping for bars_1d, bars_1h, vix, fx |
| `tests/services/market_data/test_ingest_jobs.py` | APScheduler job registration (not live HTTP); verify job names, triggers, timezone |
| `tests/services/news/test_sources.py` | RSS registry structure; `fetch_polygon_news` returns `[]` when key is empty |
| `tests/services/news/test_ingest.py` | Mock `feedparser.parse`; verify url_hash dedup (existing hash skipped); verify new items inserted |
| `tests/services/news/test_scorer.py` | Mock Anthropic client; verify score/sentiment/symbols extracted from valid JSON; stale-item (>48h) gets 0.0 without API call; JSON parse failure → 0.0 fallback |
| `tests/apps/shared/test_db.py` additions | `get_news_items` cursor mock; `get_news_by_symbol` cursor mock |

No tests for `apps/news/` tabs (consistent with portfolio/markets convention).

---

## File Manifest (new files only)

```
services/market_data/
├── yfinance_feed.py          (new)
├── polygon_feed.py           (new)
└── ingest.py                 (new)

services/news/
├── __init__.py               (new)
├── sources.py                (new)
├── ingest.py                 (new)
└── scorer.py                 (new)

apps/news/
├── __init__.py               (new)
├── app.py                    (new)
└── tabs/
    ├── __init__.py           (new)
    ├── top_stories.py        (new)
    ├── by_symbol.py          (new)
    ├── full_feed.py          (new)
    └── digest.py             (new)

db/migrations/
└── 001_news_items.sql        (new)

tests/services/market_data/
├── test_yfinance_feed.py     (new)
└── test_ingest_jobs.py       (new)

tests/services/news/
├── __init__.py               (new)
├── test_sources.py           (new)
├── test_ingest.py            (new)
└── test_scorer.py            (new)

db/schema.sql                 (modified — add news_items table)
apps/shared/db.py             (modified — add 2 news query functions)
requirements.txt              (modified — feedparser, anthropic)
```

Total: 20 new files, 3 modified files.
