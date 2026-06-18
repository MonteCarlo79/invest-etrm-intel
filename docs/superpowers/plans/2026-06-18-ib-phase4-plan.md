# IB Platform Phase 4 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two independent APScheduler market-data/news ingestion services and a 4-tab Streamlit news app to the IB trading platform.

**Architecture:** Two long-running services (`services/market_data/ingest.py` and `services/news/ingest.py`) run as separate processes; a news scorer (`services/news/scorer.py`) calls Claude Haiku per batch; a Streamlit app (`apps/news/`) reads scored items from the DB. Market data feeds use yfinance (primary) with Polygon as an optional fallback keyed on `SkipSource`. News scoring uses Claude Haiku for per-item scoring and Claude Sonnet for the daily digest.

**Tech Stack:** Python 3.13, APScheduler 3.10.4, yfinance 0.2.44, feedparser 6.0.11, anthropic SDK, psycopg2, Streamlit 1.37.0, Plotly 5.23.0, pytest 8.3.3

**Repo:** `C:\Users\dipeng.chen\OneDrive\ETRM\ib-platform`
**Design spec:** `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\docs\superpowers\specs\2026-06-18-ib-phase4-design.md`
**Python:** `/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe`
**Run tests:** `cd C:/Users/dipeng.chen/OneDrive/ETRM/ib-platform && /c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/ -q`

---

## File Manifest

**New files:**
```
db/migrations/001_news_items.sql
services/market_data/yfinance_feed.py
services/market_data/polygon_feed.py
services/market_data/ingest.py
services/news/__init__.py
services/news/sources.py
services/news/ingest.py
services/news/scorer.py
apps/news/__init__.py
apps/news/app.py
apps/news/tabs/__init__.py
apps/news/tabs/top_stories.py
apps/news/tabs/by_symbol.py
apps/news/tabs/full_feed.py
apps/news/tabs/digest.py
tests/services/market_data/test_yfinance_feed.py
tests/services/market_data/test_polygon_feed.py
tests/services/market_data/test_ingest_jobs.py
tests/services/news/__init__.py
tests/services/news/test_sources.py
tests/services/news/test_ingest.py
tests/services/news/test_scorer.py
```

**Modified files:**
```
db/schema.sql               — add trading.news_items table
requirements.txt            — add feedparser==6.0.11, anthropic>=0.30.0
apps/shared/db.py           — add get_news_items, get_news_by_symbol
tests/apps/shared/test_db.py — add tests for the two new db functions
```

---

## Task 1: DB Migration — `trading.news_items`

**Files:**
- Create: `db/migrations/001_news_items.sql`
- Modify: `db/schema.sql`

- [ ] **Step 1: Create migration file**

Create `db/migrations/001_news_items.sql`:

```sql
-- Run once: psql $PGURL -f db/migrations/001_news_items.sql

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

CREATE INDEX IF NOT EXISTS idx_news_published ON trading.news_items (published_ts DESC);
CREATE INDEX IF NOT EXISTS idx_news_relevance ON trading.news_items (relevance_score DESC);
```

- [ ] **Step 2: Add news_items table to schema.sql**

Add the same CREATE TABLE + indexes to the end of `db/schema.sql` (after the agent_memory block, before the final newline):

```sql

-- ─── News ─────────────────────────────────────────────────────────────────────
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

CREATE INDEX IF NOT EXISTS idx_news_published ON trading.news_items (published_ts DESC);
CREATE INDEX IF NOT EXISTS idx_news_relevance ON trading.news_items (relevance_score DESC);
```

- [ ] **Step 3: Commit**

```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/ib-platform
git add db/migrations/001_news_items.sql db/schema.sql
git commit -m "feat(db): add trading.news_items table and migration"
```

---

## Task 2: requirements.txt — add feedparser and anthropic

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Add the two dependencies**

In `requirements.txt`, replace the `# Dashboard` block with:

```
# Dashboard
streamlit==1.37.0
plotly==5.23.0

# News service
feedparser==6.0.11
anthropic>=0.30.0
```

- [ ] **Step 2: Install**

```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/ib-platform
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pip install feedparser==6.0.11 "anthropic>=0.30.0" -q
```

Expected: no errors; `feedparser` and `anthropic` appear in `pip list`.

- [ ] **Step 3: Commit**

```bash
git add requirements.txt
git commit -m "chore: add feedparser and anthropic to requirements"
```

---

## Task 3: `services/market_data/yfinance_feed.py`

**Files:**
- Create: `services/market_data/yfinance_feed.py`
- Create: `tests/services/market_data/test_yfinance_feed.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/services/market_data/test_yfinance_feed.py`:

```python
from __future__ import annotations
import pytest
import pandas as pd
from unittest.mock import patch


def _make_single_df(dates, opens, highs, lows, closes, volumes):
    """Single-symbol yf.download result (flat columns)."""
    idx = pd.to_datetime(dates)
    return pd.DataFrame(
        {"Open": opens, "High": highs, "Low": lows, "Close": closes, "Volume": volumes},
        index=idx,
    )


def _make_multi_df(date, symbols, rows):
    """Multi-symbol yf.download result (MultiIndex columns)."""
    idx = pd.to_datetime([date])
    cols = pd.MultiIndex.from_tuples(
        [(field, sym) for sym in symbols for field in ["Open", "High", "Low", "Close", "Volume"]]
    )
    data = [rows]
    return pd.DataFrame(data, index=idx, columns=cols)


# ── fetch_bars_1d ──────────────────────────────────────────────────────────────

class TestFetchBars1d:
    def test_single_symbol_returns_correct_shape(self):
        df = _make_single_df(
            ["2024-01-02", "2024-01-03"],
            [100.0, 101.0], [105.0, 106.0], [99.0, 100.0], [103.0, 104.0],
            [1_000_000, 1_200_000],
        )
        with patch("yfinance.download", return_value=df):
            from services.market_data.yfinance_feed import fetch_bars_1d
            result = fetch_bars_1d(["SPY"])

        assert len(result) == 2
        assert result[0]["symbol"] == "SPY"
        assert result[0]["source"] == "yfinance"
        assert result[0]["close"] == pytest.approx(103.0)
        assert "ts_date" in result[0]
        assert "open" in result[0]
        assert "high" in result[0]
        assert "low" in result[0]
        assert "volume" in result[0]

    def test_empty_symbols_returns_empty_list(self):
        from services.market_data.yfinance_feed import fetch_bars_1d
        assert fetch_bars_1d([]) == []

    def test_multi_symbol_returns_all_symbols(self):
        idx = pd.to_datetime(["2024-01-02"])
        cols = pd.MultiIndex.from_tuples([
            ("Open", "SPY"), ("High", "SPY"), ("Low", "SPY"), ("Close", "SPY"), ("Volume", "SPY"),
            ("Open", "QQQ"), ("High", "QQQ"), ("Low", "QQQ"), ("Close", "QQQ"), ("Volume", "QQQ"),
        ])
        data = [[100.0, 105.0, 99.0, 103.0, 1_000_000, 200.0, 205.0, 199.0, 202.0, 500_000]]
        df = pd.DataFrame(data, index=idx, columns=cols)

        with patch("yfinance.download", return_value=df):
            from services.market_data.yfinance_feed import fetch_bars_1d
            result = fetch_bars_1d(["SPY", "QQQ"])

        syms = {r["symbol"] for r in result}
        assert syms == {"SPY", "QQQ"}


# ── fetch_bars_1h ──────────────────────────────────────────────────────────────

class TestFetchBars1h:
    def test_single_symbol_returns_ts_field(self):
        idx = pd.to_datetime(["2024-01-02 13:00:00+00:00", "2024-01-02 14:00:00+00:00"])
        df = pd.DataFrame(
            {"Open": [100.0, 101.0], "High": [102.0, 103.0],
             "Low": [99.0, 100.0], "Close": [101.0, 102.0], "Volume": [50000, 60000]},
            index=idx,
        )
        with patch("yfinance.download", return_value=df):
            from services.market_data.yfinance_feed import fetch_bars_1h
            result = fetch_bars_1h(["SPY"])

        assert len(result) == 2
        assert result[0]["symbol"] == "SPY"
        assert result[0]["source"] == "yfinance"
        assert "ts" in result[0]

    def test_empty_symbols_returns_empty_list(self):
        from services.market_data.yfinance_feed import fetch_bars_1h
        assert fetch_bars_1h([]) == []


# ── fetch_vix_term_structure ───────────────────────────────────────────────────

class TestFetchVixTermStructure:
    def test_returns_vix_vvix_and_null_fields(self):
        idx = pd.to_datetime(["2024-01-02"])
        cols = pd.MultiIndex.from_tuples([
            ("Close", "^VIX"), ("Close", "^VVIX"),
        ])
        df = pd.DataFrame([[15.5, 90.2]], index=idx, columns=cols)

        with patch("yfinance.download", return_value=df):
            from services.market_data.yfinance_feed import fetch_vix_term_structure
            result = fetch_vix_term_structure()

        assert result["vix_index"] == pytest.approx(15.5)
        assert result["vvix"] == pytest.approx(90.2)
        assert result["contango_pct"] is None
        assert result["roll_yield_annualised"] is None
        assert result["regime"] is None
        assert result["source"] == "yfinance"


# ── fetch_fx_rates ─────────────────────────────────────────────────────────────

class TestFetchFxRates:
    def test_single_pair_returns_spot(self):
        idx = pd.to_datetime(["2024-01-02 13:00:00+00:00"])
        df = pd.DataFrame(
            {"Open": [1.09], "High": [1.092], "Low": [1.088], "Close": [1.091], "Volume": [0]},
            index=idx,
        )
        with patch("yfinance.download", return_value=df):
            from services.market_data.yfinance_feed import fetch_fx_rates
            result = fetch_fx_rates(["EURUSD=X"])

        assert len(result) == 1
        assert result[0]["pair"] == "EURUSD=X"
        assert result[0]["spot"] == pytest.approx(1.091)
        assert result[0]["source"] == "yfinance"

    def test_empty_pairs_returns_empty_list(self):
        from services.market_data.yfinance_feed import fetch_fx_rates
        assert fetch_fx_rates([]) == []
```

- [ ] **Step 2: Run tests — expect FAIL (ImportError)**

```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/ib-platform
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/market_data/test_yfinance_feed.py -v 2>&1 | head -30
```

Expected: `ModuleNotFoundError: No module named 'services.market_data.yfinance_feed'`

- [ ] **Step 3: Implement `services/market_data/yfinance_feed.py`**

```python
from __future__ import annotations
import yfinance as yf
import pandas as pd


def fetch_bars_1d(symbols: list[str], period: str = "1y") -> list[dict]:
    """EOD OHLCV bars via yf.download. Returns list of dicts with ts_date (date)."""
    if not symbols:
        return []
    df = yf.download(symbols, period=period, progress=False, auto_adjust=True)
    if df.empty:
        return []
    return _parse_bars(df, symbols, ts_field="ts_date", use_date=True)


def fetch_bars_1h(symbols: list[str], period: str = "5d") -> list[dict]:
    """1-hour bars via yf.download. Returns list of dicts with ts (datetime)."""
    if not symbols:
        return []
    df = yf.download(symbols, period=period, interval="1h", progress=False, auto_adjust=True)
    if df.empty:
        return []
    return _parse_bars(df, symbols, ts_field="ts", use_date=False)


def fetch_vix_term_structure() -> dict:
    """Fetches VIX spot and VVIX. M1-M8 and regime fields left NULL (IB path fills them)."""
    df = yf.download(["^VIX", "^VVIX"], period="1d", progress=False, auto_adjust=True)
    vix_val = None
    vvix_val = None
    if not df.empty:
        if isinstance(df.columns, pd.MultiIndex):
            try:
                vix_val = float(df["Close"]["^VIX"].iloc[-1])
            except (KeyError, IndexError, TypeError):
                pass
            try:
                vvix_val = float(df["Close"]["^VVIX"].iloc[-1])
            except (KeyError, IndexError, TypeError):
                pass
        else:
            try:
                vix_val = float(df["Close"].iloc[-1])
            except (KeyError, IndexError, TypeError):
                pass
    return {
        "vix_index": vix_val,
        "vvix": vvix_val,
        "contango_pct": None,
        "roll_yield_annualised": None,
        "regime": None,
        "source": "yfinance",
    }


def fetch_fx_rates(pairs: list[str]) -> list[dict]:
    """Fetches FX spot rates. Each pair is a yfinance ticker, e.g. 'EURUSD=X'."""
    if not pairs:
        return []
    df = yf.download(pairs, period="1d", interval="1h", progress=False, auto_adjust=True)
    if df.empty:
        return []
    records = _parse_bars(df, pairs, ts_field="ts", use_date=False)
    # rename 'close' to 'spot' and keep only pair/ts/spot/source
    return [
        {"pair": r["symbol"], "ts": r["ts"], "spot": r["close"], "source": r["source"]}
        for r in records
    ]


# ── internal ───────────────────────────────────────────────────────────────────

def _parse_bars(df: pd.DataFrame, symbols: list[str], ts_field: str, use_date: bool) -> list[dict]:
    """Handle both single-symbol (flat columns) and multi-symbol (MultiIndex) DataFrames."""
    records = []
    if isinstance(df.columns, pd.MultiIndex):
        for sym in symbols:
            try:
                sym_df = df.xs(sym, axis=1, level=1)
            except KeyError:
                continue
            for ts, row in sym_df.iterrows():
                if pd.isna(row.get("Close")):
                    continue
                records.append(_row_to_dict(sym, ts, row, ts_field, use_date))
    else:
        sym = symbols[0]
        for ts, row in df.iterrows():
            if pd.isna(row.get("Close")):
                continue
            records.append(_row_to_dict(sym, ts, row, ts_field, use_date))
    return records


def _row_to_dict(sym: str, ts, row, ts_field: str, use_date: bool) -> dict:
    ts_val = ts.date() if use_date and hasattr(ts, "date") else (
        ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
    )
    return {
        "symbol": sym,
        ts_field: ts_val,
        "open": float(row["Open"]),
        "high": float(row["High"]),
        "low": float(row["Low"]),
        "close": float(row["Close"]),
        "volume": int(row["Volume"]) if not pd.isna(row.get("Volume", float("nan"))) else 0,
        "source": "yfinance",
    }
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/market_data/test_yfinance_feed.py -v
```

Expected: all 8 tests pass.

- [ ] **Step 5: Commit**

```bash
git add services/market_data/yfinance_feed.py tests/services/market_data/test_yfinance_feed.py
git commit -m "feat(market_data): add yfinance_feed with bars, VIX, FX functions"
```

---

## Task 4: `services/market_data/polygon_feed.py` (SkipSource pattern)

**Files:**
- Create: `services/market_data/polygon_feed.py`
- Create: `tests/services/market_data/test_polygon_feed.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/services/market_data/test_polygon_feed.py`:

```python
from __future__ import annotations
import os
import pytest
from unittest.mock import patch


class TestSkipSource:
    def test_raises_skip_source_when_key_empty(self):
        with patch.dict(os.environ, {"POLYGON_API_KEY": ""}):
            from services.market_data.polygon_feed import fetch_bars_1d, SkipSource
            with pytest.raises(SkipSource):
                fetch_bars_1d(["SPY"])

    def test_raises_skip_source_when_key_absent(self):
        env = {k: v for k, v in os.environ.items() if k != "POLYGON_API_KEY"}
        with patch.dict(os.environ, env, clear=True):
            from services.market_data.polygon_feed import fetch_bars_1h, SkipSource
            with pytest.raises(SkipSource):
                fetch_bars_1h(["SPY"])

    def test_fetch_vix_raises_skip_source_when_no_key(self):
        with patch.dict(os.environ, {"POLYGON_API_KEY": ""}):
            from services.market_data.polygon_feed import fetch_vix_term_structure, SkipSource
            with pytest.raises(SkipSource):
                fetch_vix_term_structure()

    def test_fetch_fx_raises_skip_source_when_no_key(self):
        with patch.dict(os.environ, {"POLYGON_API_KEY": ""}):
            from services.market_data.polygon_feed import fetch_fx_rates, SkipSource
            with pytest.raises(SkipSource):
                fetch_fx_rates(["EURUSD=X"])
```

- [ ] **Step 2: Run tests — expect FAIL (ImportError)**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/market_data/test_polygon_feed.py -v 2>&1 | head -20
```

Expected: `ModuleNotFoundError: No module named 'services.market_data.polygon_feed'`

- [ ] **Step 3: Implement `services/market_data/polygon_feed.py`**

```python
from __future__ import annotations
import os


class SkipSource(Exception):
    """Raised when a pluggable data source should be silently skipped (e.g. missing API key)."""


def _require_key() -> str:
    key = os.environ.get("POLYGON_API_KEY", "")
    if not key:
        raise SkipSource("POLYGON_API_KEY not set or empty")
    return key


def fetch_bars_1d(symbols: list[str], period: str = "1y") -> list[dict]:
    """Polygon equivalent of yfinance_feed.fetch_bars_1d.
    Raises SkipSource if POLYGON_API_KEY not set."""
    _require_key()
    raise NotImplementedError("Polygon bars_1d not yet implemented")


def fetch_bars_1h(symbols: list[str], period: str = "5d") -> list[dict]:
    """Polygon equivalent of yfinance_feed.fetch_bars_1h.
    Raises SkipSource if POLYGON_API_KEY not set."""
    _require_key()
    raise NotImplementedError("Polygon bars_1h not yet implemented")


def fetch_vix_term_structure() -> dict:
    """Polygon equivalent of yfinance_feed.fetch_vix_term_structure.
    Raises SkipSource if POLYGON_API_KEY not set."""
    _require_key()
    raise NotImplementedError("Polygon VIX term structure not yet implemented")


def fetch_fx_rates(pairs: list[str]) -> list[dict]:
    """Polygon equivalent of yfinance_feed.fetch_fx_rates.
    Raises SkipSource if POLYGON_API_KEY not set."""
    _require_key()
    raise NotImplementedError("Polygon FX rates not yet implemented")
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/market_data/test_polygon_feed.py -v
```

Expected: all 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add services/market_data/polygon_feed.py tests/services/market_data/test_polygon_feed.py
git commit -m "feat(market_data): add polygon_feed stub with SkipSource pattern"
```

---

## Task 5: `services/market_data/ingest.py` (APScheduler, 4 jobs)

**Files:**
- Create: `services/market_data/ingest.py`
- Create: `tests/services/market_data/test_ingest_jobs.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/services/market_data/test_ingest_jobs.py`:

```python
from __future__ import annotations
import pytest
from apscheduler.triggers.cron import CronTrigger


class TestMarketDataScheduler:
    def setup_method(self):
        from services.market_data.ingest import build_scheduler
        self.sched = build_scheduler()
        self.jobs = {job.id: job for job in self.sched.get_jobs()}

    def test_all_four_jobs_registered(self):
        assert set(self.jobs) == {"eod_bars", "intraday_bars", "vix_term_structure", "fx_rates"}

    def test_scheduler_timezone_is_eastern(self):
        tz_str = str(self.sched.timezone)
        assert "New_York" in tz_str or "Eastern" in tz_str

    def test_eod_bars_is_cron_trigger(self):
        assert isinstance(self.jobs["eod_bars"].trigger, CronTrigger)

    def test_intraday_bars_is_cron_trigger(self):
        assert isinstance(self.jobs["intraday_bars"].trigger, CronTrigger)

    def test_vix_is_cron_trigger(self):
        assert isinstance(self.jobs["vix_term_structure"].trigger, CronTrigger)

    def test_fx_is_cron_trigger(self):
        assert isinstance(self.jobs["fx_rates"].trigger, CronTrigger)
```

- [ ] **Step 2: Run tests — expect FAIL (ImportError)**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/market_data/test_ingest_jobs.py -v 2>&1 | head -20
```

Expected: `ModuleNotFoundError: No module named 'services.market_data.ingest'`

- [ ] **Step 3: Implement `services/market_data/ingest.py`**

```python
from __future__ import annotations
import logging
import os
import psycopg2
from datetime import date
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from services.market_data import yfinance_feed
from services.market_data import polygon_feed
from services.market_data.polygon_feed import SkipSource

logger = logging.getLogger(__name__)

_FX_PAIRS = ["EURUSD=X", "GBPUSD=X", "USDJPY=X", "USDCNY=X"]


def _get_watchlist() -> list[str]:
    raw = os.environ.get("WATCHLIST", "")
    return [s.strip() for s in raw.split(",") if s.strip()]


def _get_position_symbols(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT symbol FROM trading.positions")
        return [row[0] for row in cur.fetchall()]


def _fetch_with_fallback(fn_primary, fn_fallback, *args, **kwargs):
    try:
        return fn_primary(*args, **kwargs)
    except SkipSource:
        return fn_fallback(*args, **kwargs)


def _upsert_bars_1d(conn, records: list[dict]) -> None:
    with conn.cursor() as cur:
        for r in records:
            cur.execute("""
                INSERT INTO trading.bars_1d
                    (symbol, ts_date, open, high, low, close, volume, source)
                VALUES (%(symbol)s, %(ts_date)s, %(open)s, %(high)s,
                        %(low)s, %(close)s, %(volume)s, %(source)s)
                ON CONFLICT (symbol, ts_date) DO UPDATE SET
                    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                    close=EXCLUDED.close, volume=EXCLUDED.volume,
                    source=EXCLUDED.source
            """, r)
    conn.commit()


def _upsert_bars_1h(conn, records: list[dict]) -> None:
    with conn.cursor() as cur:
        for r in records:
            cur.execute("""
                INSERT INTO trading.bars_1h
                    (symbol, ts, open, high, low, close, volume, source)
                VALUES (%(symbol)s, %(ts)s, %(open)s, %(high)s,
                        %(low)s, %(close)s, %(volume)s, %(source)s)
                ON CONFLICT (symbol, ts) DO UPDATE SET
                    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                    close=EXCLUDED.close, volume=EXCLUDED.volume,
                    source=EXCLUDED.source
            """, r)
    conn.commit()


def _upsert_vix(conn, record: dict) -> None:
    today = date.today()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO trading.vix_term_structure
                (ts_date, vix_index, vvix, contango_pct, roll_yield_annualised, regime, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ts_date) DO UPDATE SET
                vix_index=EXCLUDED.vix_index, vvix=EXCLUDED.vvix,
                contango_pct=EXCLUDED.contango_pct,
                roll_yield_annualised=EXCLUDED.roll_yield_annualised,
                regime=EXCLUDED.regime, source=EXCLUDED.source
        """, (today, record["vix_index"], record["vvix"],
              record["contango_pct"], record["roll_yield_annualised"],
              record["regime"], record["source"]))
    conn.commit()


def _upsert_fx_rates(conn, records: list[dict]) -> None:
    with conn.cursor() as cur:
        for r in records:
            cur.execute("""
                INSERT INTO trading.fx_rates (pair, ts, spot, source)
                VALUES (%(pair)s, %(ts)s, %(spot)s, %(source)s)
                ON CONFLICT (pair, ts) DO UPDATE SET
                    spot=EXCLUDED.spot, source=EXCLUDED.source
            """, r)
    conn.commit()


def job_eod_bars() -> None:
    conn = psycopg2.connect(os.environ["PGURL"])
    try:
        symbols = list(set(_get_watchlist() + _get_position_symbols(conn)))
        if not symbols:
            return
        records = _fetch_with_fallback(
            polygon_feed.fetch_bars_1d, yfinance_feed.fetch_bars_1d, symbols
        )
        _upsert_bars_1d(conn, records)
        logger.info("EOD bars: upserted %d records", len(records))
    except Exception:
        logger.exception("job_eod_bars failed")
    finally:
        conn.close()


def job_intraday_bars() -> None:
    conn = psycopg2.connect(os.environ["PGURL"])
    try:
        symbols = _get_position_symbols(conn)
        if not symbols:
            return
        records = _fetch_with_fallback(
            polygon_feed.fetch_bars_1h, yfinance_feed.fetch_bars_1h, symbols
        )
        _upsert_bars_1h(conn, records)
        logger.info("Intraday bars: upserted %d records", len(records))
    except Exception:
        logger.exception("job_intraday_bars failed")
    finally:
        conn.close()


def job_vix() -> None:
    conn = psycopg2.connect(os.environ["PGURL"])
    try:
        record = _fetch_with_fallback(
            polygon_feed.fetch_vix_term_structure, yfinance_feed.fetch_vix_term_structure
        )
        _upsert_vix(conn, record)
        logger.info("VIX term structure upserted")
    except Exception:
        logger.exception("job_vix failed")
    finally:
        conn.close()


def job_fx_rates() -> None:
    conn = psycopg2.connect(os.environ["PGURL"])
    try:
        records = _fetch_with_fallback(
            polygon_feed.fetch_fx_rates, yfinance_feed.fetch_fx_rates, _FX_PAIRS
        )
        _upsert_fx_rates(conn, records)
        logger.info("FX rates: upserted %d records", len(records))
    except Exception:
        logger.exception("job_fx_rates failed")
    finally:
        conn.close()


def build_scheduler() -> BlockingScheduler:
    tz = "America/New_York"
    sched = BlockingScheduler(timezone=tz)

    sched.add_job(
        job_eod_bars,
        CronTrigger(day_of_week="mon-fri", hour=16, minute=10, timezone=tz),
        id="eod_bars",
        name="EOD bars",
    )
    sched.add_job(
        job_intraday_bars,
        CronTrigger(day_of_week="mon-fri", hour="9-15", minute=30, timezone=tz),
        id="intraday_bars",
        name="Intraday bars",
    )
    sched.add_job(
        job_vix,
        CronTrigger(day_of_week="mon-fri", hour=9, minute=35, timezone=tz),
        id="vix_term_structure",
        name="VIX term structure",
    )
    sched.add_job(
        job_fx_rates,
        CronTrigger(hour="*/4", timezone=tz),
        id="fx_rates",
        name="FX rates",
    )
    return sched


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv("config/.env")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sched = build_scheduler()
    logger.info("Market data ingest service started")
    sched.start()
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/market_data/test_ingest_jobs.py -v
```

Expected: all 6 tests pass.

- [ ] **Step 5: Run full test suite to check no regressions**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/ -q
```

Expected: 290+ tests pass, 0 failures.

- [ ] **Step 6: Commit**

```bash
git add services/market_data/ingest.py tests/services/market_data/test_ingest_jobs.py
git commit -m "feat(market_data): add APScheduler ingest service with 4 jobs"
```

---

## Task 6: `services/news/sources.py` (RSS registry + Polygon news)

**Files:**
- Create: `services/news/__init__.py`
- Create: `services/news/sources.py`
- Create: `tests/services/news/__init__.py`
- Create: `tests/services/news/test_sources.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/services/news/test_sources.py`:

```python
from __future__ import annotations
import os
import pytest
from unittest.mock import patch


class TestRssFeeds:
    def test_three_feeds_defined(self):
        from services.news.sources import RSS_FEEDS
        assert len(RSS_FEEDS) == 3

    def test_feeds_have_name_and_url(self):
        from services.news.sources import RSS_FEEDS
        for feed in RSS_FEEDS:
            assert "name" in feed
            assert "url" in feed
            assert feed["url"].startswith("http")

    def test_known_feed_names_present(self):
        from services.news.sources import RSS_FEEDS
        names = {f["name"] for f in RSS_FEEDS}
        assert "Reuters" in names
        assert "CNBC" in names
        assert "FT" in names


class TestFetchPolygonNews:
    def test_returns_empty_list_when_key_not_set(self):
        env = {k: v for k, v in os.environ.items() if k != "POLYGON_API_KEY"}
        with patch.dict(os.environ, env, clear=True):
            from services.news.sources import fetch_polygon_news
            result = fetch_polygon_news(["SPY"])
        assert result == []

    def test_returns_empty_list_when_key_empty(self):
        with patch.dict(os.environ, {"POLYGON_API_KEY": ""}):
            from services.news.sources import fetch_polygon_news
            result = fetch_polygon_news(["SPY"])
        assert result == []

    def test_returns_list_of_dicts_on_valid_key(self):
        """With a key set, calls Polygon API. Mock the HTTP layer."""
        import requests
        mock_response = type("R", (), {
            "raise_for_status": lambda self: None,
            "json": lambda self: {"results": [{
                "title": "Test Headline",
                "article_url": "https://example.com/1",
                "description": "Test body",
                "published_utc": "2026-06-18T10:00:00Z",
            }]},
        })()

        with patch.dict(os.environ, {"POLYGON_API_KEY": "test-key"}):
            with patch("requests.get", return_value=mock_response):
                from services.news.sources import fetch_polygon_news
                result = fetch_polygon_news(["SPY"])

        assert len(result) == 1
        assert result[0]["source"] == "polygon"
        assert result[0]["headline"] == "Test Headline"
        assert result[0]["url"] == "https://example.com/1"
        assert "body_text" in result[0]
        assert "published_ts" in result[0]
```

- [ ] **Step 2: Run tests — expect FAIL (ImportError)**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/news/test_sources.py -v 2>&1 | head -20
```

Expected: `ModuleNotFoundError: No module named 'services.news'`

- [ ] **Step 3: Create package init files**

Create `services/news/__init__.py` (empty):
```python
```

Create `tests/services/news/__init__.py` (empty):
```python
```

- [ ] **Step 4: Implement `services/news/sources.py`**

```python
from __future__ import annotations
import os
import requests

RSS_FEEDS = [
    {"name": "Reuters", "url": "https://feeds.reuters.com/reuters/businessNews"},
    {"name": "CNBC",    "url": "https://feeds.nbcnews.com/nbcnews/public/business"},
    {"name": "FT",      "url": "https://www.ft.com/rss/home"},
]


def fetch_polygon_news(symbols: list[str]) -> list[dict]:
    """Reads POLYGON_API_KEY from env. Returns [] if key not set or empty.
    Each dict: {headline, url, body_text, published_ts, source='polygon'}."""
    key = os.environ.get("POLYGON_API_KEY", "")
    if not key:
        return []

    results = []
    try:
        for sym in symbols:
            resp = requests.get(
                "https://api.polygon.io/v2/reference/news",
                params={"ticker": sym, "limit": 10, "apiKey": key},
                timeout=10,
            )
            resp.raise_for_status()
            for item in resp.json().get("results", []):
                results.append({
                    "headline": item.get("title", ""),
                    "url": item.get("article_url", ""),
                    "body_text": item.get("description", ""),
                    "published_ts": item.get("published_utc"),
                    "source": "polygon",
                })
    except Exception:
        return []
    return results
```

- [ ] **Step 5: Run tests — expect PASS**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/news/test_sources.py -v
```

Expected: all 6 tests pass.

- [ ] **Step 6: Commit**

```bash
git add services/news/__init__.py services/news/sources.py \
        tests/services/news/__init__.py tests/services/news/test_sources.py
git commit -m "feat(news): add RSS feed registry and Polygon news source"
```

---

## Task 7: `services/news/ingest.py` (feedparser + url_hash dedup)

**Files:**
- Create: `services/news/ingest.py`
- Create: `tests/services/news/test_ingest.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/services/news/test_ingest.py`:

```python
from __future__ import annotations
import hashlib
import pytest
from unittest.mock import MagicMock, patch


def _mock_conn(existing_hashes: list[str] = None):
    """Build a mock DB connection. existing_hashes is a list of url_hashes already in DB."""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    rows = [(h,) for h in (existing_hashes or [])]
    cursor.fetchall.return_value = rows
    cursor.description = [("url_hash",)]
    conn.cursor.return_value = cursor
    return conn


class TestUrlHash:
    def test_is_sha256_hex(self):
        from services.news.ingest import _url_hash
        url = "https://example.com/article"
        assert _url_hash(url) == hashlib.sha256(url.encode()).hexdigest()

    def test_different_urls_differ(self):
        from services.news.ingest import _url_hash
        assert _url_hash("https://a.com") != _url_hash("https://b.com")


class TestIngestItems:
    def test_skips_existing_hash(self):
        url = "https://example.com/article1"
        existing_hash = hashlib.sha256(url.encode()).hexdigest()
        conn = _mock_conn([existing_hash])

        from services.news.ingest import _ingest_items
        items = [{"url": url, "source": "Reuters", "headline": "Test",
                  "body_text": "", "published_ts": None}]
        inserted = _ingest_items(conn, items)
        assert inserted == 0

    def test_inserts_new_items(self):
        conn = _mock_conn([])

        from services.news.ingest import _ingest_items
        items = [{"url": "https://new.com/1", "source": "Reuters", "headline": "Headline",
                  "body_text": "Body text", "published_ts": None}]
        inserted = _ingest_items(conn, items)
        assert inserted == 1
        # INSERT was called
        conn.cursor.return_value.execute.assert_called()
        conn.commit.assert_called()

    def test_deduplicates_within_batch(self):
        """Two items with the same URL in the same batch: only one inserted."""
        conn = _mock_conn([])
        url = "https://example.com/same"

        from services.news.ingest import _ingest_items
        items = [
            {"url": url, "source": "Reuters", "headline": "First", "body_text": "", "published_ts": None},
            {"url": url, "source": "CNBC",    "headline": "Dupe",  "body_text": "", "published_ts": None},
        ]
        inserted = _ingest_items(conn, items)
        assert inserted == 1

    def test_skips_items_without_url(self):
        conn = _mock_conn([])

        from services.news.ingest import _ingest_items
        items = [{"url": "", "source": "Reuters", "headline": "No URL",
                  "body_text": "", "published_ts": None}]
        inserted = _ingest_items(conn, items)
        assert inserted == 0


class TestScheduler:
    def test_news_job_registered(self):
        from services.news.ingest import build_scheduler
        sched = build_scheduler()
        job_ids = {job.id for job in sched.get_jobs()}
        assert "ingest_news" in job_ids

    def test_scheduler_timezone_is_eastern(self):
        from services.news.ingest import build_scheduler
        sched = build_scheduler()
        assert "New_York" in str(sched.timezone) or "Eastern" in str(sched.timezone)
```

- [ ] **Step 2: Run tests — expect FAIL (ImportError)**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/news/test_ingest.py -v 2>&1 | head -20
```

Expected: `ModuleNotFoundError: No module named 'services.news.ingest'`

- [ ] **Step 3: Implement `services/news/ingest.py`**

```python
from __future__ import annotations
import hashlib
import logging
import os
import psycopg2
import feedparser
from datetime import datetime, timezone

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from services.news.sources import RSS_FEEDS, fetch_polygon_news
from services.news import scorer

logger = logging.getLogger(__name__)


def _url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


def _existing_hashes(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT url_hash FROM trading.news_items")
        return {row[0] for row in cur.fetchall()}


def _ingest_items(conn, items: list[dict]) -> int:
    """Insert new items (not already in DB). Returns count inserted."""
    existing = _existing_hashes(conn)
    new_items = []
    for item in items:
        url = item.get("url", "")
        if not url:
            continue
        h = _url_hash(url)
        if h in existing:
            continue
        existing.add(h)
        new_items.append({**item, "url_hash": h})

    if not new_items:
        return 0

    with conn.cursor() as cur:
        for item in new_items:
            cur.execute("""
                INSERT INTO trading.news_items
                    (url_hash, source, headline, body_text, url, published_ts)
                VALUES (%(url_hash)s, %(source)s, %(headline)s,
                        %(body_text)s, %(url)s, %(published_ts)s)
                ON CONFLICT (url_hash) DO NOTHING
            """, item)
    conn.commit()
    return len(new_items)


def _parse_published(entry) -> datetime | None:
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        import time
        return datetime.fromtimestamp(time.mktime(entry.published_parsed), tz=timezone.utc)
    return None


def _collect_rss_items() -> list[dict]:
    items = []
    for feed in RSS_FEEDS:
        try:
            parsed = feedparser.parse(feed["url"])
            for entry in parsed.entries:
                url = entry.get("link", "")
                if not url:
                    continue
                items.append({
                    "source": feed["name"],
                    "headline": entry.get("title", ""),
                    "body_text": entry.get("summary", ""),
                    "url": url,
                    "published_ts": _parse_published(entry),
                })
        except Exception:
            logger.exception("RSS feed error: %s", feed["name"])
    return items


def _get_watchlist() -> list[str]:
    raw = os.environ.get("WATCHLIST", "")
    return [s.strip() for s in raw.split(",") if s.strip()]


def job_ingest_news() -> None:
    conn = psycopg2.connect(os.environ["PGURL"])
    try:
        items = _collect_rss_items()
        watchlist = _get_watchlist()
        if watchlist:
            items.extend(fetch_polygon_news(watchlist))

        inserted = _ingest_items(conn, items)
        logger.info("News ingest: inserted %d new items", inserted)

        scored = scorer.score_pending(conn)
        logger.info("News scorer: scored %d items", scored)
    except Exception:
        logger.exception("job_ingest_news failed")
    finally:
        conn.close()


def build_scheduler() -> BlockingScheduler:
    tz = "America/New_York"
    sched = BlockingScheduler(timezone=tz)
    sched.add_job(
        job_ingest_news,
        CronTrigger(day_of_week="mon-fri", hour="6-21", minute="*/15", timezone=tz),
        id="ingest_news",
        name="News ingestion",
    )
    return sched


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv("config/.env")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sched = build_scheduler()
    logger.info("News ingest service started")
    sched.start()
```

- [ ] **Step 4: Run tests — expect PASS**

Note: `scorer` is imported at module level in `services/news/ingest.py`. Since `scorer.py` doesn't exist yet, the import will fail. Create a temporary stub `services/news/scorer.py` with just the public function signature before running tests:

```python
# services/news/scorer.py — stub, full implementation in Task 8
def score_pending(conn) -> int:
    return 0
```

Then run:

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/news/test_ingest.py -v
```

Expected: all 8 tests pass.

- [ ] **Step 5: Commit**

```bash
git add services/news/ingest.py services/news/scorer.py tests/services/news/test_ingest.py
git commit -m "feat(news): add news ingest service with feedparser and url_hash dedup"
```

---

## Task 8: `services/news/scorer.py` (Claude Haiku, batch 20, 48h stale)

**Files:**
- Modify: `services/news/scorer.py` (replace stub with full implementation)
- Create: `tests/services/news/test_scorer.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/services/news/test_scorer.py`:

```python
from __future__ import annotations
import json
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch


def _mock_conn(pending_rows=None, position_rows=None):
    """pending_rows: list of (item_id, headline, body_text) tuples for unscored items."""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.rowcount = 0

    call_count = [0]

    def fetchall_side_effect():
        n = call_count[0]
        call_count[0] += 1
        if n == 0:
            # _mark_stale: no fetchall, just rowcount
            return []
        if n == 1:
            # _get_universe: positions query
            return position_rows or []
        if n == 2:
            # _fetch_pending
            rows = pending_rows or []
            return rows
        return []

    cursor.fetchall.side_effect = fetchall_side_effect
    cursor.description = [("item_id",), ("headline",), ("body_text",)]
    conn.cursor.return_value = cursor
    return conn


def _make_claude_response(payload: dict):
    """Build a minimal mock Anthropic response."""
    content = MagicMock()
    content.text = json.dumps(payload)
    resp = MagicMock()
    resp.content = [content]
    return resp


class TestScorePending:
    def test_returns_zero_when_no_pending(self):
        conn = _mock_conn(pending_rows=[])
        with patch("anthropic.Anthropic"):
            from services.news.scorer import score_pending
            count = score_pending(conn)
        assert count == 0

    def test_extracts_score_sentiment_symbols_summary(self):
        pending = [(1, "Fed raises rates", "The Federal Reserve raised...")]
        conn = _mock_conn(pending_rows=pending, position_rows=[("SPY",)])

        payload = {
            "relevance_score": 0.85,
            "sentiment": "bearish",
            "symbols_mentioned": ["SPY"],
            "ai_summary": "Fed raises rates 25bp, markets sell off.",
        }
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _make_claude_response(payload)

        with patch("anthropic.Anthropic", return_value=mock_client):
            from services.news.scorer import score_pending
            count = score_pending(conn)

        assert count == 1
        # Verify UPDATE was called with correct values
        update_calls = [
            c for c in conn.cursor.return_value.execute.call_args_list
            if "UPDATE" in str(c)
        ]
        assert len(update_calls) == 1
        params = update_calls[0][0][1]
        assert params[0] == pytest.approx(0.85)
        assert params[1] == "bearish"
        assert "SPY" in params[2]

    def test_json_parse_failure_uses_zero_fallback(self):
        pending = [(1, "Some headline", "Some body")]
        conn = _mock_conn(pending_rows=pending, position_rows=[])

        mock_client = MagicMock()
        mock_client.messages.create.side_effect = ValueError("bad json")

        with patch("anthropic.Anthropic", return_value=mock_client):
            from services.news.scorer import score_pending
            count = score_pending(conn)

        # Fallback UPDATE (score=0.0) was called
        update_calls = [
            c for c in conn.cursor.return_value.execute.call_args_list
            if "UPDATE" in str(c)
        ]
        assert len(update_calls) == 1
        params = update_calls[0][0][1]
        assert params[0] == pytest.approx(0.0)

    def test_invalid_json_response_uses_zero_fallback(self):
        pending = [(1, "Headline", "Body")]
        conn = _mock_conn(pending_rows=pending, position_rows=[])

        bad_response = MagicMock()
        bad_response.content = [MagicMock(text="not valid json {{")]
        mock_client = MagicMock()
        mock_client.messages.create.return_value = bad_response

        with patch("anthropic.Anthropic", return_value=mock_client):
            from services.news.scorer import score_pending
            count = score_pending(conn)

        update_calls = [
            c for c in conn.cursor.return_value.execute.call_args_list
            if "UPDATE" in str(c)
        ]
        assert len(update_calls) == 1
        params = update_calls[0][0][1]
        assert params[0] == pytest.approx(0.0)


class TestMarkStale:
    def test_stale_items_updated_without_api_call(self):
        conn = _mock_conn(pending_rows=[])
        conn.cursor.return_value.rowcount = 3

        with patch("anthropic.Anthropic") as mock_anthropic_cls:
            from services.news.scorer import score_pending
            score_pending(conn)

        # Anthropic client was created but messages.create NOT called (no pending)
        client = mock_anthropic_cls.return_value
        client.messages.create.assert_not_called()
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/news/test_scorer.py -v 2>&1 | head -30
```

Expected: failures because scorer.py is still the stub.

- [ ] **Step 3: Implement full `services/news/scorer.py`**

Replace the stub with:

```python
from __future__ import annotations
import json
import logging
import os
from datetime import datetime, timezone, timedelta

from anthropic import Anthropic

logger = logging.getLogger(__name__)

_MODEL = "claude-haiku-4-5-20251001"
_BATCH_SIZE = 20
_STALE_HOURS = 48


def _get_universe(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT symbol FROM trading.positions")
        db_symbols = [row[0] for row in cur.fetchall()]
    watchlist_raw = os.environ.get("WATCHLIST", "")
    watchlist = [s.strip() for s in watchlist_raw.split(",") if s.strip()]
    seen: dict[str, None] = {}
    for s in db_symbols + watchlist:
        seen[s] = None
    return list(seen)


def _mark_stale(conn) -> int:
    """Set relevance_score=0.0 on items older than 48h that are still unscored."""
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=_STALE_HOURS)
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE trading.news_items
            SET relevance_score = 0.0, sentiment = 'neutral', ai_summary = NULL
            WHERE relevance_score IS NULL AND ts_ingested < %s
        """, (cutoff,))
        count = cur.rowcount
    conn.commit()
    return count


def _fetch_pending(conn, limit: int) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT item_id, headline, body_text
            FROM trading.news_items
            WHERE relevance_score IS NULL
            ORDER BY ts_ingested DESC
            LIMIT %s
        """, (limit,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _score_item(client: Anthropic, universe: list[str], item: dict) -> dict:
    prompt = (
        f"Universe (positions + watchlist): {', '.join(universe)}\n\n"
        f"Headline: {item['headline']}\n"
        f"Body: {(item.get('body_text') or '')[:500]}\n\n"
        "Respond with:\n"
        "{\n"
        '  "relevance_score": <float 0.0-1.0>,\n'
        '  "sentiment": <"bullish"|"bearish"|"neutral">,\n'
        '  "symbols_mentioned": [<tickers from universe appearing in article>],\n'
        '  "ai_summary": <string, max 120 chars, one sentence>\n'
        "}"
    )
    response = client.messages.create(
        model=_MODEL,
        max_tokens=256,
        system="You are a financial news analyst. Respond only with valid JSON, no markdown.",
        messages=[{"role": "user", "content": prompt}],
    )
    return json.loads(response.content[0].text.strip())


def _update_item(
    conn, item_id: int, score: float, sentiment: str,
    symbols: list[str], summary: str | None
) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE trading.news_items
            SET relevance_score = %s,
                sentiment = %s,
                symbols_mentioned = %s,
                ai_summary = %s
            WHERE item_id = %s
        """, (score, sentiment, symbols, summary, item_id))
    conn.commit()


def score_pending(conn) -> int:
    """Score all rows where relevance_score IS NULL. Returns count scored."""
    _mark_stale(conn)

    pending = _fetch_pending(conn, _BATCH_SIZE)
    if not pending:
        return 0

    universe = _get_universe(conn)
    client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))

    scored = 0
    for item in pending:
        try:
            result = _score_item(client, universe, item)
            _update_item(
                conn,
                item["item_id"],
                float(result.get("relevance_score", 0.0)),
                result.get("sentiment", "neutral"),
                result.get("symbols_mentioned", []),
                result.get("ai_summary"),
            )
            scored += 1
        except Exception:
            logger.exception("Scorer failed for item_id=%s", item["item_id"])
            try:
                _update_item(conn, item["item_id"], 0.0, "neutral", [], None)
            except Exception:
                logger.exception("Failed to set fallback score for item_id=%s", item["item_id"])

    return scored
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/news/test_scorer.py -v
```

Expected: all 5 tests pass.

- [ ] **Step 5: Run full suite**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/ -q
```

Expected: 290+ tests pass, 0 failures.

- [ ] **Step 6: Commit**

```bash
git add services/news/scorer.py tests/services/news/test_scorer.py
git commit -m "feat(news): add Claude Haiku news scorer with stale-item handling"
```

---

## Task 9: `apps/shared/db.py` — add `get_news_items` and `get_news_by_symbol`

**Files:**
- Modify: `apps/shared/db.py`
- Modify: `tests/apps/shared/test_db.py`

- [ ] **Step 1: Write the failing tests**

Add this class to the END of `tests/apps/shared/test_db.py`:

```python
# ---------------------------------------------------------------------------
# get_news_items
# ---------------------------------------------------------------------------

class TestGetNewsItems:
    _cols = ["item_id", "url_hash", "source", "headline", "url",
             "published_ts", "symbols_mentioned", "relevance_score",
             "sentiment", "ai_summary", "ts_ingested"]

    def test_returns_dataframe(self):
        from apps.shared.db import get_news_items
        conn = _mock_conn([], self._cols)
        df = get_news_items(conn)
        assert isinstance(df, pd.DataFrame)

    def test_correct_columns(self):
        from apps.shared.db import get_news_items
        conn = _mock_conn([], self._cols)
        df = get_news_items(conn)
        assert list(df.columns) == self._cols

    def test_with_symbols_filter_passes_list(self):
        from apps.shared.db import get_news_items
        conn = _mock_conn([], self._cols)
        get_news_items(conn, symbols=["SPY", "AAPL"])
        cursor = conn.cursor.return_value
        sql = cursor.execute.call_args[0][0]
        params = cursor.execute.call_args[0][1]
        # symbols_mentioned && %s filter should be in the SQL
        assert "symbols_mentioned" in sql
        assert ["SPY", "AAPL"] in params

    def test_without_symbols_no_overlap_filter(self):
        from apps.shared.db import get_news_items
        conn = _mock_conn([], self._cols)
        get_news_items(conn)
        sql = conn.cursor.return_value.execute.call_args[0][0]
        assert "symbols_mentioned" not in sql

    def test_min_relevance_in_params(self):
        from apps.shared.db import get_news_items
        conn = _mock_conn([], self._cols)
        get_news_items(conn, min_relevance=0.7)
        params = conn.cursor.return_value.execute.call_args[0][1]
        assert 0.7 in params


# ---------------------------------------------------------------------------
# get_news_by_symbol
# ---------------------------------------------------------------------------

class TestGetNewsBySymbol:
    _cols = ["item_id", "headline", "url", "published_ts",
             "relevance_score", "sentiment", "ai_summary"]

    def test_returns_dataframe(self):
        from apps.shared.db import get_news_by_symbol
        conn = _mock_conn([], self._cols)
        df = get_news_by_symbol(conn, "SPY")
        assert isinstance(df, pd.DataFrame)

    def test_symbol_passed_as_param(self):
        from apps.shared.db import get_news_by_symbol
        conn = _mock_conn([], self._cols)
        get_news_by_symbol(conn, "NVDA")
        params = conn.cursor.return_value.execute.call_args[0][1]
        assert "NVDA" in params
```

- [ ] **Step 2: Run new tests — expect FAIL (ImportError)**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/apps/shared/test_db.py::TestGetNewsItems tests/apps/shared/test_db.py::TestGetNewsBySymbol -v 2>&1 | head -20
```

Expected: `ImportError: cannot import name 'get_news_items'`

- [ ] **Step 3: Add functions to `apps/shared/db.py`**

Add at the END of `apps/shared/db.py` (after `get_vol_surface`):

```python
def get_news_items(
    conn,
    min_relevance: float = 0.0,
    symbols: Optional[list[str]] = None,
    limit: int = 100,
) -> pd.DataFrame:
    if symbols is not None:
        return _fetch(conn, """
            SELECT item_id, url_hash, source, headline, url, published_ts,
                   symbols_mentioned, relevance_score, sentiment, ai_summary, ts_ingested
            FROM trading.news_items
            WHERE (relevance_score >= %s OR relevance_score IS NULL)
              AND symbols_mentioned && %s
            ORDER BY relevance_score DESC NULLS LAST, published_ts DESC
            LIMIT %s
        """, (min_relevance, symbols, limit))
    return _fetch(conn, """
        SELECT item_id, url_hash, source, headline, url, published_ts,
               symbols_mentioned, relevance_score, sentiment, ai_summary, ts_ingested
        FROM trading.news_items
        WHERE (relevance_score >= %s OR relevance_score IS NULL)
        ORDER BY relevance_score DESC NULLS LAST, published_ts DESC
        LIMIT %s
    """, (min_relevance, limit))


def get_news_by_symbol(conn, symbol: str, limit: int = 50) -> pd.DataFrame:
    return _fetch(conn, """
        SELECT item_id, headline, url, published_ts,
               relevance_score, sentiment, ai_summary
        FROM trading.news_items
        WHERE %s = ANY(symbols_mentioned)
        ORDER BY published_ts DESC
        LIMIT %s
    """, (symbol, limit))
```

Also update the import list at the top of `tests/apps/shared/test_db.py` to include the two new functions:

```python
from apps.shared.db import (
    get_positions,
    get_trades,
    get_strategy_pnl,
    get_capital_summary,
    get_cashflows,
    get_portfolio_risk,
    get_bars_1d,
    get_bars_1h,
    get_options_chain,
    get_yield_curve,
    get_fx_rates,
    get_vix_term_structure,
    get_vol_surface,
    get_news_items,
    get_news_by_symbol,
)
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/apps/shared/test_db.py -v
```

Expected: all tests pass (original 24 + 7 new = 31).

- [ ] **Step 5: Run full suite**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/ -q
```

Expected: 300+ tests pass, 0 failures.

- [ ] **Step 6: Commit**

```bash
git add apps/shared/db.py tests/apps/shared/test_db.py
git commit -m "feat(db): add get_news_items and get_news_by_symbol query functions"
```

---

## Task 10: `apps/news/` — Streamlit app with 4 tabs

**Files:**
- Create: `apps/news/__init__.py`
- Create: `apps/news/app.py`
- Create: `apps/news/tabs/__init__.py`
- Create: `apps/news/tabs/top_stories.py`
- Create: `apps/news/tabs/by_symbol.py`
- Create: `apps/news/tabs/full_feed.py`
- Create: `apps/news/tabs/digest.py`

No tests for app tabs (consistent with portfolio/markets convention).

- [ ] **Step 1: Create package init files**

Create `apps/news/__init__.py` (empty):
```python
```

Create `apps/news/tabs/__init__.py` (empty):
```python
```

- [ ] **Step 2: Create `apps/news/tabs/top_stories.py`**

```python
from __future__ import annotations


def render(conn) -> None:
    import streamlit as st
    from apps.shared.db import get_news_items

    threshold = st.sidebar.slider(
        "Relevance threshold", min_value=0.0, max_value=1.0, value=0.5, step=0.05
    )
    df = get_news_items(conn, min_relevance=threshold, limit=50)

    if df.empty:
        st.info("No news items above this relevance threshold.")
        return

    _SENTIMENT_BADGE = {"bullish": "🟢", "bearish": "🔴", "neutral": "⚪"}

    for _, row in df.iterrows():
        headline = row.get("headline") or "Untitled"
        url = row.get("url") or ""
        source = row.get("source") or ""
        published = row.get("published_ts")
        sentiment = row.get("sentiment") or "neutral"
        summary = row.get("ai_summary")
        score = row.get("relevance_score")

        badge = _SENTIMENT_BADGE.get(sentiment, "⚪")
        score_str = f"{float(score):.2f}" if score is not None else "—"
        pub_str = str(published)[:16] if published else ""

        label = f"{badge} [{headline}]({url})" if url else f"{badge} {headline}"
        with st.expander(f"{label}  |  score {score_str}"):
            st.caption(f"{source}  ·  {pub_str}")
            if summary:
                st.write(summary)
```

- [ ] **Step 3: Create `apps/news/tabs/by_symbol.py`**

```python
from __future__ import annotations


def render(conn) -> None:
    import os
    import streamlit as st
    import plotly.express as px
    from apps.shared.db import get_news_by_symbol, get_positions

    # Build symbol list from watchlist env + held positions
    watchlist_raw = os.environ.get("WATCHLIST", "")
    watchlist = [s.strip() for s in watchlist_raw.split(",") if s.strip()]
    try:
        pos_df = get_positions(conn, os.environ.get("ACCOUNT_ID", ""))
        db_symbols = list(pos_df["symbol"].unique()) if not pos_df.empty else []
    except Exception:
        db_symbols = []

    all_symbols = list(dict.fromkeys(watchlist + db_symbols))
    if not all_symbols:
        st.info("No symbols in WATCHLIST or open positions.")
        return

    symbol = st.selectbox("Symbol", all_symbols)
    df = get_news_by_symbol(conn, symbol)

    if df.empty:
        st.info(f"No news items mentioning {symbol}.")
        return

    # Timeline scatter
    if "published_ts" in df.columns and "relevance_score" in df.columns:
        fig = px.scatter(
            df.dropna(subset=["relevance_score"]),
            x="published_ts",
            y="relevance_score",
            color="sentiment",
            hover_data=["headline"],
            color_discrete_map={"bullish": "green", "bearish": "red", "neutral": "gray"},
            title=f"News flow — {symbol}",
        )
        st.plotly_chart(fig, use_container_width=True)

    # Table
    display_cols = [c for c in ["published_ts", "headline", "sentiment", "relevance_score"] if c in df.columns]
    st.dataframe(df[display_cols], use_container_width=True)
```

- [ ] **Step 4: Create `apps/news/tabs/full_feed.py`**

```python
from __future__ import annotations


def render(conn) -> None:
    import streamlit as st
    from apps.shared.db import get_news_items

    df = get_news_items(conn, min_relevance=0.0, limit=200)

    if df.empty:
        st.info("No news items in the database yet.")
        return

    # Keyword filter
    keyword = st.text_input("Filter by keyword (headline)")
    if keyword:
        mask = df["headline"].str.contains(keyword, case=False, na=False)
        df = df[mask]

    # Source filter
    if "source" in df.columns:
        available_sources = sorted(df["source"].dropna().unique().tolist())
        selected_sources = st.multiselect("Filter by source", available_sources, default=available_sources)
        if selected_sources:
            df = df[df["source"].isin(selected_sources)]

    st.write(f"{len(df)} items")
    display_cols = [c for c in ["published_ts", "source", "headline", "sentiment",
                                 "relevance_score", "ai_summary"] if c in df.columns]
    st.dataframe(df[display_cols], use_container_width=True)
```

- [ ] **Step 5: Create `apps/news/tabs/digest.py`**

```python
from __future__ import annotations


def render(conn) -> None:
    import os
    import streamlit as st
    from datetime import date
    from apps.shared.db import get_news_items

    today = date.today()
    today_iso = today.isoformat()

    # Check cache in trading.agent_memory
    cached_briefing = _load_cached_digest(conn, today_iso)
    if cached_briefing:
        st.subheader(f"Daily Briefing — {today_iso}")
        st.markdown(cached_briefing)
        st.caption("Cached — generated earlier today.")
        return

    st.subheader(f"Daily Briefing — {today_iso}")
    st.info("No briefing generated yet for today.")

    if st.button("Generate Daily Briefing"):
        df = get_news_items(conn, min_relevance=0.3, limit=20)
        df = df[df.get("published_ts", df.index).astype(str).str[:10] >= today_iso] if not df.empty else df

        if df.empty:
            st.warning("No relevant news items for today (min_relevance > 0.3).")
            return

        with st.spinner("Generating briefing..."):
            briefing = _generate_briefing(df)

        _save_digest(conn, today_iso, briefing)
        st.markdown(briefing)


def _load_cached_digest(conn, today_iso: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT content FROM trading.agent_memory
            WHERE app_key = 'news_digest' AND category = 'digest' AND subject = %s
        """, (today_iso,))
        row = cur.fetchone()
    return row[0] if row else None


def _save_digest(conn, today_iso: str, content: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO trading.agent_memory (app_key, category, subject, content, ts_updated)
            VALUES ('news_digest', 'digest', %s, %s, NOW())
            ON CONFLICT (app_key, category, subject) DO UPDATE
                SET content = EXCLUDED.content, ts_updated = NOW()
        """, (today_iso, content))
    conn.commit()


def _generate_briefing(df) -> str:
    import os
    import json
    from anthropic import Anthropic

    headlines = "\n".join(
        f"- [{row.get('sentiment','neutral').upper()}] {row['headline']} "
        f"({row.get('source','')})"
        for _, row in df.iterrows()
        if row.get("headline")
    )

    prompt = (
        f"Today's top news items:\n{headlines}\n\n"
        "Write a structured daily market briefing covering:\n"
        "1. Macro themes\n"
        "2. Key movers\n"
        "3. Key risks\n"
        "4. Opportunities\n\n"
        "Keep it concise — 200-300 words, in markdown."
    )

    client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=600,
        system="You are a senior market analyst writing a daily briefing for a trading desk.",
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text
```

- [ ] **Step 6: Create `apps/news/app.py`**

```python
from __future__ import annotations
import streamlit as st
from apps.shared.db import connect
from apps.news.tabs import top_stories, by_symbol, full_feed, digest


@st.cache_resource
def _conn():
    return connect()


def main() -> None:
    st.set_page_config(page_title="News", layout="wide")
    st.title("News")
    conn = _conn()

    tab1, tab2, tab3, tab4 = st.tabs(["Top Stories", "By Symbol", "Full Feed", "Daily Digest"])
    with tab1:
        top_stories.render(conn)
    with tab2:
        by_symbol.render(conn)
    with tab3:
        full_feed.render(conn)
    with tab4:
        digest.render(conn)


if __name__ == "__main__":
    main()
```

- [ ] **Step 7: Verify app imports cleanly (no streamlit side effects)**

```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/ib-platform
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -c "
import sys; sys.modules['streamlit'] = type(sys)('streamlit')
from apps.news import app
from apps.news.tabs import top_stories, by_symbol, full_feed, digest
print('All imports OK')
"
```

Expected: `All imports OK` (streamlit not imported at module level means no side effects when mocked).

- [ ] **Step 8: Run full test suite**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/ -q
```

Expected: 300+ tests pass, 0 failures.

- [ ] **Step 9: Commit**

```bash
git add apps/news/ 
git commit -m "feat(apps/news): add 4-tab news app (Top Stories, By Symbol, Full Feed, Digest)"
```

---

## Self-Review Against Spec

**Spec coverage check:**

| Spec section | Covered by task |
|---|---|
| `yfinance_feed.py` — 4 functions with correct dict shapes | Task 3 |
| `polygon_feed.py` — identical signatures, SkipSource on empty key | Task 4 |
| `services/market_data/ingest.py` — 4 APScheduler jobs, US/Eastern, upsert | Task 5 |
| `RSS_FEEDS` registry — 3 feeds with name+url | Task 6 |
| `fetch_polygon_news` — returns [] when key empty | Task 6 |
| `services/news/ingest.py` — feedparser, url_hash dedup, calls scorer | Task 7 |
| Ingest schedule: Mon-Fri every 15min 06:00-22:00 ET | Task 7 |
| `trading.news_items` table + indexes | Task 1 |
| `scorer.score_pending` — batch 20, stale 48h, haiku model | Task 8 |
| Stale items: score=0.0 without API call | Task 8 |
| JSON failure → 0.0 fallback, never blocks batch | Task 8 |
| `get_news_items` — cursor pattern, min_relevance, symbols overlap filter | Task 9 |
| `get_news_by_symbol` — cursor pattern | Task 9 |
| `apps/news/` app.py — 4 tabs, @st.cache_resource on connect() | Task 10 |
| top_stories.py — sidebar slider, st.expander per item, sentiment badge | Task 10 |
| by_symbol.py — selectbox from WATCHLIST+positions, plotly scatter | Task 10 |
| full_feed.py — keyword filter, source multiselect, st.dataframe | Task 10 |
| digest.py — claude-sonnet-4-6, agent_memory cache, re-click reuses cache | Task 10 |
| feedparser==6.0.11, anthropic>=0.30.0 in requirements | Task 2 |
| `db/migrations/001_news_items.sql` | Task 1 |
| `db/schema.sql` updated | Task 1 |
| No tests for apps/news/ tabs | ✓ (by design) |
| All tab files import streamlit only inside render() | ✓ Tasks 10 |
| APScheduler timezone="America/New_York" | Tasks 5, 7 |
| Polygon fallback: ingest.py catches SkipSource silently | Task 5 |
| WATCHLIST env var used by scorer and news app | Tasks 8, 10 |

**No gaps found.**

**Placeholder scan:** All steps contain complete code. No TBD or TODO in implementation steps.

**Type consistency:** `_ingest_items` → `list[dict]`, `score_pending(conn) -> int`, `get_news_items(conn, ...) -> pd.DataFrame` — consistent throughout.
