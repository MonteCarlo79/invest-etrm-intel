# IB Platform Phase 5 — Knowledge Base Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a knowledge ingestion service (`services/knowledge/`) that fetches macro/rates/research content daily via 5 connectors, stores it in `trading.kb_docs`, and extracts durable trading insights into `trading.kb_insights` via Claude Haiku.

**Architecture:** APScheduler service with two jobs: `job_ingest_docs` (Mon–Fri 06:00 ET) runs 5 connectors and upserts docs; `job_digest_docs` (Mon–Fri 06:30 ET) calls Claude Haiku on undigested docs. A standalone `extract_from_trade_outcome()` function is also built here for Phase 6 to wire up. Backend only — no app.

**Tech Stack:** Python 3.13, requests, feedparser, anthropic, apscheduler, psycopg2. No new packages.

---

## File Map

**Create:**
```
services/knowledge/__init__.py
services/knowledge/config.py
services/knowledge/base.py
services/knowledge/expert_memory.py
services/knowledge/ingest.py
services/knowledge/connectors/__init__.py
services/knowledge/connectors/fred.py
services/knowledge/connectors/fed_speeches.py
services/knowledge/connectors/treasury.py
services/knowledge/connectors/bis.py
services/knowledge/connectors/news_rss.py
db/migrations/002_kb_tables.sql
tests/services/knowledge/__init__.py
tests/services/knowledge/test_connectors.py
tests/services/knowledge/test_expert_memory.py
tests/services/knowledge/test_ingest_jobs.py
```

**Modify:** `config/.env` — add `FRED_API_KEY=` comment

---

### Task 1: Package scaffold + `config.py`

**Files:**
- Create: `services/knowledge/__init__.py`
- Create: `services/knowledge/connectors/__init__.py`
- Create: `tests/services/knowledge/__init__.py`
- Create: `services/knowledge/config.py`
- Test: `tests/services/knowledge/test_connectors.py` (partial — config test only)

- [ ] **Step 1: Write the failing test**

Create `tests/services/knowledge/__init__.py` (empty) and `tests/services/knowledge/test_connectors.py`:

```python
from __future__ import annotations


class TestConfig:
    def test_fred_series_contains_dgs10(self):
        from services.knowledge.config import FRED_SERIES
        assert "DGS10" in FRED_SERIES

    def test_rss_feeds_has_three_entries(self):
        from services.knowledge.config import RSS_FEEDS
        assert len(RSS_FEEDS) == 3

    def test_trade_outcome_min_pnl(self):
        from services.knowledge.config import TRADE_OUTCOME_MIN_PNL
        assert TRADE_OUTCOME_MIN_PNL == 50.0

    def test_digest_stale_days(self):
        from services.knowledge.config import DIGEST_STALE_DAYS
        assert DIGEST_STALE_DAYS == 30
```

- [ ] **Step 2: Run test to verify it fails**

```
cd C:/Users/dipeng.chen/OneDrive/ETRM/ib-platform
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_connectors.py::TestConfig -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'services.knowledge'`

- [ ] **Step 3: Create package init files and `config.py`**

Create `services/knowledge/__init__.py` — empty file.

Create `services/knowledge/connectors/__init__.py` — empty file.

Create `services/knowledge/config.py`:

```python
FRED_SERIES = {
    "DGS2":     "US 2Y Treasury yield",
    "DGS5":     "US 5Y Treasury yield",
    "DGS10":    "US 10Y Treasury yield",
    "DGS30":    "US 30Y Treasury yield",
    "FEDFUNDS": "Fed Funds effective rate",
    "SOFR":     "SOFR overnight rate",
    "CPIAUCSL": "CPI all items",
    "UNRATE":   "Unemployment rate",
    "GDP":      "Real GDP (quarterly)",
    "T10YIE":   "10Y breakeven inflation",
    "T10Y2Y":   "10Y-2Y Treasury spread",
}

RSS_FEEDS = [
    {"name": "Reuters", "url": "https://feeds.reuters.com/reuters/businessNews"},
    {"name": "CNBC",    "url": "https://feeds.nbcnews.com/nbcnews/public/business"},
    {"name": "FT",      "url": "https://www.ft.com/rss/home"},
]
# Defined here (not imported from services/news/sources.py) to avoid cross-service coupling.

INSIGHT_TYPES = [
    "market_regime", "price_driver", "vol_signal", "macro_risk",
    "opportunity", "strategy", "model_insight",
    "strategy_backtest", "trade_outcome",
]

TRADE_OUTCOME_MIN_PNL = 50.0   # |pnl| threshold below which extraction is skipped
DIGEST_STALE_DAYS = 30         # docs older than this with short content are skipped in digest
```

- [ ] **Step 4: Run test to verify it passes**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_connectors.py::TestConfig -v
```
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add services/knowledge/__init__.py services/knowledge/connectors/__init__.py \
        services/knowledge/config.py \
        tests/services/knowledge/__init__.py tests/services/knowledge/test_connectors.py
git commit -m "feat(knowledge): add package scaffold and config"
```

---

### Task 2: `base.py` — BaseConnector, `_parse_feedparser_date`, `upsert_doc`

**Files:**
- Create: `services/knowledge/base.py`
- Test: `tests/services/knowledge/test_connectors.py` (extend)

- [ ] **Step 1: Write the failing tests**

Add `TestBase` to `tests/services/knowledge/test_connectors.py`:

```python
import time
from datetime import timezone
from unittest.mock import MagicMock


class TestParseFeedparserDate:
    def test_returns_utc_datetime_from_struct(self):
        from services.knowledge.base import _parse_feedparser_date
        struct = time.strptime("2026-06-18 10:30:00", "%Y-%m-%d %H:%M:%S")
        entry = type("Entry", (), {"published_parsed": struct})()
        result = _parse_feedparser_date(entry)
        assert result is not None
        assert result.year == 2026
        assert result.month == 6
        assert result.day == 18
        assert result.hour == 10
        assert result.tzinfo == timezone.utc

    def test_returns_none_when_no_published_parsed(self):
        from services.knowledge.base import _parse_feedparser_date
        entry = type("Entry", (), {})()
        assert _parse_feedparser_date(entry) is None


def _mock_conn(fetchone_result=None):
    """Build a mock DB connection for upsert_doc tests."""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.fetchone.return_value = fetchone_result
    conn.cursor.return_value = cursor
    return conn, cursor


class TestUpsertDoc:
    def _make_doc(self, url="https://fred.stlouisfed.org/series/DGS10"):
        return {
            "source": "fred",
            "doc_type": "rate_series",
            "title": "US 10Y Treasury yield",
            "url": url,
            "published_date": None,
            "content": '{"observations": []}',
        }

    def test_returns_true_when_row_inserted(self):
        from services.knowledge.base import upsert_doc
        conn, cursor = _mock_conn(fetchone_result=(1,))
        result = upsert_doc(conn, self._make_doc())
        assert result is True
        conn.commit.assert_called_once()

    def test_returns_false_when_content_unchanged(self):
        from services.knowledge.base import upsert_doc
        conn, cursor = _mock_conn(fetchone_result=None)
        result = upsert_doc(conn, self._make_doc())
        assert result is False
        conn.commit.assert_called_once()

    def test_upsert_uses_conflict_url(self):
        from services.knowledge.base import upsert_doc
        conn, cursor = _mock_conn(fetchone_result=(1,))
        upsert_doc(conn, self._make_doc())
        sql = cursor.execute.call_args[0][0]
        assert "ON CONFLICT (url)" in sql
        assert "IS DISTINCT FROM" in sql
```

- [ ] **Step 2: Run tests to verify they fail**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_connectors.py::TestParseFeedparserDate tests/services/knowledge/test_connectors.py::TestUpsertDoc -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'services.knowledge.base'`

- [ ] **Step 3: Create `base.py`**

```python
from __future__ import annotations
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    source: str

    @abstractmethod
    def fetch(self, lookback_days: int = 7) -> list[dict]:
        """Returns list of doc dicts. Never raises — returns [] on any error."""
        ...


def _parse_feedparser_date(entry) -> datetime | None:
    """Return UTC datetime from feedparser entry.published_parsed, or None."""
    struct = getattr(entry, "published_parsed", None)
    if struct is None:
        return None
    return datetime(*struct[:6], tzinfo=timezone.utc)


def upsert_doc(conn, doc: dict) -> bool:
    """Insert or update a kb_doc row.

    Returns True if the row was inserted or its content changed.
    Returns False if the URL already existed with identical content.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO trading.kb_docs (source, doc_type, title, url, published_date, content)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO UPDATE SET
                source         = EXCLUDED.source,
                doc_type       = EXCLUDED.doc_type,
                title          = EXCLUDED.title,
                published_date = EXCLUDED.published_date,
                content        = EXCLUDED.content,
                fetched_at     = NOW()
            WHERE kb_docs.content IS DISTINCT FROM EXCLUDED.content
            RETURNING id
            """,
            (
                doc["source"],
                doc["doc_type"],
                doc.get("title"),
                doc["url"],
                doc.get("published_date"),
                doc["content"],
            ),
        )
        row = cur.fetchone()
    conn.commit()
    return row is not None
```

- [ ] **Step 4: Run tests to verify they pass**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_connectors.py::TestParseFeedparserDate tests/services/knowledge/test_connectors.py::TestUpsertDoc -v
```
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add services/knowledge/base.py tests/services/knowledge/test_connectors.py
git commit -m "feat(knowledge): add BaseConnector ABC, upsert_doc, _parse_feedparser_date"
```

---

### Task 3: `connectors/fred.py`

**Files:**
- Create: `services/knowledge/connectors/fred.py`
- Test: `tests/services/knowledge/test_connectors.py` (extend)

- [ ] **Step 1: Write the failing tests**

Add `TestFredConnector` to `tests/services/knowledge/test_connectors.py`:

```python
from unittest.mock import patch, MagicMock
import json


class TestFredConnector:
    def test_returns_empty_when_api_key_missing(self, monkeypatch):
        monkeypatch.delenv("FRED_API_KEY", raising=False)
        from services.knowledge.connectors.fred import FredConnector
        assert FredConnector().fetch() == []

    def test_returns_one_doc_per_series(self, monkeypatch):
        monkeypatch.setenv("FRED_API_KEY", "testkey")
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "observations": [{"date": "2026-06-18", "value": "5.25"}]
        }
        mock_resp.raise_for_status = MagicMock()
        with patch("requests.get", return_value=mock_resp):
            from services.knowledge.connectors.fred import FredConnector
            from services.knowledge.config import FRED_SERIES
            docs = FredConnector().fetch(lookback_days=1)
        assert len(docs) == len(FRED_SERIES)
        doc = docs[0]
        assert doc["source"] == "fred"
        assert doc["doc_type"] == "rate_series"
        assert doc["title"]
        assert doc["url"].startswith("https://fred.stlouisfed.org/series/")
        assert "content" in doc
        assert "observations" in json.loads(doc["content"])

    def test_returns_empty_on_network_error(self, monkeypatch):
        monkeypatch.setenv("FRED_API_KEY", "testkey")
        with patch("requests.get", side_effect=ConnectionError("network error")):
            from services.knowledge.connectors.fred import FredConnector
            docs = FredConnector().fetch()
        assert docs == []
```

- [ ] **Step 2: Run tests to verify they fail**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_connectors.py::TestFredConnector -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'services.knowledge.connectors.fred'`

- [ ] **Step 3: Create `connectors/fred.py`**

```python
from __future__ import annotations
import json
import logging
import os

import requests

from services.knowledge.base import BaseConnector
from services.knowledge.config import FRED_SERIES

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


class FredConnector(BaseConnector):
    source = "fred"

    def fetch(self, lookback_days: int = 7) -> list[dict]:
        api_key = os.environ.get("FRED_API_KEY", "")
        if not api_key:
            return []

        docs: list[dict] = []
        for series_id, description in FRED_SERIES.items():
            try:
                resp = requests.get(
                    _BASE_URL,
                    params={
                        "series_id": series_id,
                        "api_key": api_key,
                        "sort_order": "desc",
                        "limit": lookback_days,
                        "file_type": "json",
                    },
                    timeout=10,
                )
                resp.raise_for_status()
                observations = resp.json().get("observations", [])
                docs.append({
                    "source": self.source,
                    "doc_type": "rate_series",
                    "title": description,
                    "url": f"https://fred.stlouisfed.org/series/{series_id}",
                    "published_date": None,
                    "content": json.dumps(observations),
                })
            except Exception:
                logger.exception("FRED fetch failed for series %s", series_id)
        return docs
```

- [ ] **Step 4: Run tests to verify they pass**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_connectors.py::TestFredConnector -v
```
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add services/knowledge/connectors/fred.py tests/services/knowledge/test_connectors.py
git commit -m "feat(knowledge): add FredConnector"
```

---

### Task 4: `connectors/fed_speeches.py`

**Files:**
- Create: `services/knowledge/connectors/fed_speeches.py`
- Test: `tests/services/knowledge/test_connectors.py` (extend)

- [ ] **Step 1: Write the failing tests**

Add `TestFedSpeechesConnector` to `tests/services/knowledge/test_connectors.py`:

```python
import time as time_module


class TestFedSpeechesConnector:
    def _make_entry(self, title, link, summary, days_ago=1):
        import time as t
        from datetime import datetime, timezone, timedelta
        dt = datetime.now(tz=timezone.utc) - timedelta(days=days_ago)
        struct = t.strptime(dt.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        return type("Entry", (), {
            "title": title,
            "link": link,
            "summary": summary,
            "published_parsed": struct,
        })()

    def test_returns_doc_dict_shape(self):
        entry = self._make_entry("Fed Speech", "https://fed.gov/speech1", "Some content", days_ago=1)
        mock_feed = MagicMock()
        mock_feed.entries = [entry]
        with patch("feedparser.parse", return_value=mock_feed):
            from services.knowledge.connectors.fed_speeches import FedSpeechesConnector
            docs = FedSpeechesConnector().fetch(lookback_days=7)
        assert len(docs) >= 1
        doc = docs[0]
        assert doc["source"] == "fed_speeches"
        assert doc["doc_type"] in ("speech", "minutes")
        assert "title" in doc and "url" in doc and "content" in doc

    def test_skips_entries_older_than_lookback(self):
        entry = self._make_entry("Old Speech", "https://fed.gov/old", "old content", days_ago=30)
        mock_feed = MagicMock()
        mock_feed.entries = [entry]
        with patch("feedparser.parse", return_value=mock_feed):
            from services.knowledge.connectors.fed_speeches import FedSpeechesConnector
            docs = FedSpeechesConnector().fetch(lookback_days=7)
        assert docs == []

    def test_returns_empty_on_network_error(self):
        with patch("feedparser.parse", side_effect=Exception("timeout")):
            from services.knowledge.connectors.fed_speeches import FedSpeechesConnector
            docs = FedSpeechesConnector().fetch()
        assert docs == []
```

- [ ] **Step 2: Run tests to verify they fail**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_connectors.py::TestFedSpeechesConnector -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Create `connectors/fed_speeches.py`**

```python
from __future__ import annotations
import logging
from datetime import date, timedelta

import feedparser

from services.knowledge.base import BaseConnector, _parse_feedparser_date

logger = logging.getLogger(__name__)

_FEEDS = [
    ("https://www.federalreserve.gov/feeds/speeches.xml", "speech"),
    ("https://www.federalreserve.gov/feeds/press_monetary.xml", "minutes"),
]


class FedSpeechesConnector(BaseConnector):
    source = "fed_speeches"

    def fetch(self, lookback_days: int = 7) -> list[dict]:
        cutoff = date.today() - timedelta(days=lookback_days)
        docs: list[dict] = []

        for feed_url, doc_type in _FEEDS:
            try:
                parsed = feedparser.parse(feed_url)
                for entry in parsed.entries:
                    pub_dt = _parse_feedparser_date(entry)
                    if pub_dt is not None and pub_dt.date() < cutoff:
                        continue
                    docs.append({
                        "source": self.source,
                        "doc_type": doc_type,
                        "title": getattr(entry, "title", ""),
                        "url": getattr(entry, "link", ""),
                        "published_date": pub_dt.date() if pub_dt else None,
                        "content": getattr(entry, "summary", ""),
                    })
            except Exception:
                logger.exception("FedSpeeches fetch failed for %s", feed_url)

        return docs
```

- [ ] **Step 4: Run tests to verify they pass**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_connectors.py::TestFedSpeechesConnector -v
```
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add services/knowledge/connectors/fed_speeches.py tests/services/knowledge/test_connectors.py
git commit -m "feat(knowledge): add FedSpeechesConnector"
```

---

### Task 5: `connectors/treasury.py`

**Files:**
- Create: `services/knowledge/connectors/treasury.py`
- Test: `tests/services/knowledge/test_connectors.py` (extend)

- [ ] **Step 1: Write the failing tests**

Add `TestTreasuryConnector` to `tests/services/knowledge/test_connectors.py`:

```python
class TestTreasuryConnector:
    _CSV = "Date,1 Mo,3 Mo,6 Mo,1 Yr,2 Yr,5 Yr,10 Yr,30 Yr\n06/18/2026,5.25,5.20,5.10,4.90,4.50,4.20,4.10,4.30\n06/17/2026,5.24,5.19,5.09,4.89,4.49,4.19,4.09,4.29\n"

    def test_returns_one_doc_per_date(self):
        mock_resp = MagicMock()
        mock_resp.text = self._CSV
        mock_resp.raise_for_status = MagicMock()
        with patch("requests.get", return_value=mock_resp):
            from services.knowledge.connectors.treasury import TreasuryConnector
            docs = TreasuryConnector().fetch(lookback_days=7)
        assert len(docs) == 2
        doc = docs[0]
        assert doc["source"] == "treasury"
        assert doc["doc_type"] == "yield_curve"
        assert doc["url"].startswith("treasury://yield-curve/")
        assert "content" in doc

    def test_url_is_synthetic_unique_key(self):
        mock_resp = MagicMock()
        mock_resp.text = self._CSV
        mock_resp.raise_for_status = MagicMock()
        with patch("requests.get", return_value=mock_resp):
            from services.knowledge.connectors.treasury import TreasuryConnector
            docs = TreasuryConnector().fetch(lookback_days=7)
        urls = [d["url"] for d in docs]
        assert len(urls) == len(set(urls))  # all unique

    def test_returns_empty_on_network_error(self):
        with patch("requests.get", side_effect=ConnectionError("timeout")):
            from services.knowledge.connectors.treasury import TreasuryConnector
            docs = TreasuryConnector().fetch()
        assert docs == []
```

- [ ] **Step 2: Run tests to verify they fail**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_connectors.py::TestTreasuryConnector -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Create `connectors/treasury.py`**

```python
from __future__ import annotations
import csv
import io
import json
import logging
from datetime import date, datetime, timedelta

import requests

from services.knowledge.base import BaseConnector

logger = logging.getLogger(__name__)


def _parse_treasury_date(date_str: str) -> date | None:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            pass
    return None


class TreasuryConnector(BaseConnector):
    source = "treasury"

    def fetch(self, lookback_days: int = 7) -> list[dict]:
        today = date.today()
        year = today.year
        cutoff = today - timedelta(days=lookback_days)

        url = (
            f"https://home.treasury.gov/resource-center/data-chart-center/"
            f"interest-rates/daily-treasury-rates.csv/{year}/all"
            f"?type=daily_treasury_yield_curve&field_tdr_date_value={year}"
        )
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
        except Exception:
            logger.exception("Treasury fetch failed")
            return []

        docs: list[dict] = []
        reader = csv.DictReader(io.StringIO(resp.text))
        for row in reader:
            raw_date = row.get("Date", "")
            row_date = _parse_treasury_date(raw_date)
            if row_date is None or row_date < cutoff:
                continue
            docs.append({
                "source": self.source,
                "doc_type": "yield_curve",
                "title": f"Treasury yield curve {row_date.isoformat()}",
                "url": f"treasury://yield-curve/{row_date.isoformat()}",
                "published_date": row_date,
                "content": json.dumps(dict(row)),
            })
        return docs
```

- [ ] **Step 4: Run tests to verify they pass**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_connectors.py::TestTreasuryConnector -v
```
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add services/knowledge/connectors/treasury.py tests/services/knowledge/test_connectors.py
git commit -m "feat(knowledge): add TreasuryConnector"
```

---

### Task 6: `connectors/bis.py`

**Files:**
- Create: `services/knowledge/connectors/bis.py`
- Test: `tests/services/knowledge/test_connectors.py` (extend)

- [ ] **Step 1: Write the failing tests**

Add `TestBisConnector` to `tests/services/knowledge/test_connectors.py`:

```python
class TestBisConnector:
    def _make_entry(self, title, link, summary, days_ago=1):
        import time as t
        from datetime import datetime, timezone, timedelta
        dt = datetime.now(tz=timezone.utc) - timedelta(days=days_ago)
        struct = t.strptime(dt.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        return type("Entry", (), {
            "title": title, "link": link, "summary": summary,
            "published_parsed": struct,
        })()

    def test_returns_doc_dict_shape(self):
        entry = self._make_entry("BIS Paper", "https://bis.org/paper1", "Abstract", days_ago=1)
        mock_feed = MagicMock()
        mock_feed.entries = [entry]
        with patch("feedparser.parse", return_value=mock_feed):
            from services.knowledge.connectors.bis import BisConnector
            docs = BisConnector().fetch(lookback_days=7)
        assert len(docs) >= 1
        doc = docs[0]
        assert doc["source"] == "bis"
        assert doc["doc_type"] == "research_paper"
        assert "title" in doc and "url" in doc and "content" in doc

    def test_returns_empty_on_network_error(self):
        with patch("feedparser.parse", side_effect=Exception("timeout")):
            from services.knowledge.connectors.bis import BisConnector
            docs = BisConnector().fetch()
        assert docs == []
```

- [ ] **Step 2: Run tests to verify they fail**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_connectors.py::TestBisConnector -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Create `connectors/bis.py`**

```python
from __future__ import annotations
import logging
from datetime import date, timedelta

import feedparser

from services.knowledge.base import BaseConnector, _parse_feedparser_date

logger = logging.getLogger(__name__)

_FEEDS = [
    "https://www.bis.org/rss/quarterly-review.xml",
    "https://www.bis.org/rss/working-papers.xml",
]


class BisConnector(BaseConnector):
    source = "bis"

    def fetch(self, lookback_days: int = 7) -> list[dict]:
        cutoff = date.today() - timedelta(days=lookback_days)
        docs: list[dict] = []

        for feed_url in _FEEDS:
            try:
                parsed = feedparser.parse(feed_url)
                for entry in parsed.entries:
                    pub_dt = _parse_feedparser_date(entry)
                    if pub_dt is not None and pub_dt.date() < cutoff:
                        continue
                    docs.append({
                        "source": self.source,
                        "doc_type": "research_paper",
                        "title": getattr(entry, "title", ""),
                        "url": getattr(entry, "link", ""),
                        "published_date": pub_dt.date() if pub_dt else None,
                        "content": getattr(entry, "summary", ""),
                    })
            except Exception:
                logger.exception("BIS fetch failed for %s", feed_url)

        return docs
```

- [ ] **Step 4: Run tests to verify they pass**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_connectors.py::TestBisConnector -v
```
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add services/knowledge/connectors/bis.py tests/services/knowledge/test_connectors.py
git commit -m "feat(knowledge): add BisConnector"
```

---

### Task 7: `connectors/news_rss.py`

**Files:**
- Create: `services/knowledge/connectors/news_rss.py`
- Test: `tests/services/knowledge/test_connectors.py` (extend)

- [ ] **Step 1: Write the failing tests**

Add `TestNewsRssConnector` to `tests/services/knowledge/test_connectors.py`:

```python
class TestNewsRssConnector:
    def _make_entry(self, title, link, summary, days_ago=1):
        import time as t
        from datetime import datetime, timezone, timedelta
        dt = datetime.now(tz=timezone.utc) - timedelta(days=days_ago)
        struct = t.strptime(dt.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        return type("Entry", (), {
            "title": title, "link": link, "summary": summary,
            "published_parsed": struct,
        })()

    def test_returns_news_article_doc_type(self):
        entry = self._make_entry("Oil rises", "https://reuters.com/oil", "Oil up 2%", days_ago=1)
        mock_feed = MagicMock()
        mock_feed.entries = [entry]
        with patch("feedparser.parse", return_value=mock_feed):
            from services.knowledge.connectors.news_rss import NewsRssConnector
            docs = NewsRssConnector().fetch(lookback_days=7)
        assert len(docs) >= 1
        assert docs[0]["source"] == "news_rss"
        assert docs[0]["doc_type"] == "news_article"

    def test_does_not_import_from_services_news(self):
        """Cross-service import guard: news_rss must define its own feed list."""
        import ast, pathlib
        src = pathlib.Path(
            "services/knowledge/connectors/news_rss.py"
        ).read_text()
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                module = getattr(node, "module", "") or ""
                assert "services.news" not in module, \
                    "news_rss.py must not import from services.news"

    def test_returns_empty_on_network_error(self):
        with patch("feedparser.parse", side_effect=Exception("timeout")):
            from services.knowledge.connectors.news_rss import NewsRssConnector
            docs = NewsRssConnector().fetch()
        assert docs == []
```

- [ ] **Step 2: Run tests to verify they fail**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_connectors.py::TestNewsRssConnector -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Create `connectors/news_rss.py`**

```python
from __future__ import annotations
import logging
from datetime import date, timedelta

import feedparser

from services.knowledge.base import BaseConnector, _parse_feedparser_date
from services.knowledge.config import RSS_FEEDS

logger = logging.getLogger(__name__)


class NewsRssConnector(BaseConnector):
    source = "news_rss"

    def fetch(self, lookback_days: int = 7) -> list[dict]:
        cutoff = date.today() - timedelta(days=lookback_days)
        docs: list[dict] = []

        for feed_def in RSS_FEEDS:
            try:
                parsed = feedparser.parse(feed_def["url"])
                for entry in parsed.entries:
                    pub_dt = _parse_feedparser_date(entry)
                    if pub_dt is not None and pub_dt.date() < cutoff:
                        continue
                    docs.append({
                        "source": self.source,
                        "doc_type": "news_article",
                        "title": getattr(entry, "title", ""),
                        "url": getattr(entry, "link", ""),
                        "published_date": pub_dt.date() if pub_dt else None,
                        "content": getattr(entry, "summary", ""),
                    })
            except Exception:
                logger.exception("NewsRss fetch failed for %s", feed_def.get("name"))

        return docs
```

- [ ] **Step 4: Run tests to verify they pass**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_connectors.py::TestNewsRssConnector -v
```
Expected: 3 passed

- [ ] **Step 5: Run the full connector test suite**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_connectors.py -v
```
Expected: all tests pass

- [ ] **Step 6: Commit**

```bash
git add services/knowledge/connectors/news_rss.py tests/services/knowledge/test_connectors.py
git commit -m "feat(knowledge): add NewsRssConnector"
```

---

### Task 8: `expert_memory.py` — `digest_kb_docs` (Channel 2)

**Files:**
- Create: `services/knowledge/expert_memory.py`
- Create: `tests/services/knowledge/test_expert_memory.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/services/knowledge/test_expert_memory.py`:

```python
from __future__ import annotations
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


def _mock_haiku_response(insight_text="rates will stay elevated", insight_type="macro_risk", confidence="high"):
    mock_client = MagicMock()
    mock_msg = MagicMock()
    mock_msg.content[0].text = json.dumps({
        "insights": [{"insight": insight_text, "type": insight_type, "confidence": confidence}]
    })
    mock_client.messages.create.return_value = mock_msg
    return mock_client


_SAMPLE_DOC = {
    "id": 1,
    "source": "fred",
    "doc_type": "rate_series",
    "title": "US 10Y Treasury yield",
    "url": "https://fred.stlouisfed.org/series/DGS10",
    "content": "observations with enough content to pass the stale filter and process",
    "published_date": None,
    "fetched_at": datetime(2026, 6, 18, 6, 0, tzinfo=timezone.utc),
}


class TestDigestKbDocs:
    def test_undigested_doc_triggers_haiku_and_writes_insight(self):
        mock_client = _mock_haiku_response()
        conn = MagicMock()

        with patch("services.knowledge.expert_memory._fetch_undigested", return_value=[_SAMPLE_DOC]), \
             patch("services.knowledge.expert_memory._write_insight") as mock_write, \
             patch("anthropic.Anthropic", return_value=mock_client):
            from services.knowledge.expert_memory import digest_kb_docs
            count = digest_kb_docs(conn, api_key="test_key")

        assert count == 1
        mock_client.messages.create.assert_called_once()
        mock_write.assert_called_once()
        call_kwargs = mock_write.call_args[1]
        assert call_kwargs["source_doc_url"] == _SAMPLE_DOC["url"]
        assert call_kwargs["insight_type"] == "macro_risk"

    def test_no_undigested_docs_makes_no_api_call(self):
        mock_client = MagicMock()
        conn = MagicMock()

        with patch("services.knowledge.expert_memory._fetch_undigested", return_value=[]), \
             patch("anthropic.Anthropic", return_value=mock_client):
            from services.knowledge.expert_memory import digest_kb_docs
            count = digest_kb_docs(conn, api_key="test_key")

        assert count == 0
        mock_client.messages.create.assert_not_called()

    def test_stale_doc_with_short_content_is_skipped(self):
        """Doc older than DIGEST_STALE_DAYS with content < 100 chars → no API call."""
        stale_doc = {
            **_SAMPLE_DOC,
            "content": "short",
            "fetched_at": datetime(2020, 1, 1, tzinfo=timezone.utc),
        }
        mock_client = MagicMock()
        conn = MagicMock()

        with patch("services.knowledge.expert_memory._fetch_undigested", return_value=[stale_doc]), \
             patch("anthropic.Anthropic", return_value=mock_client):
            from services.knowledge.expert_memory import digest_kb_docs
            count = digest_kb_docs(conn, api_key="test_key")

        assert count == 0
        mock_client.messages.create.assert_not_called()

    def test_returns_count_of_insights_written(self):
        mock_client = MagicMock()
        mock_msg = MagicMock()
        mock_msg.content[0].text = json.dumps({
            "insights": [
                {"insight": "insight 1", "type": "macro_risk", "confidence": "high"},
                {"insight": "insight 2", "type": "vol_signal", "confidence": "medium"},
            ]
        })
        mock_client.messages.create.return_value = mock_msg
        conn = MagicMock()

        with patch("services.knowledge.expert_memory._fetch_undigested", return_value=[_SAMPLE_DOC]), \
             patch("services.knowledge.expert_memory._write_insight"), \
             patch("anthropic.Anthropic", return_value=mock_client):
            from services.knowledge.expert_memory import digest_kb_docs
            count = digest_kb_docs(conn, api_key="test_key")

        assert count == 2
```

- [ ] **Step 2: Run tests to verify they fail**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_expert_memory.py::TestDigestKbDocs -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'services.knowledge.expert_memory'`

- [ ] **Step 3: Create `expert_memory.py` with `digest_kb_docs`**

```python
from __future__ import annotations
import json
import logging
import re
from datetime import datetime, timedelta, timezone

import anthropic

from services.knowledge.config import DIGEST_STALE_DAYS

logger = logging.getLogger(__name__)

_HAIKU = "claude-haiku-4-5-20251001"
_SYSTEM_PROMPT = (
    "You are extracting durable insights for a professional trading knowledge base.\n"
    "Extract ONLY insights that are: non-obvious, validated by context, durable (relevant for\n"
    "weeks or months), and domain-specific to markets, instruments, risk, macro, vol, rates, FX,\n"
    "or specific strategies.\n\n"
    "Insight types: market_regime | price_driver | vol_signal | macro_risk |\n"
    "               opportunity | strategy | model_insight | strategy_backtest | trade_outcome\n\n"
    'Respond ONLY with valid JSON, no markdown:\n'
    '{"insights": [{"insight": "...", "type": "...", "confidence": "high|medium|low"}]}'
)


def _call_haiku(client: anthropic.Anthropic, user_content: str) -> list[dict]:
    """Call Haiku, parse JSON. Returns list of insight dicts."""
    response = client.messages.create(
        model=_HAIKU,
        max_tokens=512,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_content}],
    )
    text = response.content[0].text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    data = json.loads(text)
    return data.get("insights", [])


def _write_insight(
    conn,
    *,
    insight: str,
    insight_type: str,
    confidence: str,
    source_doc_url: str | None = None,
    source_trade_id: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO trading.kb_insights
                (insight_text, insight_type, confidence, source_doc_url, source_trade_id)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (insight, insight_type, confidence, source_doc_url, source_trade_id),
        )
    conn.commit()


def _fetch_undigested(conn, batch_size: int) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, source, doc_type, title, url, content, published_date, fetched_at
            FROM trading.kb_docs
            WHERE url NOT IN (
                SELECT DISTINCT source_doc_url
                FROM trading.kb_insights
                WHERE source_doc_url IS NOT NULL
            )
            LIMIT %s
            """,
            (batch_size,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _is_stale(doc: dict) -> bool:
    if len(doc["content"]) >= 100:
        return False
    fetched_at = doc.get("fetched_at")
    if fetched_at is None:
        return False
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=DIGEST_STALE_DAYS)
    return fetched_at < cutoff


def digest_kb_docs(conn, api_key: str, batch_size: int = 20) -> int:
    """Channel 2 — document digestion. Returns count of insights inserted."""
    docs = _fetch_undigested(conn, batch_size)
    if not docs:
        return 0

    client = anthropic.Anthropic(api_key=api_key)
    total = 0

    for doc in docs:
        if _is_stale(doc):
            continue

        user_content = (
            f"Document type: {doc['doc_type']}\n"
            f"Title: {doc.get('title', '')}\n"
            f"Content: {doc['content'][:2000]}\n\n"
            "Extract durable trading insights from this document."
        )
        try:
            insights = _call_haiku(client, user_content)
        except Exception:
            logger.exception("Haiku extraction failed for doc url=%s", doc["url"])
            continue

        for item in insights:
            try:
                _write_insight(
                    conn,
                    insight=item.get("insight", ""),
                    insight_type=item.get("type", "market_regime"),
                    confidence=item.get("confidence", "medium"),
                    source_doc_url=doc["url"],
                )
                total += 1
            except Exception:
                logger.exception("Failed to write insight for doc url=%s", doc["url"])

    return total
```

- [ ] **Step 4: Run tests to verify they pass**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_expert_memory.py::TestDigestKbDocs -v
```
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add services/knowledge/expert_memory.py tests/services/knowledge/test_expert_memory.py
git commit -m "feat(knowledge): add digest_kb_docs (Channel 2)"
```

---

### Task 9: `expert_memory.py` — `extract_from_trade_outcome` (Channel 5)

**Files:**
- Modify: `services/knowledge/expert_memory.py`
- Test: `tests/services/knowledge/test_expert_memory.py` (extend)

- [ ] **Step 1: Write the failing tests**

Add `TestExtractFromTradeOutcome` to `tests/services/knowledge/test_expert_memory.py`:

```python
class TestExtractFromTradeOutcome:
    _PNL_EXPLAIN = {"delta_pct": 60, "gamma_pct": 20, "vega_pct": 15, "theta_pct": 5}
    _MARKET_CTX = {"vix": 18.5, "spx": 5400, "regime": "risk_on"}

    def test_returns_false_without_api_call_when_pnl_below_threshold(self):
        mock_client = MagicMock()
        conn = MagicMock()
        with patch("anthropic.Anthropic", return_value=mock_client):
            from services.knowledge.expert_memory import extract_from_trade_outcome
            result = extract_from_trade_outcome(
                conn, trade_id=1, signal_source="vix_model",
                expected_direction="long", actual_pnl=30.0,
                pnl_explain=self._PNL_EXPLAIN, market_context=self._MARKET_CTX,
                api_key="test_key",
            )
        assert result is False
        mock_client.messages.create.assert_not_called()

    def test_negative_pnl_below_threshold_returns_false(self):
        conn = MagicMock()
        with patch("anthropic.Anthropic") as mock_cls:
            from services.knowledge.expert_memory import extract_from_trade_outcome
            result = extract_from_trade_outcome(
                conn, trade_id=2, signal_source="vix_model",
                expected_direction="short", actual_pnl=-30.0,
                pnl_explain=self._PNL_EXPLAIN, market_context=self._MARKET_CTX,
                api_key="test_key",
            )
        assert result is False
        mock_cls.assert_not_called()

    def test_writes_insight_and_returns_true_on_valid_pnl(self):
        mock_client = _mock_haiku_response("delta dominated — signal captured move well", "trade_outcome", "high")
        conn = MagicMock()

        with patch("services.knowledge.expert_memory._write_insight") as mock_write, \
             patch("anthropic.Anthropic", return_value=mock_client):
            from services.knowledge.expert_memory import extract_from_trade_outcome
            result = extract_from_trade_outcome(
                conn, trade_id=42, signal_source="vix_model",
                expected_direction="long", actual_pnl=250.0,
                pnl_explain=self._PNL_EXPLAIN, market_context=self._MARKET_CTX,
                api_key="test_key",
            )

        assert result is True
        mock_client.messages.create.assert_called_once()
        mock_write.assert_called_once()
        call_kwargs = mock_write.call_args[1]
        assert call_kwargs["insight_type"] == "trade_outcome"
        assert call_kwargs["source_trade_id"] == "42"
        assert call_kwargs["source_doc_url"] is None

    def test_returns_false_on_api_error(self):
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = Exception("API error")
        conn = MagicMock()

        with patch("anthropic.Anthropic", return_value=mock_client):
            from services.knowledge.expert_memory import extract_from_trade_outcome
            result = extract_from_trade_outcome(
                conn, trade_id=99, signal_source="vix_model",
                expected_direction="long", actual_pnl=500.0,
                pnl_explain=self._PNL_EXPLAIN, market_context=self._MARKET_CTX,
                api_key="test_key",
            )
        assert result is False
```

- [ ] **Step 2: Run tests to verify they fail**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_expert_memory.py::TestExtractFromTradeOutcome -v
```
Expected: FAIL — `ImportError: cannot import name 'extract_from_trade_outcome'`

- [ ] **Step 3: Add `extract_from_trade_outcome` to `expert_memory.py`**

Append to the end of `services/knowledge/expert_memory.py`:

```python
from services.knowledge.config import TRADE_OUTCOME_MIN_PNL


def extract_from_trade_outcome(
    conn,
    trade_id: int | str,
    signal_source: str,
    expected_direction: str,
    actual_pnl: float,
    pnl_explain: dict,
    market_context: dict,
    api_key: str,
) -> bool:
    """Channel 5 — trade outcome learning.

    Returns True if an insight was extracted and written.
    Returns False immediately if |pnl| < TRADE_OUTCOME_MIN_PNL.
    Never raises.
    """
    if abs(actual_pnl) < TRADE_OUTCOME_MIN_PNL:
        return False

    delta_pct = pnl_explain.get("delta_pct", 0)
    gamma_pct = pnl_explain.get("gamma_pct", 0)
    vega_pct = pnl_explain.get("vega_pct", 0)
    theta_pct = pnl_explain.get("theta_pct", 0)

    user_content = (
        f"Trade closed. Signal source: {signal_source}. "
        f"Expected direction: {expected_direction}.\n"
        f"Actual P&L: ${actual_pnl:.2f}.\n"
        f"P&L attribution: delta={delta_pct:.0f}%, gamma={gamma_pct:.0f}%, "
        f"vega={vega_pct:.0f}%, theta={theta_pct:.0f}%.\n"
        f"Market context at entry: {market_context}.\n"
        "Extract 1-2 durable lessons about what the model got right or wrong."
    )

    client = anthropic.Anthropic(api_key=api_key)
    try:
        insights = _call_haiku(client, user_content)
    except Exception:
        logger.exception("extract_from_trade_outcome failed for trade_id=%s", trade_id)
        return False

    for item in insights:
        try:
            _write_insight(
                conn,
                insight=item.get("insight", ""),
                insight_type="trade_outcome",
                confidence=item.get("confidence", "medium"),
                source_trade_id=str(trade_id),
            )
        except Exception:
            logger.exception("Failed to write trade outcome insight for trade_id=%s", trade_id)
            return False

    return True
```

Note: move the `from services.knowledge.config import TRADE_OUTCOME_MIN_PNL` import to the top of the file alongside the existing `DIGEST_STALE_DAYS` import.

- [ ] **Step 4: Fix the import at the top of `expert_memory.py`**

The existing import line reads:
```python
from services.knowledge.config import DIGEST_STALE_DAYS
```

Change it to:
```python
from services.knowledge.config import DIGEST_STALE_DAYS, TRADE_OUTCOME_MIN_PNL
```

Then remove the duplicate import added at the bottom of the function block.

- [ ] **Step 5: Run tests to verify they pass**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_expert_memory.py -v
```
Expected: all tests pass

- [ ] **Step 6: Commit**

```bash
git add services/knowledge/expert_memory.py tests/services/knowledge/test_expert_memory.py
git commit -m "feat(knowledge): add extract_from_trade_outcome (Channel 5)"
```

---

### Task 10: `ingest.py` + DB migration

**Files:**
- Create: `services/knowledge/ingest.py`
- Create: `db/migrations/002_kb_tables.sql`
- Create: `tests/services/knowledge/test_ingest_jobs.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/services/knowledge/test_ingest_jobs.py`:

```python
from __future__ import annotations
from apscheduler.triggers.cron import CronTrigger


class TestKnowledgeScheduler:
    def setup_method(self):
        from services.knowledge.ingest import build_scheduler
        self.sched = build_scheduler()
        self.jobs = {job.id: job for job in self.sched.get_jobs()}

    def test_exactly_two_jobs_registered(self):
        assert set(self.jobs) == {"ingest_docs", "digest_docs"}

    def test_scheduler_timezone_is_eastern(self):
        tz_str = str(self.sched.timezone)
        assert "New_York" in tz_str or "Eastern" in tz_str

    def test_ingest_docs_is_cron_trigger(self):
        assert isinstance(self.jobs["ingest_docs"].trigger, CronTrigger)

    def test_digest_docs_is_cron_trigger(self):
        assert isinstance(self.jobs["digest_docs"].trigger, CronTrigger)
```

- [ ] **Step 2: Run tests to verify they fail**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_ingest_jobs.py -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'services.knowledge.ingest'`

- [ ] **Step 3: Create `ingest.py`**

```python
from __future__ import annotations
import logging
import os

import psycopg2
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from services.knowledge.base import upsert_doc
from services.knowledge.connectors.fred import FredConnector
from services.knowledge.connectors.fed_speeches import FedSpeechesConnector
from services.knowledge.connectors.treasury import TreasuryConnector
from services.knowledge.connectors.bis import BisConnector
from services.knowledge.connectors.news_rss import NewsRssConnector
from services.knowledge.expert_memory import digest_kb_docs

logger = logging.getLogger(__name__)

_CONNECTORS = [
    FredConnector(),
    FedSpeechesConnector(),
    TreasuryConnector(),
    BisConnector(),
    NewsRssConnector(),
]


def job_ingest_docs() -> None:
    conn = None
    try:
        conn = psycopg2.connect(os.environ["PGURL"])
        for connector in _CONNECTORS:
            try:
                docs = connector.fetch()
                for doc in docs:
                    upsert_doc(conn, doc)
                logger.info("Ingested %d docs from %s", len(docs), connector.source)
            except Exception:
                logger.exception("Connector %s failed", connector.source)
    except Exception:
        logger.exception("job_ingest_docs failed")
    finally:
        if conn:
            conn.close()


def job_digest_docs() -> None:
    conn = None
    try:
        conn = psycopg2.connect(os.environ["PGURL"])
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        count = digest_kb_docs(conn, api_key=api_key, batch_size=20)
        logger.info("Digested %d insights", count)
    except Exception:
        logger.exception("job_digest_docs failed")
    finally:
        if conn:
            conn.close()


def build_scheduler() -> BlockingScheduler:
    tz = "America/New_York"
    sched = BlockingScheduler(timezone=tz)
    sched.add_job(
        job_ingest_docs,
        CronTrigger(day_of_week="mon-fri", hour=6, minute=0, timezone=tz),
        id="ingest_docs",
        name="Ingest KB docs",
    )
    sched.add_job(
        job_digest_docs,
        CronTrigger(day_of_week="mon-fri", hour=6, minute=30, timezone=tz),
        id="digest_docs",
        name="Digest KB docs",
    )
    return sched


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    build_scheduler().start()
```

- [ ] **Step 4: Run tests to verify they pass**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/services/knowledge/test_ingest_jobs.py -v
```
Expected: 4 passed

- [ ] **Step 5: Create the DB migration file**

Create `db/migrations/002_kb_tables.sql` — extracts `kb_docs`, `kb_insights`, and `kb_briefings` DDL from `db/schema.sql`. Run once to ensure tables exist on any target DB.

```sql
-- 002_kb_tables.sql
-- Extracts kb_docs, kb_insights, kb_briefings from schema.sql into a standalone file.
-- Run once: psql $PGURL -f db/migrations/002_kb_tables.sql

CREATE TABLE IF NOT EXISTS trading.kb_docs (
    id              BIGSERIAL PRIMARY KEY,
    source          TEXT NOT NULL,
    doc_type        TEXT NOT NULL,
    title           TEXT,
    url             TEXT UNIQUE,
    published_date  DATE,
    content         TEXT NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    search_vector   TSVECTOR GENERATED ALWAYS AS (
                        to_tsvector('english',
                            coalesce(title, '') || ' ' || left(content, 100000))
                    ) STORED
);
CREATE INDEX IF NOT EXISTS idx_kb_docs_fts ON trading.kb_docs USING GIN(search_vector);

CREATE TABLE IF NOT EXISTS trading.kb_insights (
    id                  BIGSERIAL PRIMARY KEY,
    insight_text        TEXT NOT NULL,
    insight_type        TEXT NOT NULL,
    confidence          TEXT NOT NULL DEFAULT 'medium',
    source_session      TEXT,
    source_doc_url      TEXT,
    source_model_run    TEXT,
    source_backtest_id  TEXT,
    source_trade_id     TEXT,
    validated_at        TIMESTAMPTZ,
    active              BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_kb_insights_active ON trading.kb_insights(active, created_at DESC);

CREATE TABLE IF NOT EXISTS trading.kb_briefings (
    id                 BIGSERIAL PRIMARY KEY,
    briefing_date      DATE NOT NULL,
    market_section     TEXT NOT NULL,
    content            TEXT NOT NULL,
    model_outputs_json JSONB,
    generated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (briefing_date, market_section)
);
```

- [ ] **Step 6: Run the full test suite**

```
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/ -q
```
Expected: all existing tests pass plus new tests. Count should be 337 + new knowledge tests.

- [ ] **Step 7: Commit**

```bash
git add services/knowledge/ingest.py db/migrations/002_kb_tables.sql \
        tests/services/knowledge/test_ingest_jobs.py
git commit -m "feat(knowledge): add ingest scheduler and DB migration"
```

---

## Self-Review

**Spec coverage check:**
- ✅ Section 1 (file structure): all files listed in Tasks 1–10
- ✅ Section 2 (config.py): Task 1
- ✅ Section 3 (base.py): Task 2
- ✅ Section 4 (connectors): Tasks 3–7
- ✅ Section 5 (expert_memory channels 2+5): Tasks 8–9
- ✅ Section 6 (ingest.py scheduler, 2 jobs, Mon–Fri 06:00/06:30 ET): Task 10
- ✅ Section 7 (DB migration): Task 10
- ✅ Section 8 (testing scope): all three test files covered

**Placeholder scan:** none found — all steps contain actual code.

**Type consistency:**
- `upsert_doc(conn, doc: dict) -> bool` — used in Task 2 tests and Task 10 ingest.py ✅
- `digest_kb_docs(conn, api_key, batch_size=20) -> int` — consistent across Task 8 and Task 10 ✅
- `extract_from_trade_outcome(conn, trade_id, signal_source, expected_direction, actual_pnl, pnl_explain, market_context, api_key) -> bool` — consistent across Task 9 ✅
- `_write_insight(conn, *, insight, insight_type, confidence, source_doc_url=None, source_trade_id=None)` — keyword-only args, consistent in Tasks 8 and 9 ✅
- `kb_insights.insight_text` (not `insight`) — used correctly in all INSERT statements ✅
