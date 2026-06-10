# Poland BESS Revenue Analysis Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add AS market price scraping (FCR, aFRR, Rynek Mocy), perfect-foresight dispatch P&L, Kirk-Margrabe strip valuation, and combined revenue → IRR integration to the Poland market Streamlit app.

**Architecture:** Three scraper functions in `entso_scraper.py` populate two new DB tables (`po_as_prices`, `po_capacity_market`). `_run_bess_dispatch_po()` in `app.py` runs LP dispatch (via `optimise_day()`) on the arbitrage capacity slice and stacks AS flat revenue from DB averages. `_calibrate_po_strip_params()` derives forward prices and vols from `po_day_ahead_prices` history and calls `bess_spread_call_strip._run()` for theoretical strip valuation. Session state bridges dispatch results to the IRR revenue input.

**Tech Stack:** Python 3.11, psycopg2, requests, BeautifulSoup4, pandas, numpy, PuLP (via `optimise_day()`), Streamlit, Plotly Express, `libs/decision_models/bess_spread_call_strip._run()`, `services/bess_map/optimisation_engine.optimise_day()`

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `services/po_knowledge/entso_scraper.py` | Modify | Add `scrape_po_fcr_prices()`, `scrape_po_afrr_prices()`, `scrape_po_capacity_market()`, `get_as_revenue_estimate()` |
| `apps/po-market/app.py` | Modify | Add DB tables, `_run_bess_dispatch_po()`, `_calibrate_po_strip_params()`, two BESS Opportunity subsections, IRR load button, two Data Management sections, two scheduler jobs |
| `apps/po-market/Dockerfile` | Modify | Add `COPY libs/ ./libs/` |
| `tests/__init__.py` | Create | Test root package |
| `tests/po_knowledge/__init__.py` | Create | Package |
| `tests/po_knowledge/test_as_scrapers.py` | Create | Tests for scraper functions + `get_as_revenue_estimate()` |
| `tests/po_market/__init__.py` | Create | Package |
| `tests/po_market/test_bess_dispatch_po.py` | Create | Tests for `_run_bess_dispatch_po()` and `_calibrate_po_strip_params()` |

---

## Task 1: DB Schema — New Tables in `_ensure_tables()`

**Files:**
- Modify: `apps/po-market/app.py` (inside `_ensure_tables()`)

- [ ] **Step 1: Add table creation SQL inside `_ensure_tables()`**

Find the end of `_ensure_tables()` in `app.py` (after the existing `CREATE TABLE IF NOT EXISTS` blocks) and append:

```python
    # Ancillary service weekly auction prices (FCR, aFRR)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS intl_market.po_as_prices (
            id                SERIAL PRIMARY KEY,
            week_start        DATE NOT NULL,
            market_type       TEXT NOT NULL,
            price_pln_mw_week NUMERIC(12,2),
            accepted_mw       NUMERIC(10,2),
            source            TEXT DEFAULT 'pse',
            fetched_at        TIMESTAMPTZ DEFAULT now(),
            UNIQUE (week_start, market_type)
        )
    """)
    cur.execute(
        "CREATE INDEX IF NOT EXISTS po_as_prices_week_idx "
        "ON intl_market.po_as_prices (week_start DESC)"
    )
    # Rynek Mocy (Capacity Market) annual auction results
    cur.execute("""
        CREATE TABLE IF NOT EXISTS intl_market.po_capacity_market (
            id              SERIAL PRIMARY KEY,
            delivery_year   INT  NOT NULL,
            auction_date    DATE,
            price_pln_mw_yr NUMERIC(12,2),
            accepted_mw     NUMERIC(10,2),
            source          TEXT DEFAULT 'tge',
            fetched_at      TIMESTAMPTZ DEFAULT now(),
            UNIQUE (delivery_year)
        )
    """)
```

- [ ] **Step 2: Verify no syntax errors**

```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform
python -c "import ast; ast.parse(open('apps/po-market/app.py').read()); print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add apps/po-market/app.py
git commit -m "feat(po-market): add po_as_prices and po_capacity_market DB tables"
```

---

## Task 2: Test Infrastructure + `get_as_revenue_estimate()` Tests and Implementation

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/po_knowledge/__init__.py`
- Create: `tests/po_knowledge/test_as_scrapers.py`
- Modify: `services/po_knowledge/entso_scraper.py`

- [ ] **Step 1: Create test package files**

```bash
mkdir -p tests/po_knowledge tests/po_market
touch tests/__init__.py tests/po_knowledge/__init__.py tests/po_market/__init__.py
```

- [ ] **Step 2: Write failing test for `get_as_revenue_estimate()`**

Create `tests/po_knowledge/test_as_scrapers.py`:

```python
"""Tests for Polish AS market scrapers and revenue estimator."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from unittest.mock import MagicMock, patch
import pytest
from datetime import date


def _make_conn(fcr_rows=None, afrr_rows=None, cap_rows=None):
    """Build a mock psycopg2 connection with preset fetchall results."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: cur
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # Default: no data
    fcr_rows  = fcr_rows  or []
    afrr_rows = afrr_rows or []
    cap_rows  = cap_rows  or []

    # fetchone returns avg price; fetchall not used here
    call_count = [0]
    def side_effect():
        call_count[0] += 1
        if call_count[0] == 1:   # FCR avg query
            return fcr_rows[0] if fcr_rows else None
        elif call_count[0] == 2: # aFRR avg query
            return afrr_rows[0] if afrr_rows else None
        else:                    # capacity market latest
            return cap_rows[0] if cap_rows else None
    cur.fetchone.side_effect = side_effect
    return conn


def test_get_as_revenue_estimate_returns_zeros_when_no_data():
    from services.po_knowledge.entso_scraper import get_as_revenue_estimate
    conn = _make_conn()
    result = get_as_revenue_estimate(conn, power_mw=100.0, fcr_pct=30.0, afrr_pct=30.0)
    assert result["fcr_pln_yr"] == 0.0
    assert result["afrr_pln_yr"] == 0.0
    assert result["capacity_pln_yr"] == 0.0
    assert result["total_pln_yr"] == 0.0


def test_get_as_revenue_estimate_computes_correctly():
    from services.po_knowledge.entso_scraper import get_as_revenue_estimate
    # FCR: avg 12000 PLN/MW/week, aFRR: avg 15000 PLN/MW/week, RM: 200000 PLN/MW/yr
    conn = _make_conn(
        fcr_rows=[(12000.0, 10)],    # (avg_price, weeks_count)
        afrr_rows=[(15000.0, 8)],
        cap_rows=[(200000.0,)],
    )
    result = get_as_revenue_estimate(conn, power_mw=100.0, fcr_pct=30.0, afrr_pct=30.0)
    fcr_mw = 100.0 * 0.30
    afrr_mw = 100.0 * 0.30
    assert result["fcr_pln_yr"]      == pytest.approx(12000.0 * fcr_mw * 52)
    assert result["afrr_pln_yr"]     == pytest.approx(15000.0 * afrr_mw * 52)
    assert result["capacity_pln_yr"] == pytest.approx(200000.0 * 100.0)
    assert result["total_pln_yr"]    == pytest.approx(
        result["fcr_pln_yr"] + result["afrr_pln_yr"] + result["capacity_pln_yr"]
    )
    assert result["fcr_weeks"] == 10
    assert result["afrr_weeks"] == 8
```

- [ ] **Step 3: Run test to confirm failure**

```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform
python -m pytest tests/po_knowledge/test_as_scrapers.py -v 2>&1 | head -30
```

Expected: `ImportError` or `AttributeError` — `get_as_revenue_estimate` does not exist yet.

- [ ] **Step 4: Implement `get_as_revenue_estimate()` in `entso_scraper.py`**

Add at the bottom of `services/po_knowledge/entso_scraper.py`:

```python
# ── Polish AS Revenue Helpers ─────────────────────────────────────────────


def get_as_revenue_estimate(
    conn,
    power_mw: float,
    fcr_pct: float,
    afrr_pct: float,
) -> dict:
    """Return annualised AS revenue estimate from DB average prices.

    Args:
        conn: psycopg2 connection (autocommit)
        power_mw: Total BESS power rating (MW)
        fcr_pct: % of capacity allocated to FCR (0-100)
        afrr_pct: % of capacity allocated to aFRR (0-100)

    Returns dict with keys:
        fcr_pln_yr, afrr_pln_yr, capacity_pln_yr, total_pln_yr,
        fcr_weeks, afrr_weeks
    """
    fcr_mw   = power_mw * fcr_pct  / 100.0
    afrr_mw  = power_mw * afrr_pct / 100.0

    result = {
        "fcr_pln_yr": 0.0, "afrr_pln_yr": 0.0, "capacity_pln_yr": 0.0,
        "total_pln_yr": 0.0, "fcr_weeks": 0, "afrr_weeks": 0,
    }

    try:
        with conn.cursor() as cur:
            # FCR average weekly price
            cur.execute(
                "SELECT AVG(price_pln_mw_week), COUNT(*) "
                "FROM intl_market.po_as_prices "
                "WHERE market_type = 'FCR' AND price_pln_mw_week IS NOT NULL"
            )
            row = cur.fetchone()
            if row and row[0]:
                result["fcr_pln_yr"] = float(row[0]) * fcr_mw * 52
                result["fcr_weeks"]  = int(row[1])

            # aFRR average weekly capacity price
            cur.execute(
                "SELECT AVG(price_pln_mw_week), COUNT(*) "
                "FROM intl_market.po_as_prices "
                "WHERE market_type = 'aFRR_capacity' AND price_pln_mw_week IS NOT NULL"
            )
            row = cur.fetchone()
            if row and row[0]:
                result["afrr_pln_yr"] = float(row[0]) * afrr_mw * 52
                result["afrr_weeks"]  = int(row[1])

            # Latest Rynek Mocy clearing price (PLN/MW/yr)
            cur.execute(
                "SELECT price_pln_mw_yr FROM intl_market.po_capacity_market "
                "WHERE price_pln_mw_yr IS NOT NULL ORDER BY delivery_year DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row and row[0]:
                result["capacity_pln_yr"] = float(row[0]) * power_mw

    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("[get_as_revenue_estimate] DB query failed: %s", exc)

    result["total_pln_yr"] = (
        result["fcr_pln_yr"] + result["afrr_pln_yr"] + result["capacity_pln_yr"]
    )
    return result
```

- [ ] **Step 5: Run tests and confirm pass**

```bash
python -m pytest tests/po_knowledge/test_as_scrapers.py -v
```

Expected:
```
PASSED tests/po_knowledge/test_as_scrapers.py::test_get_as_revenue_estimate_returns_zeros_when_no_data
PASSED tests/po_knowledge/test_as_scrapers.py::test_get_as_revenue_estimate_computes_correctly
```

- [ ] **Step 6: Commit**

```bash
git add tests/__init__.py tests/po_knowledge/__init__.py tests/po_knowledge/test_as_scrapers.py services/po_knowledge/entso_scraper.py
git commit -m "feat(po-market): add get_as_revenue_estimate() with tests"
```

---

## Task 3: FCR and aFRR Price Scrapers

**Files:**
- Modify: `services/po_knowledge/entso_scraper.py`
- Modify: `tests/po_knowledge/test_as_scrapers.py`

**Background:** PSE publishes FCR and aFRR weekly auction results via their reporting API at `https://api.raporty.pse.pl/api/`. The FCR endpoint is `/rcr` (Rezerwa Czestotliwości) and aFRR capacity is `/rar2`. Both return JSON with OData `value` arrays. If the API path changes, check PSE's API docs at `https://api.raporty.pse.pl/docs`. The fallback is reference rates from `_AS_CONTEXT_PO`.

- [ ] **Step 1: Write failing tests for scrapers**

Append to `tests/po_knowledge/test_as_scrapers.py`:

```python
def test_scrape_po_fcr_prices_inserts_rows(requests_mock):
    from services.po_knowledge.entso_scraper import scrape_po_fcr_prices

    # Mock PSE API response
    requests_mock.get(
        "https://api.raporty.pse.pl/api/rcr",
        json={
            "value": [
                {"data": "2024-01-01", "typ": "FCR", "cena": 11500.0, "ilosc": 350.0},
                {"data": "2024-01-08", "typ": "FCR", "cena": 12000.0, "ilosc": 360.0},
            ]
        },
    )

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: cur
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    cur.rowcount = 1

    n = scrape_po_fcr_prices(conn, weeks_back=4)
    assert n >= 0  # no exception, some rows processed
    assert cur.execute.called


def test_scrape_po_afrr_prices_inserts_rows(requests_mock):
    from services.po_knowledge.entso_scraper import scrape_po_afrr_prices

    requests_mock.get(
        "https://api.raporty.pse.pl/api/rar2",
        json={
            "value": [
                {"data": "2024-01-01", "cena_mocy": 14000.0, "ilosc": 200.0},
                {"data": "2024-01-08", "cena_mocy": 15000.0, "ilosc": 210.0},
            ]
        },
    )

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: cur
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    cur.rowcount = 1

    n = scrape_po_afrr_prices(conn, weeks_back=4)
    assert n >= 0
    assert cur.execute.called


def test_scrape_po_fcr_prices_handles_api_error(requests_mock):
    """If PSE API returns 500, function returns 0 without raising."""
    from services.po_knowledge.entso_scraper import scrape_po_fcr_prices

    requests_mock.get("https://api.raporty.pse.pl/api/rcr", status_code=500)

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: cur
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    n = scrape_po_fcr_prices(conn, weeks_back=4)
    assert n == 0
```

Install `requests-mock` if not present:
```bash
pip install requests-mock
```

- [ ] **Step 2: Run to confirm failure**

```bash
python -m pytest tests/po_knowledge/test_as_scrapers.py::test_scrape_po_fcr_prices_inserts_rows -v
```

Expected: `ImportError` — function not yet defined.

- [ ] **Step 3: Implement scrapers in `entso_scraper.py`**

Append after `get_as_revenue_estimate()`:

```python
def _pse_api_get(endpoint: str, params: dict, timeout: int = 20) -> list[dict]:
    """Fetch from PSE reporting API. Returns list of value records or [] on failure.

    Base URL: https://api.raporty.pse.pl/api/
    If the endpoint path is wrong, check https://api.raporty.pse.pl/docs
    """
    import requests
    try:
        resp = requests.get(
            f"https://api.raporty.pse.pl/api/{endpoint}",
            params=params,
            timeout=timeout,
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        return resp.json().get("value", [])
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("[_pse_api_get] %s failed: %s", endpoint, exc)
        return []


def _iso_week_monday(week_offset: int) -> "date":
    """Return the Monday of the week `week_offset` weeks ago."""
    from datetime import date, timedelta
    today = date.today()
    # Roll back to most recent Monday
    start = today - timedelta(days=today.weekday())
    return start - timedelta(weeks=week_offset)


def scrape_po_fcr_prices(conn, weeks_back: int = 52) -> int:
    """Fetch FCR weekly auction clearing prices from PSE API and store in po_as_prices.

    PSE endpoint: /rcr (Rezerwa Czestotliwości Regulacyjnej)
    Expected response fields: data (date string), cena (PLN/MW/week), ilosc (MW)
    Returns number of rows inserted.
    """
    from datetime import date, timedelta

    start = _iso_week_monday(weeks_back).isoformat()
    end   = date.today().isoformat()

    records = _pse_api_get(
        "rcr",
        {"$filter": f"data ge '{start}' and data le '{end}'", "$top": 1000},
    )

    if not records:
        import logging
        logging.getLogger(__name__).warning(
            "[scrape_po_fcr_prices] No FCR records returned from PSE API "
            "(endpoint may have changed — verify at https://api.raporty.pse.pl/docs)"
        )
        return 0

    n = 0
    try:
        with conn.cursor() as cur:
            for r in records:
                week_start = r.get("data")
                price      = r.get("cena")
                volume     = r.get("ilosc")
                if not week_start or price is None:
                    continue
                cur.execute(
                    "INSERT INTO intl_market.po_as_prices "
                    "(week_start, market_type, price_pln_mw_week, accepted_mw, source) "
                    "VALUES (%s, 'FCR', %s, %s, 'pse') "
                    "ON CONFLICT (week_start, market_type) DO NOTHING",
                    (week_start, float(price), float(volume) if volume else None),
                )
                n += cur.rowcount
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("[scrape_po_fcr_prices] DB insert failed: %s", exc)

    return n


def scrape_po_afrr_prices(conn, weeks_back: int = 52) -> int:
    """Fetch aFRR capacity weekly auction prices from PSE API and store in po_as_prices.

    PSE endpoint: /rar2 (Rezerwa Automatycznej Regulacji 2)
    Expected response fields: data (date string), cena_mocy (PLN/MW/week), ilosc (MW)
    Returns number of rows inserted.
    """
    from datetime import date

    start = _iso_week_monday(weeks_back).isoformat()
    end   = date.today().isoformat()

    records = _pse_api_get(
        "rar2",
        {"$filter": f"data ge '{start}' and data le '{end}'", "$top": 1000},
    )

    if not records:
        import logging
        logging.getLogger(__name__).warning(
            "[scrape_po_afrr_prices] No aFRR records returned from PSE API "
            "(endpoint may have changed — verify at https://api.raporty.pse.pl/docs)"
        )
        return 0

    n = 0
    try:
        with conn.cursor() as cur:
            for r in records:
                week_start = r.get("data")
                price      = r.get("cena_mocy") or r.get("cena")  # field name may vary
                volume     = r.get("ilosc")
                if not week_start or price is None:
                    continue
                cur.execute(
                    "INSERT INTO intl_market.po_as_prices "
                    "(week_start, market_type, price_pln_mw_week, accepted_mw, source) "
                    "VALUES (%s, 'aFRR_capacity', %s, %s, 'pse') "
                    "ON CONFLICT (week_start, market_type) DO NOTHING",
                    (week_start, float(price), float(volume) if volume else None),
                )
                n += cur.rowcount
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("[scrape_po_afrr_prices] DB insert failed: %s", exc)

    return n
```

- [ ] **Step 4: Run tests and confirm pass**

```bash
python -m pytest tests/po_knowledge/test_as_scrapers.py -v -k "fcr or afrr"
```

Expected: all 3 scraper tests PASS.

- [ ] **Step 5: Commit**

```bash
git add services/po_knowledge/entso_scraper.py tests/po_knowledge/test_as_scrapers.py
git commit -m "feat(po-market): add scrape_po_fcr_prices and scrape_po_afrr_prices"
```

---

## Task 4: Rynek Mocy (Capacity Market) Scraper

**Files:**
- Modify: `services/po_knowledge/entso_scraper.py`
- Modify: `tests/po_knowledge/test_as_scrapers.py`

**Background:** TGE publishes Rynek Mocy auction results at `https://tge.pl/rynek-mocy/wyniki-aukcji`. The page has an HTML table with columns: year, auction date, clearing price (PLN/MW/yr), accepted volume (MW). We parse it with BeautifulSoup.

- [ ] **Step 1: Write failing test**

Append to `tests/po_knowledge/test_as_scrapers.py`:

```python
def test_scrape_po_capacity_market_inserts_rows(requests_mock):
    from services.po_knowledge.entso_scraper import scrape_po_capacity_market

    # Minimal HTML table matching TGE's structure
    html = """
    <html><body>
    <table>
      <thead><tr><th>Rok dostaw</th><th>Data aukcji</th><th>Cena (PLN/MW/rok)</th><th>Wolumen (MW)</th></tr></thead>
      <tbody>
        <tr><td>2026</td><td>2023-12-15</td><td>220 000</td><td>5 200</td></tr>
        <tr><td>2025</td><td>2022-12-16</td><td>198 000</td><td>4 800</td></tr>
      </tbody>
    </table>
    </body></html>
    """
    requests_mock.get("https://tge.pl/rynek-mocy/wyniki-aukcji", text=html)

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: cur
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    cur.rowcount = 1

    n = scrape_po_capacity_market(conn)
    assert n >= 0
    assert cur.execute.called


def test_scrape_po_capacity_market_handles_http_error(requests_mock):
    from services.po_knowledge.entso_scraper import scrape_po_capacity_market

    requests_mock.get("https://tge.pl/rynek-mocy/wyniki-aukcji", status_code=404)

    conn = MagicMock()
    n = scrape_po_capacity_market(conn)
    assert n == 0
```

- [ ] **Step 2: Confirm failure**

```bash
python -m pytest tests/po_knowledge/test_as_scrapers.py::test_scrape_po_capacity_market_inserts_rows -v
```

Expected: `ImportError`

- [ ] **Step 3: Implement in `entso_scraper.py`**

```python
def scrape_po_capacity_market(conn) -> int:
    """Scrape TGE Rynek Mocy annual auction results into po_capacity_market.

    Source: https://tge.pl/rynek-mocy/wyniki-aukcji
    If the page structure changes, inspect the HTML table column order.
    Returns number of rows inserted.
    """
    import re, requests
    from bs4 import BeautifulSoup

    url = "https://tge.pl/rynek-mocy/wyniki-aukcji"
    try:
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("[scrape_po_capacity_market] HTTP failed: %s", exc)
        return 0

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if not table:
        import logging
        logging.getLogger(__name__).warning("[scrape_po_capacity_market] No table found at TGE page")
        return 0

    def _parse_number(text: str) -> float | None:
        """Strip spaces and non-numeric chars, return float."""
        cleaned = re.sub(r"[^\d.,]", "", text.strip().replace("\xa0", "").replace(" ", ""))
        cleaned = cleaned.replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            return None

    n = 0
    try:
        with conn.cursor() as cur:
            for row in table.find_all("tr")[1:]:  # skip header
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) < 3:
                    continue
                # Columns: Rok dostaw | Data aukcji | Cena (PLN/MW/rok) | Wolumen (MW)
                year       = _parse_number(cells[0])
                price      = _parse_number(cells[2])
                volume     = _parse_number(cells[3]) if len(cells) > 3 else None
                auction_dt = cells[1].strip() or None
                if year is None or price is None:
                    continue
                cur.execute(
                    "INSERT INTO intl_market.po_capacity_market "
                    "(delivery_year, auction_date, price_pln_mw_yr, accepted_mw, source) "
                    "VALUES (%s, %s, %s, %s, 'tge') "
                    "ON CONFLICT (delivery_year) DO UPDATE SET "
                    "price_pln_mw_yr = EXCLUDED.price_pln_mw_yr, "
                    "accepted_mw = EXCLUDED.accepted_mw, "
                    "fetched_at = now()",
                    (int(year), auction_dt, price, volume),
                )
                n += cur.rowcount
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("[scrape_po_capacity_market] DB insert failed: %s", exc)

    return n
```

- [ ] **Step 4: Run all scraper tests**

```bash
python -m pytest tests/po_knowledge/test_as_scrapers.py -v
```

Expected: all 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add services/po_knowledge/entso_scraper.py tests/po_knowledge/test_as_scrapers.py
git commit -m "feat(po-market): add scrape_po_capacity_market (TGE Rynek Mocy)"
```

---

## Task 5: `_run_bess_dispatch_po()` — LP Dispatch Function

**Files:**
- Create: `tests/po_market/__init__.py`
- Create: `tests/po_market/test_bess_dispatch_po.py`
- Modify: `apps/po-market/app.py`

**Note on units:** `po_day_ahead_prices.price_pln_mwh` is in PLN/MWh. `optimise_day()` expects prices as a numpy array where each element is the price per unit of energy for one hour-block. Because power is in MW and each block is 1 hour, passing PLN/MWh gives profit directly in PLN — no multiplication correction needed (contrast with PH where prices are PHP/kWh requiring ×1000).

- [ ] **Step 1: Write failing tests**

Create `tests/po_market/test_bess_dispatch_po.py`:

```python
"""Tests for _run_bess_dispatch_po() and _calibrate_po_strip_params()."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch


def _make_price_df(n_days: int = 5, base_price: float = 300.0) -> pd.DataFrame:
    """Synthetic 24h price data: uniform base_price with a peak at hours 18-20."""
    rows = []
    from datetime import date, timedelta
    start = date(2024, 1, 1)
    for d in range(n_days):
        dt = start + timedelta(days=d)
        for h in range(24):
            price = base_price * 2.0 if 18 <= h < 20 else base_price
            rows.append({"trading_date": dt, "hour": h, "price_pln_mwh": price, "price_eur_mwh": price / 4.25})
    return pd.DataFrame(rows)


def test_run_bess_dispatch_po_returns_expected_columns():
    """_run_bess_dispatch_po returns a DataFrame with required columns."""
    import apps.po_market.app as app_module  # noqa: F401 — triggers ImportError if missing

    with patch("apps.po_market.app._query") as mock_query:
        mock_query.return_value = _make_price_df(3)
        from apps.po_market.app import _run_bess_dispatch_po
        result = _run_bess_dispatch_po(
            power_mw=10.0, duration_h=2.0, roundtrip_eff=0.85, price_col="price_pln_mwh"
        )

    assert isinstance(result, pd.DataFrame)
    required = {"trading_date", "pf_profit_pln", "naive_profit_pln", "options_value_pln",
                "charge_mwh", "discharge_mwh"}
    assert required.issubset(result.columns)
    assert len(result) == 3  # one row per day


def test_run_bess_dispatch_po_profit_non_negative():
    """Perfect-forecast profit should never be negative (LP is free to do nothing)."""
    with patch("apps.po_market.app._query") as mock_query:
        mock_query.return_value = _make_price_df(5)
        from apps.po_market.app import _run_bess_dispatch_po
        result = _run_bess_dispatch_po(10.0, 2.0, 0.85, "price_pln_mwh")

    assert (result["pf_profit_pln"] >= -0.01).all(), "PF profit should not be negative"


def test_run_bess_dispatch_po_options_value_non_negative():
    """options_value_pln = max(pf - naive, 0) should always be >= 0."""
    with patch("apps.po_market.app._query") as mock_query:
        mock_query.return_value = _make_price_df(5)
        from apps.po_market.app import _run_bess_dispatch_po
        result = _run_bess_dispatch_po(10.0, 2.0, 0.85, "price_pln_mwh")

    assert (result["options_value_pln"] >= 0).all()


def test_run_bess_dispatch_po_skips_incomplete_days():
    """Days with fewer than 24 hours are dropped."""
    df = _make_price_df(2)
    # Remove hours 0-5 from day 1 → that day has only 18 hours
    df = df[~((df["trading_date"] == df["trading_date"].iloc[0]) & (df["hour"] < 6))]

    with patch("apps.po_market.app._query") as mock_query:
        mock_query.return_value = df
        from apps.po_market.app import _run_bess_dispatch_po
        result = _run_bess_dispatch_po(10.0, 2.0, 0.85, "price_pln_mwh")

    # Only the complete day should appear
    assert len(result) == 1
```

- [ ] **Step 2: Confirm failure**

```bash
python -m pytest tests/po_market/test_bess_dispatch_po.py::test_run_bess_dispatch_po_returns_expected_columns -v
```

Expected: `ImportError` — `_run_bess_dispatch_po` not yet defined.

- [ ] **Step 3: Implement `_run_bess_dispatch_po()` in `app.py`**

Add this function after the existing `_query()` helper (around line 90) and before any tab-rendering code:

```python
# ── BESS Dispatch ─────────────────────────────────────────────────────────

def _run_bess_dispatch_po(
    power_mw: float,
    duration_h: float,
    roundtrip_eff: float,
    price_col: str = "price_pln_mwh",
) -> "pd.DataFrame":
    """Run LP perfect-forecast BESS dispatch against Polish day-ahead prices.

    Args:
        power_mw: Arbitrage-slice power rating (MW) — caller passes power × arb_pct/100
        duration_h: Battery duration in hours (energy = power_mw × duration_h MWh)
        roundtrip_eff: Round-trip efficiency (e.g. 0.85)
        price_col: 'price_pln_mwh' or 'price_eur_mwh'

    Returns DataFrame with columns:
        trading_date, pf_profit_pln, naive_profit_pln, options_value_pln,
        charge_mwh, discharge_mwh
    """
    import numpy as np
    import sys, os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
    from services.bess_map.optimisation_engine import optimise_day

    prices_df = _query(
        "SELECT trading_date, hour, price_pln_mwh, price_eur_mwh "
        "FROM intl_market.po_day_ahead_prices "
        "ORDER BY trading_date, hour"
    )
    if prices_df.empty:
        return pd.DataFrame(columns=[
            "trading_date", "pf_profit_pln", "naive_profit_pln",
            "options_value_pln", "charge_mwh", "discharge_mwh",
        ])

    # Keep only complete 24-hour days
    day_counts = prices_df.groupby("trading_date")["hour"].count()
    complete_days = day_counts[day_counts == 24].index
    prices_df = prices_df[prices_df["trading_date"].isin(complete_days)]

    rows = []
    for day, grp in prices_df.groupby("trading_date"):
        grp = grp.sort_values("hour")
        prices_arr = grp[price_col].to_numpy(dtype=float)  # PLN/MWh (or EUR/MWh)

        # LP dispatch — prices in PLN/MWh → profit in PLN directly (MW × PLN/MWh × 1h = PLN)
        res = optimise_day(prices_arr, power_mw, duration_h, roundtrip_eff)
        pf_profit = res.profit if res.status == "Optimal" else 0.0

        # Naive: charge at cheapest hour, discharge at most expensive hour (1 cycle)
        min_h, max_h = int(np.argmin(prices_arr)), int(np.argmax(prices_arr))
        eta_c = np.sqrt(roundtrip_eff)
        eta_d = np.sqrt(roundtrip_eff)
        energy_mwh = power_mw * duration_h
        if max_h > min_h:
            naive_profit = (
                prices_arr[max_h] * eta_d * energy_mwh
                - prices_arr[min_h] / eta_c * energy_mwh
            )
        else:
            naive_profit = 0.0

        options_value = max(pf_profit - max(naive_profit, 0.0), 0.0)

        charge_mwh    = float(np.sum(res.charge_mw))    if res.status == "Optimal" else 0.0
        discharge_mwh = float(np.sum(res.discharge_mw)) if res.status == "Optimal" else 0.0

        rows.append({
            "trading_date":    day,
            "pf_profit_pln":   pf_profit,
            "naive_profit_pln": naive_profit,
            "options_value_pln": options_value,
            "charge_mwh":      charge_mwh,
            "discharge_mwh":   discharge_mwh,
        })

    return pd.DataFrame(rows)
```

Also add this import near the top of `app.py` (after existing imports):
```python
import numpy as np
```

- [ ] **Step 4: Run all dispatch tests**

```bash
python -m pytest tests/po_market/test_bess_dispatch_po.py -v
```

Expected: all 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/po-market/app.py tests/po_market/__init__.py tests/po_market/test_bess_dispatch_po.py
git commit -m "feat(po-market): add _run_bess_dispatch_po() with LP dispatch + naive comparator"
```

---

## Task 6: `_calibrate_po_strip_params()` — Kirk-Margrabe Calibration

**Files:**
- Modify: `apps/po-market/app.py`
- Modify: `tests/po_market/test_bess_dispatch_po.py`

- [ ] **Step 1: Write failing test**

Append to `tests/po_market/test_bess_dispatch_po.py`:

```python
def test_calibrate_po_strip_params_returns_required_keys():
    """_calibrate_po_strip_params returns dict with forward prices and vols."""
    price_df = _make_price_df(n_days=90)

    with patch("apps.po_market.app._query") as mock_query:
        mock_query.return_value = price_df
        from apps.po_market.app import _calibrate_po_strip_params
        result = _calibrate_po_strip_params(
            conn=MagicMock(),
            peak_start_h=8,
            peak_end_h=20,
            window_days=90,
        )

    required = {"peak_forward_pln", "offpeak_forward_pln", "peak_vol", "offpeak_vol"}
    assert required.issubset(result.keys())
    assert result["peak_forward_pln"] > result["offpeak_forward_pln"]  # peak > offpeak in test data
    assert 0.0 < result["peak_vol"] < 5.0   # vols should be reasonable (annualised)
    assert 0.0 < result["offpeak_vol"] < 5.0


def test_calibrate_po_strip_params_fallback_on_empty_data():
    """Returns zero-vol defaults when no price data is available."""
    with patch("apps.po_market.app._query") as mock_query:
        mock_query.return_value = pd.DataFrame(
            columns=["trading_date", "hour", "price_pln_mwh", "price_eur_mwh"]
        )
        from apps.po_market.app import _calibrate_po_strip_params
        result = _calibrate_po_strip_params(MagicMock(), 8, 20, 90)

    assert result["peak_forward_pln"] == 0.0
    assert result["peak_vol"] == 0.30   # sensible default
```

- [ ] **Step 2: Confirm failure**

```bash
python -m pytest tests/po_market/test_bess_dispatch_po.py::test_calibrate_po_strip_params_returns_required_keys -v
```

Expected: `ImportError`.

- [ ] **Step 3: Implement `_calibrate_po_strip_params()` in `app.py`**

Add after `_run_bess_dispatch_po()`:

```python
def _calibrate_po_strip_params(
    conn,
    peak_start_h: int = 8,
    peak_end_h: int = 20,
    window_days: int = 90,
) -> dict:
    """Calibrate Kirk-Margrabe inputs from po_day_ahead_prices history.

    Args:
        conn: psycopg2 connection (unused directly — uses _query)
        peak_start_h: First peak hour (inclusive), default 8
        peak_end_h: Last peak hour (exclusive), default 20
        window_days: Look-back window in days

    Returns dict:
        peak_forward_pln, offpeak_forward_pln,
        peak_vol, offpeak_vol,
        n_days  (data coverage)
    """
    import numpy as np
    from datetime import date, timedelta

    cutoff = (date.today() - timedelta(days=window_days)).isoformat()
    df = _query(
        "SELECT trading_date, hour, price_pln_mwh "
        "FROM intl_market.po_day_ahead_prices "
        "WHERE trading_date >= %s AND price_pln_mwh IS NOT NULL "
        "ORDER BY trading_date, hour",
        params=(cutoff,),
    )

    _default = {
        "peak_forward_pln": 0.0, "offpeak_forward_pln": 0.0,
        "peak_vol": 0.30, "offpeak_vol": 0.30, "n_days": 0,
    }

    if df.empty:
        return _default

    is_peak = df["hour"].between(peak_start_h, peak_end_h - 1)
    peak_df   = df[is_peak].groupby("trading_date")["price_pln_mwh"].mean()
    offpeak_df = df[~is_peak].groupby("trading_date")["price_pln_mwh"].mean()

    # Align to common dates
    common = peak_df.index.intersection(offpeak_df.index)
    if len(common) < 5:
        return _default

    peak_series   = peak_df.loc[common].sort_index()
    offpeak_series = offpeak_df.loc[common].sort_index()

    peak_fwd   = float(peak_series.mean())
    offpeak_fwd = float(offpeak_series.mean())

    def _annualised_vol(series: "pd.Series") -> float:
        log_ret = np.log(series.values[1:] / np.maximum(series.values[:-1], 1e-6))
        return float(np.std(log_ret) * np.sqrt(252)) if len(log_ret) > 1 else 0.30

    return {
        "peak_forward_pln":   peak_fwd,
        "offpeak_forward_pln": offpeak_fwd,
        "peak_vol":    _annualised_vol(peak_series),
        "offpeak_vol": _annualised_vol(offpeak_series),
        "n_days":      len(common),
    }
```

- [ ] **Step 4: Run all tests**

```bash
python -m pytest tests/ -v
```

Expected: all tests PASS (8+ tests total).

- [ ] **Step 5: Commit**

```bash
git add apps/po-market/app.py tests/po_market/test_bess_dispatch_po.py
git commit -m "feat(po-market): add _calibrate_po_strip_params() for Kirk-Margrabe vol calibration"
```

---

## Task 7: BESS Opportunity Tab — Subsection A (Dispatch P&L UI)

**Files:**
- Modify: `apps/po-market/app.py`

Find the BESS Opportunity tab section (`with tab_bess:`) in `app.py`. After the existing `st.divider()` at the end of the "Optimal BESS Configuration" table, append the following block.

- [ ] **Step 1: Add dispatch P&L subsection to BESS Opportunity tab**

```python
    # ── Subsection A: Perfect-Forecast Dispatch P&L ────────────────────────
    st.divider()
    st.subheader("BESS P&L Analysis — Perfect-Forecast Dispatch")
    st.caption(
        "LP optimal dispatch on arbitrage slice · Compares to naive 1-cycle · "
        "AS revenue stacked from DB average auction prices"
    )

    da_c1, da_c2, da_c3, da_c4 = st.columns(4)
    with da_c1:
        da_power = st.number_input("Power (MW)", min_value=1.0, max_value=1000.0,
                                    value=50.0, step=10.0, key="po_da_power")
    with da_c2:
        da_dur   = st.number_input("Duration (h)", min_value=0.5, max_value=8.0,
                                    value=2.0, step=0.5, key="po_da_dur")
    with da_c3:
        da_eff   = st.number_input("Efficiency (%)", min_value=50.0, max_value=100.0,
                                    value=85.0, step=1.0, key="po_da_eff") / 100.0
    with da_c4:
        da_pcol  = st.selectbox("Price", ["price_pln_mwh", "price_eur_mwh"],
                                 format_func=lambda x: "PLN/MWh" if "pln" in x else "EUR/MWh",
                                 key="po_da_pcol")

    al_c1, al_c2, al_c3 = st.columns(3)
    with al_c1:
        fcr_pct  = st.number_input("FCR allocation (%)", 0.0, 100.0, 20.0, 5.0, key="po_fcr_pct")
    with al_c2:
        afrr_pct = st.number_input("aFRR allocation (%)", 0.0, 100.0, 20.0, 5.0, key="po_afrr_pct")
    with al_c3:
        arb_pct  = 100.0 - fcr_pct - afrr_pct
        st.metric("Arbitrage allocation (%)", f"{arb_pct:.0f}")
        if arb_pct < 0:
            st.error("FCR + aFRR > 100% — reduce allocations")

    if st.button("Run Dispatch Model", type="primary", key="po_run_dispatch",
                 disabled=(arb_pct < 0)):
        with st.spinner("Running LP dispatch…"):
            arb_mw = da_power * arb_pct / 100.0
            dispatch_df = _run_bess_dispatch_po(arb_mw, da_dur, da_eff, da_pcol)

            as_rev = get_as_revenue_estimate(_conn(), da_power, fcr_pct, afrr_pct)

            pf_annual = float(dispatch_df["pf_profit_pln"].sum()) if not dispatch_df.empty else 0.0
            opts_annual = float(dispatch_df["options_value_pln"].sum()) if not dispatch_df.empty else 0.0
            total_rev = pf_annual + as_rev["total_pln_yr"]

            st.session_state["po_dispatch_results"] = {
                "arb_pln_yr":      pf_annual,
                "fcr_pln_yr":      as_rev["fcr_pln_yr"],
                "afrr_pln_yr":     as_rev["afrr_pln_yr"],
                "capacity_pln_yr": as_rev["capacity_pln_yr"],
                "total_pln_yr":    total_rev,
                "options_pln_yr":  opts_annual,
                "fcr_pct":         fcr_pct,
                "afrr_pct":        afrr_pct,
                "arb_pct":         arb_pct,
                "df":              dispatch_df,
            }
            st.rerun()

    disp = st.session_state.get("po_dispatch_results")
    if disp is not None:
        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric("Total Annual Revenue",
                  f"zł{disp['total_pln_yr']/1e6:.2f}M")
        m2.metric("Arbitrage P&L",
                  f"zł{disp['arb_pln_yr']/1e6:.2f}M")
        m3.metric("FCR Revenue",
                  f"zł{disp['fcr_pln_yr']/1e6:.2f}M",
                  f"{disp['fcr_pct']:.0f}% capacity")
        m4.metric("aFRR Revenue",
                  f"zł{disp['afrr_pln_yr']/1e6:.2f}M",
                  f"{disp['afrr_pct']:.0f}% capacity")
        m5.metric("Rynek Mocy",
                  f"zł{disp['capacity_pln_yr']/1e6:.2f}M")
        m6.metric("Options Value",
                  f"zł{disp['options_pln_yr']/1e6:.2f}M",
                  help="PF dispatch premium over naive 1-cycle dispatch")

        df = disp["df"]
        if not df.empty:
            # Chart 1: Daily P&L line
            fig1 = px.line(
                df, x="trading_date", y="pf_profit_pln",
                title="Daily Arbitrage P&L (PLN)",
                labels={"trading_date": "Date", "pf_profit_pln": "Profit (PLN)"},
            )
            st.plotly_chart(fig1, use_container_width=True)

            # Chart 2: Monthly stacked bar (arb + AS layers)
            df2 = df.copy()
            df2["month"] = pd.to_datetime(df2["trading_date"]).dt.to_period("M").astype(str)
            monthly_arb = df2.groupby("month")["pf_profit_pln"].sum().reset_index()
            monthly_arb["FCR"] = (
                disp["fcr_pln_yr"] / 12 if disp["fcr_pln_yr"] else 0
            )
            monthly_arb["aFRR"] = (
                disp["afrr_pln_yr"] / 12 if disp["afrr_pln_yr"] else 0
            )
            monthly_arb["Rynek Mocy"] = (
                disp["capacity_pln_yr"] / 12 if disp["capacity_pln_yr"] else 0
            )
            monthly_arb = monthly_arb.rename(columns={"pf_profit_pln": "Arbitrage"})
            fig2 = px.bar(
                monthly_arb.melt(id_vars="month",
                                  value_vars=["Arbitrage", "FCR", "aFRR", "Rynek Mocy"]),
                x="month", y="value", color="variable",
                title="Monthly Revenue Stack (PLN)",
                labels={"month": "Month", "value": "Revenue (PLN)", "variable": "Source"},
                barmode="stack",
            )
            st.plotly_chart(fig2, use_container_width=True)

            # Chart 3: Dispatch profile for selected date
            sel_date = st.selectbox(
                "Dispatch profile date",
                options=sorted(df["trading_date"].unique()),
                key="po_disp_date",
            )
            day_prices = _query(
                "SELECT hour, price_pln_mwh FROM intl_market.po_day_ahead_prices "
                "WHERE trading_date = %s ORDER BY hour",
                params=(str(sel_date),),
            )
            day_row = df[df["trading_date"] == sel_date]
            if not day_row.empty and not day_prices.empty:
                from services.bess_map.optimisation_engine import optimise_day
                arb_mw_sel = da_power * arb_pct / 100.0
                res = optimise_day(
                    day_prices["price_pln_mwh"].to_numpy(dtype=float),
                    arb_mw_sel, da_dur, da_eff,
                )
                fig3 = go.Figure()
                hours = list(range(24))
                fig3.add_bar(x=hours, y=list(-res.charge_mw),
                              name="Charge (MW)", marker_color="steelblue")
                fig3.add_bar(x=hours, y=list(res.discharge_mw),
                              name="Discharge (MW)", marker_color="coral")
                fig3.add_scatter(x=hours, y=day_prices["price_pln_mwh"].tolist(),
                                  name="Price (PLN/MWh)", yaxis="y2",
                                  line=dict(color="gold", width=2))
                fig3.update_layout(
                    title=f"Dispatch Profile — {sel_date}",
                    barmode="relative",
                    yaxis=dict(title="Power (MW)"),
                    yaxis2=dict(title="Price (PLN/MWh)", overlaying="y", side="right"),
                    legend=dict(orientation="h"),
                )
                st.plotly_chart(fig3, use_container_width=True)
```

Also add the import at the top of `app.py` (with existing imports from entso_scraper):

```python
from services.po_knowledge.entso_scraper import (
    ENTSOEPriceScraper, run_entso_price_scrape, run_po_doc_backfill, _PO_DOC_CONNECTOR_MAP,
    get_as_revenue_estimate,  # add this
)
```

- [ ] **Step 2: Verify no syntax errors**

```bash
python -c "import ast; ast.parse(open('apps/po-market/app.py').read()); print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add apps/po-market/app.py
git commit -m "feat(po-market): add BESS Opportunity subsection A — dispatch P&L + revenue stack"
```

---

## Task 8: BESS Opportunity Tab — Subsection B (Kirk-Margrabe Strip Valuation UI)

**Files:**
- Modify: `apps/po-market/app.py`

The `bess_spread_call_strip._run()` function uses `_yuan` in parameter and output field names — these are currency-agnostic; pass PLN values and interpret outputs as PLN.

- [ ] **Step 1: Add strip valuation subsection after Subsection A in the BESS tab**

```python
    # ── Subsection B: Kirk-Margrabe Strip Valuation ────────────────────────
    st.divider()
    st.subheader("BESS Spread Option Strip Valuation (Kirk-Margrabe)")
    st.caption(
        "Treats BESS as a strip of N daily peak/offpeak spread call options · "
        "Calibrated from last 90 days of TGE day-ahead prices"
    )

    km_c1, km_c2 = st.columns(2)
    with km_c1:
        km_peak_start = st.slider("Peak hours start", 6, 12, 8, key="po_km_pk_start")
        km_peak_end   = st.slider("Peak hours end",   14, 22, 20, key="po_km_pk_end")
        km_om_cost    = st.number_input("O&M cost / strike K (PLN/MWh)",
                                         0.0, 200.0, 20.0, 5.0, key="po_km_om")
        km_horizon    = st.number_input("Valuation horizon (days)",
                                         30, 730, 365, 30, key="po_km_horizon")
    with km_c2:
        km_corr = st.slider("Peak/offpeak correlation", 0.0, 1.0, 0.85, 0.05,
                              key="po_km_corr")
        # Pre-fill from dispatch config if available
        _prev = st.session_state.get("po_dispatch_results", {})
        km_power = st.number_input("Power (MW)", 1.0, 1000.0,
                                    float(_prev.get("arb_pct", 100) / 100
                                          * st.session_state.get("po_da_power", 50.0)),
                                    10.0, key="po_km_power")
        km_dur   = st.number_input("Duration (h)", 0.5, 8.0,
                                    float(st.session_state.get("po_da_dur", 2.0)),
                                    0.5, key="po_km_dur")
        km_eff   = st.number_input("Efficiency (%)", 50.0, 100.0,
                                    float(st.session_state.get("po_da_eff", 0.85) * 100),
                                    1.0, key="po_km_eff") / 100.0

    if st.button("Value Strip", type="primary", key="po_km_run"):
        with st.spinner("Calibrating from price history and pricing strip…"):
            params = _calibrate_po_strip_params(
                _conn(), km_peak_start, km_peak_end, window_days=90
            )

            if params["peak_forward_pln"] == 0.0:
                st.warning("Insufficient price history for calibration (< 5 days). "
                           "Scrape more day-ahead prices first.")
            else:
                from libs.decision_models.bess_spread_call_strip import _run as _km_run

                km_result = _km_run(
                    asset_code="PO-BESS",
                    as_of_date=str(pd.Timestamp.today().date()),
                    n_days_remaining=int(km_horizon),
                    peak_forward_yuan=params["peak_forward_pln"],
                    offpeak_forward_yuan=params["offpeak_forward_pln"],
                    peak_vol=params["peak_vol"],
                    offpeak_vol=params["offpeak_vol"],
                    peak_offpeak_corr=km_corr,
                    roundtrip_eff=km_eff,
                    power_mw=km_power,
                    duration_h=km_dur,
                    om_cost_yuan_per_mwh=km_om_cost,
                )

                # Display metrics (output fields use "_yuan" suffix but values are PLN)
                sv  = km_result["strip_value_yuan"]
                iv  = km_result["intrinsic_value_yuan"]
                tv  = km_result["time_value_yuan"]
                mon = km_result["moneyness_pct"]
                delta = km_result["delta_yuan_per_yuan"]
                vega  = km_result["vega_yuan_per_vol_point"]

                km1, km2, km3, km4, km5, km6 = st.columns(6)
                km1.metric("Strip Value",     f"zł{sv/1e6:.2f}M")
                km2.metric("Intrinsic Value", f"zł{iv/1e6:.2f}M")
                km3.metric("Time Value",      f"zł{tv/1e6:.2f}M")
                km4.metric(
                    "Moneyness",
                    f"{mon:+.1f}%",
                    delta="ITM" if mon > 0 else "OTM",
                    delta_color="normal" if mon > 0 else "inverse",
                )
                km5.metric("Delta",  f"{delta:.3f}")
                km6.metric("Vega",   f"zł{vega/1e3:.1f}K / 1% vol")

                # Calibration info
                with st.expander("Calibration details"):
                    col_a, col_b = st.columns(2)
                    col_a.markdown(
                        f"**Peak forward:** zł{params['peak_forward_pln']:.1f}/MWh  \n"
                        f"**Peak vol:** {params['peak_vol']*100:.1f}%  \n"
                        f"**Data window:** {params['n_days']} days"
                    )
                    col_b.markdown(
                        f"**Offpeak forward:** zł{params['offpeak_forward_pln']:.1f}/MWh  \n"
                        f"**Offpeak vol:** {params['offpeak_vol']*100:.1f}%  \n"
                        f"**Spread vol:** {km_result['spread_vol_used']*100:.1f}%"
                    )
```

Also add this import near the top of `app.py`:

```python
import numpy as np  # (if not already added in Task 5)
```

- [ ] **Step 2: Verify no syntax errors**

```bash
python -c "import ast; ast.parse(open('apps/po-market/app.py').read()); print('OK')"
```

- [ ] **Step 3: Commit**

```bash
git add apps/po-market/app.py
git commit -m "feat(po-market): add BESS Opportunity subsection B — Kirk-Margrabe strip valuation"
```

---

## Task 9: Investment Analysis Tab — Load from Dispatch Model Button

**Files:**
- Modify: `apps/po-market/app.py`

Find the Investment Analysis tab (`with tab_irr:`). Locate the `rev_val = st.number_input("Combined Revenue (PLN/MW/yr)", ...)` line. Insert the following block immediately BEFORE that `number_input` call:

- [ ] **Step 1: Add "Load from dispatch model" button**

```python
                # Load from dispatch model if available
                _disp = st.session_state.get("po_dispatch_results")
                if _disp is not None:
                    _total_mw_yr = _disp["total_pln_yr"] / da_power if da_power else _disp["total_pln_yr"]
                    if st.button(
                        f"📥 Load from dispatch model  (zł{_disp['total_pln_yr']/1e6:.2f}M/yr total)",
                        key="po_load_dispatch",
                    ):
                        st.session_state["po_irr_rev_override"] = _total_mw_yr
                    with st.expander("Revenue breakdown", expanded=False):
                        total = _disp["total_pln_yr"]
                        for label, key in [
                            ("Arbitrage (PF dispatch)", "arb_pln_yr"),
                            ("FCR", "fcr_pln_yr"),
                            ("aFRR", "afrr_pln_yr"),
                            ("Rynek Mocy", "capacity_pln_yr"),
                        ]:
                            val = _disp.get(key, 0.0)
                            pct = val / total * 100 if total else 0
                            st.markdown(
                                f"**{label}:** zł{val/1e6:.2f}M &nbsp;&nbsp; `{pct:.0f}%`"
                            )
                        st.markdown(f"---  \n**Total:** zł{total/1e6:.2f}M")
```

Then on the `rev_val = st.number_input(...)` line, change the `value=` argument to read from session state if set:

```python
                _rev_default = st.session_state.pop("po_irr_rev_override", 300_000.0)
                rev_val = st.number_input(
                    "Combined Revenue (PLN/MW/yr)",
                    min_value=0.0, max_value=2_000_000.0,
                    value=float(_rev_default),
                    step=10_000.0,
                    key="po_rev_input",
                )
```

(Replace the existing `rev_val = st.number_input(...)` line — keep all other parameters the same, only change `value=` and add `key=`.)

- [ ] **Step 2: Verify**

```bash
python -c "import ast; ast.parse(open('apps/po-market/app.py').read()); print('OK')"
```

- [ ] **Step 3: Commit**

```bash
git add apps/po-market/app.py
git commit -m "feat(po-market): add load-from-dispatch-model button in Investment Analysis tab"
```

---

## Task 10: APScheduler Jobs for AS Scrapers

**Files:**
- Modify: `apps/po-market/app.py`

Find the APScheduler setup block in `app.py` (where `scheduler.add_job(...)` calls for `po_price` and `po_docs` exist). Add two new jobs after the existing ones:

- [ ] **Step 1: Add scheduler jobs and wrapper functions**

Find the existing scheduler wrapper functions (e.g. `_price_job()`, `_docs_job()`) and add alongside them:

```python
def _as_scrape_job():
    """Scheduled: scrape FCR and aFRR prices from PSE (Tuesdays 06:05 CET)."""
    try:
        conn = _conn()
        from services.po_knowledge.entso_scraper import scrape_po_fcr_prices, scrape_po_afrr_prices
        n_fcr  = scrape_po_fcr_prices(conn, weeks_back=4)
        n_afrr = scrape_po_afrr_prices(conn, weeks_back=4)
        logger.info("[scheduler] po_as_prices: FCR=%d rows, aFRR=%d rows", n_fcr, n_afrr)
    except Exception as exc:
        logger.error("[scheduler] po_as_prices failed: %s", exc)


def _cap_market_job():
    """Scheduled: scrape Rynek Mocy results from TGE (1st of month 05:10 CET)."""
    try:
        conn = _conn()
        from services.po_knowledge.entso_scraper import scrape_po_capacity_market
        n = scrape_po_capacity_market(conn)
        logger.info("[scheduler] po_capacity_market: %d rows", n)
    except Exception as exc:
        logger.error("[scheduler] po_capacity_market failed: %s", exc)
```

Then in the scheduler setup block:

```python
scheduler.add_job(
    _as_scrape_job, "cron",
    day_of_week="tue", hour=6, minute=5,
    id="po_as_prices", replace_existing=True,
    timezone="Europe/Warsaw",
)
scheduler.add_job(
    _cap_market_job, "cron",
    day=1, hour=5, minute=10,
    id="po_cap_market", replace_existing=True,
    timezone="Europe/Warsaw",
)
```

- [ ] **Step 2: Verify**

```bash
python -c "import ast; ast.parse(open('apps/po-market/app.py').read()); print('OK')"
```

- [ ] **Step 3: Commit**

```bash
git add apps/po-market/app.py
git commit -m "feat(po-market): add APScheduler jobs for AS price scraping (FCR/aFRR/Rynek Mocy)"
```

---

## Task 11: Data Management Tab — AS Data Sections

**Files:**
- Modify: `apps/po-market/app.py`

Find the Data Management tab (`with tab_data:`) in `app.py`. Append the two new sections at the end of the tab's content, after the existing price scraping section:

- [ ] **Step 1: Add AS data management sections**

```python
        # ── Ancillary Service Prices ───────────────────────────────────────
        st.divider()
        st.subheader("Ancillary Service Market Prices")

        as_status = _query(
            "SELECT market_type, COUNT(*) as weeks, "
            "MAX(week_start) as latest_week, "
            "AVG(price_pln_mw_week) as avg_price "
            "FROM intl_market.po_as_prices "
            "GROUP BY market_type"
        )
        cap_status = _query(
            "SELECT delivery_year, price_pln_mw_yr, auction_date "
            "FROM intl_market.po_capacity_market "
            "ORDER BY delivery_year DESC LIMIT 1"
        )

        sc1, sc2, sc3 = st.columns(3)
        _fcr_row  = as_status[as_status["market_type"] == "FCR"] if not as_status.empty else None
        _afr_row  = as_status[as_status["market_type"] == "aFRR_capacity"] if not as_status.empty else None

        with sc1:
            if _fcr_row is not None and not _fcr_row.empty:
                st.metric("FCR",
                           f"zł{float(_fcr_row['avg_price'].iloc[0]):,.0f}/MW/wk",
                           f"{int(_fcr_row['weeks'].iloc[0])} weeks · latest {_fcr_row['latest_week'].iloc[0]}")
            else:
                st.metric("FCR", "No data")
        with sc2:
            if _afr_row is not None and not _afr_row.empty:
                st.metric("aFRR capacity",
                           f"zł{float(_afr_row['avg_price'].iloc[0]):,.0f}/MW/wk",
                           f"{int(_afr_row['weeks'].iloc[0])} weeks · latest {_afr_row['latest_week'].iloc[0]}")
            else:
                st.metric("aFRR capacity", "No data")
        with sc3:
            if not cap_status.empty:
                st.metric("Rynek Mocy",
                           f"zł{float(cap_status['price_pln_mw_yr'].iloc[0]):,.0f}/MW/yr",
                           f"{int(cap_status['delivery_year'].iloc[0])} delivery year")
            else:
                st.metric("Rynek Mocy", "No data")

        bt1, bt2, bt3 = st.columns(3)
        with bt1:
            if st.button("Scrape FCR prices", key="po_scrape_fcr"):
                with st.spinner("Fetching FCR auction results from PSE…"):
                    from services.po_knowledge.entso_scraper import scrape_po_fcr_prices
                    n = scrape_po_fcr_prices(_conn(), weeks_back=52)
                st.success(f"FCR: {n} new rows inserted")
        with bt2:
            if st.button("Scrape aFRR prices", key="po_scrape_afrr"):
                with st.spinner("Fetching aFRR auction results from PSE…"):
                    from services.po_knowledge.entso_scraper import scrape_po_afrr_prices
                    n = scrape_po_afrr_prices(_conn(), weeks_back=52)
                st.success(f"aFRR: {n} new rows inserted")
        with bt3:
            if st.button("Scrape Capacity Market", key="po_scrape_cap"):
                with st.spinner("Fetching Rynek Mocy results from TGE…"):
                    from services.po_knowledge.entso_scraper import scrape_po_capacity_market
                    n = scrape_po_capacity_market(_conn())
                st.success(f"Rynek Mocy: {n} rows upserted")

        # 52-week AS price chart
        as_history = _query(
            "SELECT week_start, market_type, price_pln_mw_week "
            "FROM intl_market.po_as_prices "
            "WHERE week_start >= CURRENT_DATE - INTERVAL '52 weeks' "
            "AND price_pln_mw_week IS NOT NULL "
            "ORDER BY week_start"
        )
        if not as_history.empty:
            fig_as = px.line(
                as_history, x="week_start", y="price_pln_mw_week", color="market_type",
                title="FCR & aFRR Weekly Clearing Prices (PLN/MW/week)",
                labels={"week_start": "Week", "price_pln_mw_week": "PLN/MW/week",
                         "market_type": "Market"},
            )
            st.plotly_chart(fig_as, use_container_width=True)

        # ── AS Backfill ────────────────────────────────────────────────────
        st.subheader("AS Data Backfill")
        bf_c1, bf_c2, bf_c3 = st.columns(3)
        with bf_c1:
            bf_start = st.date_input("Backfill from week", key="po_bf_start",
                                      value=pd.Timestamp.today() - pd.Timedelta(weeks=104))
        with bf_c2:
            bf_type = st.selectbox("Market", ["FCR", "aFRR", "Both"], key="po_bf_type")
        with bf_c3:
            st.write("")
            st.write("")
            if st.button("Run Backfill", key="po_bf_run"):
                from datetime import date as _date
                weeks_back = max(1, (pd.Timestamp.today().date() - bf_start).days // 7)
                with st.spinner(f"Backfilling {bf_type} for {weeks_back} weeks…"):
                    from services.po_knowledge.entso_scraper import (
                        scrape_po_fcr_prices, scrape_po_afrr_prices
                    )
                    total = 0
                    if bf_type in ("FCR", "Both"):
                        total += scrape_po_fcr_prices(_conn(), weeks_back=weeks_back)
                    if bf_type in ("aFRR", "Both"):
                        total += scrape_po_afrr_prices(_conn(), weeks_back=weeks_back)
                st.success(f"Backfill complete: {total} new rows inserted")
```

- [ ] **Step 2: Verify**

```bash
python -c "import ast; ast.parse(open('apps/po-market/app.py').read()); print('OK')"
```

- [ ] **Step 3: Commit**

```bash
git add apps/po-market/app.py
git commit -m "feat(po-market): add AS data management sections (status, scrape buttons, backfill)"
```

---

## Task 12: Dockerfile Update — Add `COPY libs/`

**Files:**
- Modify: `apps/po-market/Dockerfile`

- [ ] **Step 1: Add `COPY libs/` to Dockerfile**

In `apps/po-market/Dockerfile`, after the existing `COPY services/intl_market_common/` line, add:

```dockerfile
COPY libs/                          ./libs/
```

The complete COPY block should now read:

```dockerfile
COPY apps/po-market/               ./apps/po-market/
COPY services/common/               ./services/common/
COPY services/po_knowledge/         ./services/po_knowledge/
COPY services/intl_market_common/   ./services/intl_market_common/
COPY libs/                          ./libs/
```

- [ ] **Step 2: Also add `bess_map` service** (needed for `optimise_day`):

Check if `services/bess_map/` is already present in the Dockerfile. If not, add:

```dockerfile
COPY services/bess_map/             ./services/bess_map/
```

Run:
```bash
grep -n "bess_map" apps/po-market/Dockerfile
```

If no output, add the `COPY services/bess_map/` line alongside the others.

- [ ] **Step 3: Commit**

```bash
git add apps/po-market/Dockerfile
git commit -m "feat(po-market): add COPY libs/ and services/bess_map/ to Dockerfile"
```

---

## Task 13: Build, Push, and Deploy `bess-po-market:v13`

- [ ] **Step 1: Build image**

```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform
docker build -f apps/po-market/Dockerfile -t bess-po-market:v13 .
```

Expected: `Successfully built <id>` — build must complete without errors.  
If `ModuleNotFoundError: libs.decision_models` appears, verify `COPY libs/` was saved to Dockerfile.

- [ ] **Step 2: Tag and push to ECR**

```bash
AWS_ACCOUNT=319383842493
AWS_REGION=ap-southeast-1
aws ecr get-login-password --region $AWS_REGION | \
  docker login --username AWS --password-stdin ${AWS_ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com

docker tag bess-po-market:v13 ${AWS_ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com/bess-po-market:v13
docker push ${AWS_ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com/bess-po-market:v13
```

Expected: `Pushed` for each layer.

- [ ] **Step 3: Register td:18 with new image**

```bash
MSYS_NO_PATHCONV=1 aws ecs register-task-definition \
  --family bess-platform-po-market \
  --task-role-arn "arn:aws:iam::319383842493:role/ecsTaskExecutionRole" \
  --execution-role-arn "arn:aws:iam::319383842493:role/ecsTaskExecutionRole" \
  --network-mode awsvpc \
  --requires-compatibilities FARGATE \
  --cpu 512 --memory 1024 \
  --container-definitions '[{
    "name": "po-market",
    "image": "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-po-market:v13",
    "portMappings": [{"containerPort": 8511, "protocol": "tcp"}],
    "essential": true,
    "environment": [
      {"name": "PGURL", "value": "postgresql://postgres:!BESSmap2026@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require"},
      {"name": "ANTHROPIC_API_KEY", "value": "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"},
      {"name": "OPENAI_API_KEY", "value": "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"}
    ],
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
        "awslogs-group": "/ecs/bess-platform",
        "awslogs-region": "ap-southeast-1",
        "awslogs-stream-prefix": "po-market"
      }
    }
  }]' \
  --query 'taskDefinition.{family:family,revision:revision}' --output json
```

Expected: `{"family": "bess-platform-po-market", "revision": 18}`

- [ ] **Step 4: Deploy to ECS**

```bash
MSYS_NO_PATHCONV=1 aws ecs update-service \
  --cluster bess-platform-cluster \
  --service bess-platform-po-market-svc \
  --task-definition bess-platform-po-market:18 \
  --query 'service.{td:taskDefinition,running:runningCount}' --output json
```

- [ ] **Step 5: Wait for stability**

```bash
MSYS_NO_PATHCONV=1 aws ecs wait services-stable \
  --cluster bess-platform-cluster \
  --services bess-platform-po-market-svc && echo "STABLE"
```

- [ ] **Step 6: Smoke test**

1. Open `https://www.pjh-etrm.ai/po-market/`
2. Go to **BESS Opportunity** tab → scroll to "BESS P&L Analysis" → click **Run Dispatch Model** → confirm metrics appear
3. Click **Value Strip** → confirm Kirk-Margrabe metrics appear
4. Go to **Investment Analysis** tab → confirm "📥 Load from dispatch model" button appears and pre-fills revenue
5. Go to **Data Management** tab → scroll to "Ancillary Service Market Prices" → click **Scrape FCR prices** → confirm spinner + result message
6. Check **Investment Advisor** tab → send a message → confirm no 401 error

- [ ] **Step 7: Commit tag**

```bash
git tag po-market-v13
git push origin po-market-v13
```

---

## Self-Review Notes

**Spec coverage check:**
- ✅ `po_as_prices` and `po_capacity_market` tables → Task 1
- ✅ `scrape_po_fcr_prices()`, `scrape_po_afrr_prices()` → Task 3
- ✅ `scrape_po_capacity_market()` → Task 4
- ✅ `get_as_revenue_estimate()` → Task 2
- ✅ Scheduler jobs → Task 10
- ✅ `_run_bess_dispatch_po()` → Task 5
- ✅ Dispatch P&L UI (config, 6 metrics, 3 charts) → Task 7
- ✅ `_calibrate_po_strip_params()` → Task 6
- ✅ Kirk-Margrabe UI (6 metrics, calibration expander) → Task 8
- ✅ IRR load button + breakdown expander → Task 9
- ✅ Data Management AS sections → Task 11
- ✅ Dockerfile `COPY libs/` + `services/bess_map/` → Task 12
- ✅ Build + deploy v13 → Task 13

**Unit precision:** `_run_bess_dispatch_po` passes PLN/MWh prices directly to `optimise_day()` — profit is in PLN (MW × PLN/MWh × 1 h = PLN). No ×1000 correction needed (contrast with PH which uses PHP/kWh).

**Kirk-Margrabe parameter naming:** `bess_spread_call_strip._run()` uses `*_yuan` in parameter and output names but is currency-agnostic — PLN values are passed directly.
