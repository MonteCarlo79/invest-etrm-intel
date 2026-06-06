"""WESM (IEMOP) daily data scrapers for the Philippines market app.

Two components:
  WESMPriceScraper     -- scrapes daily settlement interval prices from iemop.ph
                          and stores them in intl_market.ph_wesm_prices
  WESMReportConnector  -- downloads IEMOP market bulletins (PDF/HTML) and stores
                          them in intl_market.ph_knowledge_docs (same schema as other
                          knowledge connectors, so it slots into run_knowledge_ingest)

WESM data source: https://www.iemop.ph/market-operations/market-results/
  - Daily Average Spot Prices are published for Luzon, Visayas and Mindanao reference
    trading nodes (RTN) at 06:00 MNL the following day.
  - Settlement interval (5-min) prices are available as CSV downloads.
  - Market bulletins are published as PDFs under /market-operations/market-bulletins/

Table created by app.py _ensure_tables():
    intl_market.ph_wesm_prices (
        id SERIAL PK, trading_date DATE, hour INT, interval_no INT,
        region TEXT, node TEXT, price_php_kwh NUMERIC(10,4),
        price_type TEXT, fetched_at TIMESTAMPTZ
    )
"""
from __future__ import annotations

import logging
import os
import re
import time
from datetime import date, datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; BESSPlatformBot/2.0; "
        "+https://www.pjh-etrm.ai; investment-research)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_IEMOP_BASE       = "https://www.iemop.ph"
_RESULTS_URL      = "https://www.iemop.ph/market-operations/market-results/"
_BULLETINS_URL    = "https://www.iemop.ph/market-operations/market-bulletins/"
_OASIS_BASE       = "https://www.iemop.ph/oasis"  # data portal if available

# RTN (Reference Trading Node) identifiers used by WESM
_REGION_NODES = {
    "Luzon":    ["WESM_NG_LUZON",   "LUZON_RTN",   "LZN",  "Luzon"],
    "Visayas":  ["WESM_NG_VISAYAS", "VISAYAS_RTN",  "VIS",  "Visayas"],
    "Mindanao": ["WESM_NG_MINDANAO","MINDANAO_RTN", "MIN",  "Mindanao"],
}


# ─────────────────────────────────────────────────────────────────────────────
# WESM Price Scraper
# ─────────────────────────────────────────────────────────────────────────────

class WESMPriceScraper:
    """Scrapes WESM LWAP (Load-Weighted Average Price) data from IEMOP.

    IEMOP publishes daily 5-minute interval LWAP CSVs at:
      https://www.iemop.ph/market-data/load-weighted-average-prices-final/
      ?post=276300&sort=desc&page=N&start=&end=

    Each page returns a ZIP archive containing CSVs named final_lwap_YYYYMMDD.csv
    Each CSV has columns: RUN_TIME, MKT_TYPE, TIME_INTERVAL, REGION_NAME, LWAP
    LWAP values are in PHP/MWh.  Region codes: CLUZ, CMIN, CVIS, SYSTEM.
    Approximately 24 days of data per page (desc order, page 1 = most recent).
    """

    _LWAP_URL  = "https://www.iemop.ph/market-data/load-weighted-average-prices-final/"
    _POST_ID   = 276300
    _DAYS_PER_PAGE = 25   # conservative estimate for page-count calculations

    _REGION_MAP = {
        "CLUZ":   "Luzon",
        "CMIN":   "Mindanao",
        "CVIS":   "Visayas",
    }

    # ── Public API ────────────────────────────────────────────────────────────

    def fetch_pages(
        self,
        session,
        max_pages: int = 1,
        since: Optional[date] = None,
    ) -> dict[str, list[dict]]:
        """Fetch up to *max_pages* pages; return {date_str: [records]}.

        Stops early when all dates on a page are older than *since* (if given).
        """
        import io
        import zipfile

        all_data: dict[str, list[dict]] = {}

        for page in range(1, max_pages + 1):
            url = (
                f"{self._LWAP_URL}"
                f"?post={self._POST_ID}&sort=desc&page={page}&start=&end="
            )
            try:
                resp = session.get(url, timeout=30)
                resp.raise_for_status()
            except Exception as exc:
                logger.warning("[wesm_price] page %d fetch failed: %s", page, exc)
                break

            # Response must be a ZIP (magic bytes PK)
            if resp.content[:2] != b"PK":
                logger.debug("[wesm_price] page %d: not a ZIP (got HTML?)", page)
                break

            try:
                with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                    for name in zf.namelist():
                        m = re.match(r"final_lwap_(\d{4})(\d{2})(\d{2})\.csv", name)
                        if not m:
                            continue
                        trading_date = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                        date_str = str(trading_date)
                        if date_str in all_data:
                            continue
                        csv_bytes = zf.read(name)
                        records = self._parse_lwap_csv(
                            csv_bytes.decode("utf-8", errors="replace"),
                            trading_date,
                        )
                        if records:
                            all_data[date_str] = records
            except Exception as exc:
                logger.warning("[wesm_price] page %d ZIP parse failed: %s", page, exc)
                break

            if not all_data:
                break

            # Early stop: if all dates on this page are before *since*, done
            if since is not None:
                page_dates = sorted(all_data.keys())
                if page_dates and page_dates[0] < str(since):
                    break

            time.sleep(0.5)

        return all_data

    def fetch_daily_prices(self, target_date: Optional[date] = None) -> list[dict]:
        """Return LWAP records for *target_date* (default: yesterday).

        Searches pages in descending order until the date is found.
        """
        if target_date is None:
            target_date = date.today() - timedelta(days=1)

        try:
            import requests
        except ImportError:
            logger.warning("[wesm_price] requests not installed")
            return []

        session = requests.Session()
        session.headers.update(_HEADERS)
        target_str = str(target_date)

        # Estimate which page the date is on (page 1 = most recent ~25 days)
        days_back = (date.today() - target_date).days
        start_page = max(1, days_back // self._DAYS_PER_PAGE)
        max_pages = start_page + 2  # fetch a small window around estimate

        data = self.fetch_pages(session, max_pages=max_pages)
        records = data.get(target_str, [])
        if not records:
            logger.warning("[wesm_price] no LWAP data found for %s", target_date)
        return records

    # ── CSV parser ────────────────────────────────────────────────────────────

    def _parse_lwap_csv(self, csv_text: str, trading_date: date) -> list[dict]:
        """Parse a final_lwap_YYYYMMDD.csv file into price records."""
        import io
        import pandas as pd
        from datetime import datetime as _dt

        try:
            df = pd.read_csv(io.StringIO(csv_text))
        except Exception:
            return []

        if df.empty or "LWAP" not in df.columns or "REGION_NAME" not in df.columns:
            return []

        records = []
        for _, row in df.iterrows():
            region_code = str(row.get("REGION_NAME", "")).strip()
            region = self._REGION_MAP.get(region_code)
            if region is None:
                continue  # skip SYSTEM and unrecognised codes

            try:
                lwap_mwh = float(row["LWAP"])
                price_kwh = round(lwap_mwh / 1000.0, 4)  # PHP/MWh → PHP/kWh

                # Parse 5-min interval timestamp
                interval_str = str(row.get("TIME_INTERVAL", "")).strip()
                hour, interval_no = 0, 0
                try:
                    dt = _dt.strptime(interval_str, "%m/%d/%Y %I:%M:%S %p")
                    hour = dt.hour
                    interval_no = dt.hour * 12 + dt.minute // 5
                except Exception:
                    pass

                records.append({
                    "trading_date": trading_date,
                    "hour":         hour,
                    "interval_no":  interval_no,
                    "region":       region,
                    "node":         region_code,
                    "price_php_kwh": price_kwh,
                    "price_type":   "LWAP",
                })
            except Exception:
                continue

        return records

    # ── DB storage ────────────────────────────────────────────────────────────

    def store_prices(self, conn, records: list[dict]) -> int:
        """Insert price records; skip duplicates. Returns count of new rows."""
        if not records:
            return 0
        n = 0
        with conn.cursor() as cur:
            for r in records:
                cur.execute(
                    "INSERT INTO intl_market.ph_wesm_prices "
                    "(trading_date, hour, interval_no, region, node, price_php_kwh, price_type) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (trading_date, hour, region, price_type) DO NOTHING",
                    (
                        r["trading_date"], r["hour"], r.get("interval_no", r["hour"]),
                        r["region"], r.get("node", r["region"]),
                        r["price_php_kwh"], r.get("price_type", "LWAP"),
                    ),
                )
                if cur.rowcount > 0:
                    n += 1
        conn.commit()
        return n

    def run(self, conn, target_date: Optional[date] = None) -> int:
        """Fetch single day and store. Returns new row count."""
        records = self.fetch_daily_prices(target_date)
        return self.store_prices(conn, records)


# ─────────────────────────────────────────────────────────────────────────────
# WESM Report Connector  (plugs into run_knowledge_ingest)
# ─────────────────────────────────────────────────────────────────────────────

class WESMReportConnector:
    """Scrapes IEMOP market bulletins, daily/weekly reports, and market notices.

    Compatible with the existing connector protocol in ph_knowledge/ingest.py:
      connector.source  -> str
      connector.fetch() -> list[dict]   (doc_type, title, url, published_date, content)
    """

    source = "wesm_iemop"

    # URLs to crawl
    _SOURCES = [
        (_BULLETINS_URL,                              "bulletin"),
        ("https://www.iemop.ph/market-operations/market-advisories/",  "advisory"),
        ("https://www.iemop.ph/market-operations/market-notices/",     "notice"),
        ("https://www.iemop.ph/market-operations/",                    "report"),
    ]

    def fetch(self) -> list[dict]:
        try:
            import requests
            from bs4 import BeautifulSoup
        except ImportError:
            logger.warning("[wesm_report] requests/bs4 not installed")
            return []

        session = requests.Session()
        session.headers.update(_HEADERS)

        docs = []
        seen_urls: set[str] = set()

        for base_url, doc_type in self._SOURCES:
            try:
                resp = session.get(base_url, timeout=30)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "html.parser")
            except Exception as exc:
                logger.debug("[wesm_report] fetch %s: %s", base_url, exc)
                continue

            for link in soup.find_all("a", href=True):
                href    = link["href"]
                title   = link.get_text(" ", strip=True)
                if not title or len(title) < 8:
                    continue
                if not href.startswith("http"):
                    href = _IEMOP_BASE + href if href.startswith("/") else href
                if href in seen_urls:
                    continue
                # Filter for content that looks like a market document
                is_pdf  = href.lower().endswith(".pdf")
                is_page = any(kw in (title + href).lower() for kw in [
                    "bulletin", "notice", "advisory", "market result", "report",
                    "dispatch", "settlement", "price", "wesm",
                ])
                if not (is_pdf or is_page):
                    continue
                seen_urls.add(href)

                published_date = _infer_date_from_text(title + " " + href)

                if is_pdf:
                    content = _fetch_pdf_text(session, href)
                else:
                    content = _fetch_page_text(session, href)

                if not content or len(content.strip()) < 100:
                    continue

                docs.append({
                    "doc_type":       doc_type,
                    "title":          title[:250],
                    "url":            href,
                    "published_date": published_date,
                    "content":        f"IEMOP WESM — {title}\n\n{content[:80_000]}",
                })

                if len(docs) >= 50:  # cap per run to avoid long ingestion
                    return docs
                time.sleep(0.5)      # polite crawl rate

        return docs


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _infer_date_from_text(text: str):
    """Try to extract a publication date from a filename or title string."""
    from datetime import date as _date
    # ISO: 2026-06-03
    m = re.search(r"(\d{4})[-_](\d{2})[-_](\d{2})", text)
    if m:
        try:
            return _date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    # Month name: June 3, 2026 / 3 June 2026
    months = {
        "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
        "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
    }
    m = re.search(
        r"(\d{1,2})\s+(" + "|".join(months) + r")\s+(\d{4})",
        text, re.IGNORECASE,
    )
    if m:
        try:
            return _date(int(m.group(3)), months[m.group(2).lower()], int(m.group(1)))
        except ValueError:
            pass
    m = re.search(
        r"(" + "|".join(months) + r")\s+(\d{1,2}),?\s+(\d{4})",
        text, re.IGNORECASE,
    )
    if m:
        try:
            return _date(int(m.group(3)), months[m.group(1).lower()], int(m.group(2)))
        except ValueError:
            pass
    return None


def _fetch_pdf_text(session, url: str, max_pages: int = 10) -> str:
    try:
        import io
        from pypdf import PdfReader
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        reader = PdfReader(io.BytesIO(resp.content))
        pages = []
        for page in reader.pages[:max_pages]:
            t = page.extract_text()
            if t:
                pages.append(t)
        return "\n\n".join(pages)
    except Exception as exc:
        logger.debug("[wesm_report] PDF extract %s: %s", url, exc)
        return ""


def _fetch_page_text(session, url: str) -> str:
    try:
        from bs4 import BeautifulSoup
        resp = session.get(url, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup.find_all(["script", "style", "nav", "header", "footer"]):
            tag.decompose()
        return "\n\n".join(
            el.get_text(" ", strip=True)
            for el in soup.find_all(["p", "h1", "h2", "h3", "li", "td"])
            if el.get_text(strip=True)
        )
    except Exception as exc:
        logger.debug("[wesm_report] page fetch %s: %s", url, exc)
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# Generic WESM / IEMOP document connector  (base + 7 concrete subclasses)
# ─────────────────────────────────────────────────────────────────────────────

class WESMDocumentConnector:
    """Paginated WESM / IEMOP listing-page scraper.

    Subclasses declare:
      source        - str identifier stored in ph_knowledge_docs.source
      _LISTING_URLS - list of listing page URLs to crawl
      _DOC_KEYWORDS - keywords used to filter anchor links
      _BASE         - base URL for resolving relative hrefs
    """

    source: str = "wesm_doc"
    _LISTING_URLS: list[str] = []
    _DOC_KEYWORDS: list[str] = ["report", "assessment", "market watch", "bess"]
    _BASE: str = "https://www.wesm.ph"

    def fetch(self, max_pages: int = 3, since: date | None = None) -> list[dict]:
        """Scrape listing pages and return list of doc dicts for _run_connector().

        Args:
            max_pages: Maximum paginated pages to fetch per listing URL.
            since:     If set, stop paginating when all docs on a page are older.
        """
        try:
            import requests
            from bs4 import BeautifulSoup
        except ImportError:
            logger.warning("[wesm_doc] requests/bs4 not installed")
            return []

        session = requests.Session()
        session.headers.update(_HEADERS)

        docs: list[dict] = []
        seen_urls: set[str] = set()

        for base_url in self._LISTING_URLS:
            for page_num in range(1, max_pages + 1):
                # Build paginated URL
                if page_num == 1:
                    page_url = base_url
                elif "?" in base_url:
                    page_url = f"{base_url}&page={page_num}"
                else:
                    page_url = f"{base_url}?page={page_num}"

                try:
                    resp = session.get(page_url, timeout=30)
                    if resp.status_code == 404:
                        break  # no more pages
                    resp.raise_for_status()
                    soup = BeautifulSoup(resp.text, "html.parser")
                except Exception as exc:
                    logger.debug("[wesm_doc:%s] page %s fetch failed: %s", self.source, page_url, exc)
                    break

                page_docs = self._extract_docs_from_page(soup, session, seen_urls)

                if not page_docs:
                    break  # empty page → stop paginating

                # Date-based early stop for backfill
                if since is not None:
                    dated = [d for d in page_docs if d.get("published_date")]
                    if dated and all(d["published_date"] < since for d in dated):
                        break  # all docs on this page are older than cutoff

                docs.extend(page_docs)

                # Also check for "paged" style pagination (WordPress)
                has_next = bool(soup.find("a", string=re.compile(r"next|»|›", re.I)))
                if not has_next and page_num > 1:
                    break

                time.sleep(1.0)  # polite crawl rate

        return docs

    def _extract_docs_from_page(
        self,
        soup,
        session,
        seen_urls: set[str],
    ) -> list[dict]:
        """Extract document links from a parsed listing page."""
        page_docs: list[dict] = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            title = link.get_text(" ", strip=True)

            if not title or len(title) < 8:
                continue

            # Resolve relative URLs
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                href = self._BASE + href
            elif not href.startswith("http"):
                continue

            if href in seen_urls:
                continue

            is_pdf = href.lower().endswith(".pdf")
            keyword_match = any(kw.lower() in (title + href).lower() for kw in self._DOC_KEYWORDS)

            if not (is_pdf or keyword_match):
                continue

            seen_urls.add(href)
            published_date = _infer_date_from_text(title + " " + href)

            if is_pdf:
                content = _fetch_pdf_text(session, href, max_pages=15)
                doc_type = "pdf"
            else:
                content = _fetch_page_text(session, href)
                doc_type = "report"

            if not content or len(content.strip()) < 100:
                continue

            page_docs.append({
                "doc_type":       doc_type,
                "title":          title[:250],
                "url":            href,
                "published_date": published_date,
                "content":        f"{self.source} — {title}\n\n{content[:80_000]}",
            })
            time.sleep(0.5)

        return page_docs


class WESMMarketWatchConnector(WESMDocumentConnector):
    """WESM Market Watch weekly reports from wesm.ph."""
    source = "wesm_market_watch"
    _LISTING_URLS = ["https://www.wesm.ph/market-outcomes/market-watch"]
    _DOC_KEYWORDS = ["market watch", "weekly", "report", "market outcome"]


class WESMMarketAssessmentConnector(WESMDocumentConnector):
    """WESM Market Assessment reports (monthly/quarterly/seasonal/annual)."""
    source = "wesm_assessment"
    _LISTING_URLS = ["https://www.wesm.ph/market-outcomes/market-assessment-reports"]
    _DOC_KEYWORDS = ["market assessment", "monthly", "quarterly", "seasonal", "annual", "assessment report"]


class WESMRetailAssessmentConnector(WESMDocumentConnector):
    """WESM Retail Market Assessment reports."""
    source = "wesm_retail_assessment"
    _LISTING_URLS = ["https://www.wesm.ph/market-outcomes/retail-market-assessment-reports"]
    _DOC_KEYWORDS = ["retail", "assessment", "retail market"]


class WESMOverridingConstraintsConnector(WESMDocumentConnector):
    """WESM Over-riding Constraints reports."""
    source = "wesm_overriding_constraints"
    _LISTING_URLS = ["https://www.wesm.ph/market-outcomes/over-riding-constraints"]
    _DOC_KEYWORDS = ["over-riding", "constraint", "overriding", "report"]


class IEMOPKnowledgeCenterConnector(WESMDocumentConnector):
    """IEMOP Knowledge Center publications."""
    source = "iemop_knowledge_center"
    _LISTING_URLS = ["https://www.iemop.ph/services/knowledge-center/"]
    _BASE = "https://www.iemop.ph"
    _DOC_KEYWORDS = ["knowledge", "manual", "guide", "publication", "study", "primer", "market"]


class WESMBESSStudyConnector(WESMDocumentConnector):
    """WESM BESS study reports."""
    source = "wesm_bess_study"
    _LISTING_URLS = [
        "https://www.wesm.ph/library/downloads/view-download/documents/market-study/bess-wesm-design-and-power-wrangler"
    ]
    _DOC_KEYWORDS = ["bess", "battery", "storage", "wesm", "design", "wrangler", "download"]


class IEMOPMarketDataConnector(WESMDocumentConnector):
    """IEMOP Market Data publications and downloads."""
    source = "iemop_market_data"
    _LISTING_URLS = ["https://www.iemop.ph/the-market/market-data/"]
    _BASE = "https://www.iemop.ph"
    _DOC_KEYWORDS = [
        "market data", "settlement", "dispatch", "price", "report",
        "statement", "billing", "invoice", "download", "luzon", "visayas", "mindanao",
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Historical backfill convenience function
# ─────────────────────────────────────────────────────────────────────────────

_WESM_DOC_CONNECTOR_MAP: dict[str, WESMDocumentConnector] = {
    "wesm_market_watch":           WESMMarketWatchConnector(),
    "wesm_assessment":             WESMMarketAssessmentConnector(),
    "wesm_retail_assessment":      WESMRetailAssessmentConnector(),
    "wesm_overriding_constraints": WESMOverridingConstraintsConnector(),
    "iemop_knowledge_center":      IEMOPKnowledgeCenterConnector(),
    "wesm_bess_study":             WESMBESSStudyConnector(),
    "iemop_market_data":           IEMOPMarketDataConnector(),
}


def run_wesm_doc_backfill(
    conn,
    sources: list[str],
    start_date: date,
    prefix: str,
) -> dict[str, int]:
    """Backfill historical WESM/IEMOP documents for *sources* since *start_date*.

    Returns {source: new_rows_inserted} for each source processed.
    """
    results: dict[str, int] = {}
    for src in sources:
        connector = _WESM_DOC_CONNECTOR_MAP.get(src)
        if connector is None:
            logger.warning("[wesm_backfill] unknown source: %s", src)
            results[src] = 0
            continue
        logger.info("[wesm_backfill] backfilling %s since %s …", src, start_date)
        try:
            docs = connector.fetch(max_pages=50, since=start_date)
            filtered = [
                d for d in docs
                if not d.get("published_date") or d["published_date"] >= start_date
            ]
            n = 0
            for doc in filtered:
                with conn.cursor() as cur:
                    cur.execute(
                        f"INSERT INTO intl_market.{prefix}knowledge_docs "
                        "(source, doc_type, title, url, published_date, content) "
                        "VALUES (%s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (url) DO UPDATE SET "
                        "  content=EXCLUDED.content, fetched_at=NOW()",
                        (
                            src,
                            doc.get("doc_type", "report"),
                            doc.get("title", ""),
                            doc.get("url"),
                            doc.get("published_date"),
                            doc["content"],
                        ),
                    )
                    if cur.rowcount > 0:
                        n += 1
            conn.commit()
            results[src] = n
            logger.info("[wesm_backfill] %s: %d new docs", src, n)
        except Exception as exc:
            logger.error("[wesm_backfill] %s failed: %s", src, exc)
            results[src] = -1

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Top-level convenience function (called by scheduler + manual trigger)
# ─────────────────────────────────────────────────────────────────────────────

def run_wesm_price_scrape(conn, days_back: int = 1) -> dict[str, int]:
    """Scrape WESM LWAP prices for the last *days_back* days.

    Fetches the minimum number of IEMOP ZIP pages to cover the requested range,
    then stores all records found.  Returns {date_str: rows_inserted}.
    """
    try:
        import requests
    except ImportError:
        logger.warning("[wesm_price] requests not installed")
        return {}

    scraper = WESMPriceScraper()
    today   = date.today()
    since   = today - timedelta(days=days_back)

    # Pages needed: each page covers ~25 days; add 1 for safety
    max_pages = max(1, (days_back // scraper._DAYS_PER_PAGE) + 2)

    session = requests.Session()
    session.headers.update(_HEADERS)

    logger.info("[wesm_price] fetching %d day(s) across up to %d page(s)", days_back, max_pages)
    page_data = scraper.fetch_pages(session, max_pages=max_pages, since=since)

    results: dict[str, int] = {}
    for date_str, records in page_data.items():
        if date_str < str(since):
            continue  # outside requested window
        try:
            n = scraper.store_prices(conn, records)
            results[date_str] = n
        except Exception as exc:
            logger.error("[wesm_price] store failed for %s: %s", date_str, exc)
            results[date_str] = -1

    # Ensure every requested date appears in results (even if 0 rows found)
    for i in range(1, days_back + 1):
        d = str(today - timedelta(days=i))
        if d not in results:
            results[d] = 0

    return results
