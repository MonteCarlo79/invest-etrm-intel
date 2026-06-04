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
    """Scrapes daily WESM spot prices (Luzon / Visayas / Mindanao) from IEMOP.

    Strategy (in order of preference):
      1. IEMOP OASIS CSV download if the endpoint is available
      2. Parse the HTML market-results page for tabulated prices
      3. Fall back to scraping the market bulletin PDFs for price summaries

    Prices are stored into intl_market.ph_wesm_prices.
    """

    def fetch_daily_prices(
        self,
        target_date: Optional[date] = None,
    ) -> list[dict]:
        """Return a list of price records for *target_date* (default: yesterday)."""
        if target_date is None:
            target_date = date.today() - timedelta(days=1)

        try:
            import requests
        except ImportError:
            logger.warning("[wesm_price] requests not installed")
            return []

        session = requests.Session()
        session.headers.update(_HEADERS)

        # Strategy 1: try OASIS CSV download
        records = self._fetch_oasis_csv(session, target_date)
        if records:
            logger.info("[wesm_price] OASIS CSV: %d records for %s", len(records), target_date)
            return records

        # Strategy 2: parse the market-results HTML page
        records = self._fetch_html_table(session, target_date)
        if records:
            logger.info("[wesm_price] HTML table: %d records for %s", len(records), target_date)
            return records

        # Strategy 3: scrape market bulletin for the date
        records = self._fetch_bulletin_prices(session, target_date)
        if records:
            logger.info("[wesm_price] bulletin: %d records for %s", len(records), target_date)
            return records

        logger.warning("[wesm_price] no price data found for %s", target_date)
        return []

    # ── Strategy 1: OASIS / data portal CSV ──────────────────────────────────

    def _fetch_oasis_csv(self, session, target_date: date) -> list[dict]:
        """Try IEMOP OASIS data portal for CSV price downloads."""
        date_str = target_date.strftime("%Y-%m-%d")
        # Common OASIS URL patterns observed on IEMOP
        candidates = [
            f"{_IEMOP_BASE}/oasis/daily-prices?date={date_str}&format=csv",
            f"{_IEMOP_BASE}/market-operations/market-results/daily-prices?date={date_str}",
            f"{_IEMOP_BASE}/market-data/spot-prices/{target_date.year}/{target_date.month:02d}/{date_str}.csv",
        ]
        for url in candidates:
            try:
                resp = session.get(url, timeout=20)
                if resp.status_code == 200 and "text/csv" in resp.headers.get("content-type", ""):
                    return self._parse_csv(resp.text, target_date)
                if resp.status_code == 200 and len(resp.content) > 200:
                    # Try parsing as CSV anyway
                    records = self._parse_csv(resp.text, target_date)
                    if records:
                        return records
            except Exception as exc:
                logger.debug("[wesm_price] OASIS URL %s failed: %s", url, exc)
        return []

    def _parse_csv(self, csv_text: str, trading_date: date) -> list[dict]:
        """Parse a WESM price CSV (flexible header detection)."""
        import io
        import pandas as pd

        try:
            df = pd.read_csv(io.StringIO(csv_text))
        except Exception:
            return []

        if df.empty:
            return []

        cols_lower = {c.lower().strip(): c for c in df.columns}
        records = []

        # Detect column names flexibly
        date_col   = next((cols_lower[k] for k in cols_lower if "date" in k), None)
        hour_col   = next((cols_lower[k] for k in cols_lower if "hour" in k or "interval" in k), None)
        region_col = next((cols_lower[k] for k in cols_lower if "region" in k or "node" in k or "area" in k), None)
        price_col  = next((cols_lower[k] for k in cols_lower
                           if any(p in k for p in ["price", "lmp", "php", "kwh", "mwh"])), None)

        if price_col is None:
            return []

        for _, row in df.iterrows():
            try:
                price_raw = float(row[price_col])
                # Convert PHP/MWh → PHP/kWh if price looks like MWh scale
                price_php_kwh = price_raw / 1000 if price_raw > 100 else price_raw
                region = str(row[region_col]).strip() if region_col else "Unknown"
                hour   = int(row[hour_col]) if hour_col else 0
                tdate  = pd.to_datetime(row[date_col]).date() if date_col else trading_date
                records.append({
                    "trading_date":   tdate,
                    "hour":           hour,
                    "interval_no":    hour,
                    "region":         region,
                    "node":           region,
                    "price_php_kwh":  round(price_php_kwh, 4),
                    "price_type":     "HSIP",
                })
            except Exception:
                continue
        return records

    # ── Strategy 2: HTML market-results table ─────────────────────────────────

    def _fetch_html_table(self, session, target_date: date) -> list[dict]:
        """Parse the IEMOP market-results page for daily average prices."""
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return []

        try:
            resp = session.get(_RESULTS_URL, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
        except Exception as exc:
            logger.debug("[wesm_price] HTML fetch failed: %s", exc)
            return []

        date_str = target_date.strftime("%B %d, %Y")  # e.g. "June 03, 2026"
        alt_str  = target_date.strftime("%Y-%m-%d")

        records = []

        # Find tables that contain price data for the target date
        for table in soup.find_all("table"):
            text = table.get_text(" ")
            if not (date_str in text or alt_str in text):
                continue
            # Extract rows
            rows = table.find_all("tr")
            for row in rows:
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if len(cells) < 3:
                    continue
                # Look for rows with region name + numeric price
                for region, aliases in _REGION_NODES.items():
                    if any(alias.lower() in cells[0].lower() for alias in aliases):
                        for cell in cells[1:]:
                            try:
                                price = float(cell.replace(",", ""))
                                price_kwh = price / 1000 if price > 100 else price
                                records.append({
                                    "trading_date":  target_date,
                                    "hour":          0,
                                    "interval_no":   0,
                                    "region":        region,
                                    "node":          region,
                                    "price_php_kwh": round(price_kwh, 4),
                                    "price_type":    "daily_avg",
                                })
                                break
                            except (ValueError, AttributeError):
                                continue

        # Fallback: scan all text for price patterns near region names
        if not records:
            records = self._scrape_price_patterns(soup, target_date)

        return records

    def _scrape_price_patterns(self, soup, target_date: date) -> list[dict]:
        """Last-resort: scan page text for price numbers near region keywords."""
        text = soup.get_text(" ")
        records = []
        for region in ["Luzon", "Visayas", "Mindanao"]:
            # Look for  "Luzon ... PHP X.XX/kWh" or similar
            pattern = rf"{region}[^.]*?(\d+[\.,]\d{{2,4}})"
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                try:
                    price = float(m.group(1).replace(",", ""))
                    price_kwh = price / 1000 if price > 100 else price
                    if 1.0 < price_kwh < 30.0:  # sanity: PHP 1–30/kWh is plausible
                        records.append({
                            "trading_date":  target_date,
                            "hour":          0,
                            "interval_no":   0,
                            "region":        region,
                            "node":          region,
                            "price_php_kwh": round(price_kwh, 4),
                            "price_type":    "scraped",
                        })
                except (ValueError, AttributeError):
                    pass
        return records

    # ── Strategy 3: bulletin PDF summary ──────────────────────────────────────

    def _fetch_bulletin_prices(self, session, target_date: date) -> list[dict]:
        """Try to extract price summaries from the latest market bulletin."""
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return []

        try:
            resp = session.get(_BULLETINS_URL, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
        except Exception as exc:
            logger.debug("[wesm_price] bulletin list fetch failed: %s", exc)
            return []

        # Find bulletin links for or near target_date
        date_strs = [
            target_date.strftime("%B %d, %Y"),
            target_date.strftime("%d %B %Y"),
            target_date.strftime("%Y-%m-%d"),
            target_date.strftime("%d/%m/%Y"),
        ]
        for link in soup.find_all("a", href=True):
            href = link["href"]
            title = link.get_text(" ", strip=True)
            if not any(ds.lower() in (href + title).lower() for ds in date_strs):
                continue
            if not href.lower().endswith(".pdf"):
                continue
            if not href.startswith("http"):
                href = _IEMOP_BASE + href if href.startswith("/") else href
            try:
                pdf_resp = session.get(href, timeout=30)
                if pdf_resp.status_code == 200:
                    return self._parse_bulletin_pdf(pdf_resp.content, target_date)
            except Exception:
                pass
        return []

    def _parse_bulletin_pdf(self, pdf_bytes: bytes, trading_date: date) -> list[dict]:
        """Extract regional prices from a WESM market bulletin PDF."""
        try:
            import io
            from pypdf import PdfReader
            reader = PdfReader(io.BytesIO(pdf_bytes))
            text = "\n".join(p.extract_text() or "" for p in reader.pages[:5])
        except Exception:
            return []

        records = []
        for region in ["Luzon", "Visayas", "Mindanao"]:
            pattern = rf"{region}[^.]*?(\d+[\.,]\d{{2,4}})\s*(?:PHP|₱)?[^.]*?(?:kWh|MWh)"
            m = re.search(pattern, text, re.IGNORECASE)
            if not m:
                pattern = rf"{region}[^.]*?(\d+[\.,]\d{{2,4}})"
                m = re.search(pattern, text, re.IGNORECASE)
            if m:
                try:
                    price = float(m.group(1).replace(",", ""))
                    price_kwh = price / 1000 if price > 100 else price
                    if 1.0 < price_kwh < 30.0:
                        records.append({
                            "trading_date":  trading_date,
                            "hour":          0,
                            "interval_no":   0,
                            "region":        region,
                            "node":          region,
                            "price_php_kwh": round(price_kwh, 4),
                            "price_type":    "bulletin_pdf",
                        })
                except (ValueError, AttributeError):
                    pass
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
                        r["price_php_kwh"], r.get("price_type", "HSIP"),
                    ),
                )
                if cur.rowcount > 0:
                    n += 1
        conn.commit()
        return n

    def run(self, conn, target_date: Optional[date] = None) -> int:
        """Full fetch-and-store cycle. Returns count of new price rows inserted."""
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
# Top-level convenience function (called by scheduler + manual trigger)
# ─────────────────────────────────────────────────────────────────────────────

def run_wesm_price_scrape(conn, days_back: int = 1) -> dict[str, int]:
    """Scrape WESM prices for the last *days_back* days.

    Returns {date_str: rows_inserted} for each date processed.
    """
    scraper = WESMPriceScraper()
    results: dict[str, int] = {}
    today = date.today()
    for i in range(1, days_back + 1):
        target = today - timedelta(days=i)
        try:
            n = scraper.run(conn, target_date=target)
            results[str(target)] = n
        except Exception as exc:
            logger.error("[wesm_price] date %s failed: %s", target, exc)
            results[str(target)] = -1
    return results
