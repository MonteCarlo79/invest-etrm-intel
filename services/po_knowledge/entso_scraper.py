"""ENTSO-E / PSE data scrapers for the Poland market app.

Two components:
  ENTSOEPriceScraper        -- scrapes day-ahead prices from energy-charts.info (primary,
                               free, no auth — Fraunhofer ISE EPEX SPOT data for Poland) or
                               ENTSO-E API (optional, requires ENTSOE_API_KEY env var) and
                               stores them in intl_market.po_day_ahead_prices
  PolishMarketDocConnector  -- base class for downloading PSE/TGE/URE/ENTSO-E publications
                               into intl_market.po_knowledge_docs

Data sources:
  energy-charts: https://api.energy-charts.info (EPEX SPOT PL day-ahead, free, no auth)
  PSE.pl:        https://www.pse.pl  (balancing/grid reports)
  TGE:           https://tge.pl
  URE:           https://www.ure.gov.pl
  ENTSO-E:       https://transparency.entsoe.eu (optional API key)

DB table created by app.py _ensure_tables():
    intl_market.po_day_ahead_prices (
        id           SERIAL PRIMARY KEY,
        trading_date DATE          NOT NULL,
        hour         INTEGER       NOT NULL,
        price_pln_mwh NUMERIC(10,4),
        price_eur_mwh NUMERIC(10,4),
        source       TEXT          NOT NULL DEFAULT 'pse_csv',
        fetched_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        CONSTRAINT po_day_ahead_prices_uq UNIQUE (trading_date, hour, source)
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
    "Accept-Language": "en-GB,en;q=0.9,pl;q=0.8",
}

_PSE_BASE          = "https://www.pse.pl"
_TGE_BASE          = "https://tge.pl"
_URE_BASE          = "https://www.ure.gov.pl"
_ENTSOE_BASE       = "https://transparency.entsoe.eu"
_ENTSOE_WEBAPI     = "https://web-api.tp.entsoe.eu/api"
_ENERGY_CHARTS_API = "https://api.energy-charts.info/price"

# Poland bidding zone EIC code (ENTSO-E)
_PL_ZONE_EIC = "10YPL-AREA-----S"

# EUR/PLN exchange rate (fallback for conversion when source provides EUR only)
_EUR_PLN_FALLBACK = 4.25


# ─────────────────────────────────────────────────────────────────────────────
# Day-ahead Price Scraper
# ─────────────────────────────────────────────────────────────────────────────

class ENTSOEPriceScraper:
    """Scrapes Polish day-ahead electricity prices.

    Two strategies, tried in order:

    1. energy-charts.info (primary, free, no auth) — Fraunhofer ISE, EPEX SPOT data:
       https://api.energy-charts.info/price?bzn=PL&start={YYYY-MM-DD}&end={YYYY-MM-DD}
       Returns 15-min intervals in EUR/MWh; aggregated to hourly averages.

    2. ENTSO-E Transparency API (secondary, requires ENTSOE_API_KEY env var):
       documentType=A44, bidding zone 10YPL-AREA-----S, hourly resolution.
    """

    # ── Public API ────────────────────────────────────────────────────────────

    def fetch_range(self, start: date, end: date) -> dict[str, list[dict]]:
        """Fetch day-ahead prices for [start, end] inclusive.

        Returns {date_str: [hourly records]}.
        """
        data = self._fetch_energy_charts(start, end)
        if not data:
            logger.info("[entso_price] energy-charts empty, trying ENTSO-E API")
            data = self._fetch_entsoe_api(start, end)
        return data

    def fetch_daily_prices(self, target_date: Optional[date] = None) -> list[dict]:
        """Return hourly records for target_date (default: yesterday)."""
        if target_date is None:
            target_date = date.today() - timedelta(days=1)
        data = self.fetch_range(target_date, target_date)
        return data.get(str(target_date), [])

    # ── energy-charts.info strategy (primary) ─────────────────────────────────

    def _fetch_energy_charts(self, start: date, end: date) -> dict[str, list[dict]]:
        """Fetch EPEX SPOT Poland day-ahead prices from Fraunhofer ISE energy-charts.info.

        URL: https://api.energy-charts.info/price?bzn=PL&start=YYYY-MM-DD&end=YYYY-MM-DD
        Response: {unix_seconds: [...], price: [...EUR/MWh...], unit: "EUR / MWh"}
        15-min intervals are averaged to hourly. Warsaw local date is used for trading_date.
        """
        try:
            import requests
        except ImportError:
            return {}

        eur_pln = float(os.environ.get("EUR_PLN", str(_EUR_PLN_FALLBACK)))
        results: dict[str, list[dict]] = {}

        # Fetch one week at a time to avoid overly large requests
        current = start
        while current <= end:
            chunk_end = min(current + timedelta(days=6), end)
            params = {
                "bzn": "PL",
                "start": current.strftime("%Y-%m-%d"),
                "end": chunk_end.strftime("%Y-%m-%d"),
            }
            try:
                resp = requests.get(
                    _ENERGY_CHARTS_API,
                    params=params,
                    headers={"User-Agent": "BESSPlatformBot/2.0 (investment-research)"},
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                logger.warning("[energy_charts] fetch failed for %s–%s: %s", current, chunk_end, exc)
                current = chunk_end + timedelta(days=1)
                continue

            timestamps = data.get("unix_seconds", [])
            prices_eur = data.get("price", [])

            if not timestamps or not prices_eur:
                current = chunk_end + timedelta(days=1)
                continue

            # Aggregate 15-min intervals to hourly in Warsaw local time
            # Warsaw = UTC+1 (CET) or UTC+2 (CEST)
            hourly_buckets: dict[tuple, list[float]] = {}  # (date_str, hour) -> [prices]

            for ts, price_eur in zip(timestamps, prices_eur):
                if price_eur is None:
                    continue
                try:
                    # Convert UTC → Warsaw (approximate: use CET UTC+1 for simplicity;
                    # CEST UTC+2 is April-October but the hour difference is minor for daily avg)
                    dt_utc = datetime.utcfromtimestamp(ts)
                    # Determine UTC offset: +2 in summer (Mar last Sun → Oct last Sun), +1 otherwise
                    month = dt_utc.month
                    is_summer = 4 <= month <= 9 or (month == 3 and dt_utc.day >= 26) or (month == 10 and dt_utc.day < 26)
                    offset_h = 2 if is_summer else 1
                    dt_waw = dt_utc + timedelta(hours=offset_h)
                    trading_date = dt_waw.date()
                    hour = dt_waw.hour
                    key = (str(trading_date), hour)
                    hourly_buckets.setdefault(key, []).append(float(price_eur))
                except Exception:
                    continue

            for (date_str, hour), bucket_prices in hourly_buckets.items():
                avg_eur = sum(bucket_prices) / len(bucket_prices)
                record = {
                    "trading_date": date.fromisoformat(date_str),
                    "hour": hour,
                    "price_eur_mwh": round(avg_eur, 4),
                    "price_pln_mwh": round(avg_eur * eur_pln, 4),
                    "source": "energy_charts",
                }
                results.setdefault(date_str, []).append(record)

            current = chunk_end + timedelta(days=1)
            time.sleep(0.5)

        logger.info("[energy_charts] fetched %d dates", len(results))
        return results

    # ── ENTSO-E API strategy ──────────────────────────────────────────────────

    def _fetch_entsoe_api(self, start: date, end: date) -> dict[str, list[dict]]:
        token = os.environ.get("ENTSOE_API_KEY", "")
        if not token:
            logger.debug("[entsoe_api] ENTSOE_API_KEY not set, skipping")
            return {}
        try:
            import requests
        except ImportError:
            return {}

        params = {
            "securityToken": token,
            "documentType": "A44",
            "in_Domain": _PL_ZONE_EIC,
            "out_Domain": _PL_ZONE_EIC,
            "periodStart": start.strftime("%Y%m%d") + "0000",
            "periodEnd": end.strftime("%Y%m%d") + "2300",
        }
        try:
            resp = requests.get(_ENTSOE_WEBAPI, params=params, timeout=30)
            resp.raise_for_status()
            return self._parse_entsoe_xml(resp.text)
        except Exception as exc:
            logger.warning("[entsoe_api] fetch failed: %s", exc)
            return {}

    def _parse_entsoe_xml(self, xml_text: str) -> dict[str, list[dict]]:
        """Parse ENTSO-E A44 Publication_MarketDocument XML into price records."""
        try:
            import xml.etree.ElementTree as ET
        except ImportError:
            return {}

        eur_pln = float(os.environ.get("EUR_PLN", str(_EUR_PLN_FALLBACK)))
        results: dict[str, list[dict]] = {}

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            logger.warning("[entsoe_xml] parse error: %s", exc)
            return {}

        ns = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0"}
        # Handle namespace-less documents too
        if not root.tag.startswith("{"):
            ns = {}
            def _find(el, path):
                return el.find(path)
            def _findall(el, path):
                return el.findall(path)
        else:
            def _find(el, path):
                return el.find(path.replace("/", "/ns:").replace("ns:ns:", "ns:"), ns)
            def _findall(el, path):
                return el.findall(path.replace("/", "/ns:").replace("ns:ns:", "ns:"), ns)

        for ts in root.iter():
            if not ts.tag.endswith("TimeSeries"):
                continue
            # Period
            for period in ts.iter():
                if not period.tag.endswith("Period"):
                    continue
                start_el = None
                res_el = None
                for child in period:
                    if child.tag.endswith("timeInterval"):
                        for tc in child:
                            if tc.tag.endswith("start"):
                                start_el = tc
                    if child.tag.endswith("resolution"):
                        res_el = child

                if start_el is None:
                    continue
                try:
                    start_dt = datetime.strptime(start_el.text.strip(), "%Y-%m-%dT%H:%MZ")
                except Exception:
                    continue

                for point in period.iter():
                    if not point.tag.endswith("Point"):
                        continue
                    pos_el = price_el = None
                    for pc in point:
                        if pc.tag.endswith("position"):
                            pos_el = pc
                        if pc.tag.endswith("price.amount"):
                            price_el = pc
                    if pos_el is None or price_el is None:
                        continue
                    try:
                        position = int(pos_el.text) - 1  # 1-indexed → 0-indexed hours
                        price_eur = float(price_el.text)
                        dt = start_dt + timedelta(hours=position)
                        trading_date = dt.date()
                        hour = dt.hour
                        record = {
                            "trading_date": trading_date,
                            "hour": hour,
                            "price_eur_mwh": round(price_eur, 4),
                            "price_pln_mwh": round(price_eur * eur_pln, 4),
                            "source": "entsoe_api",
                        }
                        results.setdefault(str(trading_date), []).append(record)
                    except Exception:
                        continue

        return results

    # ── DB storage ────────────────────────────────────────────────────────────

    def store_prices(self, conn, records: list[dict]) -> int:
        """Insert price records; skip duplicates. Returns count of new rows."""
        if not records:
            return 0
        n = 0
        with conn.cursor() as cur:
            for r in records:
                cur.execute(
                    "INSERT INTO intl_market.po_day_ahead_prices "
                    "(trading_date, hour, price_pln_mwh, price_eur_mwh, source) "
                    "VALUES (%s, %s, %s, %s, %s) "
                    "ON CONFLICT (trading_date, hour, source) DO NOTHING",
                    (
                        r["trading_date"],
                        r["hour"],
                        r.get("price_pln_mwh"),
                        r.get("price_eur_mwh"),
                        r.get("source", "pse_csv"),
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
# Base document connector for Polish market sources
# ─────────────────────────────────────────────────────────────────────────────

class PolishMarketDocConnector:
    """Paginated Polish market document scraper.

    Subclasses declare:
      source        - str identifier stored in po_knowledge_docs.source
      _LISTING_URLS - list of listing page URLs to crawl
      _DOC_KEYWORDS - keywords used to filter anchor links
      _BASE         - base URL for resolving relative hrefs
    """

    source: str = "po_market_doc"
    _LISTING_URLS: list[str] = []
    _DOC_KEYWORDS: list[str] = ["report", "bulletin", "market", "grid", "publication"]
    _BASE: str = "https://www.pse.pl"

    def fetch(self, max_pages: int = 3, since: date | None = None) -> list[dict]:
        try:
            import requests
            from bs4 import BeautifulSoup
        except ImportError:
            logger.warning("[po_doc] requests/bs4 not installed")
            return []

        session = requests.Session()
        session.headers.update(_HEADERS)

        docs: list[dict] = []
        seen_urls: set[str] = set()

        for base_url in self._LISTING_URLS:
            for page_num in range(1, max_pages + 1):
                if page_num == 1:
                    page_url = base_url
                elif "?" in base_url:
                    page_url = f"{base_url}&page={page_num}"
                else:
                    page_url = f"{base_url}?page={page_num}"

                try:
                    resp = session.get(page_url, timeout=30)
                    if resp.status_code == 404:
                        break
                    resp.raise_for_status()
                    soup = BeautifulSoup(resp.text, "html.parser")
                except Exception as exc:
                    logger.debug("[po_doc:%s] page %s fetch failed: %s", self.source, page_url, exc)
                    break

                page_docs = self._extract_docs_from_page(soup, session, seen_urls)
                if not page_docs:
                    break

                if since is not None:
                    dated = [d for d in page_docs if d.get("published_date")]
                    if dated and all(d["published_date"] < since for d in dated):
                        break

                docs.extend(page_docs)

                has_next = bool(soup.find("a", string=re.compile(r"next|»|›|następn", re.I)))
                if not has_next and page_num > 1:
                    break

                time.sleep(1.0)

        return docs

    def _extract_docs_from_page(self, soup, session, seen_urls: set[str]) -> list[dict]:
        page_docs: list[dict] = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            title = link.get_text(" ", strip=True)

            if not title or len(title) < 8:
                continue

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
                "doc_type": doc_type,
                "title": title[:250],
                "url": href,
                "published_date": published_date,
                "content": f"{self.source} — {title}\n\n{content[:80_000]}",
            })
            time.sleep(0.5)

        return page_docs


# ─────────────────────────────────────────────────────────────────────────────
# Concrete document connectors
# ─────────────────────────────────────────────────────────────────────────────

class PSEBalancingReportsConnector(PolishMarketDocConnector):
    """PSE (Polish TSO) balancing market reports and publications."""
    source = "pse_balancing"
    _LISTING_URLS = [
        "https://www.pse.pl/web/pse-eng/areas-of-activity/market/balancing-market",
        "https://www.pse.pl/web/pse-eng/areas-of-activity/market/market-publications",
    ]
    _DOC_KEYWORDS = [
        "balancing", "report", "bulletin", "market publication", "fcr", "afrr",
        "reserve", "settlement", "price", "procurement", "constraint",
    ]
    _BASE = "https://www.pse.pl"


class PSEGridReportsConnector(PolishMarketDocConnector):
    """PSE transmission grid data, congestion reports, and operational publications."""
    source = "pse_grid"
    _LISTING_URLS = [
        "https://www.pse.pl/web/pse-eng/areas-of-activity/transmission-system",
        "https://www.pse.pl/web/pse-eng/areas-of-activity/market/capacity-market",
    ]
    _DOC_KEYWORDS = [
        "grid", "transmission", "capacity", "auction", "rynek mocy",
        "congestion", "interconnection", "cross-border", "grid plan",
    ]
    _BASE = "https://www.pse.pl"


class PSEAFRRDataConnector(PolishMarketDocConnector):
    """PSE aFRR/FCR tender results and procurement notices."""
    source = "pse_afrr"
    _LISTING_URLS = [
        "https://www.pse.pl/web/pse-eng/areas-of-activity/market/ancillary-services",
        "https://www.pse.pl/web/pse-eng/news/-/news",
    ]
    _DOC_KEYWORDS = [
        "afrr", "fcr", "primary reserve", "secondary reserve", "ancillary",
        "frequency", "tender", "capacity procurement", "reserve market",
    ]
    _BASE = "https://www.pse.pl"


class TGEMarketReportsConnector(PolishMarketDocConnector):
    """TGE (Polish Power Exchange) market reports and statistics."""
    source = "tge_reports"
    _LISTING_URLS = [
        "https://tge.pl/raporty-i-biuletyny",
        "https://tge.pl/energia-elektryczna-rdn",
    ]
    _DOC_KEYWORDS = [
        "report", "biuletyn", "statistics", "rdn", "rdt", "market", "price",
        "electricity", "power", "gas", "bulletin", "monthly",
    ]
    _BASE = "https://tge.pl"


class UREPublicationsConnector(PolishMarketDocConnector):
    """URE (Polish energy regulator) decisions, licenses, and regulatory publications."""
    source = "ure_regulatory"
    _LISTING_URLS = [
        "https://www.ure.gov.pl/en/news",
        "https://www.ure.gov.pl/en/about-ure/publications",
    ]
    _DOC_KEYWORDS = [
        "license", "decision", "regulatory", "publication", "report", "energy",
        "electricity", "tariff", "bess", "storage", "renewable", "press release",
    ]
    _BASE = "https://www.ure.gov.pl"


class ENTSOEPublicationsConnector(PolishMarketDocConnector):
    """ENTSO-E news, reports, and publications relevant to Poland / CE region."""
    source = "entsoe_publications"
    _LISTING_URLS = [
        "https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html",
        "https://www.entsoe.eu/news/",
        "https://www.entsoe.eu/publications/",
    ]
    _DOC_KEYWORDS = [
        "poland", "ce region", "central europe", "fcr", "afrr", "frequency",
        "grid", "market report", "transparency", "balancing", "publication",
        "annual report", "winter outlook", "summer outlook",
    ]
    _BASE = "https://www.entsoe.eu"


# ─────────────────────────────────────────────────────────────────────────────
# Connector map
# ─────────────────────────────────────────────────────────────────────────────

_PO_DOC_CONNECTOR_MAP: dict[str, PolishMarketDocConnector] = {
    "pse_balancing":      PSEBalancingReportsConnector(),
    "pse_grid":           PSEGridReportsConnector(),
    "pse_afrr":           PSEAFRRDataConnector(),
    "tge_reports":        TGEMarketReportsConnector(),
    "ure_regulatory":     UREPublicationsConnector(),
    "entsoe_publications": ENTSOEPublicationsConnector(),
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _infer_date_from_text(text: str) -> date | None:
    """Try to extract a publication date from a filename or title string."""
    m = re.search(r"(\d{4})[-_./](\d{2})[-_./](\d{2})", text)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    months = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
        "jan": 1, "feb": 2, "mar": 3, "apr": 4,
        "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
        # Polish month abbreviations
        "sty": 1, "lut": 2, "mar": 3, "kwi": 4, "maj": 5, "cze": 6,
        "lip": 7, "sie": 8, "wrz": 9, "paz": 10, "lis": 11, "gru": 12,
    }
    m = re.search(
        r"(\d{1,2})\s+(" + "|".join(months) + r")\s+(\d{4})",
        text, re.IGNORECASE,
    )
    if m:
        try:
            return date(int(m.group(3)), months[m.group(2).lower()], int(m.group(1)))
        except ValueError:
            pass
    m = re.search(
        r"(" + "|".join(months) + r")\s+(\d{1,2}),?\s+(\d{4})",
        text, re.IGNORECASE,
    )
    if m:
        try:
            return date(int(m.group(3)), months[m.group(1).lower()], int(m.group(2)))
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
        logger.debug("[po_doc] PDF extract %s: %s", url, exc)
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
        logger.debug("[po_doc] page fetch %s: %s", url, exc)
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# Historical backfill convenience function
# ─────────────────────────────────────────────────────────────────────────────

def run_po_doc_backfill(
    conn,
    sources: list[str],
    start_date: date,
    prefix: str,
) -> dict[str, int]:
    """Backfill historical Polish market documents for *sources* since *start_date*.

    Returns {source: new_rows_inserted} for each source processed.
    """
    results: dict[str, int] = {}
    for src in sources:
        connector = _PO_DOC_CONNECTOR_MAP.get(src)
        if connector is None:
            logger.warning("[po_backfill] unknown source: %s", src)
            results[src] = 0
            continue
        logger.info("[po_backfill] backfilling %s since %s …", src, start_date)
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
            logger.info("[po_backfill] %s: %d new docs", src, n)
        except Exception as exc:
            logger.error("[po_backfill] %s failed: %s", src, exc)
            results[src] = -1

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Top-level convenience function (called by scheduler + manual trigger)
# ─────────────────────────────────────────────────────────────────────────────

def run_entso_price_scrape(conn, days_back: int = 1) -> dict[str, int]:
    """Scrape Polish day-ahead prices for the last *days_back* days.

    Returns {date_str: rows_inserted}.
    """
    scraper = ENTSOEPriceScraper()
    today = date.today()
    since = today - timedelta(days=days_back)

    logger.info("[entso_price] fetching %d day(s) from %s to %s", days_back, since, today - timedelta(days=1))
    try:
        page_data = scraper.fetch_range(since, today - timedelta(days=1))
    except Exception as exc:
        logger.error("[entso_price] fetch_range failed: %s", exc)
        return {}

    results: dict[str, int] = {}
    for date_str, records in page_data.items():
        if date_str < str(since):
            continue
        try:
            n = scraper.store_prices(conn, records)
            results[date_str] = n
        except Exception as exc:
            logger.error("[entso_price] store failed for %s: %s", date_str, exc)
            results[date_str] = -1

    # Ensure every requested date appears in results
    for i in range(1, days_back + 1):
        d = str(today - timedelta(days=i))
        if d not in results:
            results[d] = 0

    return results


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
    fcr_mw  = power_mw * fcr_pct  / 100.0
    afrr_mw = power_mw * afrr_pct / 100.0

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
        logger.warning("[get_as_revenue_estimate] DB query failed: %s", exc)

    result["total_pln_yr"] = (
        result["fcr_pln_yr"] + result["afrr_pln_yr"] + result["capacity_pln_yr"]
    )
    return result


def _pse_api_get(endpoint: str, params: dict, timeout: int = 20) -> list:
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
        logger.warning("[_pse_api_get] %s failed: %s", endpoint, exc)
        return []


def _iso_week_monday(week_offset: int) -> date:
    """Return the Monday of the week `week_offset` weeks ago."""
    from datetime import timedelta
    today = date.today()
    start = today - timedelta(days=today.weekday())
    return start - timedelta(weeks=week_offset)


def scrape_po_fcr_prices(conn, weeks_back: int = 52) -> int:
    """Fetch FCR weekly auction clearing prices from PSE API and store in po_as_prices.

    PSE endpoint: /rcr (Rezerwa Czestotliwosci Regulacyjnej)
    Expected response fields: data (date string), cena (PLN/MW/week), ilosc (MW)
    Returns number of rows inserted.
    """
    start = _iso_week_monday(weeks_back).isoformat()
    end   = date.today().isoformat()

    records = _pse_api_get(
        "rcr",
        {"$filter": f"data ge '{start}' and data le '{end}'", "$top": 1000},
    )

    if not records:
        logger.warning(
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
        logger.warning("[scrape_po_fcr_prices] DB insert failed: %s", exc)

    return n


def scrape_po_afrr_prices(conn, weeks_back: int = 52) -> int:
    """Fetch aFRR capacity weekly auction prices from PSE API and store in po_as_prices.

    PSE endpoint: /rar2 (Rezerwa Automatycznej Regulacji 2)
    Expected response fields: data (date string), cena_mocy (PLN/MW/week), ilosc (MW)
    Returns number of rows inserted.
    """
    start = _iso_week_monday(weeks_back).isoformat()
    end   = date.today().isoformat()

    records = _pse_api_get(
        "rar2",
        {"$filter": f"data ge '{start}' and data le '{end}'", "$top": 1000},
    )

    if not records:
        logger.warning(
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
        logger.warning("[scrape_po_afrr_prices] DB insert failed: %s", exc)

    return n


def scrape_po_capacity_market(conn) -> int:
    """Scrape TGE Rynek Mocy annual auction results into po_capacity_market.

    Source: https://tge.pl/rynek-mocy/wyniki-aukcji
    If the page structure changes, inspect the HTML table column order.
    Returns number of rows inserted.
    """
    import re
    import requests
    from bs4 import BeautifulSoup

    url = "https://tge.pl/rynek-mocy/wyniki-aukcji"
    try:
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("[scrape_po_capacity_market] HTTP failed: %s", exc)
        return 0

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if not table:
        logger.warning("[scrape_po_capacity_market] No table found at TGE page")
        return 0

    def _parse_number(text: str):
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
        logger.warning("[scrape_po_capacity_market] DB insert failed: %s", exc)

    return n
