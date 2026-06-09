"""ENTSO-E / PSE data scrapers for the Poland market app.

Two components:
  ENTSOEPriceScraper        -- scrapes day-ahead prices from PSE.pl (primary) or ENTSO-E
                               API (optional, requires ENTSOE_API_KEY env var) and stores
                               them in intl_market.po_day_ahead_prices
  PolishMarketDocConnector  -- base class for downloading PSE/TGE/URE/ENTSO-E publications
                               into intl_market.po_knowledge_docs

Data sources:
  PSE.pl:   https://www.pse.pl  (day-ahead CSV + balancing/grid reports)
  TGE:      https://tge.pl
  URE:      https://www.ure.gov.pl
  ENTSO-E:  https://transparency.entsoe.eu (optional API; web scraping for publications)

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

_PSE_BASE     = "https://www.pse.pl"
_TGE_BASE     = "https://tge.pl"
_URE_BASE     = "https://www.ure.gov.pl"
_ENTSOE_BASE  = "https://transparency.entsoe.eu"
_ENTSOE_WEBAPI = "https://web-api.tp.entsoe.eu/api"

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

    1. PSE.pl CSV export (primary, no auth) — URL pattern:
       https://www.pse.pl/getcsv/-/export/csv/PL_CENY_RDN/data_od/{start}/data_do/{end}
       CSV columns: Data, Godzina, CRO (PLN/MWh)

    2. ENTSO-E Transparency API (secondary, requires ENTSOE_API_KEY env var):
       documentType=A44, bidding zone 10YPL-AREA-----S, hourly resolution
       Returns EUR/MWh; converted to PLN using EUR_PLN env var or fallback 4.25.
    """

    # ── Public API ────────────────────────────────────────────────────────────

    def fetch_range(self, start: date, end: date) -> dict[str, list[dict]]:
        """Fetch day-ahead prices for [start, end] inclusive.

        Returns {date_str: [hourly records]}.
        """
        data = self._fetch_pse_csv(start, end)
        if not data:
            logger.info("[entso_price] PSE CSV empty, trying ENTSO-E API")
            data = self._fetch_entsoe_api(start, end)
        return data

    def fetch_daily_prices(self, target_date: Optional[date] = None) -> list[dict]:
        """Return hourly records for target_date (default: yesterday)."""
        if target_date is None:
            target_date = date.today() - timedelta(days=1)
        data = self.fetch_range(target_date, target_date)
        return data.get(str(target_date), [])

    # ── PSE CSV strategy ──────────────────────────────────────────────────────

    def _fetch_pse_csv(self, start: date, end: date) -> dict[str, list[dict]]:
        try:
            import requests
        except ImportError:
            return {}

        url = (
            f"{_PSE_BASE}/getcsv/-/export/csv/PL_CENY_RDN"
            f"/data_od/{start.strftime('%Y-%m-%d')}"
            f"/data_do/{end.strftime('%Y-%m-%d')}"
        )
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=30)
            resp.raise_for_status()
            content_type = resp.headers.get("Content-Type", "")
            # PSE can return a login redirect or empty file instead of CSV
            if len(resp.content) < 50 or b"<!DOCTYPE" in resp.content[:200]:
                logger.debug("[pse_csv] Response looks like HTML redirect, not CSV")
                return {}
        except Exception as exc:
            logger.warning("[pse_csv] fetch failed: %s", exc)
            return {}

        return self._parse_pse_csv(resp.text, source="pse_csv")

    def _parse_pse_csv(self, csv_text: str, source: str = "pse_csv") -> dict[str, list[dict]]:
        """Parse PSE day-ahead price CSV.

        Expected columns (Polish): Data, Godzina, CRO
        Also handles semicolon-separated format common on PSE exports.
        """
        import io
        import pandas as pd

        try:
            # Try semicolon separator first (PSE typical), then comma
            for sep in (";", ","):
                try:
                    df = pd.read_csv(io.StringIO(csv_text), sep=sep, dtype=str)
                    if df.shape[1] >= 2:
                        break
                except Exception:
                    continue
            else:
                return {}
        except Exception:
            return {}

        if df.empty:
            return {}

        # Normalise column names: strip whitespace and lowercase
        df.columns = [c.strip().lower() for c in df.columns]

        # Map to canonical names
        col_map: dict[str, str] = {}
        for col in df.columns:
            if col in ("data", "date"):
                col_map[col] = "date"
            elif col in ("godzina", "hour", "godz"):
                col_map[col] = "hour"
            elif col in ("cro", "cena", "price", "price_pln_mwh"):
                col_map[col] = "price_pln"
        df = df.rename(columns=col_map)

        if "date" not in df.columns or "price_pln" not in df.columns:
            logger.debug("[pse_csv] unexpected columns: %s", list(df.columns))
            return {}

        results: dict[str, list[dict]] = {}
        for _, row in df.iterrows():
            try:
                date_str = str(row["date"]).strip()
                # Accept YYYY-MM-DD or YYYY.MM.DD
                date_str = date_str.replace(".", "-")
                trading_date = date.fromisoformat(date_str[:10])
                hour = int(str(row.get("hour", 0)).strip()) if "hour" in df.columns else 0
                # PSE hours are 1-24; normalise to 0-23
                if hour == 24:
                    hour = 23
                elif hour > 0:
                    hour = hour - 1
                price_str = str(row["price_pln"]).strip().replace(",", ".")
                price_pln = float(price_str)
                record = {
                    "trading_date": trading_date,
                    "hour": hour,
                    "price_pln_mwh": round(price_pln, 4),
                    "price_eur_mwh": round(price_pln / _EUR_PLN_FALLBACK, 4),
                    "source": source,
                }
                ds = str(trading_date)
                results.setdefault(ds, []).append(record)
            except Exception:
                continue

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
