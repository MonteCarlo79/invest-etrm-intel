"""Modo Energy research/insights scraper and API probe.

Two ingestion paths:
 1. API probe  — tries undocumented Modo API endpoints that may expose qualitative
                 research content (insights, reports). Gracefully skips 403/404.
 2. Web scrape — fetches https://modoenergy.com/research (and fallback URLs)
                 to capture market outlooks, technology reports, and policy analysis
                 not already ingested via the market-data pipeline.

All documents are stored with source="modo", doc_type="article" or "report".
"""

from __future__ import annotations

import logging
import os
import re
import time
from datetime import date, datetime
from typing import Iterator

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from services.gb_knowledge.base import BaseConnector, ensure_table, get_db_conn
from services.modo_energy.client import ModoClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; BESSPlatformBot/1.0)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}

# Undocumented API endpoint paths to probe for qualitative content.
# These are tried opportunistically; 404/403 are silently skipped.
_API_PROBE_PATHS = [
    "/gb/modo/insights",
    "/gb/modo/research",
    "/gb/modo/reports",
    "/insights",
    "/research",
    "/reports",
    "/blog",
    "/articles",
]

# Candidate public website URLs (Modo's site may have changed structure).
# We try each in order and use the first that returns content.
_WEBSITE_LISTING_URLS = [
    ("article", "https://modoenergy.com/research"),
    ("article", "https://modoenergy.com/blog"),
    ("article", "https://modoenergy.com/insights"),
    ("report",  "https://modoenergy.com/resources"),
    ("report",  "https://modoenergy.com/reports"),
]

MAX_PAGES = 5
ARTICLE_SLEEP = 1.0
REQUEST_TIMEOUT = 30

# ---------------------------------------------------------------------------
# Selector chains — ordered by specificity
# ---------------------------------------------------------------------------

LISTING_CARD_SELECTORS = [
    # React/Next.js apps often use data attributes or specific class prefixes
    ("article", {}),
    ("div", {"class": re.compile(r"research-item|insight-card|blog-card|post-card|article-card")}),
    ("div", {"class": re.compile(r"resource-item|report-card|content-card")}),
    ("div", {"class": re.compile(r"post|article|blog-post|entry")}),
    ("li",  {"class": re.compile(r"post|article|insight|resource")}),
    # Generic "card" pattern common in Tailwind/component library sites
    ("div", {"class": re.compile(r"card")}),
]

BODY_SELECTORS = [
    ("div", {"class": re.compile(r"entry-content|post-content|article-content")}),
    ("div", {"class": re.compile(r"research-body|insight-body|blog-body")}),
    ("div", {"class": re.compile(r"prose|article-body|content-body")}),   # Tailwind prose
    ("div", {"class": re.compile(r"page-content|main-content")}),
    ("main", {}),
    ("article", {}),
]

DATE_META_SELECTORS = [
    ("time", {}),
    ("span", {"class": re.compile(r"date|published|post-date|entry-date")}),
    ("p",   {"class": re.compile(r"date|published|post-date")}),
    ("div", {"class": re.compile(r"date|published")}),
]

_URL_DATE_RE = re.compile(
    r"/(?P<y>20\d{2})[/-](?P<m>0[1-9]|1[0-2])(?:[/-](?P<d>[0-3]\d))?/"
)


# ---------------------------------------------------------------------------
# Helpers (same utilities as timera.py, kept local to avoid coupling)
# ---------------------------------------------------------------------------

def _parse_date(text: str) -> date | None:
    text = text.strip()
    formats = [
        "%B %d, %Y",
        "%d %B %Y",
        "%b %d, %Y",
        "%d %b %Y",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y/%m/%d",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _date_from_url(url: str) -> date | None:
    m = _URL_DATE_RE.search(url)
    if not m:
        return None
    y = int(m.group("y"))
    mo = int(m.group("m"))
    d = int(m.group("d")) if m.group("d") else 1
    try:
        return date(y, mo, d)
    except ValueError:
        return None


def _clean_text(soup_element) -> str:
    if soup_element is None:
        return ""
    for tag in soup_element.find_all(["script", "style", "nav", "header",
                                       "footer", "aside", "form", "noscript",
                                       "iframe", "button", "svg"]):
        tag.decompose()
    parts: list[str] = []
    for elem in soup_element.find_all(
            ["p", "h1", "h2", "h3", "h4", "li", "blockquote"]):
        txt = elem.get_text(" ", strip=True)
        if txt:
            parts.append(txt)
    if parts:
        text = "\n\n".join(parts)
    else:
        text = soup_element.get_text(" ", strip=True)
    lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text.splitlines()]
    text = "\n".join(ln for ln in lines if ln)
    return text.encode("utf-8", errors="replace").decode("utf-8")


# ---------------------------------------------------------------------------
# Main connector
# ---------------------------------------------------------------------------

class ModoReportsConnector(BaseConnector):
    """Ingests Modo Energy qualitative research via API probe + web scrape."""

    source = "modo"

    def __init__(self, api_key: str | None = None):
        self._api_key = api_key or os.environ.get("MODO_API_KEY", "")
        self._session = requests.Session()
        self._session.headers.update(HEADERS)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fetch(self) -> Iterator[dict]:
        """Yield one dict per research document."""
        yield from self._fetch_via_api()
        yield from self._fetch_via_website()

    # ------------------------------------------------------------------
    # Path 1: API probe
    # ------------------------------------------------------------------

    def _fetch_via_api(self) -> Iterator[dict]:
        """Try undocumented Modo API paths; skip gracefully on 4xx errors."""
        if not self._api_key:
            logger.info("[modo] No API key — skipping API probe")
            return

        try:
            client = ModoClient(self._api_key)
        except Exception as exc:
            logger.warning("[modo] Could not initialise ModoClient: %s", exc)
            return

        for path in _API_PROBE_PATHS:
            logger.info("[modo] Probing API path: %s", path)
            try:
                records = client.get(path)
            except requests.HTTPError as exc:
                code = exc.response.status_code if exc.response is not None else 0
                if code in (400, 403, 404, 405):
                    logger.debug("[modo] API path %s returned %s — skipping", path, code)
                    continue
                logger.warning("[modo] API path %s HTTP error: %s", path, exc)
                continue
            except Exception as exc:
                logger.warning("[modo] API path %s error: %s", path, exc)
                continue

            if not records:
                continue

            logger.info("[modo] API path %s returned %d records", path, len(records))
            for rec in records:
                doc = self._api_record_to_doc(rec, path)
                if doc:
                    yield doc

    def _api_record_to_doc(self, rec: dict, path: str) -> dict | None:
        """Convert a raw API record to a knowledge doc dict."""
        # Field names vary by endpoint; try common patterns
        title = (rec.get("title") or rec.get("name") or rec.get("headline") or "").strip()

        # Content: prefer longer fields
        content_candidates = [
            rec.get("content") or "",
            rec.get("body") or "",
            rec.get("description") or "",
            rec.get("summary") or "",
            rec.get("excerpt") or "",
        ]
        content = max(content_candidates, key=len).strip()

        if not content and not title:
            return None

        # Build minimal content if only a title is available
        if not content:
            content = title

        url = (rec.get("url") or rec.get("link") or rec.get("permalink") or "").strip() or None

        # Date
        published_date: date | None = None
        for field in ("published_at", "published_date", "date", "created_at",
                      "updated_at", "publish_date"):
            raw = rec.get(field)
            if raw:
                d = _parse_date(str(raw)[:10])
                if d:
                    published_date = d
                    break

        # doc_type
        doc_type = "report" if "report" in path or "research" in path else "article"

        # Strip HTML if present in API response
        if "<" in content:
            soup = BeautifulSoup(content, "html.parser")
            content = _clean_text(soup)

        content = content.encode("utf-8", errors="replace").decode("utf-8")

        return {
            "doc_type": doc_type,
            "title": title,
            "url": url,
            "published_date": published_date,
            "content": content,
        }

    # ------------------------------------------------------------------
    # Path 2: Website scrape
    # ------------------------------------------------------------------

    def _fetch_via_website(self) -> Iterator[dict]:
        """Scrape public Modo Energy website for research/blog content."""
        attempted: set[str] = set()

        for doc_type, base_url in _WEBSITE_LISTING_URLS:
            # Avoid scraping the same resolved URL twice (after redirects)
            canonical = base_url.rstrip("/")
            if canonical in attempted:
                continue

            logger.info("[modo] Scraping listing: %s", base_url)
            attempted.add(canonical)

            yield from self._fetch_section(doc_type, base_url)

    def _fetch_section(self, doc_type: str, base_url: str) -> Iterator[dict]:
        seen_urls: set[str] = set()

        for page_num in range(1, MAX_PAGES + 1):
            # Try both WordPress-style pagination and query-param style
            if page_num == 1:
                listing_url = base_url
            else:
                # Try /page/N/ first; fall back to ?page=N for non-WP sites
                listing_url = f"{base_url.rstrip('/')}/page/{page_num}/"

            logger.info("[modo] Fetching listing page: %s", listing_url)
            soup = self._get_html(listing_url)

            if soup is None:
                # If page 1 also fails, stop this section entirely
                if page_num == 1:
                    logger.info("[modo] Listing %s not accessible — skipping", base_url)
                break

            # Detect "not found" pages that return 200 but show 404 content
            page_text_lower = (soup.find("body") or soup).get_text().lower()
            if page_num > 1 and ("page not found" in page_text_lower or
                                  "404" in page_text_lower[:500]):
                break

            article_links = self._extract_article_links(soup, base_url)
            if not article_links:
                if page_num == 1:
                    logger.info("[modo] No article links on page 1 of %s — trying inline extraction", base_url)
                    # Some SPA/React sites render content inline; try extracting from page directly
                    doc = self._extract_inline_content(soup, base_url, doc_type)
                    if doc:
                        yield doc
                break

            new_links = [lnk for lnk in article_links if lnk not in seen_urls]
            if not new_links:
                logger.info("[modo] All links already seen at page %d — stopping", page_num)
                break

            for url in new_links:
                seen_urls.add(url)
                doc = self._fetch_article(url, doc_type)
                if doc:
                    yield doc
                time.sleep(ARTICLE_SLEEP)

    # ------------------------------------------------------------------
    # Listing-page parsing
    # ------------------------------------------------------------------

    def _extract_article_links(self, soup: BeautifulSoup, section_url: str) -> list[str]:
        links: list[str] = []
        base_domain = self._domain(section_url)

        # Strategy 1: article card containers
        for tag, attrs in LISTING_CARD_SELECTORS:
            cards = soup.find_all(tag, attrs or True) if attrs else soup.find_all(tag)
            if cards:
                for card in cards:
                    a = card.find("a", href=True)
                    if a:
                        href = a["href"].strip()
                        if self._is_article_url(href, section_url, base_domain):
                            links.append(self._absolute(href, base_domain))
                if links:
                    return self._deduplicate(links)

        # Strategy 2: all <a> tags matching article permalink pattern
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if self._is_article_url(href, section_url, base_domain):
                links.append(self._absolute(href, base_domain))

        return self._deduplicate(links)

    def _is_article_url(self, href: str, section_url: str, domain: str) -> bool:
        if not href or href.startswith("#") or href.startswith("mailto:"):
            return False
        if href.startswith("http") and domain not in href:
            return False
        normalised = href.rstrip("/")
        section_norm = section_url.rstrip("/")
        if normalised == section_norm:
            return False
        if re.search(r"/page/\d+", href):
            return False
        bad_patterns = ("/tag/", "/category/", "/author/", "/feed/",
                        "/wp-content/", "/wp-admin/", "/cdn-cgi/",
                        "/login", "/signup", "/pricing", "/contact")
        return not any(p in href for p in bad_patterns)

    @staticmethod
    def _domain(url: str) -> str:
        m = re.match(r"https?://([^/]+)", url)
        return m.group(1) if m else "modoenergy.com"

    def _absolute(self, href: str, domain: str) -> str:
        if href.startswith("http"):
            return href
        return f"https://{domain}{href}"

    @staticmethod
    def _deduplicate(lst: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for item in lst:
            if item not in seen:
                seen.add(item)
                out.append(item)
        return out

    # ------------------------------------------------------------------
    # Single article fetch
    # ------------------------------------------------------------------

    def _fetch_article(self, url: str, doc_type: str) -> dict | None:
        logger.info("[modo] Fetching article: %s", url)
        soup = self._get_html(url)
        if soup is None:
            return None

        title = self._extract_title(soup)
        published_date = self._extract_date(soup, url)
        content = self._extract_body(soup)

        if not content:
            logger.warning("[modo] Empty content for %s", url)
            return None

        return {
            "doc_type": doc_type,
            "title": title,
            "url": url,
            "published_date": published_date,
            "content": content,
        }

    def _extract_inline_content(self, soup: BeautifulSoup, url: str, doc_type: str) -> dict | None:
        """Fallback for SPA pages: try to extract meaningful text from the whole page."""
        title = self._extract_title(soup)
        published_date = _date_from_url(url)
        content = self._extract_body(soup)
        if not content or len(content) < 200:
            return None
        return {
            "doc_type": doc_type,
            "title": title,
            "url": url,
            "published_date": published_date,
            "content": content,
        }

    def _extract_title(self, soup: BeautifulSoup) -> str:
        for tag, attrs in [
            ("h1", {"class": re.compile(r"entry-title|post-title|page-title|article-title")}),
            ("h1", {}),
            ("h2", {"class": re.compile(r"entry-title|post-title|article-title")}),
        ]:
            el = soup.find(tag, attrs or True) if attrs else soup.find(tag)
            if el:
                return el.get_text(" ", strip=True)
        t = soup.find("title")
        if t:
            # Strip site name suffix e.g. "Article Title | Modo Energy"
            raw = t.get_text(" ", strip=True)
            return re.split(r"\s*[|–—]\s*Modo", raw)[0].strip()
        return ""

    def _extract_date(self, soup: BeautifulSoup, url: str) -> date | None:
        # 1. <time datetime="…">
        time_el = soup.find("time")
        if time_el:
            dt_attr = time_el.get("datetime", "")
            if dt_attr:
                d = _parse_date(dt_attr[:10])
                if d:
                    return d
            d = _parse_date(time_el.get_text(strip=True))
            if d:
                return d

        # 2. Meta tags
        for meta_name in ("article:published_time", "datePublished", "date"):
            meta = soup.find("meta", {"property": meta_name}) or \
                   soup.find("meta", {"name": meta_name}) or \
                   soup.find("meta", {"itemprop": meta_name})
            if meta and meta.get("content"):
                d = _parse_date(meta["content"][:10])
                if d:
                    return d

        # 3. JSON-LD structured data
        for script in soup.find_all("script", {"type": "application/ld+json"}):
            try:
                import json
                data = json.loads(script.string or "{}")
                for field in ("datePublished", "dateCreated", "dateModified"):
                    if field in data:
                        d = _parse_date(str(data[field])[:10])
                        if d:
                            return d
            except Exception:
                pass

        # 4. Visible date elements
        for tag, attrs in DATE_META_SELECTORS:
            els = soup.find_all(tag, attrs or True) if attrs else soup.find_all(tag)
            for el in els:
                txt = el.get_text(strip=True)
                d = _parse_date(txt)
                if d:
                    return d

        # 5. URL slug
        return _date_from_url(url)

    def _extract_body(self, soup: BeautifulSoup) -> str:
        for tag, attrs in BODY_SELECTORS:
            el = soup.find(tag, attrs or True) if attrs else soup.find(tag)
            if el:
                text = _clean_text(el)
                if len(text) > 100:
                    return text
        body = soup.find("body")
        return _clean_text(body) if body else ""

    # ------------------------------------------------------------------
    # HTTP helper
    # ------------------------------------------------------------------

    def _get_html(self, url: str) -> BeautifulSoup | None:
        try:
            resp = self._session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        except requests.RequestException as exc:
            logger.warning("[modo] Request error for %s: %s", url, exc)
            return None
        if resp.status_code != 200:
            logger.warning("[modo] HTTP %s for %s", resp.status_code, url)
            return None
        resp.encoding = resp.apparent_encoding or "utf-8"
        return BeautifulSoup(resp.text, "html.parser")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    load_dotenv()

    parser = argparse.ArgumentParser(description="Scrape Modo Energy research/insights")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print articles without writing to DB")
    parser.add_argument("--skip-api", action="store_true",
                        help="Skip API endpoint probing")
    parser.add_argument("--skip-web", action="store_true",
                        help="Skip website scraping")
    args = parser.parse_args()

    connector = ModoReportsConnector()

    if args.dry_run:
        count = 0
        for doc in connector.fetch():
            if args.skip_api and doc.get("_source") == "api":
                continue
            if args.skip_web and doc.get("_source") == "web":
                continue
            count += 1
            print(f"\n{'='*60}")
            print(f"[{count}] {doc['doc_type'].upper()} | {doc['published_date']} | {doc['title']}")
            print(f"    URL: {doc['url']}")
            print(f"    Content ({len(doc['content'])} chars): {doc['content'][:300]}...")
    else:
        conn = get_db_conn()
        ensure_table(conn)
        n = connector.run(conn)
        conn.close()
        print(f"Modo research scrape complete — {n} new documents inserted.")
