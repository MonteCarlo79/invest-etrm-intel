"""Elexon knowledge connector — fetches GB electricity market documents.

Two data sources:
  1. Elexon Insights API  — SYSWARN (system warnings) and NTO (national
     transmission operator notices) for the last N days.
  2. Elexon website news  — article titles, dates, and body text scraped
     from elexon.co.uk/news (latest 2 pages).

Usage (standalone):
    python -m services.gb_knowledge.elexon
"""

import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Iterator

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Allow running as a top-level script from the repo root
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from services.gb_knowledge.base import BaseConnector  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Elexon Insights API (public, no auth required).
# Official base documented at developer.data.elexon.co.uk
_API_BASE = "https://data.elexon.co.uk/bmrs/api/v1"

# Elexon website
_NEWS_BASE = "https://www.elexon.co.uk/news/"

_DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": (
        "BESS-Platform-GBKnowledge/1.0 "
        "(internal research tool; contact: ops@bess-platform.internal)"
    ),
}
_HTML_HEADERS = {
    "User-Agent": (
        "BESS-Platform-GBKnowledge/1.0 "
        "(internal research tool; contact: ops@bess-platform.internal)"
    ),
}

_REQUEST_TIMEOUT = 30   # seconds
_INTER_REQUEST_SLEEP = 0.5  # seconds — be polite to the server


# ---------------------------------------------------------------------------
# Elexon connector
# ---------------------------------------------------------------------------

class ElexonConnector(BaseConnector):
    """Fetches Elexon system notices and website news into the GB knowledge base."""

    source = "elexon"

    def __init__(self, lookback_days: int = 90, news_pages: int = 2):
        self.lookback_days = lookback_days
        self.news_pages = news_pages
        self._session = requests.Session()
        self._session.headers.update(_DEFAULT_HEADERS)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fetch(self) -> Iterator[dict]:
        """Yield document dicts compatible with BaseConnector.run()."""
        yield from self._fetch_syswarn()
        time.sleep(_INTER_REQUEST_SLEEP)
        yield from self._fetch_nto()
        time.sleep(_INTER_REQUEST_SLEEP)
        yield from self._fetch_news()

    # ------------------------------------------------------------------
    # Source 1a: SYSWARN — system warnings
    # ------------------------------------------------------------------

    def _fetch_syswarn(self) -> Iterator[dict]:
        """Yield system warning notices from the Insights API."""
        date_from, date_to = self._date_range()
        url = f"{_API_BASE}/datasets/SYSWARN"
        params = {
            "publishDateTimeFrom": date_from,
            "publishDateTimeTo": date_to,
            "format": "json",
        }
        try:
            data = self._api_get(url, params)
        except Exception as exc:
            print(f"[elexon] SYSWARN fetch failed: {exc}")
            return

        items = data if isinstance(data, list) else data.get("data", [])
        for item in items:
            try:
                yield self._parse_syswarn(item)
            except Exception as exc:
                print(f"[elexon] SYSWARN parse error ({exc}): {item!r:.120s}")
                continue

    def _parse_syswarn(self, item: dict) -> dict:
        publish_time_raw = (
            item.get("publishTime")
            or item.get("publishDateTime")
            or item.get("createdDateTime")
            or ""
        )
        pub_date = _parse_date(publish_time_raw)

        warning_type = item.get("warningType", item.get("notificationType", ""))
        message = (
            item.get("message")
            or item.get("warningText")
            or item.get("body")
            or item.get("notificationMessage")
            or ""
        )
        # Build a stable URL-like identifier from the publish time + type so
        # the upsert ON CONFLICT (url) deduplication works reliably.
        warn_id = (
            item.get("id")
            or item.get("warningId")
            or item.get("notificationId")
            or _stable_id(publish_time_raw, message)
        )
        url = f"https://data.elexon.co.uk/bmrs/api/v1/datasets/SYSWARN#{warn_id}"

        title = f"System Warning ({warning_type}): {pub_date}" if warning_type else f"System Warning: {pub_date}"
        content = _build_api_content(
            doc_type="System Warning",
            fields={
                "Warning Type": warning_type,
                "Published": publish_time_raw,
                "Message": message,
            },
        )
        return {
            "doc_type": "notice",
            "title": title,
            "url": url,
            "published_date": pub_date,
            "content": content,
        }

    # ------------------------------------------------------------------
    # Source 1b: NTO — National Transmission Operator notices
    # ------------------------------------------------------------------

    def _fetch_nto(self) -> Iterator[dict]:
        """Yield NTO notices from the Insights API."""
        date_from, date_to = self._date_range()
        url = f"{_API_BASE}/datasets/NTO"
        params = {
            "publishDateTimeFrom": date_from,
            "publishDateTimeTo": date_to,
            "format": "json",
        }
        try:
            data = self._api_get(url, params)
        except Exception as exc:
            print(f"[elexon] NTO fetch failed: {exc}")
            return

        items = data if isinstance(data, list) else data.get("data", [])
        for item in items:
            try:
                yield self._parse_nto(item)
            except Exception as exc:
                print(f"[elexon] NTO parse error ({exc}): {item!r:.120s}")
                continue

    def _parse_nto(self, item: dict) -> dict:
        publish_time_raw = (
            item.get("publishTime")
            or item.get("publishDateTime")
            or item.get("createdDateTime")
            or ""
        )
        pub_date = _parse_date(publish_time_raw)

        subject = (
            item.get("subjectLine")
            or item.get("subject")
            or item.get("title")
            or f"NTO Notice: {pub_date}"
        )
        body = (
            item.get("body")
            or item.get("message")
            or item.get("notificationMessage")
            or ""
        )
        notice_id = (
            item.get("id")
            or item.get("ntoId")
            or item.get("notificationId")
            or _stable_id(publish_time_raw, body)
        )
        url = f"https://data.elexon.co.uk/bmrs/api/v1/datasets/NTO#{notice_id}"

        content = _build_api_content(
            doc_type="NTO Notice",
            fields={
                "Subject": subject,
                "Published": publish_time_raw,
                "Body": body,
            },
        )
        return {
            "doc_type": "notice",
            "title": subject[:500],
            "url": url,
            "published_date": pub_date,
            "content": content,
        }

    # ------------------------------------------------------------------
    # Source 2: Elexon website news articles
    # ------------------------------------------------------------------

    def _fetch_news(self) -> Iterator[dict]:
        """Scrape article links from elexon.co.uk/news then fetch each article."""
        article_links = self._scrape_news_index()
        for i, (href, index_title, index_date) in enumerate(article_links):
            time.sleep(_INTER_REQUEST_SLEEP)
            try:
                doc = self._fetch_article(href, index_title, index_date)
                if doc:
                    yield doc
            except Exception as exc:
                print(f"[elexon] Article fetch error {href}: {exc}")
                continue

    def _scrape_news_index(self) -> list[tuple[str, str, date | None]]:
        """Return list of (url, title, date) from the news index pages."""
        links: list[tuple[str, str, date | None]] = []
        for page_num in range(1, self.news_pages + 1):
            url = _NEWS_BASE if page_num == 1 else f"{_NEWS_BASE}page/{page_num}/"
            try:
                resp = self._html_get(url)
            except Exception as exc:
                print(f"[elexon] News index page {page_num} failed: {exc}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            found = _extract_article_links(soup, url)
            links.extend(found)
            print(f"[elexon] News index page {page_num}: found {len(found)} articles")
            time.sleep(_INTER_REQUEST_SLEEP)

        # Deduplicate by URL while preserving order
        seen: set[str] = set()
        unique: list[tuple[str, str, date | None]] = []
        for item in links:
            if item[0] not in seen:
                seen.add(item[0])
                unique.append(item)
        return unique

    def _fetch_article(
        self, url: str, fallback_title: str, fallback_date: date | None
    ) -> dict | None:
        """Fetch and parse a single news article page."""
        resp = self._html_get(url)
        soup = BeautifulSoup(resp.text, "html.parser")

        title = _extract_article_title(soup) or fallback_title
        pub_date = _extract_article_date(soup) or fallback_date
        content = _extract_article_body(soup)

        if not content:
            return None

        return {
            "doc_type": "article",
            "title": title[:500],
            "url": url,
            "published_date": pub_date,
            "content": content,
        }

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _api_get(self, url: str, params: dict) -> dict | list:
        resp = self._session.get(url, params=params, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def _html_get(self, url: str) -> requests.Response:
        resp = self._session.get(
            url,
            headers=_HTML_HEADERS,
            timeout=_REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        resp.raise_for_status()
        return resp

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def _date_range(self) -> tuple[str, str]:
        """ISO 8601 UTC datetime strings for the lookback window."""
        now = datetime.now(tz=timezone.utc)
        date_from = (now - timedelta(days=self.lookback_days)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        date_to = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        return date_from, date_to


# ---------------------------------------------------------------------------
# HTML extraction helpers
# ---------------------------------------------------------------------------

def _extract_article_links(
    soup: BeautifulSoup, base_url: str
) -> list[tuple[str, str, date | None]]:
    """Extract (url, title, date) tuples from a news index page.

    Elexon's news listing uses article cards.  We look for common patterns:
      - <article> elements containing <a> and <time>
      - <h2>/<h3> with <a> inside a listing context
    This is intentionally broad so minor template changes don't break it.
    """
    results: list[tuple[str, str, date | None]] = []
    from urllib.parse import urljoin

    # Strategy 1: <article> cards
    for article in soup.find_all("article"):
        a_tag = article.find("a", href=True)
        if not a_tag:
            continue
        href = urljoin(base_url, a_tag["href"])
        # Skip pagination / category links — must look like a news article URL
        if not _looks_like_article(href):
            continue
        title = _text(a_tag)
        # Try to find a <time> element for the date
        time_tag = article.find("time")
        pub_date = _parse_date(time_tag.get("datetime", "") if time_tag else "")
        if not title:
            title = href
        results.append((href, title, pub_date))

    if results:
        return results

    # Strategy 2: heading links in a list/grid context
    for heading in soup.find_all(["h2", "h3"]):
        a_tag = heading.find("a", href=True)
        if not a_tag:
            continue
        href = urljoin(base_url, a_tag["href"])
        if not _looks_like_article(href):
            continue
        title = _text(a_tag) or _text(heading)
        # Look for a sibling/parent time element
        parent = heading.parent
        time_tag = parent.find("time") if parent else None
        pub_date = _parse_date(time_tag.get("datetime", "") if time_tag else "")
        results.append((href, title, pub_date))

    return results


def _looks_like_article(url: str) -> bool:
    """Heuristic: reject pagination, category, and anchor-only links."""
    if not url.startswith("http"):
        return False
    if url.rstrip("/").endswith("/news"):
        return False
    # Must be under elexon.co.uk
    if "elexon.co.uk" not in url:
        return False
    # Skip pure category/tag pages that have no date-like slug
    skip_patterns = ["/category/", "/tag/", "/author/", "/page/", "#"]
    return not any(p in url for p in skip_patterns)


def _extract_article_title(soup: BeautifulSoup) -> str:
    """Best-effort article title extraction."""
    # Prefer OG meta tag — most reliable across CMS changes
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return og["content"].strip()
    h1 = soup.find("h1")
    if h1:
        return _text(h1)
    title_tag = soup.find("title")
    if title_tag:
        return _text(title_tag)
    return ""


def _extract_article_date(soup: BeautifulSoup) -> date | None:
    """Best-effort published date extraction."""
    # 1. Schema.org datePublished
    meta = soup.find("meta", {"property": "article:published_time"})
    if meta and meta.get("content"):
        return _parse_date(meta["content"])
    # 2. <time> element with datetime attribute
    time_tag = soup.find("time", {"datetime": True})
    if time_tag:
        return _parse_date(time_tag["datetime"])
    # 3. JSON-LD
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        import json
        try:
            ld = json.loads(script.string or "")
            if isinstance(ld, dict):
                raw = ld.get("datePublished") or ld.get("dateCreated")
                if raw:
                    return _parse_date(raw)
        except Exception:
            pass
    return None


def _extract_article_body(soup: BeautifulSoup) -> str:
    """Extract main readable text from an article page."""
    # Remove boilerplate elements
    for tag in soup.find_all(["nav", "header", "footer", "script", "style",
                               "noscript", "aside", "form"]):
        tag.decompose()

    # Try common content containers (in priority order)
    candidates = [
        soup.find("article"),
        soup.find("div", {"class": lambda c: c and "entry-content" in c}),
        soup.find("div", {"class": lambda c: c and "post-content" in c}),
        soup.find("div", {"class": lambda c: c and "article-content" in c}),
        soup.find("main"),
        soup.find("div", {"id": "content"}),
        soup.find("div", {"role": "main"}),
    ]
    container = next((c for c in candidates if c is not None), None)

    if container:
        text = container.get_text(separator="\n", strip=True)
    else:
        text = soup.get_text(separator="\n", strip=True)

    # Collapse excessive blank lines
    lines = [line.strip() for line in text.splitlines()]
    cleaned: list[str] = []
    prev_blank = False
    for line in lines:
        if not line:
            if not prev_blank:
                cleaned.append("")
            prev_blank = True
        else:
            cleaned.append(line)
            prev_blank = False
    return "\n".join(cleaned).strip()


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def _parse_date(raw: str) -> date | None:
    """Parse an ISO 8601 or date-only string to a Python date."""
    if not raw:
        return None
    # Truncate to the date part (handles "2024-03-15T10:30:00Z" etc.)
    date_part = raw[:10]
    try:
        return date.fromisoformat(date_part)
    except (ValueError, TypeError):
        return None


def _text(tag) -> str:
    """Clean text content from a BS4 tag."""
    if tag is None:
        return ""
    return tag.get_text(separator=" ", strip=True)


def _stable_id(publish_time: str, message: str) -> str:
    """Generate a short stable identifier when no API id field is present."""
    import hashlib
    raw = f"{publish_time}|{message[:200]}"
    return hashlib.sha1(raw.encode()).hexdigest()[:12]


def _build_api_content(doc_type: str, fields: dict) -> str:
    """Format an API notice as readable plain text for storage."""
    lines = [f"[{doc_type}]", ""]
    for key, value in fields.items():
        if value:
            lines.append(f"{key}: {value}")
    return "\n".join(lines).strip()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv(
        os.path.join(os.path.dirname(__file__), "..", "..", "config", ".env")
    )

    from services.gb_knowledge.base import get_db_conn, ensure_table  # noqa: E402

    conn = get_db_conn()
    ensure_table(conn)
    n = ElexonConnector().run(conn)
    print(f"Inserted {n} new documents")
