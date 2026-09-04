"""Timera Energy blog/research scraper.

Scrapes https://timera-energy.com/blog/ and https://timera-energy.com/reports/
and stores articles as knowledge documents in gb_knowledge_docs.

Timera runs a standard WordPress installation. The listing pages use the
normal WP loop (<article> with class "post-…"), and the full article body
lives inside a <div class="entry-content"> / <div class="post-content">.

Pagination: /blog/page/2/, /blog/page/3/ … up to MAX_PAGES.
Rate limiting: 1 s sleep between individual article fetches.
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

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; BESSPlatformBot/1.0)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}

LISTING_SECTIONS = [
    ("article", "https://timera-energy.com/blog/"),
    ("report",  "https://timera-energy.com/publications/"),
]

# The publications page lists PDF documents rather than HTML articles.
# We parse the card metadata directly instead of following article links.
_PUBLICATIONS_URL = "https://timera-energy.com/publications/"

MAX_PAGES = 5
ARTICLE_SLEEP = 1.0          # seconds between article fetches
REQUEST_TIMEOUT = 30         # seconds per HTTP request

# ---------------------------------------------------------------------------
# Selector chains (ordered by specificity — first match wins)
# ---------------------------------------------------------------------------

# Selectors tried in order to find the list of article links on a listing page.
# Each entry is (tag, attrs_dict).  We collect <a href> from matching elements.
LISTING_CARD_SELECTORS = [
    ("article", {}),                              # WordPress <article class="post-…">
    ("div", {"class": re.compile(r"post-item|blog-item|article-card|entry-item")}),
    ("div", {"class": re.compile(r"post|blog-post|article")}),
    ("li",  {"class": re.compile(r"post|article")}),
]

# Selectors tried in order to locate the main body of a single article page.
BODY_SELECTORS = [
    ("div", {"class": re.compile(r"entry-content|post-content")}),
    ("div", {"class": re.compile(r"article-body|article-content|single-post-content")}),
    ("div", {"class": re.compile(r"content-area|page-content")}),
    ("main", {}),
    ("article", {}),
]

# Selectors for the published date on a single article page.
DATE_META_SELECTORS = [
    ("time", {}),                                 # <time datetime="…"> is standard WP
    ("span", {"class": re.compile(r"date|published|post-date|entry-date")}),
    ("p",   {"class": re.compile(r"date|published|post-date|entry-date")}),
]

# URL path patterns that encode dates, e.g. /2024/03/my-article/ or /2024-03-15-title
_URL_DATE_RE = re.compile(
    r"/(?P<y>20\d{2})[/-](?P<m>0[1-9]|1[0-2])(?:[/-](?P<d>[0-3]\d))?/"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date(text: str) -> date | None:
    """Attempt to parse a human-readable or ISO date string."""
    text = text.strip()
    formats = [
        "%B %d, %Y",    # March 15, 2024
        "%d %B %Y",     # 15 March 2024
        "%b %d, %Y",    # Mar 15, 2024
        "%d %b %Y",     # 15 Mar 2024
        "%Y-%m-%d",     # 2024-03-15
        "%d/%m/%Y",     # 15/03/2024
        "%m/%d/%Y",     # 03/15/2024
        "%Y/%m/%d",     # 2024/03/15
    ]
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _date_from_url(url: str) -> date | None:
    """Extract a date from a URL path like /2024/03/... or /2024/03/15/..."""
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
    """Return clean plain-text from a BeautifulSoup element.

    Strips <script>, <style>, <nav>, <header>, <footer> subtrees.
    Normalises whitespace but preserves paragraph breaks.
    """
    if soup_element is None:
        return ""

    # Remove noisy subtrees in-place on a copy
    for tag in soup_element.find_all(["script", "style", "nav", "header",
                                       "footer", "aside", "form", "noscript",
                                       "iframe", "button", "svg"]):
        tag.decompose()

    # Collect text paragraph by paragraph
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

    # Collapse excessive whitespace within lines
    lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text.splitlines()]
    text = "\n".join(ln for ln in lines if ln)
    return text.encode("utf-8", errors="replace").decode("utf-8")


# ---------------------------------------------------------------------------
# Main connector
# ---------------------------------------------------------------------------

class TimeraConnector(BaseConnector):
    """Scrapes Timera Energy blog and reports pages."""

    source = "timera"

    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update(HEADERS)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fetch(self) -> Iterator[dict]:
        """Yield one dict per article/report with keys expected by BaseConnector."""
        for doc_type, base_url in LISTING_SECTIONS:
            if "publications" in base_url:
                yield from self._fetch_publications_listing(base_url)
            else:
                yield from self._fetch_section(doc_type, base_url)

    # ------------------------------------------------------------------
    # Publications page — PDF card listing
    # ------------------------------------------------------------------

    def _fetch_publications_listing(self, base_url: str) -> Iterator[dict]:
        """Parse the Timera publications page which lists PDF documents.

        Each card contains: category tag, date, title (h2-h4), description (p),
        and a PDF link.  We build one knowledge doc per card from the card
        metadata; we do not attempt to read the PDF itself.
        """
        soup = self._get_html(base_url)
        if soup is None:
            logger.warning("[timera] Could not fetch publications page %s", base_url)
            return

        for pdf_link in soup.find_all("a", href=lambda h: h and ".pdf" in h.lower()):
            href = pdf_link.get("href", "").strip()
            if not href:
                continue

            # Walk up the DOM to find the enclosing card element.
            card = pdf_link
            for _ in range(8):
                parent = card.parent
                if parent is None:
                    break
                text_len = len(parent.get_text(strip=True))
                if text_len > 80:
                    card = parent
                    break
                card = parent

            # Title — prefer an explicit heading tag inside the card.
            title_el = (
                card.find("h2") or card.find("h3") or card.find("h4") or card.find("h5")
            )
            title = title_el.get_text(strip=True) if title_el else pdf_link.get_text(strip=True)
            if not title:
                continue

            # Date — look for a <time> tag or a text matching a date pattern.
            pub_date: date | None = None
            time_el = card.find("time")
            if time_el:
                raw = time_el.get("datetime", "") or time_el.get_text(strip=True)
                pub_date = _parse_date(raw[:20].strip())
            if pub_date is None:
                card_text = card.get_text(" ", strip=True)
                date_m = re.search(
                    r"\b(\d{1,2}\s+\w+\s+20\d{2}|\d{4}-\d{2}-\d{2})\b", card_text
                )
                if date_m:
                    pub_date = _parse_date(date_m.group(1))

            # Description — first <p> tag in the card (excluding the title heading).
            desc = ""
            for p in card.find_all("p"):
                txt = p.get_text(strip=True)
                if len(txt) > 20 and txt.lower() != title.lower():
                    desc = txt
                    break

            content = f"Timera Energy publication: {title}."
            if desc:
                content += f" {desc}"
            content += f" PDF: {href}"

            yield {
                "doc_type": "report",
                "title": title,
                "url": href,
                "published_date": pub_date,
                "content": content,
            }

    # ------------------------------------------------------------------
    # Section-level pagination
    # ------------------------------------------------------------------

    def _fetch_section(self, doc_type: str, base_url: str) -> Iterator[dict]:
        seen_urls: set[str] = set()

        for page_num in range(1, MAX_PAGES + 1):
            listing_url = base_url if page_num == 1 else f"{base_url}page/{page_num}/"
            logger.info("[timera] Fetching listing page: %s", listing_url)

            soup = self._get_html(listing_url)
            if soup is None:
                logger.warning("[timera] Could not fetch %s — stopping pagination", listing_url)
                break

            article_links = self._extract_article_links(soup, base_url)
            if not article_links:
                logger.info("[timera] No article links found on page %d — stopping", page_num)
                break

            new_links = [lnk for lnk in article_links if lnk not in seen_urls]
            if not new_links:
                logger.info("[timera] All links already seen on page %d — stopping", page_num)
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
        """Return absolute article URLs found on a listing/archive page."""
        links: list[str] = []

        # Strategy 1: look for article card containers and grab their first <a>
        for tag, attrs in LISTING_CARD_SELECTORS:
            cards = soup.find_all(tag, attrs or True) if attrs else soup.find_all(tag)
            if cards:
                for card in cards:
                    a = card.find("a", href=True)
                    if a:
                        href = a["href"].strip()
                        if self._is_article_url(href, section_url):
                            links.append(self._absolute(href))
                if links:
                    return self._deduplicate(links)

        # Strategy 2: every <a> on the page that looks like an article permalink
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if self._is_article_url(href, section_url):
                links.append(self._absolute(href))

        return self._deduplicate(links)

    def _is_article_url(self, href: str, section_url: str) -> bool:
        """Heuristic: is this href an individual article within the section?"""
        if not href or href.startswith("#") or href.startswith("mailto:"):
            return False
        # Must belong to the same domain
        if href.startswith("http") and "timera-energy.com" not in href:
            return False
        # Must not be the listing page itself
        normalised = href.rstrip("/")
        section_norm = section_url.rstrip("/")
        if normalised == section_norm:
            return False
        # Exclude pagination links
        if re.search(r"/page/\d+", href):
            return False
        # Exclude non-content paths
        bad_prefixes = ("/tag/", "/category/", "/author/", "/feed/",
                        "/wp-content/", "/wp-admin/", "?")
        if any(href.rstrip("/").endswith(p.rstrip("/")) or
               ("/" + p.lstrip("/")) in href
               for p in bad_prefixes):
            return False
        return True

    @staticmethod
    def _absolute(href: str) -> str:
        if href.startswith("http"):
            return href
        return "https://timera-energy.com" + href

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
        logger.info("[timera] Fetching article: %s", url)
        soup = self._get_html(url)
        if soup is None:
            return None

        title = self._extract_title(soup)
        published_date = self._extract_date(soup, url)
        content = self._extract_body(soup)

        if not content:
            logger.warning("[timera] Empty content for %s", url)
            return None

        return {
            "doc_type": doc_type,
            "title": title,
            "url": url,
            "published_date": published_date,
            "content": content,
        }

    def _extract_title(self, soup: BeautifulSoup) -> str:
        # Try <h1 class="entry-title"> first (WordPress standard)
        for tag, attrs in [
            ("h1", {"class": re.compile(r"entry-title|post-title|page-title")}),
            ("h1", {}),
            ("h2", {"class": re.compile(r"entry-title|post-title")}),
        ]:
            el = soup.find(tag, attrs or True) if attrs else soup.find(tag)
            if el:
                return el.get_text(" ", strip=True)
        # Fall back to <title> tag
        t = soup.find("title")
        return t.get_text(" ", strip=True) if t else ""

    def _extract_date(self, soup: BeautifulSoup, url: str) -> date | None:
        # 1. <time datetime="…">
        time_el = soup.find("time")
        if time_el:
            dt_attr = time_el.get("datetime", "")
            if dt_attr:
                d = _parse_date(dt_attr[:10])   # ISO prefix
                if d:
                    return d
            # Try text content of <time>
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

        # 3. Visible date elements
        for tag, attrs in DATE_META_SELECTORS:
            els = soup.find_all(tag, attrs or True) if attrs else soup.find_all(tag)
            for el in els:
                txt = el.get_text(strip=True)
                d = _parse_date(txt)
                if d:
                    return d

        # 4. URL slug
        return _date_from_url(url)

    def _extract_body(self, soup: BeautifulSoup) -> str:
        for tag, attrs in BODY_SELECTORS:
            el = soup.find(tag, attrs or True) if attrs else soup.find(tag)
            if el:
                text = _clean_text(el)
                if len(text) > 100:   # discard near-empty matches
                    return text

        # Last resort: entire <body>
        body = soup.find("body")
        return _clean_text(body) if body else ""

    # ------------------------------------------------------------------
    # HTTP helper
    # ------------------------------------------------------------------

    def _get_html(self, url: str) -> BeautifulSoup | None:
        try:
            resp = self._session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        except requests.RequestException as exc:
            logger.warning("[timera] Request error for %s: %s", url, exc)
            return None
        if resp.status_code != 200:
            logger.warning("[timera] HTTP %s for %s", resp.status_code, url)
            return None
        # Ensure correct encoding
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

    parser = argparse.ArgumentParser(description="Scrape Timera Energy articles")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print articles without writing to DB")
    args = parser.parse_args()

    connector = TimeraConnector()

    if args.dry_run:
        for i, doc in enumerate(connector.fetch(), 1):
            print(f"\n{'='*60}")
            print(f"[{i}] {doc['doc_type'].upper()} | {doc['published_date']} | {doc['title']}")
            print(f"    URL: {doc['url']}")
            print(f"    Content ({len(doc['content'])} chars): {doc['content'][:300]}...")
    else:
        conn = get_db_conn()
        ensure_table(conn)
        n = connector.run(conn)
        conn.close()
        print(f"Timera scrape complete — {n} new documents inserted.")
