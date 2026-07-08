"""
News Screener for Hermes service.

Daily automated screening of energy-sector news from WeChat public accounts
and web sources. Ingests all new articles into the Strategist knowledge base,
scores each for relevance using Claude Haiku, and delivers a tiered Feishu
digest at 14:30 Beijing (06:30 UTC).

Entry point: screen_news_sources(pg_url, api_key, feishu, owner_open_id)
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
import requests

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

_WECHAT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.0 Mobile/15E148 Safari/604.1"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Referer": "https://mp.weixin.qq.com/",
}

_48H_AGO = lambda: datetime.now(timezone.utc) - timedelta(hours=72)  # 72h window to avoid missing articles near boundary

_AI_PROMPT = """\
You are an energy-sector news analyst focused on China's electricity market and battery energy storage (BESS).

Article title: {title}

Article excerpt (first 800 chars):
{excerpt}

Rate this article's relevance to China's power sector and energy storage industry.

SCORING GUIDE (be generous — err toward higher scores when in doubt):
  9–10 : Directly about BESS dispatch/operations, electricity spot prices, power market trading rules, capacity market, or major national energy policy with direct market impact
  7–8  : China energy storage industry news, power market reforms, grid operations, renewable energy integration, provincial/regional electricity pricing, EV battery/storage technology with grid applications
  5–6  : General China electricity / energy industry news, power company announcements, energy transition, coal/gas power, transmission infrastructure, energy regulatory updates
  3–4  : Tangential — manufacturing news, EV batteries without grid angle, general business in energy sector, international energy with some China reference
  1–2  : Barely relevant — other industries, broad technology news with minor energy mention
  0    : Completely unrelated

IMPORTANT: If the article excerpt is empty or very short (title-only scoring), be GENEROUS based on the title alone:
  - A title clearly about 储能 (energy storage), 电力市场 (power market), 新能源 (new energy), 调频 (frequency regulation), 峰谷 (peak-valley pricing), 碳市场 (carbon market), or similar key terms should score at least 6–7.
  - Official sources (国家能源局, 中电联, 电力报, 能源局) should score at least 5 even for general articles.

Respond ONLY with valid JSON (no markdown, no code block):
{{
  "relevance": <0-10 integer>,
  "region_bucket": "<华北|华东|华南|西北|西南|东北|全国>",
  "region_province": "<province name in Chinese or null>",
  "category": "<policy|market_rules|market_analytics|technology|industry_news|other>",
  "summary": "<1-2 sentence Chinese summary; if body is empty, describe the likely topic based on title>"
}}
"""

# ── DB init ───────────────────────────────────────────────────────────────────

_DDL_NEWS_SOURCES = """
CREATE TABLE IF NOT EXISTS hermes.news_sources (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    url             TEXT NOT NULL,
    source_type     TEXT NOT NULL DEFAULT 'wechat',
    biz_id          TEXT,
    region_bucket   TEXT,
    category_hint   TEXT,
    scrape_config   JSONB,
    active          BOOLEAN NOT NULL DEFAULT TRUE,
    last_scraped_at TIMESTAMPTZ,
    consecutive_failures INT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(name, url)
);
"""

_DDL_KNOWLEDGE_COLS = """
ALTER TABLE staging.spot_knowledge_docs
    ADD COLUMN IF NOT EXISTS region_bucket   TEXT,
    ADD COLUMN IF NOT EXISTS region_province TEXT,
    ADD COLUMN IF NOT EXISTS source_name     TEXT,
    ADD COLUMN IF NOT EXISTS relevance_score INT,
    ADD COLUMN IF NOT EXISTS ai_summary      TEXT,
    ADD COLUMN IF NOT EXISTS published_at    TIMESTAMPTZ;
"""

_SCHEMA_INITIALIZED = False


def _init_db(pg_url: str) -> None:
    global _SCHEMA_INITIALIZED
    if _SCHEMA_INITIALIZED:
        return
    conn = psycopg2.connect(pg_url, options="-c statement_timeout=15000")
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS hermes")
            cur.execute(_DDL_NEWS_SOURCES)
            cur.execute(_DDL_KNOWLEDGE_COLS)
        conn.commit()
    finally:
        conn.close()
    _SCHEMA_INITIALIZED = True


# ── Source CRUD ───────────────────────────────────────────────────────────────

def get_sources(pg_url: str, active_only: bool = True) -> list[dict]:
    """Return all sources from hermes.news_sources."""
    _init_db(pg_url)
    conn = psycopg2.connect(pg_url, options="-c statement_timeout=10000")
    try:
        with conn.cursor() as cur:
            if active_only:
                cur.execute(
                    "SELECT id, name, url, source_type, biz_id, region_bucket, "
                    "category_hint, scrape_config, active, last_scraped_at, consecutive_failures "
                    "FROM hermes.news_sources WHERE active = TRUE ORDER BY name",
                )
            else:
                cur.execute(
                    "SELECT id, name, url, source_type, biz_id, region_bucket, "
                    "category_hint, scrape_config, active, last_scraped_at, consecutive_failures "
                    "FROM hermes.news_sources ORDER BY name",
                )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def add_source(
    pg_url: str,
    name: str,
    url: str,
    source_type: str = "wechat",
    biz_id: Optional[str] = None,
    region_bucket: str = "全国",
    category_hint: str = "other",
) -> dict:
    """Insert a new source. Returns the created row."""
    _init_db(pg_url)
    # Auto-extract biz_id from WeChat article URL if not provided
    if source_type == "wechat" and not biz_id and "mp.weixin.qq.com/s/" in url:
        biz_id = _extract_biz_id_from_article(url)
        if biz_id:
            # Overwrite url with the profile URL
            url = f"https://mp.weixin.qq.com/mp/profile_ext?action=home&__biz={biz_id}&scene=124"
    conn = psycopg2.connect(pg_url, options="-c statement_timeout=10000")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO hermes.news_sources (name, url, source_type, biz_id, region_bucket, category_hint)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (name, url) DO UPDATE SET
                    source_type = EXCLUDED.source_type,
                    biz_id = COALESCE(EXCLUDED.biz_id, hermes.news_sources.biz_id),
                    region_bucket = EXCLUDED.region_bucket,
                    category_hint = EXCLUDED.category_hint,
                    active = TRUE
                RETURNING id, name, url, source_type, biz_id, region_bucket, category_hint, active
                """,
                (name, url, source_type, biz_id, region_bucket, category_hint),
            )
            row = cur.fetchone()
            cols = [d[0] for d in cur.description]
        conn.commit()
        return dict(zip(cols, row))
    finally:
        conn.close()


def set_source_active(pg_url: str, source_id: int, active: bool) -> None:
    conn = psycopg2.connect(pg_url, options="-c statement_timeout=10000")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE hermes.news_sources SET active = %s WHERE id = %s",
                (active, source_id),
            )
        conn.commit()
    finally:
        conn.close()


def delete_source(pg_url: str, source_id: int) -> None:
    conn = psycopg2.connect(pg_url, options="-c statement_timeout=10000")
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM hermes.news_sources WHERE id = %s", (source_id,))
        conn.commit()
    finally:
        conn.close()


# ── WeChat helpers ─────────────────────────────────────────────────────────────

def _extract_biz_id_from_article(article_url: str) -> Optional[str]:
    """Fetch a WeChat article page and extract __biz from the embedded JS."""
    try:
        resp = requests.get(article_url, headers=_WECHAT_HEADERS, timeout=20)
        resp.raise_for_status()
        # Try var biz = "..."
        m = re.search(r'var\s+biz\s*=\s*"([^"]+)"', resp.text)
        if m:
            return m.group(1)
        # Try __biz= in URL anchors
        m = re.search(r'__biz=([A-Za-z0-9=+/]+)', resp.text)
        if m:
            return m.group(1)
    except Exception as exc:
        logger.warning("_extract_biz_id_from_article failed for %s: %s", article_url, exc)
    return None


class SogouCaptchaError(Exception):
    """Raised when Sogou returns a verification/CAPTCHA page instead of an article."""


def _fetch_wechat_article(url: str) -> tuple[str, str, str]:
    """
    Fetch a WeChat article URL. Returns (body_text, title, final_url).
    final_url is the permanent mp.weixin.qq.com URL after redirect resolution —
    use this instead of the Sogou redirect URL which expires within hours.
    Raises SogouCaptchaError if Sogou serves a verification page instead of the article.
    """
    from bs4 import BeautifulSoup

    # Use a desktop UA for mp.weixin.qq.com, but keep mobile UA for Sogou redirects
    headers = dict(_WECHAT_HEADERS)
    resp = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
    resp.raise_for_status()

    # Detect Sogou CAPTCHA / anti-spider page.
    # If the redirect chain ended back at weixin.sogou.com or antispider, it's not an article.
    final_url = resp.url
    if "weixin.sogou.com" in final_url or "antispider" in final_url:
        raise SogouCaptchaError(f"Sogou served verification page for {url} (final URL: {final_url})")

    # Also detect CAPTCHA by content keywords even if URL looks ok
    raw_text = resp.text[:2000]
    if ("请输入验证码" in raw_text or "sogou_verify" in raw_text
            or ("sogou" in raw_text.lower() and "验证" in raw_text)):
        raise SogouCaptchaError(f"Sogou CAPTCHA content detected for {url}")

    soup = BeautifulSoup(resp.content, "html.parser")
    title_tag = (
        soup.find("h1", id="activity-name")
        or soup.find("h2", class_="rich_media_title")
        or soup.find("title")
    )
    title = (title_tag.get_text(strip=True) if title_tag else "") or url.split("/s/")[-1][:60]
    for tag in soup(["script", "style", "nav", "footer", "header", "iframe", "img", "svg"]):
        tag.decompose()
    content_div = (
        soup.find("div", id="js_content")
        or soup.find("div", class_="rich_media_content")
    )
    text = content_div.get_text(separator="\n") if content_div else soup.get_text(separator="\n")
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    return "\n".join(lines), title, final_url


def _discover_wechat_paginated(source: dict, start_date: datetime, max_pages: int = 30) -> list[dict]:
    """
    Paginate Sogou to discover WeChat articles back to start_date.
    Used for backfill runs; standard discovery uses _discover_wechat_articles.
    Returns list of {url, title, published_at}, oldest-first within cutoff.
    """
    from bs4 import BeautifulSoup
    from urllib.parse import quote
    import time as _time

    name = source.get("name", "")
    if not name:
        return []

    sogou_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": "https://weixin.sogou.com/",
    }

    # Ensure start_date is tz-aware
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)

    all_articles: list[dict] = []
    seen_urls: set[str] = set()

    for page in range(1, max_pages + 1):
        url = (
            f"https://weixin.sogou.com/weixin"
            f"?type=2&s_from=input&query={quote(name)}&ie=utf8"
            f"&_sug_=n&_sug_type_=&page={page}"
        )
        try:
            resp = requests.get(url, headers=sogou_headers, timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, "html.parser")
            items = soup.select("ul.news-list li")
            if not items:
                logger.info("Sogou backfill: no more results for %s at page %d", name, page)
                break

            page_articles = []
            all_before_cutoff = True
            for li in items:
                h3 = li.find("h3")
                if not h3:
                    continue
                a = h3.find("a", href=True)
                if not a:
                    continue
                href = a["href"]
                if href.startswith("/"):
                    href = "https://weixin.sogou.com" + href
                if href in seen_urls:
                    continue
                seen_urls.add(href)

                title = re.sub(r"<!--.*?-->", "", a.get_text(strip=True)).strip()

                pub_dt = None
                s2 = li.find("span", class_="s2")
                if s2:
                    ts_m = re.search(r"timeConvert\('(\d+)'", str(s2))
                    if ts_m:
                        try:
                            pub_dt = datetime.fromtimestamp(int(ts_m.group(1)), tz=timezone.utc)
                        except Exception:
                            pass
                    if not pub_dt:
                        pub_dt = _parse_sogou_date(s2.get_text(strip=True))

                if pub_dt and pub_dt >= start_date:
                    all_before_cutoff = False
                    page_articles.append({"url": href, "title": title, "published_at": pub_dt})
                elif pub_dt and pub_dt < start_date:
                    pass  # older than cutoff, skip but keep scanning
                else:
                    # No date — include it (can't tell)
                    all_before_cutoff = False
                    page_articles.append({"url": href, "title": title, "published_at": pub_dt})

            all_articles.extend(page_articles)
            logger.info(
                "Sogou backfill page %d/%d for %s: %d articles collected so far",
                page, max_pages, name, len(all_articles),
            )

            # If every article on this page was before start_date, we've gone far enough
            if all_before_cutoff and page_articles == []:
                logger.info("Sogou backfill: all articles on page %d pre-date cutoff, stopping", page)
                break

            # Polite delay between pages
            _time.sleep(1.5)

        except Exception as exc:
            logger.warning("Sogou backfill page %d failed for %s: %s", page, name, exc)
            break

    logger.info("Sogou backfill discovered %d articles for %s", len(all_articles), name)
    return all_articles


def backfill_source(
    source: dict,
    start_date: datetime,
    pg_url: str,
    api_key: str,
    feishu=None,
    owner_open_id: str = "",
) -> dict:
    """
    Backfill articles for a single source from start_date to now.
    Returns summary {discovered, ingested, skipped, errors}.
    """
    _init_db(pg_url)

    stype = source.get("source_type", "wechat")
    if stype == "wechat":
        articles = _discover_wechat_paginated(source, start_date)
    elif stype == "rss":
        articles = _discover_rss_articles(source)
    else:
        articles = _discover_web_articles(source)

    ingested = skipped = errors = 0
    captcha_hits = 0
    for art in articles:
        import time as _time
        url = art.get("url", "")
        if not url:
            continue
        try:
            body = ""
            title = art.get("title", "")
            if stype == "wechat":
                try:
                    body, fetched_title, final_url = _fetch_wechat_article(url)
                    title = title or fetched_title
                    # Replace expiring Sogou redirect with permanent mp.weixin URL
                    if final_url and "mp.weixin.qq.com" in final_url:
                        art["url"] = final_url
                    captcha_hits = 0  # reset on success
                except SogouCaptchaError:
                    captcha_hits += 1
                    logger.warning(
                        "Sogou CAPTCHA on article %d/%d for %s — scoring from title only",
                        articles.index(art) + 1, len(articles), source["name"],
                    )
                    # Back off longer after each consecutive CAPTCHA hit
                    _time.sleep(min(5 * captcha_hits, 30))
                    body = ""  # score from title only
            else:
                body, fetched_title = _fetch_web_article(url)
                title = title or fetched_title

            art["body"] = body
            art["title"] = title

            ai_result = _score_article(title, body, api_key) if api_key else {
                "relevance": None, "region_bucket": source.get("region_bucket"),
                "region_province": None, "category": source.get("category_hint"), "summary": None,
            }

            _doc_id, is_new = _ingest_article(source, art, ai_result, pg_url, api_key)
            if is_new:
                ingested += 1
            else:
                skipped += 1

            # Polite delay between article fetches to avoid Sogou rate limiting
            _time.sleep(2)

        except Exception as exc:
            logger.warning("Backfill error for %s: %s", url[:80], exc)
            errors += 1

    summary = {"discovered": len(articles), "ingested": ingested, "skipped": skipped, "errors": errors}
    logger.info("Backfill done for %s: %s", source["name"], summary)

    if feishu and owner_open_id and ingested > 0:
        try:
            feishu.send_text(
                owner_open_id,
                f"✅ 回填完成「{source['name']}」: 发现 {len(articles)} 篇，新增 {ingested} 篇入库，跳过 {skipped} 篇（已存在），错误 {errors} 篇。",
            )
        except Exception:
            pass

    return summary


def _discover_wechat_articles(source: dict) -> list[dict]:
    """
    Discover recent articles from a WeChat public account via Sogou search.

    WeChat profile pages require a logged-in session and block unauthenticated
    scraping. Sogou indexes all public WeChat accounts and exposes article
    listings without authentication.

    Returns list of {url, title, published_at}.
    """
    from bs4 import BeautifulSoup
    from urllib.parse import quote

    name = source.get("name", "")
    biz_id = source.get("biz_id")

    if not name and not biz_id:
        logger.warning("Source has neither name nor biz_id — skipping WeChat discovery")
        return []

    # Sogou article search: type=2 searches articles by account name, returns recent articles.
    # type=1 (account search) returns account cards, not articles — use type=2.
    sogou_url = (
        f"https://weixin.sogou.com/weixin"
        f"?type=2&s_from=input&query={quote(name)}&ie=utf8&_sug_=n&_sug_type_="
    )
    sogou_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": "https://weixin.sogou.com/",
    }

    articles: list[dict] = []

    try:
        resp = requests.get(sogou_url, headers=sogou_headers, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        # Sogou structure:
        #   <ul class="news-list">
        #     <li>
        #       <div class="txt-box">
        #         <h3><a href="/link?url=...">TITLE</a></h3>
        #         <span class="s2"><script>document.write(timeConvert('UNIX_TS'))</script></span>
        #       </div>
        #     </li>
        for li in soup.select("ul.news-list li"):
            h3 = li.find("h3")
            if not h3:
                continue
            a = h3.find("a", href=True)
            if not a:
                continue

            # Build absolute Sogou redirect URL (will redirect to mp.weixin.qq.com)
            href = a["href"]
            if href.startswith("/"):
                href = "https://weixin.sogou.com" + href

            title = a.get_text(strip=True)
            # Strip Sogou <em> highlighting artefacts from title
            title = re.sub(r"<!--.*?-->", "", title).strip()

            # Extract Unix timestamp from: <script>document.write(timeConvert('1234567890'))</script>
            pub_dt = None
            s2 = li.find("span", class_="s2")
            if s2:
                ts_m = re.search(r"timeConvert\('(\d+)'", str(s2))
                if ts_m:
                    try:
                        pub_dt = datetime.fromtimestamp(int(ts_m.group(1)), tz=timezone.utc)
                    except Exception:
                        pass
                if not pub_dt:
                    pub_dt = _parse_sogou_date(s2.get_text(strip=True))

            articles.append({"url": href, "title": title, "published_at": pub_dt})

    except Exception as exc:
        logger.warning("Sogou discovery failed for %s: %s", name, exc)

    # If Sogou returned nothing (rate-limited or blocked), fall back to direct Sogou
    # article search with the account name as query term
    if not articles and name:
        articles = _discover_wechat_via_sogou_article_search(name, sogou_headers)

    logger.info("Discovered %d articles from WeChat source %s (via Sogou)", len(articles), name)
    return articles[:20]


def _parse_sogou_date(text: str) -> Optional[datetime]:
    """Parse Sogou relative/absolute date strings to UTC datetime."""
    now = datetime.now(timezone.utc)
    text = text.strip()
    try:
        # "N分钟前"
        m = re.match(r"(\d+)分钟前", text)
        if m:
            return now - timedelta(minutes=int(m.group(1)))
        # "N小时前"
        m = re.match(r"(\d+)小时前", text)
        if m:
            return now - timedelta(hours=int(m.group(1)))
        # "N天前"
        m = re.match(r"(\d+)天前", text)
        if m:
            return now - timedelta(days=int(m.group(1)))
        # "昨天"
        if "昨天" in text:
            return now - timedelta(days=1)
        # "MM月DD日" (same year)
        m = re.match(r"(\d+)月(\d+)日", text)
        if m:
            month, day = int(m.group(1)), int(m.group(2))
            year = now.year
            return datetime(year, month, day, 12, 0, tzinfo=timezone.utc)
        # "YYYY-MM-DD" or "YYYY年MM月DD日"
        m = re.match(r"(\d{4})[-年](\d{1,2})[-月](\d{1,2})", text)
        if m:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), 12, 0, tzinfo=timezone.utc)
    except Exception:
        pass
    return None


def _discover_wechat_via_sogou_article_search(name: str, headers: dict) -> list[dict]:
    """
    Fallback: search Sogou for articles from the named account using
    the article search endpoint (type=2).
    """
    from bs4 import BeautifulSoup
    from urllib.parse import quote

    url = (
        f"https://weixin.sogou.com/weixin"
        f"?type=2&s_from=input&query={quote(name)}&ie=utf8&_sug_=n&_sug_type_="
    )
    articles = []
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")
        for li in soup.select("ul.news-list li, ul.lst_list li"):
            # Check if this article is from the matching account
            acct_tag = li.find("a", class_="account") or li.find("p", class_="account")
            if acct_tag and name not in acct_tag.get_text():
                continue
            a = li.find("h3", recursive=True)
            link = a.find("a") if a else None
            if not link:
                continue
            href = link.get("href", "")
            if "mp.weixin.qq.com" not in href:
                continue
            title = link.get_text(strip=True)
            date_tag = li.find("span", class_="s2") or li.find("label")
            pub_dt = _parse_sogou_date(date_tag.get_text(strip=True)) if date_tag else None
            articles.append({"url": href, "title": title, "published_at": pub_dt})
    except Exception as exc:
        logger.debug("Sogou article search fallback failed for %s: %s", name, exc)
    return articles[:10]


def _discover_web_articles(source: dict) -> list[dict]:
    """Discover recent articles from a web listing page (e.g. bjx.com.cn)."""
    from bs4 import BeautifulSoup

    cfg = source.get("scrape_config") or {}
    selector = cfg.get("article_selector", "a")
    domain_filter = cfg.get("domain_filter", "")
    cutoff = _48H_AGO()

    try:
        resp = requests.get(
            source["url"],
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
                "Accept-Language": "zh-CN,zh;q=0.9",
            },
            timeout=30,
        )
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("Web listing fetch failed for %s: %s", source["name"], exc)
        return []

    soup = BeautifulSoup(resp.content, "html.parser")
    articles: list[dict] = []

    for a in soup.select(selector):
        href = a.get("href", "")
        if not href:
            continue
        # Make absolute
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            from urllib.parse import urlparse
            base = urlparse(source["url"])
            href = f"{base.scheme}://{base.netloc}{href}"
        if domain_filter and domain_filter not in href:
            continue
        title = a.get_text(strip=True)
        if len(title) < 5:
            continue
        articles.append({"url": href, "title": title, "published_at": None})

    logger.info("Discovered %d articles from web source %s", len(articles), source["name"])
    return articles[:30]


def _discover_rss_articles(source: dict) -> list[dict]:
    """Discover recent articles from an RSS feed."""
    try:
        import feedparser
    except ImportError:
        logger.warning("feedparser not installed — skipping RSS source %s", source["name"])
        return []

    cutoff = _48H_AGO()
    try:
        feed = feedparser.parse(source["url"])
    except Exception as exc:
        logger.warning("RSS feed failed for %s: %s", source["name"], exc)
        return []

    articles = []
    for entry in feed.entries:
        pub = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            import time as _time
            pub = datetime.fromtimestamp(_time.mktime(entry.published_parsed), tz=timezone.utc)
            if pub < cutoff:
                continue
        articles.append({
            "url": entry.get("link", ""),
            "title": entry.get("title", ""),
            "published_at": pub,
        })
    return articles


# ── AI scoring ────────────────────────────────────────────────────────────────

def _synthesize_digest(articles: list[dict], api_key: str) -> str:
    """
    Generate a 3-5 sentence executive summary synthesising the key themes
    from today's high/mid-relevance articles. Returns plain Chinese text.
    Called only when there are ≥2 articles scoring ≥6.
    """
    import anthropic

    lines = []
    for a in articles[:12]:  # cap at 12 to keep prompt short
        title = a.get("title", "")[:80]
        summary = a.get("summary") or ""
        score = a.get("relevance", "?")
        lines.append(f"- [{score}] {title}" + (f"：{summary[:80]}" if summary else ""))

    prompt = (
        "以下是今日中国电力市场相关新闻标题及摘要（括号内为相关度评分）：\n\n"
        + "\n".join(lines)
        + "\n\n请用3-5句话，以中文撰写今日电力行业要点综述，重点提炼政策动态、市场价格走势、储能行业重要事件。"
        "语言简洁专业，适合能源从业者阅读。只输出综述正文，不要加标题或前言。"
    )
    try:
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()
    except Exception as exc:
        logger.warning("Digest synthesis failed: %s", exc)
        return ""


def _score_article(title: str, body: str, api_key: str) -> dict:
    """
    Call Claude Haiku to score relevance and extract metadata.
    Returns dict with keys: relevance, region_bucket, region_province, category, summary.
    """
    import anthropic

    client = anthropic.Anthropic(api_key=api_key)
    prompt = _AI_PROMPT.format(title=title, excerpt=body[:800])
    try:
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text.strip()
        # Strip markdown code fences if present
        text = re.sub(r"^```[a-z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
        return json.loads(text)
    except Exception as exc:
        logger.warning("AI scoring failed for '%s': %s", title[:60], exc)
        return {
            "relevance": None,
            "region_bucket": None,
            "region_province": None,
            "category": None,
            "summary": None,
        }


# ── Ingest ────────────────────────────────────────────────────────────────────

def _ingest_article(
    source: dict,
    article: dict,
    ai_result: dict,
    pg_url: str,
    api_key: str,
) -> tuple[int, bool]:
    """
    Ingest an article into the Strategist KB.
    Returns (doc_id, is_new).
    """
    from services.knowledge_pool.knowledge_docs import (
        init_knowledge_tables,
        register_and_ingest,
        get_conn,
    )

    body: str = article.get("body", "")
    title: str = article.get("title", "")
    category = ai_result.get("category") or source.get("category_hint") or "other"

    safe_title = title[:120].replace("/", "_").replace("\\", "_") or "article"
    filename = f"{source['name']}_{safe_title}.txt"

    doc_id, is_new, _ = register_and_ingest(
        file_bytes=body.encode("utf-8"),
        filename=filename,
        category_override=category,
        app="strategist",
        api_key=api_key,
        synthesize=False,
    )

    # Enrich with AI metadata + source metadata
    relevance = ai_result.get("relevance")
    ingest_status = "low_relevance" if (relevance is not None and relevance < 4) else "ingested"

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE staging.spot_knowledge_docs SET
                    region_bucket   = COALESCE(%s, region_bucket),
                    region_province = COALESCE(%s, region_province),
                    source_name     = COALESCE(%s, source_name),
                    relevance_score = COALESCE(%s, relevance_score),
                    ai_summary      = COALESCE(%s, ai_summary),
                    published_at    = COALESCE(%s, published_at),
                    ingest_status   = COALESCE(%s, ingest_status)
                WHERE id = %s
                """,
                (
                    ai_result.get("region_bucket"),
                    ai_result.get("region_province"),
                    source.get("name"),
                    relevance,
                    ai_result.get("summary"),
                    article.get("published_at"),
                    ingest_status,
                    doc_id,
                ),
            )
        conn.commit()

    return doc_id, is_new


# ── Feishu digest ─────────────────────────────────────────────────────────────

def _build_feishu_card(date_str: str, results: list[dict], api_key: str = "") -> dict:
    """
    Build a Feishu interactive card for the daily digest.
    results: list of {title, url, source_name, relevance, category, region_bucket, summary, published_at, is_new}

    Shows articles published in the last 24h (by published_at), regardless of whether they were
    already in the KB. This prevents "今日无新内容" after a backfill has pre-ingested today's articles.
    Falls back to is_new=True for articles with no published_at date.

    If api_key is provided and there are ≥2 high/mid-relevance articles, prepends an AI-synthesised
    executive summary at the top of the card.
    """
    cutoff_24h = datetime.now(timezone.utc) - timedelta(hours=24)

    def _is_today(r: dict) -> bool:
        pub = r.get("published_at")
        if pub is None:
            return r.get("is_new", False)  # no date: only include if new to KB
        return pub >= cutoff_24h

    new_articles = [r for r in results if _is_today(r)]
    ingested_count = sum(1 for r in new_articles if r.get("is_new"))
    total = len(new_articles)
    source_count = len({r["source_name"] for r in new_articles})

    tier_high = [r for r in new_articles if (r.get("relevance") or 0) >= 8]
    tier_mid = [r for r in new_articles if 6 <= (r.get("relevance") or 0) < 8]
    tier_low = [r for r in new_articles if (r.get("relevance") or 0) < 6]

    def _article_line(r: dict) -> str:
        url = r.get("url", "")
        title = r.get("title", "")[:60]
        src = r.get("source_name", "")
        cat = r.get("category", "")
        region = r.get("region_bucket", "")
        summary = r.get("summary", "")
        relevance = r.get("relevance")
        score_str = f"★{relevance}" if relevance is not None else ""
        # Resolve Sogou redirect URLs — they expire within hours.
        # Try a quick redirect-follow; fall back to plain text if it fails.
        if url and "weixin.sogou.com" in url:
            try:
                _r = requests.get(url, headers=_WECHAT_HEADERS, timeout=4,
                                  allow_redirects=True)
                _final = _r.url
                if "mp.weixin.qq.com" in _final:
                    url = _final
                else:
                    url = ""  # CAPTCHA / antispider page — omit link
            except Exception:
                url = ""  # timeout or network error — show plain text
        link = f"[{title}]({url})" if url else title
        meta = " · ".join(filter(None, [src, score_str, cat, region]))
        lines = [f"• {link}", f"  {meta}"]
        if summary:
            lines.append(f"  _{summary[:100]}_")
        return "\n".join(lines)

    sections = []

    new_label = f" · {ingested_count} 篇新入库" if ingested_count < total else ""
    header_text = (
        f"📰 今日能源资讯 — {date_str}\n"
        f"{total} 篇文章 · 来自 {source_count} 个来源{new_label}"
        if total > 0
        else f"📰 今日能源资讯 — {date_str}\n今日无新内容"
    )

    # AI executive summary — only when there are meaningful articles to synthesise
    notable = [r for r in (tier_high + tier_mid) if r.get("relevance") is not None]
    if api_key and len(notable) >= 2:
        synthesis = _synthesize_digest(notable, api_key)
        if synthesis:
            sections.append(f"**📝 今日要点**\n\n{synthesis}")

    if tier_high:
        body = "🔥 **重点关注** (relevance ≥ 8)\n\n"
        body += "\n\n".join(_article_line(r) for r in tier_high[:10])
        sections.append(body)

    if tier_mid:
        body = "📊 **值得关注** (relevance 6–7)\n\n"
        body += "\n\n".join(_article_line(r) for r in tier_mid[:10])
        sections.append(body)

    if tier_low:
        body = f"📋 **其他更新** (relevance < 6) — {len(tier_low)} 篇\n\n"
        body += "\n\n".join(_article_line(r) for r in tier_low[:10])
        if len(tier_low) > 10:
            body += f"\n\n…及另 {len(tier_low) - 10} 篇已录入知识库"
        sections.append(body)

    elements = []
    for s in sections:
        elements.append({
            "tag": "markdown",
            "content": s,
        })
        elements.append({"tag": "hr"})
    if elements and elements[-1]["tag"] == "hr":
        elements.pop()

    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": header_text},
            "template": "blue" if total > 0 else "grey",
        },
        "elements": elements if elements else [{"tag": "markdown", "content": "今日无新文章录入知识库。"}],
    }
    return card


# ── Main entry point ──────────────────────────────────────────────────────────

def screen_news_sources(
    pg_url: str,
    api_key: str,
    feishu,
    owner_open_id: str,
) -> dict:
    """
    Main entry point. Discovers, fetches, scores, and ingests articles from
    all active sources. Sends a Feishu digest on completion.

    Returns summary dict: {total_discovered, ingested, skipped, errors, sources_failed}
    """
    _init_db(pg_url)
    sources = get_sources(pg_url, active_only=True)
    if not sources:
        logger.info("No active news sources configured — nothing to do")
        return {"total_discovered": 0, "ingested": 0, "skipped": 0, "errors": 0, "sources_failed": 0}

    now_utc = datetime.now(timezone.utc)
    date_str = (now_utc + timedelta(hours=8)).strftime("%Y-%m-%d")

    total_ingested = 0
    total_skipped = 0
    total_errors = 0
    sources_failed = 0
    all_results: list[dict] = []

    conn_update = psycopg2.connect(pg_url, options="-c statement_timeout=10000")

    try:
        for source in sources:
            logger.info("Processing source: %s (%s)", source["name"], source["source_type"])
            try:
                # 1. Discover articles
                stype = source["source_type"]
                if stype == "wechat":
                    articles = _discover_wechat_articles(source)
                elif stype == "rss":
                    articles = _discover_rss_articles(source)
                else:
                    articles = _discover_web_articles(source)

                # 2. Fetch + process each article
                for art in articles:
                    url = art.get("url", "")
                    if not url:
                        continue
                    try:
                        title = art.get("title", "")
                        body = ""
                        if stype == "wechat":
                            try:
                                body, fetched_title, final_url = _fetch_wechat_article(url)
                                title = title or fetched_title
                                # Replace expiring Sogou redirect with permanent mp.weixin URL
                                if final_url and "mp.weixin.qq.com" in final_url:
                                    art["url"] = final_url
                            except SogouCaptchaError:
                                logger.warning("Sogou CAPTCHA for %s — scoring from title only", url[:80])
                                body = ""
                        else:
                            body, fetched_title = _fetch_web_article(url)
                            title = title or fetched_title
                        art["body"] = body
                        art["title"] = title

                        # 3. AI scoring
                        ai_result = _score_article(art["title"], body, api_key) if api_key else {
                            "relevance": None, "region_bucket": source.get("region_bucket"),
                            "region_province": None, "category": source.get("category_hint"),
                            "summary": None,
                        }

                        # 4. Ingest
                        doc_id, is_new = _ingest_article(source, art, ai_result, pg_url, api_key)

                        all_results.append({
                            "title": art["title"],
                            "url": art.get("url", url),  # use resolved URL if available
                            "source_name": source["name"],
                            "relevance": ai_result.get("relevance"),
                            "category": ai_result.get("category"),
                            "region_bucket": ai_result.get("region_bucket") or source.get("region_bucket"),
                            "summary": ai_result.get("summary"),
                            "published_at": art.get("published_at"),
                            "is_new": is_new,
                        })
                        if is_new:
                            total_ingested += 1
                        else:
                            total_skipped += 1

                    except Exception as exc:
                        logger.warning("Error processing article %s: %s", url[:80], exc)
                        total_errors += 1

                # 5. Update last_scraped_at + reset consecutive failures
                with conn_update.cursor() as cur:
                    cur.execute(
                        "UPDATE hermes.news_sources SET last_scraped_at = NOW(), consecutive_failures = 0 WHERE id = %s",
                        (source["id"],),
                    )
                conn_update.commit()

            except Exception as exc:
                logger.error("Source %s failed entirely: %s", source["name"], exc)
                sources_failed += 1
                # Increment consecutive_failures; auto-disable at 3
                with conn_update.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE hermes.news_sources
                        SET consecutive_failures = consecutive_failures + 1,
                            active = CASE WHEN consecutive_failures + 1 >= 3 THEN FALSE ELSE active END
                        WHERE id = %s
                        RETURNING consecutive_failures, active
                        """,
                        (source["id"],),
                    )
                    row = cur.fetchone()
                conn_update.commit()
                if row and not row[1]:
                    logger.warning(
                        "Source %s auto-disabled after 3 consecutive failures", source["name"]
                    )
                    if feishu and owner_open_id:
                        try:
                            feishu.send_text(
                                owner_open_id,
                                f"⚠️ 新闻来源「{source['name']}」已连续失败3次，已自动停用。请检查配置。",
                            )
                        except Exception:
                            pass

    finally:
        conn_update.close()

    # 6. Send Feishu digest
    if feishu and owner_open_id:
        try:
            card = _build_feishu_card(date_str, all_results, api_key)
            feishu.send_card(owner_open_id, card)
            logger.info("Feishu digest sent for %s", date_str)
        except Exception as exc:
            logger.error("Failed to send Feishu digest: %s", exc)

    summary = {
        "total_discovered": len(all_results),
        "ingested": total_ingested,
        "skipped": total_skipped,
        "errors": total_errors,
        "sources_failed": sources_failed,
    }
    logger.info("News screener done: %s", summary)
    return summary


def _fetch_web_article(url: str) -> tuple[str, str]:
    """Fetch a non-WeChat article. Returns (body_text, title)."""
    from bs4 import BeautifulSoup

    resp = requests.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
            "Accept-Language": "zh-CN,zh;q=0.9",
        },
        timeout=30,
    )
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "html.parser")
    title_tag = soup.find("title") or soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else ""
    for tag in soup(["script", "style", "nav", "footer", "header", "iframe"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    return "\n".join(lines[:500]), title
