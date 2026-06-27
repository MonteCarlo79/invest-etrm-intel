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

_48H_AGO = lambda: datetime.now(timezone.utc) - timedelta(hours=48)

_AI_PROMPT = """\
You are an energy-sector news analyst focused on China's electricity market and battery energy storage (BESS).

Article title: {title}

Article excerpt (first 800 chars):
{excerpt}

Rate this article's relevance to: China power markets, energy storage, BESS operations, electricity policy, market rules, nodal prices, dispatch, or related topics.

Respond ONLY with valid JSON (no markdown, no code block):
{{
  "relevance": <0-10 integer, where 10=highly relevant to China power/BESS>,
  "region_bucket": "<华北|华东|华南|西北|西南|东北|全国>",
  "region_province": "<province name in Chinese or null>",
  "category": "<policy|market_rules|market_analytics|technology|industry_news|other>",
  "summary": "<1-2 sentence Chinese summary of the article>"
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


def _fetch_wechat_article(url: str) -> tuple[str, str]:
    """Fetch a WeChat article URL. Returns (body_text, title)."""
    from bs4 import BeautifulSoup

    resp = requests.get(url, headers=_WECHAT_HEADERS, timeout=30)
    resp.raise_for_status()
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
    return "\n".join(lines), title


def _discover_wechat_articles(source: dict) -> list[dict]:
    """
    Discover recent articles from a WeChat public account profile page.
    Returns list of {url, title, published_at}.
    """
    from bs4 import BeautifulSoup

    biz_id = source.get("biz_id")
    if not biz_id:
        logger.warning("Source %s has no biz_id — skipping WeChat discovery", source["name"])
        return []

    profile_url = (
        source.get("url")
        or f"https://mp.weixin.qq.com/mp/profile_ext?action=home&__biz={biz_id}&scene=124"
    )
    try:
        resp = requests.get(profile_url, headers=_WECHAT_HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("WeChat profile fetch failed for %s: %s", source["name"], exc)
        return []

    soup = BeautifulSoup(resp.content, "html.parser")

    # WeChat profile pages render articles in a JS-embedded JSON blob
    # Try to extract from __INITIAL_STATE__ or msgList JSON in page source
    articles: list[dict] = []
    cutoff = _48H_AGO()

    # Pattern 1: msgList in JSON blob
    m = re.search(r'"msgList"\s*:\s*(\{.*?"list"\s*:\s*\[.*?\]\s*\})', resp.text, re.DOTALL)
    if m:
        try:
            msg_list = json.loads(m.group(1))
            items = msg_list.get("list", [])
            for item in items:
                app_msg = item.get("app_msg_ext_info", {})
                url = app_msg.get("content_url") or item.get("content_url", "")
                title = app_msg.get("title", "") or item.get("title", "")
                create_time = item.get("comm_msg_info", {}).get("datetime", 0)
                if url:
                    pub_dt = datetime.fromtimestamp(create_time, tz=timezone.utc) if create_time else None
                    if pub_dt and pub_dt < cutoff:
                        continue
                    articles.append({"url": url, "title": title, "published_at": pub_dt})
        except (json.JSONDecodeError, KeyError) as exc:
            logger.debug("msgList parse failed for %s: %s", source["name"], exc)

    # Pattern 2: <li> items in #msg_list
    if not articles:
        for li in soup.select("#msg_list li.album__item, #js_pc_qr_code_img, .weui_media_box"):
            a = li.find("a", href=True)
            if not a:
                continue
            href = a["href"]
            if "mp.weixin.qq.com/s" not in href and "__biz" not in href:
                continue
            title_tag = li.find("h4") or li.find("h3") or a
            title = title_tag.get_text(strip=True) if title_tag else ""
            articles.append({"url": href, "title": title, "published_at": None})

    logger.info("Discovered %d articles from WeChat source %s", len(articles), source["name"])
    return articles[:20]  # cap to avoid hammering on first run


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

def _build_feishu_card(date_str: str, results: list[dict]) -> dict:
    """
    Build a Feishu interactive card for the daily digest.
    results: list of {title, url, source_name, relevance, category, region_bucket, summary}
    """
    new_articles = [r for r in results if r["is_new"]]
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
        link = f"[{title}]({url})" if url else title
        meta = " · ".join(filter(None, [src, cat, region]))
        lines = [f"• {link}", f"  {meta}"]
        if summary:
            lines.append(f"  {summary[:80]}")
        return "\n".join(lines)

    sections = []

    header_text = (
        f"📰 今日能源资讯 — {date_str}\n"
        f"{total} 篇新文章 · 来自 {source_count} 个来源"
        if total > 0
        else f"📰 今日能源资讯 — {date_str}\n今日无新内容"
    )

    if tier_high:
        body = "🔥 **重点关注** (relevance ≥ 8)\n\n"
        body += "\n\n".join(_article_line(r) for r in tier_high[:10])
        sections.append(body)

    if tier_mid:
        body = "📊 **值得关注** (relevance 6–7)\n\n"
        body += "\n\n".join(_article_line(r) for r in tier_mid[:10])
        sections.append(body)

    if tier_low:
        count = len(tier_low)
        sections.append(f"📋 **其他更新** (relevance < 6)\n{count} 篇文章已录入知识库")

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
                        body, title = _fetch_wechat_article(url) if stype == "wechat" else _fetch_web_article(url)
                        art["body"] = body
                        art["title"] = art.get("title") or title

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
                            "url": url,
                            "source_name": source["name"],
                            "relevance": ai_result.get("relevance"),
                            "category": ai_result.get("category"),
                            "region_bucket": ai_result.get("region_bucket") or source.get("region_bucket"),
                            "summary": ai_result.get("summary"),
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
            card = _build_feishu_card(date_str, all_results)
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
