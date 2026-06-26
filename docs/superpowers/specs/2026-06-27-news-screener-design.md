# News Screener — Design Spec
_2026-06-27_

## Overview

Daily automated screening of energy-sector news from WeChat public accounts and web sources. Ingests all new articles into the Strategist knowledge base (KB), scores each for relevance using Claude Haiku, tags by region and category, and delivers a tiered Feishu digest at 14:30 Beijing.

**Scope:** Sources management UI in Spot Market Streamlit app (new "News Sources" tab). Scraper + scheduler + Feishu delivery lives in the Hermes service.

---

## Initial Sources

| Name | Type | Region hint |
|---|---|---|
| 飔合科技 | WeChat | 全国 |
| 兰木达 | WeChat | 全国 |
| 中关村储能产业技术联盟 (CNESA) | WeChat | 全国 |
| 南方能源观察 | WeChat | 华南 |
| 中国能源观察 | WeChat | 全国 |
| 储能与电力市场 | WeChat | 全国 |
| 彭博新能源财经 | WeChat | 全国 |
| 西北储能 | WeChat | 西北 |
| 北极星储能网 | WeChat + Web (chuneng.bjx.com.cn) | 全国 |

---

## 1. Data Model

### New table: `hermes.news_sources`

```sql
CREATE TABLE IF NOT EXISTS hermes.news_sources (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,           -- display name, e.g. "飔合科技"
    url             TEXT NOT NULL,           -- WeChat profile URL or web listing URL
    source_type     TEXT NOT NULL,           -- 'wechat' | 'web' | 'rss'
    biz_id          TEXT,                    -- WeChat __biz param (auto-extracted)
    region_bucket   TEXT,                    -- hint: 华北|华东|华南|西北|西南|东北|全国
    category_hint   TEXT,                    -- hint: policy|market_rules|market_analytics|technology|other
    scrape_config   JSONB,                   -- optional per-source CSS selectors for web sources
    active          BOOLEAN NOT NULL DEFAULT TRUE,
    last_scraped_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
```

### Migrations on `staging.spot_knowledge_docs`

Additive columns, backward-compatible:

```sql
ALTER TABLE staging.spot_knowledge_docs
    ADD COLUMN IF NOT EXISTS region_bucket   TEXT,        -- 华北|华东|华南|西北|西南|东北|全国
    ADD COLUMN IF NOT EXISTS region_province TEXT,        -- 内蒙古|广东|… or NULL
    ADD COLUMN IF NOT EXISTS source_name     TEXT,        -- from hermes.news_sources.name
    ADD COLUMN IF NOT EXISTS relevance_score INT,         -- 0–10, AI-assigned
    ADD COLUMN IF NOT EXISTS ai_summary      TEXT,        -- 1–2 sentence Chinese summary
    ADD COLUMN IF NOT EXISTS published_at    TIMESTAMPTZ; -- article publish date if parseable
```

---

## 2. Scraper + Discovery (`services/hermes/news_screener.py`)

### Article discovery (per source)

**WeChat accounts:**
1. GET `https://mp.weixin.qq.com/mp/profile_ext?action=home&__biz={biz_id}&scene=124` with mobile User-Agent (iPhone iOS 17).
2. Parse HTML for article entries — title, URL, publish datetime.
3. Filter to articles published within the last 48h (generous window to handle timing offsets).

**Web sources (e.g. bjx.com.cn):**
- GET the listing page; extract article links via BeautifulSoup. Selector configured per source via a `scrape_config` JSON column (optional, defaulting to `<a>` tags containing the source domain).

**RSS sources:**
- `feedparser.parse(url)` → entries from last 48h.

### `__biz` auto-extraction (for source setup)

When a user pastes any `mp.weixin.qq.com/s/...` article URL from an account, the screener fetches the page and extracts `__biz` from the embedded JS (`var biz = "..."`). This auto-populates `hermes.news_sources.biz_id` and constructs the profile URL.

### Deduplication

SHA-256 of article URL → check `staging.spot_knowledge_docs.file_hash`. If exists, skip.

### Article ingestion

Reuses `_fetch_wechat()` logic (extracted from `apps/spot-market/app.py:4637` into `services/hermes/news_screener.py`). Calls `register_and_ingest()` with:
- `app="strategist"`
- `category_override` from AI pipeline result
- New columns populated from AI pipeline

---

## 3. AI Pipeline (per new article)

Single `claude-haiku-4-5` call per article. Prompt provides article title + first ~800 chars of body. Returns structured JSON:

```json
{
  "relevance": 8,
  "region_bucket": "华北",
  "region_province": "内蒙古",
  "category": "market_rules",
  "summary": "内蒙古发布2026年现货市场结算规则修订稿，调整储能调频补偿标准。"
}
```

**Category values:** `policy` | `market_rules` | `market_analytics` | `technology` | `industry_news` | `other`

**Region bucket values:** `华北` | `华东` | `华南` | `西北` | `西南` | `东北` | `全国`

Articles with `relevance < 4` are still ingested but `ingest_status` is set to `"low_relevance"` (searchable but de-prioritised in digest).

**Cost estimate:** ~50 articles/day × ~1 000 input tokens × $0.25/MTok ≈ $0.01/day.

---

## 4. Feishu Daily Digest

**Schedule:** Hermes scheduler at **06:30 UTC (14:30 Beijing)**, after scraping completes.

**Format:** Feishu interactive card (`send_card`) with three tiers based on `relevance_score`:

```
📰 今日能源资讯 — 2026-06-27
12 篇新文章 · 来自 8 个来源

🔥 重点关注 (relevance ≥ 8)
  • [储能补贴新政策出台] ← clickable link
    中关村储能联盟 · 政策 · 全国
    内蒙古发布补贴标准…

📊 值得关注 (relevance 6–7)
  • [彭博NEF：2026储能装机展望]
    彭博新能源财经 · 市场分析 · 全国

📋 其他更新 (relevance < 6)
  4 篇文章已录入知识库（不展开）
```

If zero new articles: send a brief "今日无新内容" card rather than skipping.

---

## 5. Sources Management UI (Spot Market app — new "News Sources" tab)

**Tab location:** New tab inserted between "Data Management" and existing tabs in `apps/spot-market/app.py`.

**UI layout:**

```
📡 News Sources

[+ Add Source]  [Run Now]  [Last run: 2026-06-27 14:30]

┌─────────────────────────────────────────────────────────────┐
│ Name         │ Type   │ Region  │ Category hint │ Active │ ⋮ │
├─────────────────────────────────────────────────────────────┤
│ 飔合科技     │ WeChat │ 全国    │ technology    │ ✅     │ 🗑 │
│ 兰木达       │ WeChat │ 全国    │ market_analytics│ ✅   │ 🗑 │
│ ...          │        │         │               │        │   │
└─────────────────────────────────────────────────────────────┘

Add Source expander:
  Name: [____________]
  Type: [WeChat ▾]
  URL or WeChat article URL: [________________________]
       → paste any mp.weixin.qq.com/s/... article to auto-extract __biz
  Region hint: [全国 ▾]
  Category hint: [other ▾]
  [Add Source]
```

**"Run Now" button:** Triggers a POST to `hermes/news-screener/run` (new Hermes endpoint) and shows a progress spinner. For manual on-demand runs outside the daily schedule.

---

## 6. Hermes Changes

### New endpoint: `POST /hermes/news-screener/run`

Kicks off `screen_news_sources()` as a background task. Returns `{"status": "started"}` immediately.

### Scheduler addition

```python
# Daily at 06:00 UTC (14:00 Beijing) — scrape + ingest
# Digest card sent at 06:30 UTC after scraping
```

Added to the existing `while True` loop in `services/hermes/app.py`.

---

## 7. Error Handling

- Per-source scrape failures are logged and skipped; other sources proceed.
- Per-article AI failures: ingest without AI metadata (`relevance_score=NULL`), mark `ingest_status="ai_error"`.
- If Feishu card fails to send: log error, do not retry (next day's run will cover new articles).
- Sources with 3 consecutive scrape failures: auto-set `active=FALSE`, send a Feishu alert.

---

## 8. File Layout

```
services/hermes/
  news_screener.py        # NEW: scrape, score, ingest, digest
apps/spot-market/
  app.py                  # ADD: News Sources tab (~150 lines)
```

No new Dockerfile, no new ECS task. The `_fetch_wechat()` helper is extracted from `app.py` into `news_screener.py` and re-imported in `app.py`.

---

## Out of Scope

- Full-text WeChat account search (Sogou) — `__biz` is obtained from a single pasted article URL
- Email delivery of digest
- Per-user notification preferences
- Historical backfill beyond last 48h on first run
