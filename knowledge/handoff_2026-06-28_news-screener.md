# Handoff: Spot Market News Screener — 2026-06-28

## Context

Working in `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform` on branch `cost-optimisation`.
All changes pushed to GitHub (MonteCarlo79/invest-etrm-intel).

## What Was Built

### 1. Hermes News Screener (`services/hermes/news_screener.py`)

Automated daily news pipeline: discovers WeChat articles → fetches body → AI scores with Haiku → ingests to Strategist KB → sends tiered Feishu digest.

**Key architecture decisions:**
- WeChat discovery uses **Sogou type=2 article search** (`weixin.sogou.com/weixin?type=2&query={name}`) — NOT direct WeChat profile pages (those require login and always return 0 articles)
- `SogouCaptchaError` raised when Sogou serves a rate-limit verification page; fallback = score from **title only** (article still ingested, body="")
- 72h cutoff window (not 48h) to avoid missing articles near boundary
- Feishu digest tiers: 🔥 relevance≥8, 📊 6-7, 📋 <6 — all tiers show article titles + ★N score + AI summary (up to 10 per tier)

**Backfill:**
- `_discover_wechat_paginated(source, start_date, max_pages=30)` — paginates Sogou, 1.5s/page delay
- `backfill_source(source, start_date, pg_url, api_key)` — 2s/article delay + exponential CAPTCHA backoff
- CLI: `py scripts/backfill_news.py --start-date 2025-01-01 [--source-id N] [--dry-run]`

**HTTP endpoints on Hermes:**
- `POST /hermes/news-screener/run` — trigger daily screener (background)
- `POST /hermes/news-screener/backfill` — body: `{"start_date":"2025-01-01","source_id":5}` (source_id optional); sends per-source Feishu completion notice

### 2. Spot Market App News Sources Tab (`apps/spot-market/app.py`)

13th tab "📡 News Sources" (between Strategist and Library).

**Fragment isolation:** entire tab wrapped in `@st.fragment` → `_render_news_sources_tab()` to prevent full-page grey-out on interactions. `st.rerun(scope="fragment")` for checkbox/edit/delete/save; plain `st.rerun()` for Add Source.

**CRUD helpers inlined** (no `services.hermes` import — that module is not COPYed into the spot-market Docker image):
- `_ns_init_db()` — auto-creates `hermes` schema + `hermes.news_sources` table on first load
- `_ns_get_sources()`, `_ns_add_source()`, `_ns_set_active()`, `_ns_delete()`

**Header buttons:**
- `▶ Run Now` → `POST {HERMES_URL}/hermes/news-screener/run`
- `⏮ Backfill All (2025-01-01)` → `POST {HERMES_URL}/hermes/news-screener/backfill`
- `↻ Refresh`

**Add Source auto-backfill:** after successful insert, immediately calls `/hermes/news-screener/backfill` with the new source's ID and `start_date=2025-01-01`. Shows Feishu notification when done.

**All HTTP calls to Hermes use `verify=False`** (internal ALB; TLS cert not resolvable from ECS container).

### 3. DB Schema

`hermes.news_sources` table (auto-created):
```sql
id SERIAL PRIMARY KEY, name TEXT, url TEXT, source_type TEXT DEFAULT 'wechat',
biz_id TEXT, region_bucket TEXT, category_hint TEXT, scrape_config JSONB,
active BOOLEAN DEFAULT TRUE, last_scraped_at TIMESTAMPTZ,
consecutive_failures INT DEFAULT 0, created_at TIMESTAMPTZ,
UNIQUE(name, url)
```

`staging.spot_knowledge_docs` — extra columns added:
`region_bucket, region_province, source_name, relevance_score, ai_summary, published_at`

## Current Deployment State

| Service | Image | Task Def | Commit |
|---------|-------|----------|--------|
| bess-platform-spot-markets-svc | bess-spot-markets:v38 | rev 53 | 1ce8439 |
| bess-platform-hermes-svc | bess-platform-hermes:latest | rev 84 | e588103 |

**Deploy commands:**
```bash
# Spot Market
IMAGE_TAG=vNN py scripts/update_spot_markets_taskdef.py

# Hermes
docker build -f apps/hermes-service/Dockerfile -t bess-platform-hermes:latest .
docker tag bess-platform-hermes:latest 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
aws ecs update-service --cluster bess-platform-cluster --service bess-platform-hermes-svc --force-new-deployment --region ap-southeast-1
```

**Critical env var:** `HERMES_URL` must be `https://bess-platform-alb-1158505371.ap-southeast-1.elb.amazonaws.com` (the ALB DNS), NOT `https://pjh-etrm.ai` — the latter routes through AWS Global Accelerator which issues a 301 redirect that downgrades POST → GET, causing 405 on all Hermes endpoints. The deploy script default is now set correctly.

## Known Issues / Possible Next Work

1. **Sogou CAPTCHA on backfill** — backfill is slow (2s/article delay) and still hits CAPTCHA on large batches. Articles scored from title only when body fetch fails. This is acceptable but could be improved by resolving the Sogou redirect URL to extract the actual `mp.weixin.qq.com` URL and fetching directly.

2. **All 19 articles scoring relevance < 6** — the two sources added (兰木达, 飔合科技) may genuinely be low relevance to China power markets. More relevant sources should be added (e.g. 中电联, 国家能源局 official accounts). Consider tuning the AI prompt or lowering the tier boundary.

3. **Source seeding** — 8 sources were configured as of 2026-06-28:
   - 中关村储能、中国电力报、中国能源报、兰木达、北极星电力市场网、南方能源观察、电联新媒、飔合科技、新能源报告
   More can be added via the News Sources tab UI.

4. **Relevance calibration** — consider adding a `relevance_threshold` field to `hermes.news_sources` so each source can have a custom minimum score before ingest.

## Files Changed in This Session

```
services/hermes/news_screener.py    — Sogou type=2, backfill, CAPTCHA handling, digest improvements
services/hermes/app.py              — /backfill endpoint, import backfill_source
apps/spot-market/app.py             — News Sources tab (@st.fragment, CRUD, Run Now, Backfill All)
scripts/update_spot_markets_taskdef.py — default HERMES_URL fixed to ALB DNS
scripts/backfill_news.py            — new: CLI backfill script
```

## How to Continue

Start a new session with:

```
Read knowledge/handoff_2026-06-28_news-screener.md for context, then continue work on the 
spot-market app and Hermes news screener. Current branch: cost-optimisation.
Spot market: v38/td:53. Hermes: latest/td:84.
```
