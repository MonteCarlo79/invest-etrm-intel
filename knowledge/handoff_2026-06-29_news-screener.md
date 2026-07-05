# Handoff: Spot Market News Screener — 2026-06-29

## Context

Working in `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform` on branch `cost-optimisation`.

## Current Deployment

| Service | Image | Task Def | Notes |
|---------|-------|----------|-------|
| bess-platform-spot-markets-svc | bess-spot-markets:v38 | rev 53 | **v39 built but NOT deployed** |
| bess-platform-hermes-svc | bess-platform-hermes:latest | rev 84 | **needs redeploy for prompt fix** |

## What Was Built Today (2026-06-29)

### 1. AI Scoring Prompt Improved (`services/hermes/news_screener.py`)

Rewrote `_AI_PROMPT` with:
- Explicit 0-10 scoring guide with labelled tiers (9-10: BESS dispatch/prices, 7-8: storage industry, 5-6: general power sector, 3-4: tangential, etc.)
- **Title-only scoring instruction**: when body is empty (Sogou CAPTCHA fallback), be generous — titles with 储能/电力市场/新能源 key terms should score ≥ 6-7
- Instructions for official sources (国家能源局, 中电联, etc.) to score ≥ 5 by default
- Summary instruction: describe the title topic if body is empty

**Why this matters:** All 19 backfilled articles scored < 6 — likely because CAPTCHA was preventing body fetch → title-only AI scoring → conservative scores. The new prompt gives explicit guidance for this case.

### 2. Recent Ingested Articles Panel (`apps/spot-market/app.py`)

New collapsible section ("📋 Recent Ingested Articles") below Add Source:
- Queries `staging.spot_knowledge_docs` for last 40 articles with `source_name IS NOT NULL`
- Shows: Published / Source / Score / Title (first 80 chars) / Summary (first 100 chars) / Status
- Graceful handling if metadata columns don't exist yet (shows info message)

### 3. Suggested Sources Panel (`apps/spot-market/app.py`)

New collapsible section ("💡 Suggested Sources") with 8 pre-configured high-quality WeChat accounts:
- 国家能源局, 中电联发布, 中国储能网, 北极星储能网, 能源新媒, 国网能源研究院, 华北电力交易中心, 电力决策与舆情研究
- Filters out sources already in the DB
- "Quick Add" button adds source and triggers backfill from 2025-01-01
- Uses `st.rerun(scope="fragment")` after add

## Deploy Steps

### Deploy Hermes (prompt fix — no DB changes):
```bash
docker build -f apps/hermes-service/Dockerfile -t bess-platform-hermes:latest .
docker tag bess-platform-hermes:latest 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
aws ecs update-service --cluster bess-platform-cluster --service bess-platform-hermes-svc --force-new-deployment --region ap-southeast-1
```

### Deploy Spot Market (v39):
```bash
IMAGE_TAG=v39 py scripts/update_spot_markets_taskdef.py
```

## Known Issues / Remaining Work

1. **Sogou CAPTCHA on backfill** — root cause: Sogou redirect URLs (`weixin.sogou.com/link?url=...`) sometimes return CAPTCHA instead of redirecting to mp.weixin.qq.com. Prompt fix mitigates scoring impact, but body is still empty. Real fix: decode Sogou URL encoding to get direct `mp.weixin.qq.com` URL before fetching. The URL parameter uses a Sogou-specific character substitution over base64 — a decoder can be found in open-source Sogou scrapers.

2. **Suggested source URLs** — the `_NS_SUGGESTED` list uses Sogou query URLs as placeholders (the discovery engine searches by account name anyway). The `国家能源局` entry has a direct `__biz` URL which may or may not be current; if Quick Add fails for that one, use the Sogou URL pattern like the others.

3. **Relevance thresholds** — all tiers currently use global thresholds (6 = mid, 8 = high). Future improvement: add `min_relevance_for_digest` column to `hermes.news_sources` for per-source tuning.

## Files Changed

```
services/hermes/news_screener.py   — improved AI scoring prompt
apps/spot-market/app.py            — Recent Articles + Suggested Sources panels
```
