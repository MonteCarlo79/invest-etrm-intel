# Hermes Session Handoff — 2026-06-28

## Current Deployment State

| Item | Value |
|---|---|
| ECS cluster | `bess-platform-cluster` |
| ECS service | `bess-platform-hermes-svc` |
| Region | `ap-southeast-1` |
| Image tag | `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest` |
| Task definition | td:84 (latest as of 2026-06-28) |
| Status | **STEADY STATE** — all features live |
| Branch | `cost-optimisation` |
| HEAD | `b717185` |

---

## What Was Done This Session

| Commit | Summary |
|---|---|
| `4140304` | Fix ranking report capped at 30 plants → now shows all |
| `53b0cb1` | Use `marketdata.station_master` for 业主/MW in ranking report |
| `c488eeb` | Include `services/knowledge_pool` in Hermes Docker build context |
| `3166cf5` | Fix `HERMES_URL` defaulting to ALB DNS (GA redirect was returning 405) |
| `d1c688d` | News screener: backfill support, paginated Sogou, HTTP endpoint, UI button |
| `c14016f` | News digest: show article titles + summaries for all tiers |
| `eb4c5bc` | Use Sogou `type=2` article search for WeChat discovery |
| `e588103` | Handle Sogou CAPTCHA — score from title only + backoff |
| `76837f6` | Spot Market News Sources tab: wrap in `@st.fragment` |
| `1ce8439` | Spot Market: use `st.rerun()` not `scope=fragment` after Add Source |
| `d97a617` / `b717185` | Docs: handoff notes + news screener design spec |

---

## All Live Features in Hermes

- **Feishu WebSocket**: working (daemon thread, AES-256-CBC decryption)
- **Telegram webhook**: working (`/hermes/inbound/telegram`)
- **Auto file routing**: ALL document uploads (xlsx/pdf/docx/etc.) auto-classified by Claude Haiku → Chinese provincial or international market folders in OneDrive
- **Interactive routing card**: after upload, Feishu shows green card with re-route buttons
- **`/save` command**: folder picker card to set destination for next upload
- **Survey mode** (`市场调研` / `资产调研` menu button):
  - Region picker card → saves notes + files to `data/market-fundamentals/调研报告/{region}/` + KB ingest
  - Asset survey: region → asset name → `assets/调研/{region}/{asset}/` + KB ingest
- **`/report` command**: manually triggers Mengxi BESS ranking report PDF
- **Chart image forwarding**: agent `GENERATE_CHART` result → Feishu image / Telegram photo
- **BESS screener** (`mengxi_bess_screener.py`): daily 06:30 UTC, detects new BESS plants vs `etrm/bess-platform/data/电站.xlsx`
- **News screener** (`news_screener.py`): daily 06:00 UTC (14:00 Beijing), scrapes WeChat/web/RSS, AI-scores with Haiku, ingests to Strategist KB, sends tiered Feishu digest
- **`/news` chat command**: on-demand news screener run
- **`POST /hermes/news-screener/run`**: HTTP endpoint for Spot Market UI "Run Now" button
- **Capacity ETL**: 各省储能装机 Excel files auto-upserted to `province_installed_monthly` table
- **Reminder scheduler**: daily at 08:05 Beijing
- **Timestamps**: Beijing time appended to every Hermes reply
- **Ranking report fixes** (as of `53b0cb1`):
  - Shows all ~40 plants (not capped at 30)
  - Plants with no MW in 电站.xlsx infer MW from `max_energy × 4`
  - 业主 (owner) column populated from `marketdata.station_master` (40 rows, Claude-screened)

---

## Remaining Tasks

### 1. Seed Initial News Sources (FIRST PRIORITY)
The `hermes.news_sources` table is empty. Use the Spot Market "📡 News Sources" tab to add the 9 initial sources, or call directly:

```python
from services.hermes.news_screener import add_source
# Example:
add_source(pg_url, name="财联社-储能", url="https://...", source_type="rss", biz_id=None, region_bucket="China", category_hint="storage")
```

Initial 9 sources are documented in: `docs/superpowers/specs/2026-06-27-news-screener-design.md` (Section: Seed Sources)

### 2. Deploy Spot Market App
Spot Market v33 with News Sources tab is committed (`80a2218` or later on `cost-optimisation`) but **not yet deployed**.

```bash
bash scripts/deploy_spot_market.sh
```

Or update the ECS task def for `bess-platform-spot-market-svc`.

### 3. Verify Ranking Report 业主 Fix
Send `/report mengxi` in Feishu. The PDF should show ~39-40 plants with correct owner names (not blank). If owners still blank → check `marketdata.station_master` has data:
```sql
SELECT count(*), count(owner) FROM marketdata.station_master;
```

### 4. Outlook Token (90-day rotation)
Microsoft OAuth token for `chen_dpeng@hotmail.com` expires ~every 90 days. When Hermes says "读取邮件失败：400 Client Error":
```bash
py scripts/auth_microsoft_mail.py   # device code flow, prints new token
```
Then update `OUTLOOK_REFRESH_TOKEN` in ECS task def and redeploy.

---

## Key File Locations

| File | Purpose |
|---|---|
| `services/hermes/app.py` | Main FastAPI app — scheduler, routing, all chat commands |
| `services/hermes/news_screener.py` | News screener (729+ lines) — scraping, AI scoring, DB CRUD |
| `services/hermes/mengxi_ranking_report.py` | Mengxi BESS ranking PDF report |
| `services/hermes/mengxi_bess_screener.py` | New BESS plant detection vs 电站.xlsx |
| `services/hermes/onedrive_client.py` | OneDrive Graph API — all paths use `etrm/bess-platform/...` prefix |
| `apps/hermes-service/Dockerfile` | Docker build (gitignored — lives in `apps/hermes-service/`) |
| `scripts/deploy_hermes.sh` | ECR push + ECS force-deploy |
| `db/ddl/hermes/news_sources.sql` | DDL for hermes schema + news_sources table |
| `docs/superpowers/specs/2026-06-27-news-screener-design.md` | Full design spec for news screener |

---

## Critical Deployment Rules

1. **ALWAYS use `:latest` ECR tag** — pinned `:vN` tags cause `CannotPullContainerError` on redeploy. The deploy script now enforces this via `c['image'] = IMAGE`.
2. **OneDrive paths** all use `etrm/bess-platform/...` prefix (e.g. `etrm/bess-platform/data/电站.xlsx`).
3. **Do NOT use `DOCKER_BUILDKIT=0`** — causes issues with the Hermes build.
4. **`apps/hermes-service/`** is gitignored — only `services/hermes/` is tracked. Dockerfile lives in `apps/hermes-service/Dockerfile`.
5. **`lark` WebSocket** must run in a daemon thread (not async), or it blocks FastAPI startup.

---

## DB Tables Used by Hermes

| Table | Purpose |
|---|---|
| `marketdata.station_master` | 40 rows, Claude-screened BESS plant metadata (plant_name, mw, owner) |
| `hermes.news_sources` | News source registry — CRUD via news_screener.py helpers |
| `staging.spot_knowledge_docs` | KB docs — extended with `region_bucket, region_province, source_name, relevance_score, ai_summary, published_at` |
| `hermes_settings` | Key-value store — holds `outlook_refresh_token` (auto-updated on rotation) |
| `province_installed_monthly` | Capacity ETL output |

---

## How to Deploy

```bash
# From repo root (Git Bash on Windows):
bash scripts/deploy_hermes.sh
```

Script: logs in to ECR, builds `apps/hermes-service/Dockerfile`, pushes `:latest`, registers new task def (injecting OneDrive env vars), force-deploys ECS service.

Monitor: 
```bash
aws ecs describe-services --cluster bess-platform-cluster --services bess-platform-hermes-svc --region ap-southeast-1 --query 'services[0].events[:3]' --output json
```
