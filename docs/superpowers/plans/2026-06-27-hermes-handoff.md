# Hermes Session Handoff — 2026-06-27

## What was done this session (all pushed to `cost-optimisation`)

| Commit | Summary |
|---|---|
| `98ba8d0` | Survey mode (市场调研/资产调研), BESS screener, /report command, chart sending, lingfeng session-based collector, portal CST fix |
| `bf7f4ed` | News screener wired into Hermes: scheduler, /hermes/news-screener/run endpoint, /news chat command |
| `e7746ec` | **Bugfix**: correct OneDrive path for 电站.xlsx — `bess-platform/data/` → `etrm/bess-platform/data/` |

---

## Current Hermes State

### Features live in code (not yet deployed)

- **Survey mode**: send "调研报告" or "资产调研" in Feishu → region picker card → saves notes + files to OneDrive `调研报告/{region}/` or `assets/调研/{region}/{asset}/` + KB ingest
- **BESS screener** (`mengxi_bess_screener.py`): daily 06:30 UTC, detects new BESS plants in MD data vs `etrm/bess-platform/data/电站.xlsx`, appends new plants + notifies
- **/report** chat command: manually triggers Mengxi ranking report
- **Chart sending**: agent GENERATE_CHART → Feishu image or Telegram photo
- **News screener** (`news_screener.py`): daily 06:00 UTC (14:00 Beijing), scrapes WeChat/web/RSS, AI-scores with Haiku, ingests to Strategist KB, sends tiered Feishu digest card
- **/news** chat command: on-demand news screener
- **POST /hermes/news-screener/run**: HTTP endpoint for Spot Market "Run Now" button

### DB changes required (run on RDS before first use)

The news screener's `_init_db()` runs automatically on first call, but if you want to pre-create:
```
db/ddl/hermes/news_sources.sql
```
Creates `hermes` schema, `hermes.news_sources` table, and adds columns to `staging.spot_knowledge_docs`.

---

## Next tasks remaining

### 1. Deploy Hermes v15 (CRITICAL — bugfix in e7746ec not live yet)
```bash
# Build and push
docker build -t bess-platform-hermes:latest -f services/hermes/Dockerfile .
# Tag and push to ECR
aws ecr get-login-password --region ap-east-1 | docker login --username AWS --password-stdin <ECR_URI>
docker tag bess-platform-hermes:latest <ECR_URI>/bess-platform-hermes:latest
docker push <ECR_URI>/bess-platform-hermes:latest
# Force redeploy
aws ecs update-service --cluster bess-platform-cluster --service hermes --force-new-deployment
```
Or use `scripts/deploy_hermes.sh` / `scripts/deploy_hermes.ps1`.

### 2. Add News Sources tab to Spot Market app (Task 3)
The other Claude session may have done this. Check `apps/spot-market/app.py` first.

If not done: add a "📡 News Sources" tab using these functions from `services/hermes/news_screener.py`:
- `get_sources(pg_url, active_only=False)` → list all sources
- `add_source(pg_url, name, url, source_type, biz_id, region_bucket, category_hint)` → auto-extracts `biz_id` from WeChat article URLs
- `set_source_active(pg_url, source_id, active)` → toggle on/off
- `delete_source(pg_url, source_id)` → delete

"Run Now" button → `POST https://<hermes-host>/hermes/news-screener/run`

"Last run" info → `MAX(last_scraped_at)` from `hermes.news_sources`

### 3. Seed initial news sources
After deploying, seed the 9 initial sources via the News Sources UI, or call:
```python
from services.hermes.news_screener import _init_db, add_source
```

### 4. Deploy Spot Market app (after Task 2)
Deploy once News Sources tab is added.

---

## Key file locations

| File | Purpose |
|---|---|
| `services/hermes/app.py` | Main FastAPI app, scheduler, chat routing |
| `services/hermes/news_screener.py` | News screener (729 lines) |
| `services/hermes/mengxi_ranking_report.py` | Mengxi BESS ranking daily report |
| `services/hermes/mengxi_bess_screener.py` | New BESS plant detection |
| `services/hermes/onedrive_client.py` | OneDrive Graph API client — all paths use `etrm/bess-platform/...` prefix |
| `db/ddl/hermes/news_sources.sql` | DDL for hermes schema |
| `docs/superpowers/specs/2026-06-27-news-screener-design.md` | Full design spec for news screener |

---

## Known issue fixed this session

**OneDrive 404 for 电站.xlsx**: both `mengxi_ranking_report.py` and `mengxi_bess_screener.py` used path `"bess-platform/data/电站.xlsx"` but correct path is `"etrm/bess-platform/data/电站.xlsx"`. Fixed in `e7746ec`. **Deploy Hermes to apply the fix.**
