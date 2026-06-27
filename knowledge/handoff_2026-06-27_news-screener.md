# Handoff — 2026-06-27 — News Screener + Deployment

## Branch / repo
- Repo: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`
- Branch: **`cost-optimisation`** (pushed to origin, 7 commits ahead of last session)

---

## What was completed this session

All work is committed and pushed to `origin/cost-optimisation`.

### 1. Streaming Strategist (7288a91)
`apps/spot-market/app.py` — `_run_agent_turn` uses `client.messages.stream`; per-tool status
indicators show while waiting for tool results; `st.empty()` handles live text output.

### 2. Hermes service additions (98ba8d0 + bf7f4ed)

**98ba8d0** — survey mode, BESS screener via `/report`, chart-sending, Lingfeng session
collector.

**bf7f4ed** — Full news screener implementation:
- `services/hermes/news_screener.py` (729 lines) — DB init (`hermes.news_sources` + new
  columns on `staging.spot_knowledge_docs`), per-source discovery (WeChat/web/RSS), article
  fetch, Claude Haiku AI scoring (relevance 0–10, region, category, summary), KB ingest,
  tiered Feishu digest card, source CRUD helpers.
- `services/hermes/app.py` — cron at 06:00 UTC (14:00 Beijing), `POST /hermes/news-screener/run`
  endpoint, `/news` chat command for manual trigger.
- `db/ddl/hermes/news_sources.sql` — DDL file for hermes schema + table.

### 3. News Sources tab in Spot Market (945a1ab)
`apps/spot-market/app.py` — new "📡 News Sources" tab (between Strategist and Library):
- Sources table with per-row active toggle and delete
- Last-run timestamp from `MAX(last_scraped_at)`
- "Run Now" button → `POST HERMES_URL/hermes/news-screener/run`
- "Add Source" expander with auto-`__biz` extraction for WeChat article URLs
- EN + ZH i18n strings

---

## ECS / deployment state

| Service | ECR repo | Latest pushed image | Task def revision | Deployed? |
|---|---|---|---|---|
| Spot Markets | `bess-spot-markets` | `v33` (Jun 26 23:26) | rev 42 (v33) | ✅ YES but OLD |
| Hermes | `bess-platform-hermes` | `:latest` (Jun 27 00:12) | rev 81 (v17) | ✅ YES but OLD |

**Both images are stale** — they predate today's code changes. Deploy needed:

### Deploy Spot Markets → v34

```bash
cd C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform

docker build -t bess-spot-markets:v34 -f apps/spot-market/Dockerfile . --platform linux/amd64
docker tag bess-spot-markets:v34 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v34
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v34
```

Then register new task def (base on rev 42, swap image to v34) and update service:
```bash
# Get current task def, swap image, register, update service
aws ecs describe-task-definition --task-definition bess-platform-spot-markets --query taskDefinition > /tmp/td.json
# Edit /tmp/td.json: change image to :v34, remove revision/taskDefinitionArn/requiresAttributes/compatibilities/registeredAt/registeredBy/status
# aws ecs register-task-definition --cli-input-json file:///tmp/td.json
# aws ecs update-service --cluster bess-platform-cluster --service bess-platform-spot-markets-svc --task-definition bess-platform-spot-markets:<NEW_REV>
```

Or use the convenience script: `py scripts/update_hermes_taskdef.py` (adapt for spot-markets).

### Deploy Hermes → latest

```bash
docker build -t bess-platform-hermes:latest -f apps/hermes-service/Dockerfile . --platform linux/amd64
docker tag bess-platform-hermes:latest 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
# Force new deployment (hermes uses :latest tag so just force-new-deployment)
aws ecs update-service --cluster bess-platform-cluster --service bess-platform-hermes-svc --force-new-deployment
```

---

## Env vars needed for News Sources tab

| Var | Used by | Value |
|---|---|---|
| `PGURL` | spot-market + hermes | existing `marketdata` RDS URL |
| `HERMES_URL` | spot-market (Run Now button) | e.g. `https://hermes.pjh-etrm.ai` or internal VPC URL |
| `ANTHROPIC_API_KEY` | hermes news screener (Haiku scoring) | existing key |

`HERMES_URL` needs to be added to the spot-markets task def environment variables so the
"Run Now" button can reach Hermes.

---

## Seeding initial 9 sources

After deploying, seed the sources using the Spot Market app's "Add Source" UI, or via SQL:

```sql
-- run in marketdata DB after hermes news_screener._init_db() creates the table
INSERT INTO hermes.news_sources (name, url, source_type, biz_id, region_bucket, category_hint)
VALUES
  ('飔合科技',          'https://mp.weixin.qq.com/mp/profile_ext?action=home&__biz=<BIZ>&scene=124', 'wechat', '<BIZ>', '全国', 'technology'),
  ('兰木达',            'https://mp.weixin.qq.com/mp/profile_ext?action=home&__biz=<BIZ>&scene=124', 'wechat', '<BIZ>', '全国', 'market_analytics'),
  ('中关村储能联盟CNESA','https://mp.weixin.qq.com/mp/profile_ext?action=home&__biz=<BIZ>&scene=124', 'wechat', '<BIZ>', '全国', 'policy'),
  ('南方能源观察',       'https://mp.weixin.qq.com/mp/profile_ext?action=home&__biz=<BIZ>&scene=124', 'wechat', '<BIZ>', '华南', 'market_analytics'),
  ('中国能源观察',       'https://mp.weixin.qq.com/mp/profile_ext?action=home&__biz=<BIZ>&scene=124', 'wechat', '<BIZ>', '全国', 'industry_news'),
  ('储能与电力市场',     'https://mp.weixin.qq.com/mp/profile_ext?action=home&__biz=<BIZ>&scene=124', 'wechat', '<BIZ>', '全国', 'market_rules'),
  ('彭博新能源财经',     'https://mp.weixin.qq.com/mp/profile_ext?action=home&__biz=<BIZ>&scene=124', 'wechat', '<BIZ>', '全国', 'market_analytics'),
  ('西北储能',          'https://mp.weixin.qq.com/mp/profile_ext?action=home&__biz=<BIZ>&scene=124', 'wechat', '<BIZ>', '西北', 'industry_news'),
  ('北极星储能网',       'https://chuneng.bjx.com.cn/', 'web', NULL, '全国', 'industry_news')
ON CONFLICT DO NOTHING;
```

**To get real biz_ids**: paste any article URL from each account into the "Add Source" form
— `add_source()` auto-extracts `__biz` and rewrites the URL to the profile URL.

---

## Key files

| File | Purpose |
|---|---|
| `services/hermes/news_screener.py` | Scraper + AI + KB ingest + Feishu digest + CRUD |
| `services/hermes/app.py` | Scheduler (06:00 UTC) + `/hermes/news-screener/run` endpoint |
| `apps/spot-market/app.py` | News Sources tab (~line 4142) |
| `db/ddl/hermes/news_sources.sql` | DDL reference |

---

## Remaining / next steps

1. **Deploy both services** (commands above) — blocked on Docker build (requires local Docker)
2. **Add `HERMES_URL` env var** to spot-markets task definition
3. **Seed initial 9 sources** via UI or SQL (need real `__biz` values from article URLs)
4. **Test "Run Now"** manually once deployed — should trigger scrape + Feishu digest
5. **`backfill_embeddings.py`** — still unresolved; confirm RDS SG allows current IP
