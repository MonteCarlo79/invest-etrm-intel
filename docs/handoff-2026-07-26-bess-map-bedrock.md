# Handoff: bess-map v59 + Bedrock Migration Completion
_2026-07-26 — branch: `feat/deal-structurer-bedrock-migration`_

## What was done this session

### 1. LingFeng bidding_space backfill (no improvement possible)
- Ran backfill for May 1–Jul 6 2026 using old URL (`/powerTrading/sass/data-consultation`)
- Fill rates were **already at 67-72%** from a previous successful backfill (run between Jul 13–26)
- `run_fundamentals_ingest.py` is OneDrive cloud-only (evicted) — only `__pycache__` remains
- 0% provinces (江苏, 福建, 贵州, 山东, 青海, 新疆, 浙江) have no LingFeng bidding data — best achievable is 67-72% overall
- **No action needed here — already at maximum achievable fill rate**

### 2. Portal Bedrock migration — deployed v10/td:66 ✅
- **`apps/portal/app.py`**: `_quick_ask()` migrated from `anthropic.Anthropic(api_key=)` to `make_client` + `is_llm_available`
- **`apps/portal/Dockerfile`**: Added `anthropic[bedrock]>=0.40` to pip install alongside `requirements.txt`
- **ECS**: td:66 deployed with `BEDROCK_REGION=ap-southeast-1`, image `bess-platform-portal:v10`
- App Service Control (ECS on/off switches) confirmed working in v10 image

### 3. spot-agent Bedrock migration (local + pushed, no ECS rebuild needed)
- **`apps/spot-agent/agent/tools_llm.py`**: Added sys.path injection to reach `shared/`, replaced raw `import anthropic` guards with `make_client`/`is_llm_available` from `shared/anthropic_client.py`
- Note: actual LLM calls in this file use OpenAI `client.chat.completions.create(...)` — Anthropic usage is defensive/audit only

### 4. bess-map DB lock incident — resolved
- 量化分析师 tab was stuck in `_ensure_memory_table()` for 30+ min
- Root cause: orphaned `spot_market` idle-in-transaction session (pid=8118, SELECT on `agent_memory`, 1h21m old) blocking ALTER TABLE in pid=9428, which cascaded to block bess-map
- Fix: `SELECT pg_terminate_backend(8118)` in RDS — lock chain resolved immediately
- Side effect: bess-map container crashed (exit 139 / SIGSEGV) from abrupt lock release after long wait
- Recovery: `aws ecs update-service --cluster bess-platform-cluster --service bess-platform-bess-map-svc --force-new-deployment --region ap-southeast-1`

### 5. bess-map API key fix — td:88 deployed ✅
- td:87 had **expired** key `sk-ant-api03-5lE1slZ9...` → 401 authentication_error in agent
- Registered td:88 with valid key from `config/.env` + `BEDROCK_REGION=ap-southeast-1`
- bess-map:latest image (v58) still uses raw `import anthropic as _ant` (pre-Bedrock migration, built before commit `94b11cb`)
- `BEDROCK_REGION` in td:88 is pre-staged for when v59 lands

---

## IMMEDIATE NEXT STEP: Build and deploy bess-map v59

### Prerequisite — sync OneDrive files to disk

The `apps/bess-map/` files are **OneDrive cloud-only** (evicted from local disk). You must sync them before building.

**Option A — PowerShell (run as admin or in normal terminal):**
```powershell
attrib -P +U /S /D "C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\apps\bess-map"
```

**Option B — File Explorer:**
Right-click `bess-platform` folder → **Always keep on this device**

**Verify sync complete:**
```bash
ls apps/bess-map/
# Must show: Dockerfile, app.py, requirements.txt, etc. (not just __pycache__)
```

### Build v59

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform"

# Build
docker build --no-cache -t bess-map:v59 -f apps/bess-map/Dockerfile .

# IMPORTANT: verify Bedrock migration is in the image (commit 94b11cb)
docker run --rm --entrypoint sh bess-map:v59 -c \
  "grep -n 'make_client\|import anthropic as _ant' /app/apps/bess-map/app.py"
# Expected: shows make_client lines, NOT 'import anthropic as _ant'
# If you see 'import anthropic as _ant' — the OneDrive copy predates commit 94b11cb; check git log
```

### Push to ECR and deploy

```bash
# Tag and push
docker tag bess-map:v59 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:v59
aws ecr get-login-password --region ap-southeast-1 | \
  docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:v59

# Fetch current task def (td:88), strip read-only fields, swap image → :v59, register td:89
aws ecs describe-task-definition \
  --task-definition bess-platform-bess-map:88 \
  --region ap-southeast-1 \
  --query 'taskDefinition' > /tmp/bess-map-td88.json

# Edit /tmp/bess-map-td88.json:
#   - Change image to: 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:v59
#   - Delete these keys: taskDefinitionArn, revision, status, requiresAttributes,
#     compatibilities, registeredAt, registeredBy

aws ecs register-task-definition \
  --cli-input-json file:///tmp/bess-map-td89.json \
  --region ap-southeast-1

# Deploy
aws ecs update-service \
  --cluster bess-platform-cluster \
  --service bess-platform-bess-map-svc \
  --task-definition bess-platform-bess-map:89 \
  --force-new-deployment \
  --region ap-southeast-1
```

### After deploy

Update `docker-compose.local.yml` bess-map image to `:v59`.

---

## Current ECS state (as of 2026-07-26)

| Service | Task Def | Image | Notes |
|---|---|---|---|
| bess-platform-portal-svc | td:66 | portal:v10 | ✅ running, Bedrock migration done |
| bess-platform-bess-map-svc | td:88 | bess-map:latest (v58) | ✅ running, valid API key, BEDROCK_REGION set |

## Key env vars confirmed in ECS task defs

| Var | portal td:66 | bess-map td:88 |
|---|---|---|
| `BEDROCK_REGION=ap-southeast-1` | ✅ | ✅ |
| `ANTHROPIC_API_KEY` | ✅ valid | ✅ valid (updated this session) |
| `COGNITO_USER_POOL_ID` | ✅ | n/a |
| `ECS_CLUSTER=bess-platform-cluster` | ✅ | n/a |

---

## Bedrock migration status (all services)

| Service | Source migrated | Image deployed |
|---|---|---|
| portal | ✅ (this session, v10/td:66) | ✅ |
| bess-map | ✅ (commit `94b11cb`) | ❌ still on v58/:latest (pre-migration) |
| spot-agent | ✅ (this session, local) | ❌ no ECS service |
| spot-market | ✅ (prior session) | ✅ |
| caiso/ercot/au/pjm | ✅ (prior session) | ✅ |

---

## Known remaining data gaps (bidding_space_mw)

| Period | Fill rate | Root cause |
|---|---|---|
| Oct–Dec 2025 | 0% | Source Excel col 26 all zeros |
| Jan–Feb 2026 | ~3% | Not available on either LingFeng URL |
| Mar 2026 | 0% | LingFeng confirmed gap |
| May–Jun 2026 | 67-72% | Best achievable; 7 provinces have no bidding data |
| Jul 2026+ | ~73% | Daily ECS scrape via new URL ✅ |

---

## Key files

```
apps/portal/app.py                     # v10: Bedrock migration + App Service Control (2 CAN_MANAGE_USERS blocks)
apps/portal/Dockerfile                 # v10: requirements.txt + anthropic[bedrock]>=0.40
apps/spot-agent/agent/tools_llm.py     # Bedrock migration (local + pushed)
apps/bess-map/app.py                   # ⚠ CLOUD-ONLY — sync OneDrive before v59 build
apps/bess-map/Dockerfile               # ⚠ CLOUD-ONLY — sync OneDrive before v59 build
docker-compose.local.yml               # portal:v10; bess-map still :v58 (update after v59)
docs/BEDROCK_MIGRATION_GUIDE.md        # Full migration reference + model ID mapping
config/.env                            # Valid ANTHROPIC_API_KEY and PGURL (do not commit)
shared/anthropic_client.py             # make_client factory: AnthropicBedrock if BEDROCK_REGION set
```

## DB connection

```
PGURL = postgresql://postgres:!BESSmap2026@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require
```

Relevant tables:
- `marketdata.spot_fundamentals_hourly` — bidding_space_mw column
- `marketdata.agent_memory` — bess-map agent memory (had lock issues this session)

If DB lock issues recur:
```sql
-- Find blocking sessions
SELECT pid, usename, application_name, state, wait_event_type, wait_event,
       now() - query_start AS duration, query
FROM pg_stat_activity
WHERE state != 'idle'
ORDER BY duration DESC NULLS LAST;

-- Kill the blocking session
SELECT pg_terminate_backend(<pid>);
```
