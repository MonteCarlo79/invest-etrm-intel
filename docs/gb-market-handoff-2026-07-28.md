# GB Market — Handoff 2026-07-28

## Context for new Claude session

You are continuing work on the **bess-platform** repository.
Working directory: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`
Primary branch for GB Market: **`feat/deal-structurer-bedrock-migration`**

---

## Current deployment state

| Item | Value |
|------|-------|
| Live image | `bess-gb-market:v92` (ECR `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-gb-market:v92`) |
| ECS task def | `bess-gb-market:9` (updated 2026-07-28) |
| ECS service | `bess-platform-gb-market-svc` in cluster `bess-platform-cluster` |
| Region | `ap-southeast-1` |
| CPU / Memory | 2048 / 8192 |
| Task role | `bess-platform-task-role` |
| Exec role | `bess-platform-task-exec` |
| App URL | `pjh-etrm.ai/gb-market/` |

---

## What was done this session (2026-07-28)

### 1. Root-caused the 3:30 AM Modo magic-link email (still arriving in v92)

**Investigation method:** pulled ECS CloudWatch logs for the running task
(`gb-market/gb-market/17cdfba874f445b88af2aa5fabc4df6d` in log group `/ecs/bess-platform`).

**Finding:** The 3:30 AM scheduler fix IS working. Logs confirmed `_daily_knowledge_job`
ran at 3:30 AM SGT with only plain HTTP scrapers (no Playwright, no modo_ai).

**Actual root cause:** The email was triggered by the **20:00 SGT `_modo_ai_job`**:
1. Job starts, checks saved session → none (fresh container started at 17:36 SGT)
2. Tries `MODO_MAGIC_LINK_URL` env var → the old expired link from container-def.json
3. Modo returns `link-is-incorrect?type=login` → Modo sends a **new** magic link email
   as part of its "expired link" error flow
4. Job falls back to password login (Flow A: "sign in with a password" link) → **succeeds**
5. Session saved to `/tmp/modo_session.json`
6. All 20 questions answered, 1 new doc inserted

The email appeared at 3:30 AM (7.5h after the 20:00 trigger) due to Microsoft Exchange
phishing scan delay on magic-link URLs.

### 2. Fix: removed stale MODO_MAGIC_LINK_URL from task definition

- Removed `MODO_MAGIC_LINK_URL` from `container-def.json`
- Registered **`bess-gb-market:9`** (no Docker rebuild needed)
- Updated ECS service → new task started automatically

**Why this is safe:** Password login Flow A works reliably. The flow is:
1. Check saved session → use if valid (skips login entirely)
2. ~~Try MODO_MAGIC_LINK_URL~~ (removed)
3. Password login (Flow A) → succeeds silently

No more expired-link trigger → no more spurious magic-link emails.

---

## Nightly scheduler behaviour (confirmed from logs)

| Time (SGT) | Job | Status |
|------------|-----|--------|
| 03:00 | `_daily_market_job` | ✅ runs via Modo Energy API (no Playwright) |
| 03:30 | `_daily_knowledge_job` | ✅ runs elexon/entso_e/timera/modo_reports/meteologica only |
| 03:45 | `_kb_digest_job` | ✅ Bedrock Haiku extracts insights |
| 20:00 | `_modo_ai_job` | ✅ Playwright password login (Flow A), 20 questions |
| 04:30 | `_pricing_batch_job` | not yet verified |
| 06:00 | `_daily_report_job` | not yet verified |
| 09:15 | `_elexon_ops_job` | not yet verified |

---

## Modo authentication — current state

- **Password login (Flow A)** works: `sign in with a password` link visible on sign-in page
- **Saved session** at `/tmp/modo_session.json` — persists within the container lifetime
- **MODO_MAGIC_LINK_URL** is now **removed** from task definition
- When session expires, the job auto-falls back to password login — no action needed
- Only action needed: if Modo changes its auth flow and password login stops working,
  then update `MODO_MAGIC_LINK_URL` in container-def.json with a fresh link + re-register task def

---

## Pending tasks

1. **End-to-end test gas tools** (not yet done):
   - Open Strategist tab at `pjh-etrm.ai/gb-market/`
   - Expand "Gas & Carbon Prices" panel
   - Enter NBP (e.g. 85 p/therm), EUA (e.g. 65 €/tonne)
   - Ask: "What's the clean spark spread at today's system price?"
2. **Monitor tonight's 20:00 SGT `_modo_ai_job`** — confirm no magic-link email received
3. **Verify 09:15 / 04:30 / 06:00 jobs** fire correctly (not yet checked in logs)

---

## Critical: OneDrive + Git branch issue

`apps/gb-market/` **only exists on `feat/deal-structurer-bedrock-migration`**.
OneDrive stores other-branch files as cloud-only placeholders — `ls apps/gb-market/` on
the wrong branch shows only `__pycache__`. **Always checkout this branch before building.**

---

## How to check ECS logs

```bash
# Current running task ID: 17cdfba874f445b88af2aa5fabc4df6d
# (may change after container restarts — use list-tasks to get current ID)
aws ecs list-tasks \
  --cluster bess-platform-cluster \
  --service-name bess-platform-gb-market-svc \
  --region ap-southeast-1

# Get logs (MSYS_NO_PATHCONV=1 required on Windows Git Bash)
MSYS_NO_PATHCONV=1 aws logs get-log-events \
  --log-group-name '/ecs/bess-platform' \
  --log-stream-name 'gb-market/gb-market/<TASK_ID>' \
  --region ap-southeast-1 \
  --start-time <epoch_ms> \
  --end-time <epoch_ms> \
  --query 'events[*].[timestamp,message]' \
  --output text
```

SGT = UTC+8. 20:00 SGT = 12:00 UTC. 03:30 SGT = 19:30 UTC (previous calendar day).

---

## ECS Deploy workflow (reference — for when a new Docker image is needed)

```powershell
# 1. Switch to gb-market branch (REQUIRED — OneDrive cloud-only files)
git checkout feat/deal-structurer-bedrock-migration

# 2. ECR login (token expires every ~12h)
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com

# 3. Build (repo root as context — shared/ dir must be accessible)
docker build -t bess-gb-market:vNN -f "apps/gb-market/Dockerfile" .
docker tag bess-gb-market:vNN 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-gb-market:vNN
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-gb-market:vNN

# 4. Update container-def.json image tag to vNN, then register task def:
aws ecs register-task-definition `
  --family bess-gb-market `
  --container-definitions file://container-def.json `
  --network-mode awsvpc --requires-compatibilities FARGATE `
  --cpu 2048 --memory 8192 `
  --task-role-arn arn:aws:iam::319383842493:role/bess-platform-task-role `
  --execution-role-arn arn:aws:iam::319383842493:role/bess-platform-task-exec `
  --region ap-southeast-1

# 5. Update ECS service:
aws ecs update-service `
  --cluster bess-platform-cluster `
  --service bess-platform-gb-market-svc `
  --task-definition bess-gb-market `
  --region ap-southeast-1
```

> `container-def.json` is **gitignored** (contains secrets). Local only at repo root.
> Current image: `bess-gb-market:v92`. Task def: `bess-gb-market:9`.
> `MODO_MAGIC_LINK_URL` has been **removed** — do NOT add it back unless password login breaks.

---

## Key files

| File | Purpose |
|------|---------|
| `apps/gb-market/app.py` | Main Streamlit app — gas tools, Strategist, auth, scheduler UI |
| `apps/gb-market/scheduler_service.py` | APScheduler — all nightly jobs |
| `services/gb_knowledge/modo_ai.py` | Modo Energy AI connector — Playwright, password login Flow A |
| `services/gb_knowledge/modo_reports.py` | Modo website scraper — plain HTTP, no auth |
| `services/gb_knowledge/ingest.py` | KB ingest orchestrator — excludes modo_ai at 3:30 |
| `shared/anthropic_client.py` | Bedrock-aware Anthropic client factory |
| `container-def.json` | ECS container definition — **gitignored**, local only |

---

## Gas & Carbon tools (added in previous session — in v92)

**UI:** Strategist tab → "Gas & Carbon Prices" expander → NBP (p/therm), TTF (€/MWh), EUA (€/tonne)

**Tools available to Strategist agent:**
- `get_gas_prices` — reads session state, returns current analyst-entered prices
- `calc_spark_spread` — clean spark spread = power − gas_cost − carbon_cost

**Formula:**
```
gas_cost_£_MWh   = (nbp_pence_per_therm / 100) / 0.105506 * heat_rate_GJ_MWh
carbon_cost_£_MWh = carbon_price_eur_tonne * carbon_factor_tCO2_MWh * gas_fx_£_per_€
clean_spark_spread = power_price_£_MWh - gas_cost_£_MWh - carbon_cost_£_MWh
```

Typical CCGT: heat_rate = 7.0 GJ/MWh, carbon_factor = 0.36 tCO₂/MWh, gas_fx = 0.86 £/€

> Modo Energy API confirmed to have **no gas price data** — prices must be entered manually.
