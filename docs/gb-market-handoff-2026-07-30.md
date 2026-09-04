# GB Market Handoff — 2026-07-30

## Context for new Claude session

Continue GB Market work on branch `feat/deal-structurer-bedrock-migration`.
App is live at `pjh-etrm.ai/gb-market/`.
ECS cluster: `bess-platform-cluster`, service: `bess-platform-gb-market-svc`.

---

## Current deployment state

| Item | Value |
|------|-------|
| Docker image | `bess-gb-market:v98` |
| ECS task def | `bess-gb-market:15` (family `bess-gb-market`, NOT `bess-platform-gb-market`) |
| Task CPU/memory | 2048 / 8192 MB |
| Task exec role | `arn:aws:iam::319383842493:role/bess-platform-task-exec` |
| Task role | `arn:aws:iam::319383842493:role/bess-platform-task-role` |
| ECR repo | `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-gb-market` |
| Region | `ap-southeast-1` |

**IMPORTANT**: Always register task defs under family `bess-gb-market` (NOT `bess-platform-gb-market` which is a dead family with 106 revisions). Use cpu=2048, memory=8192.

Build & deploy commands:
```bash
# Build
docker build --platform linux/amd64 -t bess-gb-market:vXX -f apps/gb-market/Dockerfile .

# Push
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
ECR=319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-gb-market
docker tag bess-gb-market:vXX ${ECR}:vXX && docker push ${ECR}:vXX

# Update container-def.json image tag, then:
MSYS_NO_PATHCONV=1 aws ecs register-task-definition \
  --family bess-gb-market --network-mode awsvpc \
  --requires-compatibilities FARGATE --cpu 2048 --memory 8192 \
  --execution-role-arn arn:aws:iam::319383842493:role/bess-platform-task-exec \
  --task-role-arn arn:aws:iam::319383842493:role/bess-platform-task-role \
  --container-definitions file://container-def.json \
  --region ap-southeast-1 --query 'taskDefinition.revision' --output text

MSYS_NO_PATHCONV=1 aws ecs update-service \
  --cluster bess-platform-cluster \
  --service bess-platform-gb-market-svc \
  --task-definition bess-gb-market:XX \
  --region ap-southeast-1
```

---

## Problem history: Modo magic-link emails

### Root causes (all fixed)

1. **`MODO_MAGIC_LINK_URL` env var** held an expired magic link. The 20:00 SGT `_modo_ai_job` navigated to it → Modo sent a new link email. **Fixed**: removed `MODO_MAGIC_LINK_URL` from `container-def.json` entirely.

2. **`modo_reports` website scraper** was included in the 3:30 AM knowledge job (`_daily_knowledge_job`). It visited `modoenergy.com/sign-in` during scraping → Modo sent security emails. **Fixed** in `scheduler_service.py`: removed `"modo"` from the `only=` list.

3. **`playwright-stealth>=1.0`** resolved to v2.x → `ImportError` → Playwright ran without stealth. **Fixed** by pinning `playwright-stealth==1.0.6` in Dockerfile (v93).

4. **`playwright-stealth==1.0.6` itself** caused `body=''` on all modoenergy.com pages — the stealth JS conflicted with Cloudflare's challenge and prevented React from rendering. **Fixed** in v97: removed playwright-stealth entirely. Without stealth, password login Flow A works (confirmed in v92 CloudWatch logs). The emails were caused by issues #1 and #2, not the password login itself.

### Current state

- No `MODO_MAGIC_LINK_URL` in env vars ✓
- `"modo"` removed from 3:30 AM knowledge job ✓
- playwright-stealth removed ✓
- 20:00 SGT `_modo_ai_job` will try password login Flow A → should succeed
- Session saved to `/tmp/modo_session.json` after successful login (ephemeral, lost on container restart)

### If magic-link emails resume

Check CloudWatch logs for the 20:00 SGT job:
```bash
MSYS_NO_PATHCONV=1 aws logs filter-log-events \
  --log-group-name /ecs/bess-platform \
  --log-stream-name-prefix gb-market/gb-market \
  --filter-pattern "modo_ai" \
  --start-time $(date -d '2 hours ago' +%s000) \
  --region ap-southeast-1 \
  --query 'events[*].message' --output json
```

---

## Modo re-authentication (manual)

If the nightly job fails (`_modo_ai_job` logs show login failure):

### Option A — Password auth via app UI (preferred)
1. Go to `pjh-etrm.ai/gb-market/` → Data Management tab
2. Click **"Try Password Auth"** — runs in background thread, shows live counter
3. Wait for green success message

### Option B — Magic link via app UI
1. Go to `https://modoenergy.com/sign-in` in your **real browser** (not via the app)
2. Enter `dipeng.chen@envision-energy.com` → Continue → Modo sends magic link email
3. In the email: **right-click the link → Copy link address** (DO NOT click it — single-use, some email clients pre-fetch/burn the link)
4. In the app: paste URL into **"Step 2 — Paste the link from the email"**
5. Click **"Authenticate & Save Session"** — runs in background thread, shows live counter `(0s, 2s, 4s…)`
6. If result shows `link-is-incorrect` → link was already consumed; request a fresh one

### Magic link timing
Magic links are single-use and expire. The Playwright browser launch takes ~15-30s. If the link expires before navigation completes, try again with a fresh link.

---

## Key files

| File | Purpose |
|------|---------|
| `apps/gb-market/Dockerfile` | Pins `playwright-stealth==1.0.6` (kept in Dockerfile but not used in code) |
| `apps/gb-market/scheduler_service.py` | APScheduler jobs; `_daily_knowledge_job` excludes `"modo"` and `"modo_ai"` |
| `apps/gb-market/app.py` | Streamlit app; auth buttons at ~line 4013; `_MA_AUTH_STATE`/`_RL_AUTH_STATE` for thread-safe Playwright calls |
| `services/gb_knowledge/modo_ai.py` | Playwright login logic; stealth removed; `networkidle` + `wait_for_selector` waits |
| `container-def.json` | Local only (gitignored); ECS container env vars; no `MODO_MAGIC_LINK_URL` |

---

## Scheduler jobs (Asia/Singapore timezone)

| Time | Job | Notes |
|------|-----|-------|
| 03:00 | `_daily_market_job` | Modo API → RDS market data + fuel mix |
| 03:30 | `_daily_knowledge_job` | elexon, entso_e, timera, meteologica only |
| 03:45 | `_kb_digest_job` | KB → expert insights (Anthropic) |
| 04:30 | `_pricing_batch_job` | Pricing calculations |
| 06:00 | `_daily_report_job` | PDF report → email + WeCom |
| 09:15 | `_elexon_ops_job` | Settlement prices + wind forecast |
| 20:00 | `_modo_ai_job` | **Modo AI distillation** — Playwright password login |

---

## Pending work

### Immediate — verify tonight/tomorrow
- [ ] **Tonight 20:00 SGT**: Check CloudWatch that `_modo_ai_job` logs show successful password login (no `login failed` or `magic link` errors)
- [ ] **Tomorrow morning**: No magic-link emails at 3:30 AM → confirms email fix is holding
- [ ] **Tomorrow 03:00–06:00**: Check CloudWatch for market ingestion + report success

### Outstanding features (not yet done)
- [ ] End-to-end test gas spark spread tools in Strategist tab
- [ ] The `v68` version label in the app footer (`app.py:2423`) is hardcoded — update to match Docker image tag if desired

---

## CloudWatch log queries

```bash
# All gb-market logs (last 2 hours)
MSYS_NO_PATHCONV=1 aws logs filter-log-events \
  --log-group-name /ecs/bess-platform \
  --log-stream-name-prefix gb-market/gb-market \
  --start-time $(date -d '2 hours ago' +%s000) \
  --region ap-southeast-1 \
  --query 'events[*].message' --output json

# Modo AI job specifically
MSYS_NO_PATHCONV=1 aws logs filter-log-events \
  --log-group-name /ecs/bess-platform \
  --log-stream-name-prefix gb-market/gb-market \
  --filter-pattern "modo_ai" \
  --start-time $(date -d '24 hours ago' +%s000) \
  --region ap-southeast-1 \
  --query 'events[*].message' --output json

# Errors only
MSYS_NO_PATHCONV=1 aws logs filter-log-events \
  --log-group-name /ecs/bess-platform \
  --log-stream-name-prefix gb-market/gb-market \
  --filter-pattern "ERROR" \
  --start-time $(date -d '24 hours ago' +%s000) \
  --region ap-southeast-1 \
  --query 'events[*].message' --output json
```

Note: On Windows Git Bash, `MSYS_NO_PATHCONV=1` is required to prevent path conversion of the log group name.

---

## Branch & repo

- Branch: `feat/deal-structurer-bedrock-migration`
- Remote: `https://github.com/MonteCarlo79/invest-etrm-intel.git`
- Last commit: `d918077` — fix(gb-market): remove playwright-stealth + thread Playwright UI calls
