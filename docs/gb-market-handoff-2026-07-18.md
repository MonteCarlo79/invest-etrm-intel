# GB Market — Handoff 2026-07-18

## Context for new Claude session

You are continuing work on the **bess-platform** repository, branch `cost-optimisation`.
Working directory: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`

---

## What was accomplished this session

### Problem
Despite fixes committed in the 2026-07-17 session, 4 magic-link emails from Modo Energy
**still arrived at 03:30 SGT on 2026-07-18**.

### Root cause (identified 2026-07-18)

The Docker image `bess-gb-market:v81` (task def `:97`) was built **before** any of the three
fix commits were made. The committed code was correct; the deployed container was running
stale code. Evidence from CloudWatch logs:

| Evidence | Meaning |
|----------|---------|
| `_modo_ai_job` cron shows `hour='4'` not `hour='20'` | `b1cab43` (reschedule to 20:00) not in image |
| Every job fires twice with ~20ms gap | `70c73bf` (remove app.py duplicate scheduler) not in image |
| modo_ai starts 4s after knowledge job completes | `e0f20c6` (remove modo_ai from ingest.py) not in image |
| Anthropic API returning 401 | `ANTHROPIC_API_KEY` in task def `:97` was expired |

**All three previous fixes were in the repo but not in the deployed image.**

### Fixes applied this session

| Action | Detail |
|--------|--------|
| Rebuilt Docker image | `bess-gb-market:v82` — built from current `cost-optimisation` branch HEAD |
| Pushed to ECR | `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-gb-market:v82` |
| Registered task def | `bess-platform-gb-market:99` — v82 image + fresh `ANTHROPIC_API_KEY` |
| Deployed | `bess-platform-gb-market-svc` updated to `:99`, rollout COMPLETED |
| Password auth | Ran successfully post-deploy — session saved to `/tmp/modo_session.json` |

### Current deployed state

| Item | Value |
|------|-------|
| ECR image | `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-gb-market:v82` |
| ECS task definition | `bess-platform-gb-market:99` |
| ECS service | `bess-platform-gb-market-svc` (cluster: `bess-platform-cluster`, region: `ap-southeast-1`) |
| Modo session | Saved to `/tmp/modo_session.json` in running container |

---

## What to verify next

1. **Tonight's 20:00 SGT Modo AI distillation** — check CloudWatch log group `/ecs/bess-platform`,
   stream prefix `gb-market`. Look for:
   - `[modo_ai] Saved session is valid — login skipped` ← good
   - `[modo_ai] Question 1/20:` ← questions being asked
   - **Zero** "Your link for authorization" emails

2. **KB digest (03:45 SGT)** — should now succeed with the new Anthropic API key.
   Look for `[kb_digest] Done — N total insights extracted` (not 401 errors).

3. **Elexon ops backfill still pending** — `gb_elexon_sp` and `gb_wind_forecast` missing
   2026-06-09 → 2026-06-13.
   - Use Data Management → Elexon Ops Backfill (From: 2026-06-09, To: 2026-06-13)

---

## Scheduler jobs (Asia/Singapore timezone) — current schedule

| Time | Job | Notes |
|------|-----|-------|
| 03:00 | Market data ingestion | Modo Energy API → RDS |
| 03:30 | Knowledge base ingest | elexon, entso_e, timera, modo_reports, meteologica (**not** modo_ai) |
| 03:45 | KB digest → expert insights | Claude processes new KB docs |
| **20:00** | **Modo AI distillation** | Playwright → modoenergy.com/home → 20 questions |
| 04:30 | Pricing batch | Options value, PF dispatch, OLS forecast for top-50 BESS assets |
| 06:00 | Daily report | PDF → email + WeCom |
| 09:15 | Elexon ops ingest | Settlement system prices + wind forecast |

> **Note:** All jobs run exclusively in `scheduler_service.py`. The `_start_scheduler()`
> function in `app.py` creates an **empty** scheduler (no jobs) — do not add jobs there.

---

## Architecture — how the container runs

`run.sh` starts two processes:
```
python apps/gb-market/scheduler_service.py &   # ← ALL scheduled jobs live here
exec streamlit run apps/gb-market/app.py        # ← UI only, empty scheduler for status display
```

---

## Known behaviour

- Worst case on future session expiry: **1 email at 20:00** (one login attempt, detects
  magic link, aborts cleanly, sends WeCom alert)
- No duplicate job execution
- No spurious emails at 03:30

---

## Login flow reference (modo_ai.py `_login()`)

```
Flow A  email field + "sign in with a password" link visible → click → password field
Flow B  email field + Continue button → look for password field
Flow C  email + password both visible on page simultaneously
Flow D  after Continue → "check your email" page → magic link sent → self._magic_link_sent = True (no retry)
Flow E  after Continue → SSO page (envision-energy.com) → try Back/password link or page.go_back() → retry password field → self._sso_detected = True (no retry if still fails)
```

## Session persistence

- After successful login: `ctx.storage_state(path="/tmp/modo_session.json")`
- On next run: loads file → navigates to `/home` → if authenticated, skips login entirely
- `/tmp` is ephemeral per container restart — re-auth needed after each ECS redeployment
- To re-auth: **Data Management → Modo Re-Authentication → "Try Password Auth"**

---

## How to redeploy after code changes

```bash
cd "C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform"

# 1. Build
docker build -t bess-gb-market -f apps/gb-market/Dockerfile .

# 2. Push  (last used v82 — increment to vNN)
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker tag bess-gb-market:latest 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-gb-market:vNN
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-gb-market:vNN

# 3. Register new task def — use container-def.json pattern:
#    Copy container-def.json, update image tag to vNN and any changed env vars, then:
#    aws ecs register-task-definition --family bess-platform-gb-market \
#      --execution-role-arn arn:aws:iam::319383842493:role/bess-platform-task-exec \
#      --task-role-arn arn:aws:iam::319383842493:role/bess-platform-task-role \
#      --network-mode awsvpc --requires-compatibilities FARGATE \
#      --cpu 2048 --memory 8192 --region ap-southeast-1 \
#      --container-definitions file://container-def.json
#    IMPORTANT: use file:// to avoid PowerShell quote-stripping JSON

# 4. Deploy
aws ecs update-service \
  --cluster bess-platform-cluster \
  --service bess-platform-gb-market-svc \
  --task-definition bess-platform-gb-market:NN \
  --force-new-deployment \
  --region ap-southeast-1
```

**After any redeploy:** run "Try Password Auth" in the Streamlit app to restore the session.

---

## Environment variables in task definition :99

| Key | Notes |
|-----|-------|
| `PGURL` | PostgreSQL connection string → RDS in ap-southeast-1 |
| `MODO_EMAIL` | dipeng.chen@envision-energy.com |
| `MODO_PASSWORD` | see config/.env |
| `MODO_API_KEY` | see config/.env |
| `ANTHROPIC_API_KEY` | Updated in :99 — old key in :97 was expired (401) |
| `WECOM_WEBHOOK_URL` | WeCom group bot webhook |
| `SMTP_*` | Gmail SMTP credentials |
| `REPORT_TO_EMAIL` | chen_dpeng@hotmail.com |

---

## Key files

- `apps/gb-market/scheduler_service.py` — **canonical scheduler** (all 7 nightly jobs)
- `services/gb_knowledge/modo_ai.py`   — login flows A-E, session persistence, re-auth helpers
- `services/gb_knowledge/ingest.py`    — 03:30 KB ingest orchestrator (modo_ai excluded)
- `apps/gb-market/app.py`              — Streamlit app, port 8508 (empty scheduler, UI only)

---

## Branch and repo

- Repo: `https://github.com/MonteCarlo79/invest-etrm-intel.git`
- Branch: `cost-optimisation`
- Last commits:
  - `b1cab43` — reschedule Modo AI 04:00 → 20:00 SGT
  - `70c73bf` — remove duplicate scheduler from app.py
  - `e0f20c6` — remove modo_ai from 03:30 KB ingest
