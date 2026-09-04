# GB Market — Handoff 2026-07-17

## Context for new Claude session

You are continuing work on the **bess-platform** repository, branch `cost-optimisation`.
Working directory: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`

---

## What was accomplished this session

### Problem
Despite a fix deployed in the previous session (2026-07-16), 4 "Your link for authorization"
magic-link emails from Modo Energy were **still arriving nightly**.

### Root cause (identified 2026-07-17)

Two independent OS processes were running **identical APScheduler jobs**:

| Process | Scheduler | Started by |
|---------|-----------|------------|
| `scheduler_service.py` | `BackgroundScheduler` with all 7 jobs | `run.sh` (background `&`) |
| `app.py` (Streamlit) | `BackgroundScheduler` with all 7 jobs | `_start_scheduler()` called at module load (line 2358) |

At 04:00 SGT both processes fired `_modo_ai_job` simultaneously. The magic-link
detection in `_login()` was failing silently (not setting `self._magic_link_sent = True`
fast enough to break the retry loop), so each process completed **2 login attempts**.

**2 processes × 2 attempts = 4 emails.**

### Fixes applied

| Commit | File | Change |
|--------|------|--------|
| `70c73bf` | `apps/gb-market/app.py` | Removed all 7 `scheduler.add_job()` calls and nested job functions from `_start_scheduler()`. The function now creates an **empty** `BackgroundScheduler` (no jobs) used solely for the sidebar status widget. `scheduler_service.py` is the sole scheduler. |
| `b1cab43` | `apps/gb-market/scheduler_service.py` | Rescheduled Modo AI distillation from **04:00 → 20:00 SGT** (per user request). |

### Deployed state

| Item | Value |
|------|-------|
| ECR image | `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-gb-market:v81` |
| ECS task definition | `bess-platform-gb-market:97` |
| ECS service | `bess-platform-gb-market-svc` (cluster: `bess-platform-cluster`, region: `ap-southeast-1`) |
| Last commit | `b1cab43` — reschedule Modo AI 04:00 → 20:00 SGT |

### Session status
- Password auth ran successfully via Streamlit UI after deploy
- Session saved to `/tmp/modo_session.json` in the container
- Tonight's **20:00 SGT** run should load the saved session and skip login entirely

---

## What to verify next

1. **Tonight's 20:00 SGT Modo AI distillation** — check CloudWatch log group `/ecs/bess-platform`,
   stream prefix `gb-market`. Look for:
   - `[modo_ai] Saved session is valid — login skipped` ← good
   - `[modo_ai] Question 1/20:` ← questions being asked
   - **Zero** "Your link for authorization" emails

2. **If session expired** (container redeployed between now and 20:00):
   - Open app → Data Management → Modo Re-Authentication → **"Try Password Auth"**
   - Should succeed in ~60 seconds

3. **Elexon ops backfill still pending** — `gb_elexon_sp` and `gb_wind_forecast` missing
   2026-06-09 → 2026-06-13.
   - Use Data Management → Elexon Ops Backfill (From: 2026-06-09, To: 2026-06-13)

---

## Scheduler jobs (Asia/Singapore timezone) — current schedule

| Time | Job | Notes |
|------|-----|-------|
| 03:00 | Market data ingestion | Modo Energy API → RDS |
| 03:30 | Knowledge base ingest | elexon, entso_e, timera, modo, meteologica (**not** modo_ai) |
| 03:45 | KB digest → expert insights | Claude processes new KB docs |
| **20:00** | **Modo AI distillation** | Playwright → modoenergy.com/home → 20 questions (changed from 04:00) |
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

Adding jobs to `app.py`'s scheduler would cause double-execution. Keep all job logic
in `scheduler_service.py`.

---

## Known behaviour after fixes

- Worst case on future session expiry: **1 email at 20:00** (one login attempt, detects
  magic link, aborts cleanly, sends WeCom alert)
- No more spurious emails at 03:30
- No more duplicate job execution

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

# 2. Push  (last used v81 — increment to vNN)
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker tag bess-gb-market:latest 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-gb-market:vNN
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-gb-market:vNN

# 3. Register new task def (copy env vars from :97, change image tag)
# IMPORTANT: executionRoleArn = bess-platform-task-exec  (NOT task-role)
#            taskRoleArn      = bess-platform-task-role

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

## Environment variables in task definition

All values are in `config/.env` (local, not committed) and ECS task def `:97`.

| Key | Notes |
|-----|-------|
| `PGURL` | PostgreSQL connection string → RDS in ap-southeast-1 |
| `MODO_EMAIL` | dipeng.chen@envision-energy.com |
| `MODO_PASSWORD` | see config/.env |
| `MODO_API_KEY` | see config/.env |
| `ANTHROPIC_API_KEY` | see config/.env |
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
  - `e0f20c6` — remove modo_ai from 03:30 KB ingest (previous session)
