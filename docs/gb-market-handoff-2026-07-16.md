# GB Market — Handoff 2026-07-16

## Context for new Claude session

You are continuing work on the **bess-platform** repository, branch `cost-optimisation`.
Working directory: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`

---

## What was accomplished this session

### Problem
Modo AI nightly distillation was sending 4 spurious "Your link for authorization" emails every night at ~03:30 SGT.

### Root cause
`ModoAIConnector` was invoked **twice** per night:
- 03:30 → `run_knowledge_ingest()` in `ingest.py` (included `modo_ai` in its connector list)
- 04:00 → dedicated `_modo_ai_job()` in `scheduler_service.py`

Each invocation on a fresh container (no `/tmp/modo_session.json`) falls back to
Playwright password login. The retry loop runs up to 2 attempts if `_is_magic_link_page()`
fails to detect the page (cookie banner was masking body text). 2 attempts × 2 jobs = 4 emails.

### Fixes applied

| File | Change |
|------|--------|
| `services/gb_knowledge/ingest.py` | Removed `ModoAIConnector` from 03:30 KB ingest — `modo_ai` only runs at 04:00 now |
| `services/gb_knowledge/modo_ai.py` | Added `_dismiss_cookie_banner()` before each `_is_magic_link_page()` check in `_login()` (3 places) |
| `services/gb_knowledge/modo_ai.py` | Expanded `_is_magic_link_page()`: URL pattern check + 7 extra detection phrases including `"log in to my account"` and `"link for authorization"` |

### Deployed state

| Item | Value |
|------|-------|
| ECR image | `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-gb-market:v79` |
| ECS task definition | `bess-platform-gb-market:95` |
| ECS service | `bess-platform-gb-market-svc` (cluster: `bess-platform-cluster`, region: `ap-southeast-1`) |
| Last commit | `e0f20c6` — fix(modo_ai): stop double-running login |

### Session status
- Password auth ran successfully via Streamlit UI after deploy
- Session saved to `/tmp/modo_session.json` in the container
- Tonight's 04:00 SGT run should load the saved session and skip login entirely

---

## What to verify next

1. **Tonight's 04:00 SGT Modo AI distillation** — check CloudWatch log group `/ecs/bess-platform`, stream prefix `gb-market`. Look for:
   - `[modo_ai] Saved session is valid — login skipped` ← good
   - `[modo_ai] Question 1/20:` ← questions being asked
   - No "Your link for authorization" emails

2. **If session expired** (container redeployed between now and 04:00):
   - Open app → Data Management → Modo Re-Authentication → **"Try Password Auth"**
   - Should succeed in ~60 seconds

3. **Elexon ops backfill still pending** — `gb_elexon_sp` and `gb_wind_forecast` missing 2026-06-09 → 2026-06-13
   - Use Data Management → Elexon Ops Backfill (From: 2026-06-09, To: 2026-06-13)

---

## Known behaviour after this fix

- Worst case on future session expiry: **1 email at 04:00** (one login attempt, detects magic link, aborts cleanly)
- No more spurious emails at 03:30 — `modo_ai` no longer runs there

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

# 2. Push  (increment vNN — last used v79)
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker tag bess-gb-market:latest 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-gb-market:vNN
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-gb-market:vNN

# 3. Register new task def (copy env vars from :95, change image tag)
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

All values are in `config/.env` (local, not committed) and ECS task def `:95`.

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

## Scheduler jobs (Asia/Singapore timezone)

| Time | Job | Notes |
|------|-----|-------|
| 03:00 | Market data ingestion | Modo Energy API → RDS |
| 03:30 | Knowledge base ingest | elexon, entso_e, timera, modo, meteologica (**not** modo_ai) |
| 03:45 | KB digest → expert insights | Claude processes new KB docs |
| **04:00** | **Modo AI distillation** | Playwright → modoenergy.com/home → 20 questions |
| 04:30 | Pricing batch | Options value, PF dispatch, OLS forecast for top-50 BESS assets |
| 06:00 | Daily report | PDF → email + WeCom |
| 09:15 | Elexon ops ingest | Settlement system prices + wind forecast |

---

## Key files

- `services/gb_knowledge/modo_ai.py`  — login flows A-E, session persistence, re-auth helpers
- `services/gb_knowledge/ingest.py`   — 03:30 KB ingest orchestrator (modo_ai excluded)
- `apps/gb-market/app.py`             — Streamlit app, port 8508
- `apps/gb-market/scheduler_service.py`

---

## Branch and repo

- Repo: `https://github.com/MonteCarlo79/invest-etrm-intel.git`
- Branch: `cost-optimisation`
- Last commit: `e0f20c6` — fix(modo_ai): stop double-running login
