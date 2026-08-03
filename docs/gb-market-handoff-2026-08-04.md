# GB Market Handoff — 2026-08-04

## Instruction for new Claude session

Continue GB Market work on branch `feat/deal-structurer-bedrock-migration`.
App live at `pjh-etrm.ai/gb-market/`.
Read this document fully before taking any action.

---

## Deployment state

| Item | Value |
|------|-------|
| Docker image | `bess-gb-market:v99` |
| ECS task def | `bess-gb-market:16` (family `bess-gb-market`) |
| ECS service | `bess-platform-gb-market-svc` |
| ECS cluster | `bess-platform-cluster` |
| Region | `ap-southeast-1` |
| Task CPU / memory | 2048 / 8192 MB |
| Execution role | `arn:aws:iam::319383842493:role/bess-platform-task-exec` |
| Task role | `arn:aws:iam::319383842493:role/bess-platform-task-role` |
| ECR repo | `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-gb-market` |

**CRITICAL**: Always register task defs under family `bess-gb-market` (NOT `bess-platform-gb-market` — dead family). Use cpu=2048, memory=8192 for the correct roles above.

### Build & deploy snippet (PowerShell-friendly)
```bash
docker build --platform linux/amd64 -t bess-gb-market:vXX -f apps/gb-market/Dockerfile .
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
ECR=319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-gb-market
docker tag bess-gb-market:vXX ${ECR}:vXX && docker push ${ECR}:vXX
# update container-def.json image tag, then:
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
  --task-definition bess-gb-market:XX --region ap-southeast-1
```

---

## Modo magic-link email problem — full history

### Root cause (confirmed via CloudWatch)

The 4 emails arriving at **3:30 AM SGT every morning** are Modo's own security notification. They are triggered by the **20:00 SGT `_modo_ai_job`** doing a fresh password login. Exchange delays them ~7.5 h (20:00 + 7.5 h = 03:30 AM next day).

The login succeeds (Flow A: email → "sign in with a password" → password → /home). Modo sends the notification regardless of login success whenever a headless browser authenticates.

### Fix chain (v92 → v99)

| Version | Fix |
|---------|-----|
| v92 | Removed expired `MODO_MAGIC_LINK_URL` from container-def.json |
| v93 | Removed `"modo"` scraper from 3:30 AM knowledge job; pinned `playwright-stealth==1.0.6` |
| v94 | Added `wait_for_selector` after sign-in nav for React hydration |
| v95 | Added same wait in `request_magic_link_email()` |
| v96 | Added `networkidle` wait before selector search |
| v97 | **Removed playwright-stealth entirely** — stealth caused blank page body (`body=''`) on Cloudflare-protected modoenergy.com, blocking all form interaction. Without stealth, password login Flow A works (confirmed v92 logs). |
| v98 | Moved Playwright UI calls (`authenticate_with_magic_link`, `request_magic_link_email`) to background threads (`_MA_AUTH_STATE`, `_RL_AUTH_STATE`) — they were blocking Streamlit's event loop, causing WebSocket 404 disconnects. |
| **v99** | **Added `_modo_keepalive_job` at 14:00 SGT** — refreshes saved session cookies 6 h before the 20:00 job. If session still valid, Playwright navigates to /home and re-saves the updated cookies. When 20:00 job fires, it uses the restored session → skips password login → Modo sends no email. |

### Keep-alive mechanism

- Session saved to `/tmp/modo_session.json` after every successful login (ephemeral — lost if ECS task restarts)
- Modo session cookies expire in ~24 h
- Keep-alive at 14:00 SGT refreshes cookies within their window → extends another ~24 h
- 20:00 SGT job checks restored session first → if valid, skips login entirely

### Expected timeline to first email-free morning

| Day | Event |
|-----|-------|
| Aug 4 03:30 AM | Last email from Aug 3 old-container login (unavoidable) |
| Aug 4 14:00 SGT | Keep-alive fires — NO session yet (fresh container) → skips |
| Aug 4 20:00 SGT | Full login → session saved to `/tmp/modo_session.json` → email arrives Aug 5 03:30 AM |
| Aug 5 14:00 SGT | Keep-alive → session ~18 h old → refreshes ✓ |
| **Aug 5 20:00 SGT** | **Restored session → no login → no email** |
| **Aug 6 03:30 AM** | **First clean morning (no email)** |

### Verification commands (PowerShell)

```powershell
# After 14:00 SGT Aug 5 — confirm keep-alive ran
aws logs filter-log-events `
  --log-group-name /ecs/bess-platform `
  --log-stream-name-prefix gb-market/gb-market `
  --filter-pattern "keepalive" `
  --start-time ([DateTimeOffset]::UtcNow.AddHours(-4).ToUnixTimeMilliseconds()) `
  --region ap-southeast-1 `
  --query "events[*].message" --output json

# After 20:00 SGT Aug 5 — confirm session restored (no login)
aws logs filter-log-events `
  --log-group-name /ecs/bess-platform `
  --log-stream-name-prefix gb-market/gb-market `
  --filter-pattern "modo_ai" `
  --start-time ([DateTimeOffset]::UtcNow.AddHours(-4).ToUnixTimeMilliseconds()) `
  --region ap-southeast-1 `
  --query "events[*].message" --output json
```

**Success indicators:**
- Keep-alive: `"[modo_ai] keepalive: session still valid — cookies refreshed"`
- 20:00 job: `"[modo_ai] Saved session is valid — login skipped"` (NOT `"Login attempt 1/2"`)

**If session still expired (keep-alive logged "session expired"):**  
The 20:00 job will do a fresh login. One more email cycle. Session is then saved and the next day's keep-alive should catch it.

---

## If emails continue after Aug 6

Check whether the ECS task restarted between Aug 4 20:00 and Aug 5 14:00 (a restart wipes `/tmp/modo_session.json`):

```powershell
MSYS_NO_PATHCONV=1 aws ecs describe-services `
  --cluster bess-platform-cluster `
  --services bess-platform-gb-market-svc `
  --region ap-southeast-1 `
  --query "services[0].events[:5]" --output json
```

If restarts are frequent, the permanent solution is to persist the session externally (e.g. S3 or RDS) instead of `/tmp`. See `_SESSION_PATH` in `services/gb_knowledge/modo_ai.py`.

---

## Scheduler jobs (Asia/Singapore)

| Time (SGT) | UTC | Job | Notes |
|---|---|---|---|
| 03:00 | 19:00 prev | `_daily_market_job` | Modo API + fuel mix → RDS |
| 03:30 | 19:30 prev | `_daily_knowledge_job` | elexon, entso_e, timera, meteologica **only** |
| 03:45 | 19:45 prev | `_kb_digest_job` | KB → expert insights |
| 04:30 | 20:30 prev | `_pricing_batch_job` | Pricing batch |
| 06:00 | 22:00 prev | `_daily_report_job` | PDF report → email + WeCom |
| 09:15 | 01:15 | `_elexon_ops_job` | Settlement prices + wind |
| **14:00** | **06:00** | **`_modo_keepalive_job`** | **Refresh Modo session cookies** |
| 20:00 | 12:00 | `_modo_ai_job` | Playwright password login + 20 questions |

---

## Key files

| File | What changed |
|------|-------------|
| `services/gb_knowledge/modo_ai.py` | Stealth removed; `networkidle` waits; `keepalive_session()` function added; auth UI functions threaded |
| `apps/gb-market/scheduler_service.py` | `_modo_keepalive_job()` added; scheduled at 14:00 SGT |
| `apps/gb-market/app.py` | `_MA_AUTH_STATE`, `_RL_AUTH_STATE` for non-blocking Playwright UI; version label hardcoded "v68" (cosmetic only) |
| `container-def.json` | Local/gitignored; no `MODO_MAGIC_LINK_URL`; current image `bess-gb-market:v99` |

---

## Manual re-authentication (if nightly job fails)

### Option A — Password auth (preferred, no magic link needed)
1. `pjh-etrm.ai/gb-market/` → Data Management tab
2. Click **"Try Password Auth"** — runs in background, shows live counter
3. Wait for green success message

### Option B — Magic link
1. Open `https://modoenergy.com/sign-in` in your **real browser** (not via app)
2. Enter `dipeng.chen@envision-energy.com` → Continue → email sent
3. In the email: **right-click the link → Copy link address** (do NOT click — single-use)
4. In app: paste into **"Step 2 — Paste the link from the email"**
5. Click **"Authenticate & Save Session"** — background thread, shows counter

Magic links expire in ~10 min. Playwright launch takes ~15–30 s. Paste and click promptly.

---

## Outstanding work

- [ ] **Verify Aug 6 03:30 AM — no magic-link email** (confirms keep-alive fix)
- [ ] **Verify Aug 5 20:00 SGT CloudWatch** — `"Saved session is valid — login skipped"`
- [ ] End-to-end test gas spark spread tools in Strategist tab (never done)
- [ ] Consider persisting Modo session to S3/RDS instead of `/tmp` if ECS restarts frequently
- [ ] Update hardcoded `"v68"` version label in `app.py:2423`

---

## Branch & repo

- Branch: `feat/deal-structurer-bedrock-migration`
- Remote: `https://github.com/MonteCarlo79/invest-etrm-intel.git`
- Last GB commit: `55f8f98` — fix(gb-market): add Modo session keep-alive
