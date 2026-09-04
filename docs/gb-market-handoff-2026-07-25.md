# GB Market App — Session Handoff (2026-07-25)

## Current State

- **Live image**: `bess-gb-market:v89` on ECS service `bess-platform-gb-market-svc` (cluster `bess-platform-cluster`, region `ap-southeast-1`)
- **Task def**: `bess-gb-market:3` — 2 vCPU / 8 GB RAM — task role `bess-platform-task-role`, exec role `bess-platform-task-exec`
- **Bedrock**: ACTIVE — `BEDROCK_REGION=ap-southeast-1` in env; `shared/anthropic_client.py` factory auto-routes all `_make_anthropic_client()` calls to `global.anthropic.*` inference profiles

## What Was Done This Session

### 1. Bedrock Migration (app.py + daily_report.py)
- `app.py` line ~18: `import anthropic` → `from shared.anthropic_client import make_client as _make_anthropic_client`
- `app.py` line ~140: `_client = anthropic.Anthropic(api_key=_ANTHROPIC_KEY)` → `_client = _make_anthropic_client(_ANTHROPIC_KEY)`
- `daily_report.py`: replaced raw `anthropic.Anthropic()` with `make_client()` + `is_llm_available()` guard
- `Dockerfile`: `anthropic>=0.40` → `anthropic[bedrock]>=0.40`
- Requires `BEDROCK_REGION` env var to activate (already set in v89 container-def)

### 2. import time NameError Fix (app.py)
- Added `import time` at module level (line 13) — was missing, caused NameError in `_PW_AUTH_STATE["start"] = time.monotonic()`

### 3. Modo Re-Auth Button (app.py ~line 3819)
- "Try Password Auth" button uses background thread + module-level `_PW_AUTH_STATE` dict
- `st.session_state` writes from background threads silently fail — must use module-level dict
- Pattern: thread sets `_PW_AUTH_STATE["result"]`, main thread polls with `time.sleep(3); st.rerun()`
- **Status**: v89 is deployed, button should work — NOT yet verified working end-to-end

### 4. 3:30 SGT Magic-Link Email Fix (scheduler_service.py)
- **Root cause**: `_daily_knowledge_job` at 03:30 SGT called `run_knowledge_ingest(only=None)` which includes `modo_ai` source → Playwright login → magic-link email every morning
- **Fix**: changed to `only=["elexon", "entso_e", "timera", "modo", "meteologica"]` (excludes `modo_ai`)
- `modo_ai` runs separately via `_modo_ai_job` at 20:00 SGT
- Committed & pushed

### 5. ECS Deployment Lessons
- Cluster: `bess-platform-cluster` (NOT `bess-platform`)
- Service: `bess-platform-gb-market-svc`
- Task family: `bess-gb-market` (our managed family; Terraform family is `bess-platform-gb-market`)
- **Always use 2048 CPU / 8192 MB** — Playwright+Chromium needs 8 GB; earlier used 2 GB → OOM crash
- **Always specify both roles**: `--task-role-arn bess-platform-task-role` + `--execution-role-arn bess-platform-task-exec`
- `bess-platform-task-role` has inline policy `bess-platform-task-bedrock` → Bedrock calls work
- Deploy command:
  ```powershell
  aws ecs register-task-definition `
    --family bess-gb-market `
    --container-definitions file://container-def.json `
    --network-mode awsvpc --requires-compatibilities FARGATE `
    --cpu 2048 --memory 8192 `
    --task-role-arn arn:aws:iam::319383842493:role/bess-platform-task-role `
    --execution-role-arn arn:aws:iam::319383842493:role/bess-platform-task-exec `
    --region ap-southeast-1

  aws ecs update-service `
    --cluster bess-platform-cluster `
    --service bess-platform-gb-market-svc `
    --task-definition bess-gb-market `
    --region ap-southeast-1
  ```
- `container-def.json` is gitignored (contains secrets) — source of truth is local file only

## Pending / Left Off

### A. Modo Re-Auth Button — needs end-to-end verification
The "Try Password Auth" button in the app's Data Management → Modo Re-Authentication section. After hard refresh on v89, click the button and confirm:
- Spinner appears ("Running headless login… Xs elapsed")
- Either succeeds (session saved) or shows error with page dump
- If Playwright OOMs, check CloudWatch logs for `[modo_auth]` prints

### B. 3:30 Emails — confirm fixed
Watch tomorrow morning (03:30 SGT). If no magic-link email arrives, the fix works. v89 scheduler excludes `modo_ai` from knowledge ingest.

### C. Gas Market Tools for Strategist (Feature Request)
The Strategist told the user it has no gas market connectivity (NBP, TTF, EUA carbon, spark spreads). User asked if this can be improved. Proposed additions:
1. **`get_gas_prices` tool** — fetch NBP day-ahead, TTF front-month, EUA carbon from a data source (Modo or third-party API)
2. **`calc_spark_spread` tool** — derived: spark spread = power price − (gas price × heat rate) − (carbon price × emissions factor)
3. **Gas-power nexus KB** — scrape/ingest Timera/Meteologica gas-power analysis into knowledge base

This is a non-trivial feature. The Strategist tools are defined in `apps/gb-market/app.py` in the tool definitions block (search for `get_system_price` to find the tool list). Adding a gas tool requires: (a) a data source for NBP/TTF/EUA, (b) a new tool function, (c) registering it in the tool list.

## Key Files

| File | Purpose |
|------|---------|
| `apps/gb-market/app.py` | Main Streamlit app — Strategist, auth button, all UI |
| `apps/gb-market/scheduler_service.py` | Standalone scheduler (runs as separate process in container) |
| `apps/gb-market/daily_report.py` | Daily PDF report generation + email/WeCom |
| `apps/gb-market/Dockerfile` | Container build — python:3.11-slim + playwright chromium |
| `apps/gb-market/run.sh` | Entrypoint — starts scheduler in background, then Streamlit |
| `services/gb_knowledge/modo_ai.py` | Playwright-based Modo Energy login + AI question flow |
| `shared/anthropic_client.py` | Bedrock-aware Anthropic client factory |
| `container-def.json` | ECS container definition (gitignored, local only) |

## Scheduler Times (Asia/Singapore)

| Time | Job |
|------|-----|
| 03:00 | Market data ingestion (Modo API → RDS) |
| 03:30 | Knowledge-base ingestion (elexon, entso_e, timera, modo, meteologica — NOT modo_ai) |
| 03:45 | KB digest → expert insights |
| 04:30 | Pricing batch |
| 06:00 | Daily report → email + WeCom |
| 09:15 | Elexon ops ingest |
| 20:00 | Modo AI distillation (Playwright login + questions) |
