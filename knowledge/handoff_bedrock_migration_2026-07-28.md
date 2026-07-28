# Handoff — Bedrock Migration: PH, PO, CAISO, ERCOT, AU, PJM
**Date:** 2026-07-28  
**Branches:** `feat/ph-po-market-apps` (PH/PO), `feat/deal-structurer-bedrock-migration` (all others)

---

## Deployed State — All Apps on Bedrock

| App | ECR Image | Task Def | taskRoleArn | BEDROCK_REGION | Status |
|-----|-----------|----------|-------------|----------------|--------|
| ph-market | `bess-ph-market:v16` | `:25` | `bess-platform-task-role` | `ap-southeast-1` | ✅ |
| po-market | `bess-po-market:v16` | `:26` | `bess-platform-task-role` | `ap-southeast-1` | ✅ |
| caiso-market | `bess-caiso-market:v5` | `:12` | (inherited) | `ap-southeast-1` | ✅ |
| ercot-market | `bess-ercot-market:v5` | `:9` | (inherited) | `ap-southeast-1` | ✅ |
| au-market | `bess-au-market:v8` | `:18` | (inherited) | `ap-southeast-1` | ✅ |
| pjm-market | `bess-pjm-market:v5` | `:9` | (inherited) | `ap-southeast-1` | ✅ |

**The exposed Anthropic API key (`sk-ant-api03-54oaa3nF...`) should be revoked** — no ECS service depends on it anymore. Revoke at console.anthropic.com.

---

## What Was Done

### PH + PO market apps (`feat/ph-po-market-apps`, commits `4c5b02c`, `504a29d`)

**Code changes (both apps):**
- `apps/ph-market/app.py` line 20: `import anthropic` → `from shared.anthropic_client import make_client as _make_anthropic_client`
- `apps/ph-market/app.py` line 54: `anthropic.Anthropic(api_key=...)` → `_make_anthropic_client(...)`
- `apps/po-market/app.py`: same pattern
- Both Dockerfiles: `anthropic>=0.40` → `anthropic[bedrock]>=0.40`; added `COPY shared/ ./shared/`
- `shared/__init__.py` + `shared/anthropic_client.py` added to branch (branch predated these files)

**PO task role fix (td:26):** PO task def `:25` had wrong `taskRoleArn: ecsTaskExecutionRole`. Fixed to `bess-platform-task-role` in `:26`. PH was already correct.

### CAISO, ERCOT, AU, PJM apps (`feat/deal-structurer-bedrock-migration`, commit `e702e28`)

These 4 apps all use `run_market_app()` from `services/intl_market_common/app_template.py`, which already had `make_client()`. Only Dockerfile changes were needed:
- All 4 Dockerfiles: `anthropic>=0.40` → `anthropic[bedrock]>=0.40`; added `COPY shared/ ./shared/`
- `services/intl_market_common/app_template.py` line 20: removed dead `import anthropic` (client already via `make_client()`)

---

## Key Technical Facts

### How make_client() works
```python
from shared.anthropic_client import make_client as _make_anthropic_client
client = _make_anthropic_client(api_key)
# BEDROCK_REGION set → AnthropicBedrock via IAM role, api_key ignored
# BEDROCK_REGION not set → Anthropic(api_key=api_key or ANTHROPIC_API_KEY)
```
Model strings (`"claude-sonnet-4-6"`, `"claude-haiku-4-5-20251001"`) auto-mapped to Bedrock IDs.

### Bedrock model IDs (confirmed working, ap-southeast-1)
| Direct API | Bedrock ID |
|---|---|
| `claude-sonnet-4-6` | `global.anthropic.claude-sonnet-4-6` |
| `claude-opus-4-6` | `global.anthropic.claude-opus-4-6-v1` |
| `claude-haiku-4-5-20251001` | `global.anthropic.claude-haiku-4-5-20251001-v1:0` |

### IAM
- Task role `bess-platform-task-role` has `bedrock:InvokeModel` + `bedrock:InvokeModelWithResponseStream`
- **Always verify `taskRoleArn` when registering new task defs** — PO had wrong role (`ecsTaskExecutionRole`) and Bedrock calls got 403

### Docker build lesson
After rebuilding, always re-tag before pushing:
```bash
docker tag <local>:vN <ecr>/<repo>:vN   # re-tag AFTER rebuild
docker push <ecr>/<repo>:vN
```
Docker push can silently reuse a stale ECR tag if you skip the re-tag step.

---

## Remaining Services NOT yet on Bedrock

From the ECS cluster scan:
- `bess-platform-mengxi-dashboard-svc` — `live_sk_key=True`, `BEDROCK_REGION=False` ← **needs migration**
- `bess-platform-bess-map-svc` — `live_sk_key=True`, `BEDROCK_REGION=False` ← **needs migration**

(All others are either on Bedrock or don't use Claude.)

---

## Pending Work — PH + PO Apps

- **FCR/aFRR backfill** — trigger `scrape_po_fcr_prices(conn, weeks_back=104)` for 2 years of history (PO Data Management tab → "Scrape" button, or direct call)
- **PO IRR parity with PH** — add "Market-Data Driven BESS IRR" section to PO Investment Analysis tab (uses `po_day_ahead_prices`)
- **Rynek Mocy backfill** — check/fix TGE HTML scraper URL in `scrape_po_capacity_market()`

---

## AWS / Infra Quick Reference

```
ECR registry:   319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
ECS cluster:    bess-platform-cluster
Task role:      bess-platform-task-role   (has bedrock:InvokeModel)
Exec role:      bess-platform-task-exec
MSYS prefix:    MSYS_NO_PATHCONV=1 aws ecs ...

PH service:     bess-platform-ph-market-svc   port 8510
PO service:     bess-platform-po-market-svc   port 8511
CAISO service:  bess-platform-caiso-market-svc
ERCOT service:  bess-platform-ercot-market-svc
AU service:     bess-platform-au-market-svc
PJM service:    bess-platform-pjm-market-svc

Docker builds:
  docker build -f apps/ph-market/Dockerfile -t bess-ph-market:vN .
  docker build -f apps/po-market/Dockerfile -t bess-po-market:vN .
  docker build -f apps/caiso-market/Dockerfile -t bess-caiso-market:vN .
  docker build -f apps/ercot-market/Dockerfile -t bess-ercot-market:vN .
  docker build -f apps/au-market/Dockerfile -t bess-au-market:vN .
  docker build -f apps/pjm-market/Dockerfile -t bess-pjm-market:vN .
```

---

## Open Items / Stashed Work

`feat/deal-structurer-bedrock-migration` has uncommitted changes to:
- `apps/portal/Dockerfile`
- `apps/portal/app.py`
- `apps/spot-agent/agent/tools_llm.py`
- `docker-compose.local.yml`

These were stashed and restored but not committed — likely unrelated work in progress.
