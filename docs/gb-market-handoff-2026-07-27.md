# GB Market — Handoff 2026-07-27

## Context for new Claude session

You are continuing work on the **bess-platform** repository.
Working directory: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`
Primary branch for GB Market: **`feat/deal-structurer-bedrock-migration`**

---

## Current deployment state

| Item | Value |
|------|-------|
| Live image | `bess-gb-market:v92` (ECR `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-gb-market:v92`) |
| ECS task def | `bess-gb-market:8` |
| ECS service | `bess-platform-gb-market-svc` in cluster `bess-platform-cluster` |
| Region | `ap-southeast-1` |
| CPU / Memory | 2048 / 8192 |
| Task role | `bess-platform-task-role` |
| Exec role | `bess-platform-task-exec` |
| App URL | `pjh-etrm.ai/gb-market/` |
| Deployed at | 2026-07-27 ~17:37 SGT |

---

## What was done this session (2026-07-25 → 2026-07-27)

### 1. Gas & Carbon Market tools added to Strategist (Options A + C)

**Option A — Manual gas price input UI + tools**

In `apps/gb-market/app.py`:

- `st.session_state["gas_prices"]` dict `{nbp, ttf, eua}` stores analyst-provided prices
- **Gas & Carbon Prices expander** added in Strategist tab (below insight count caption):
  three columns for NBP day-ahead (p/therm), TTF front-month (€/MWh), EUA carbon (€/tonne)
- **`get_gas_prices` tool** — reads session state, returns current prices as structured text
- **`calc_spark_spread` tool** — computes clean spark spread:
  - Inputs: `power_price` (£/MWh), `nbp_pence_per_therm`, `carbon_price_eur_tonne`,
    `heat_rate` (GJ/MWh, default 7.0), `efficiency` (default None),
    `carbon_factor` (tCO₂/MWh, default 0.36), `gas_fx` (£/€, default 0.86)
  - Conversion: NBP p/therm ÷ 100 → £/therm ÷ 0.105506 GJ/therm → £/GJ × heat_rate → £/MWh_gas
  - Clean spark spread = power − gas_cost − carbon_cost
- Both tools added to `_STRATEGIST_TOOLS` and handled in `_dispatch_strategist()`

**Option C — System prompt KB routing for gas-power analysis**

`_GB_STRATEGIST_BASE_SYSTEM` updated with gas-power nexus domain context:
```
- Gas-power nexus: GB power prices are driven by the marginal gas generator
  - Spark spread = power price − (gas price × heat rate) − (carbon price × emissions factor)
  - Typical CCGT: heat rate ~7 GJ/MWh (efficiency ~49%), emissions factor ~0.36 tCO₂/MWh
  - NBP: GB wholesale gas benchmark, quoted in p/therm
  - TTF: continental European gas benchmark, quoted in €/MWh
  - EUA: EU carbon price, quoted in €/tonne CO₂
- For current NBP/TTF/EUA prices → call get_gas_prices
- For spark spread calculation → call calc_spark_spread
- For gas-power analysis → call search_knowledge_base with sources=['timera', 'meteologica']
```

`_build_strategist_system()` injects gas prices into system prompt when set.

> **Note:** Modo Energy API confirmed to have **no gas price data** (NBP/TTF/EUA).
> Prices must be entered manually in the UI expander.

### 2. Fixed 3:30 AM Modo magic-link emails

Root cause: `scheduler_service.py` `_daily_knowledge_job` at 03:30 SGT included `modo_ai` source
→ Playwright launched → Modo sent magic-link email for `dipeng.chen@envision-energy.com`.

Fix (already committed as `ed1edb9` before this session):
```python
def _daily_knowledge_job():
    results = run_knowledge_ingest(
        only=["elexon", "entso_e", "timera", "modo", "meteologica"], verbose=False
    )  # modo_ai excluded — avoids Playwright login at 3:30 SGT
```

v92 contains this fix. Previous image v89 did NOT (built before the fix commit).

**Pending confirmation:** 3:30 SGT 2026-07-28 — should be no email if v92 is working correctly.

### 3. Fixed Run Backfill 404/502 errors

Root cause: `_run_ingestion_job()` blocking main Streamlit thread → WebSocket timeout → ALB 502.

Fix: moved to background thread + 3-second polling pattern (same as auth button):
```python
# Module-level state (line ~551 in app.py)
_BACKFILL_STATE: dict = {"running": False, "result": None, "start": 0.0}

# On button click: spawn thread, rerun
# On subsequent renders: if running → sleep(3) → rerun
# When done: show result
```

Same pattern applied consistently for KB Ingest and Password Auth buttons.

### 4. Auth button result display fix

Auth result previously displayed inside narrow column → easy to miss.
Fixed: status/result display moved full-width below column layout, using
`st.session_state["pw_auth_status"]` flag + `dict.get()` with manual clear.

### 5. Modo SSO / auth situation

- `dipeng.chen@envision-energy.com` is a corporate domain → Modo routes to SSO
- SSO not configured → "SSO Is Not Available" email instead of magic link
- **"Send magic link email" button is unreliable** for this account
- **Direct Playwright login** (`_login()`) bypasses SSO and obtains magic link internally
- Current session: **authenticated** (verified 2026-07-27: "13 new docs inserted")
- When session expires: use "Authenticate & Save Session" with fresh magic link URL
  **immediately** upon receipt (links expire within a few hours)

---

## Commits in this session

| Commit | Branch | Summary |
|--------|--------|---------|
| `91786e9` | `feat/deal-structurer-bedrock-migration` | Gas market tools + UI (Options A+C) |
| `79f9b60` | `feat/deal-structurer-bedrock-migration` | Backfill background thread fix |
| `b22b0f3` | `feat/deal-structurer-bedrock-migration` | Auth button full-width result display |

---

## Critical: OneDrive + Git branch issue

`apps/gb-market/` **only exists on `feat/deal-structurer-bedrock-migration`**.
OneDrive stores other-branch files as cloud-only placeholders — `ls apps/gb-market/` on
the wrong branch shows only `__pycache__`. **Always checkout this branch before building.**

---

## ECS Deploy workflow (reference)

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

# 6. Switch back
git checkout feat/ph-po-market-apps
```

> `container-def.json` is **gitignored** (contains secrets). It lives at repo root locally.
> The file currently has image `bess-gb-market:v92` and `MODO_MAGIC_LINK_URL` set to the
> 2026-07-27 magic link (now expired — update when re-registering task def).

---

## Pending tasks

1. **Verify 3:30 AM email stops** — check 2026-07-28 03:30 SGT; v92 should suppress it
2. **Monitor 20:00 SGT Modo AI job** — should reuse saved session silently
3. **End-to-end test gas tools**:
   - Open Strategist tab at `pjh-etrm.ai/gb-market/`
   - Expand "Gas & Carbon Prices" panel
   - Enter NBP (e.g. 85 p/therm), EUA (e.g. 65 €/tonne)
   - Ask: "What's the clean spark spread at today's system price?"
4. **When Modo session expires**:
   - Receive magic link email → copy URL immediately
   - Update `MODO_MAGIC_LINK_URL` in `container-def.json`
   - Re-register task definition (no Docker rebuild needed, just `register-task-definition` + `update-service`)
   - Or: use "Authenticate & Save Session" UI button with fresh URL

---

## Key files

| File | Purpose |
|------|---------|
| `apps/gb-market/app.py` | Main Streamlit app — all gas tools, Strategist, auth, scheduler UI |
| `apps/gb-market/scheduler_service.py` | APScheduler — `_daily_knowledge_job` (03:30, no modo_ai) + `_modo_ai_job` (20:00) |
| `services/gb_knowledge/modo_ai.py` | Modo Energy connector — Playwright auth, session management |
| `shared/anthropic_client.py` | Bedrock-aware Anthropic client factory |
| `container-def.json` | ECS container definition — gitignored, local only |

---

## Spark spread formula reference

```
gas_cost_£_MWh = (nbp_pence_per_therm / 100) / 0.105506 * heat_rate_GJ_MWh
carbon_cost_£_MWh = carbon_price_eur_tonne * carbon_factor_tCO2_MWh * gas_fx_£_per_€
clean_spark_spread = power_price_£_MWh - gas_cost_£_MWh - carbon_cost_£_MWh
```

Typical CCGT parameters: heat_rate = 7.0 GJ/MWh, carbon_factor = 0.36 tCO₂/MWh
