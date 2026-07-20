# Bedrock Migration Guide — All Apps
**Date:** 2026-07-19  
**Status:** Services layer already done. Apps layer pending.

This document gives step-by-step instructions to migrate each app from direct `Anthropic(api_key=...)` to the Bedrock-aware factory, so they work without personal Anthropic API credits when `BEDROCK_REGION` is set.

---

## How the factory works (recap)

`shared/anthropic_client.py` (already in bess-platform):

```python
from shared.anthropic_client import make_client

client = make_client(api_key)          # api_key can be None
client.messages.create(model="claude-sonnet-4-6", ...)
```

- If `BEDROCK_REGION` env var is set → uses `AnthropicBedrock` + IAM role, ignores `api_key`
- Otherwise → uses `Anthropic(api_key=api_key or ANTHROPIC_API_KEY)`
- Model strings (e.g. `"claude-sonnet-4-6"`) are auto-mapped to Bedrock model IDs — no call-site changes needed

**ib-platform** needs its own copy of the factory (different repo). See §8.

---

## Migration pattern (same for every file)

### Before
```python
import anthropic
api_key = os.environ.get("ANTHROPIC_API_KEY", "")
client = anthropic.Anthropic(api_key=api_key)
```

### After (bess-platform apps)
```python
from shared.anthropic_client import make_client as _make_anthropic_client
api_key = os.environ.get("ANTHROPIC_API_KEY", "")
client = _make_anthropic_client(api_key)
```

### After (ib-platform)
```python
from libs.anthropic_client import make_client as _make_anthropic_client
api_key = os.environ.get("ANTHROPIC_API_KEY", "")
client = _make_anthropic_client(api_key)
```

The rest of the code (`.messages.create(...)`, model strings, streaming) is unchanged.

---

## Status overview

| App | Repo | Files to change | Done? |
|---|---|---|---|
| hermes | bess-platform | `services/hermes/app.py` (startup crash + 3 endpoint guards) | No |
| spot-market | bess-platform | `apps/spot-market/app.py` (×5), `spot_report.py` (×1) | No |
| gb-market | bess-platform | `apps/gb-market/app.py` (×1), `daily_report.py` (×1) | No |
| bess-map | bess-platform | `apps/bess-map/app.py` (×1) | No |
| mengxi-dashboard | bess-platform | `apps/mengxi-dashboard/app.py` (×1) | No |
| deal-structurer | bess-platform | `apps/deal_structurer/strategist.py` (×1) | No |
| crystal-ball | separate repo | unknown — source not in this repo | No |
| crystal-ball-client | separate repo | unknown — source not in this repo | No |
| ph-market | bess-platform | empty dir — no code yet | N/A |
| po-market | bess-platform | empty dir — no code yet | N/A |
| ib-platform | ib-platform repo | `services/knowledge/expert_memory.py` (×3), `daily_briefing.py` (×1), `services/news/scorer.py` (×1), `apps/news/tabs/digest.py` (×1), `apps/portfolio/tabs/advisor_pretrade.py` (×1) | No |

**Already done (bess-platform services layer):** 37 files under `services/` — commits `ae66e2c` + `2ac42a1`.  
**Partially done (hermes):** All agent/screener/report modules inside `services/hermes/` already use `make_client`. Only `app.py` (the FastAPI entrypoint) still has raw `ANTHROPIC_API_KEY` references.

---

## 1. hermes

**File:** `services/hermes/app.py` only.

All underlying modules (`agent.py`, `thinking_agent.py`, `bayesian_agent.py`, `internet_agent.py`, `market_report.py`, `report_drafter.py`, `scheduler.py`) were already migrated to `make_client` in commit `ae66e2c`. Only the FastAPI entrypoint `app.py` still has raw `ANTHROPIC_API_KEY` references.

### Issue A — Startup crash (line 687)

`_make_clients()` uses the bracket form `os.environ["ANTHROPIC_API_KEY"]`, which throws `KeyError` at startup when only `BEDROCK_REGION` is set and `ANTHROPIC_API_KEY` is absent from the ECS env.

```python
# Before (line 687):
agent = HermesAgent(
    tasks=tasks,
    anthropic_api_key=os.environ["ANTHROPIC_API_KEY"],   # ← KeyError crash
    onedrive=onedrive,
)

# After:
agent = HermesAgent(
    tasks=tasks,
    anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY", ""),  # make_client handles Bedrock
    onedrive=onedrive,
)
```

`HermesAgent.__init__` already calls `_make_anthropic_client(anthropic_api_key)` internally, so passing an empty string is correct — the factory will use Bedrock when `BEDROCK_REGION` is set.

### Issue B — Three endpoint guards reject requests when key is absent

Three FastAPI endpoints check `if not _api_key: return Response(status_code=503)` before dispatching background tasks. With Bedrock, the key is empty but the LLM is available. Replace each guard with `is_llm_available`.

**`/hermes/capcomp/scan` (lines ~1147–1149):**
```python
# Before:
_api_key = os.environ.get("ANTHROPIC_API_KEY", "")
if not _api_key:
    return Response(content="API key not configured", status_code=503)

# After:
from shared.anthropic_client import is_llm_available
_api_key = os.environ.get("ANTHROPIC_API_KEY", "")
if not is_llm_available(_api_key):
    return Response(content="No LLM configured (set ANTHROPIC_API_KEY or BEDROCK_REGION)", status_code=503)
```

**`/hermes/jizhi/scan` (lines ~1192–1194):**
```python
# Before:
_api_key = os.environ.get("ANTHROPIC_API_KEY", "")
if not _api_key:
    return Response(content="ANTHROPIC_API_KEY not set", status_code=503)

# After:
from shared.anthropic_client import is_llm_available
_api_key = os.environ.get("ANTHROPIC_API_KEY", "")
if not is_llm_available(_api_key):
    return Response(content="No LLM configured (set ANTHROPIC_API_KEY or BEDROCK_REGION)", status_code=503)
```

**`/hermes/knowledge/digest` (lines ~1205–1207):**
```python
# Before:
_api_key = os.environ.get("ANTHROPIC_API_KEY", "")
if not _api_key:
    return Response(content="ANTHROPIC_API_KEY not set", status_code=503)

# After:
from shared.anthropic_client import is_llm_available
_api_key = os.environ.get("ANTHROPIC_API_KEY", "")
if not is_llm_available(_api_key):
    return Response(content="No LLM configured (set ANTHROPIC_API_KEY or BEDROCK_REGION)", status_code=503)
```

> Tip: add `from shared.anthropic_client import is_llm_available` once at the top of `app.py` rather than inline in each endpoint.

### Issue C — Log warnings reference key by name (cosmetic, optional)

Lines 122 and 172 log `"skipped — ANTHROPIC_API_KEY not set"`. These are harmless but misleading on Bedrock. Optionally update to `"skipped — no LLM configured"`.

### All scheduler `api_key=` kwargs are fine as-is

Every scheduler job passes `api_key=os.environ.get("ANTHROPIC_API_KEY", "")` to functions that are already migrated. When `BEDROCK_REGION` is set, `make_client("")` inside those functions uses Bedrock. No changes needed to the scheduler block.

### ECS — hermes already has BEDROCK_REGION in Terraform

`infra/terraform/hermes.tf` already has `{ name = "BEDROCK_REGION", value = "us-east-1" }` (added in `ae66e2c`). After the three code changes above, force-redeploy:

```bash
aws ecs update-service --cluster bess-platform --service bess-platform-hermes-svc --force-new-deployment
```

### Summary of changes

| Location | Change |
|---|---|
| `app.py:687` | `os.environ["ANTHROPIC_API_KEY"]` → `.get("ANTHROPIC_API_KEY", "")` |
| `app.py:~1148` | `if not _api_key:` → `if not is_llm_available(_api_key):` |
| `app.py:~1193` | same |
| `app.py:~1206` | same |
| `app.py` top imports | add `from shared.anthropic_client import is_llm_available` |

---

## 3. spot-market

**Files:** `apps/spot-market/app.py`, `apps/spot-market/spot_report.py`

### app.py — 5 instantiations

The app uses `_anthropic.Anthropic(...)` (lazy-imported as `_ant_tr` or `_anthropic`). Locate by searching for `Anthropic(api_key=` in `app.py`. The lines are:

| Line | Pattern | Change |
|---|---|---|
| 1119–1120 | `import anthropic as _ant_tr` + `_ant_tr.Anthropic(api_key=api_key)` | See below |
| 2957–2959 | `_anthropic.Anthropic(api_key=_api_key)` | See below |
| 2986–2988 | `_anthropic.Anthropic(api_key=_api_key)` | See below |
| 3110–3112 | `_anthropic.Anthropic(api_key=api_key)` | See below |
| 3376–3378 | `_anthropic.Anthropic(api_key=_api_key)` | See below |

All follow the same pattern — replace each inline `anthropic.Anthropic(api_key=X)` with `_make_anthropic_client(X)`.

**Add at the top of app.py** (after existing imports):
```python
from shared.anthropic_client import make_client as _make_anthropic_client
```

Then replace every `_ant_tr.Anthropic(api_key=...)` / `_anthropic.Anthropic(api_key=...)` with `_make_anthropic_client(...)`.

The local `import anthropic as _ant_tr` / `import anthropic as _anthropic` blocks can be removed if the only use was client creation. Keep them if other `anthropic.*` symbols (e.g. `anthropic.APIStatusError`) are used nearby.

### spot_report.py — 1 instantiation (line 408)

```python
# Before (lines ~336–408):
api_key = os.environ.get("ANTHROPIC_API_KEY", "")
if not api_key:
    logger.warning("ANTHROPIC_API_KEY not set — skipping AI commentary")
    ...
import anthropic as _anthropic
client = _anthropic.Anthropic(api_key=api_key)

# After:
from shared.anthropic_client import make_client as _make_anthropic_client, is_llm_available
api_key = os.environ.get("ANTHROPIC_API_KEY", "")
if not is_llm_available(api_key):
    logger.warning("No LLM available (set ANTHROPIC_API_KEY or BEDROCK_REGION) — skipping AI commentary")
    ...
client = _make_anthropic_client(api_key)
```

### Dockerfile check
`apps/spot-market/Dockerfile` already has `"anthropic[bedrock]>=0.40"` — no change needed.

### ECS env var
`BEDROCK_REGION=us-east-1` is already in the spot-market task definition (added in `ae66e2c`). Just needs a force-redeploy after the code change.

---

## 4. gb-market

**Files:** `apps/gb-market/app.py`, `apps/gb-market/daily_report.py`

### app.py — 1 instantiation (line 140, module-level)

```python
# Before (lines 18, 139–140):
import anthropic
_ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
_client = anthropic.Anthropic(api_key=_ANTHROPIC_KEY)

# After:
from shared.anthropic_client import make_client as _make_anthropic_client
_ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
_client = _make_anthropic_client(_ANTHROPIC_KEY)
```

Note: `app.py` line 1574 uses `if provider == "anthropic":` — that's a provider-string comparison, not client creation. Leave it untouched.

### daily_report.py — 1 instantiation (line 223)

```python
# Before:
import anthropic as _anthropic
client = _anthropic.Anthropic(api_key=api_key)

# After:
from shared.anthropic_client import make_client as _make_anthropic_client
client = _make_anthropic_client(api_key)
```

### scheduler_service.py — no direct client, but passes api_key

Line 129–130 calls `digest_kb_docs(anthropic_key, limit=100)` from `services.gb_knowledge.expert_memory` — that service is **already migrated** to use `make_client()`. The `anthropic_key` argument is now ignored when `BEDROCK_REGION` is set, so no change needed here.

### Dockerfile check
`apps/gb-market/Dockerfile` has `"anthropic>=0.40"` — **change to `"anthropic[bedrock]>=0.40"`** to include the Bedrock extras.

---

## 5. bess-map

**File:** `apps/bess-map/app.py`

### app.py — 1 instantiation (line 3853)

```python
# Before (lines 3834, 3853):
import anthropic as _ant
_ant_client = _ant.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))

# After:
from shared.anthropic_client import make_client as _make_anthropic_client
_ant_client = _make_anthropic_client(os.environ.get("ANTHROPIC_API_KEY", ""))
```

The `import anthropic as _ant` line at 3834 can be removed if `_ant` is only used for client creation. Verify no other `_ant.*` symbols (e.g. exception types) are referenced below line 3853.

### Dockerfile check
`apps/bess-map/Dockerfile` has `"anthropic>=0.40"` — **change to `"anthropic[bedrock]>=0.40"`**.  
`apps/bess-map/requirements.txt` has `anthropic>=0.40` — **change to `anthropic[bedrock]>=0.40`**.

---

## 6. mengxi-dashboard

**File:** `apps/mengxi-dashboard/app.py`

### app.py — 1 instantiation (line 1570)

```python
# Before (lines 1561, 1565, 1570):
import anthropic as _ant
_TRADER_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
if not _TRADER_API_KEY:
    st.error("ANTHROPIC_API_KEY not set — Trader agent unavailable.")
    ...
_trader_client = _ant.Anthropic(api_key=_TRADER_API_KEY)

# After:
from shared.anthropic_client import make_client as _make_anthropic_client, is_llm_available
_TRADER_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
if not is_llm_available(_TRADER_API_KEY):
    st.error("No LLM configured — Trader agent unavailable. Set ANTHROPIC_API_KEY or BEDROCK_REGION.")
    ...
_trader_client = _make_anthropic_client(_TRADER_API_KEY)
```

### Dockerfile / requirements check
`apps/mengxi-dashboard/requirements.txt` has `anthropic>=0.40` — **change to `anthropic[bedrock]>=0.40`**.  
Check `apps/mengxi-dashboard/Dockerfile` — add `[bedrock]` extra if it installs anthropic.

---

## 7. deal-structurer

**File:** `apps/deal_structurer/strategist.py`

### strategist.py — 1 instantiation (line 38)

```python
# Before (lines 5, 34–38):
import anthropic as _ant
api_key = os.environ.get("ANTHROPIC_API_KEY", "")
if not api_key:
    return "ANTHROPIC_API_KEY not set.", messages
client = _ant.Anthropic(api_key=api_key)

# After:
from shared.anthropic_client import make_client as _make_anthropic_client, is_llm_available
api_key = os.environ.get("ANTHROPIC_API_KEY", "")
if not is_llm_available(api_key):
    return "No LLM configured (set ANTHROPIC_API_KEY or BEDROCK_REGION).", messages
client = _make_anthropic_client(api_key)
```

### Dockerfile / requirements check
`apps/deal_structurer/Dockerfile` already has `"anthropic[bedrock]>=0.40"` — no change needed.  
`apps/deal_structurer/requirements.txt` already has `anthropic[bedrock]>=0.40` — no change needed.

---

## 8. crystal-ball and crystal-ball-client

These apps live in **separate repos** (images pushed to ECR `crystal-ball-fortune` and `crystal-ball-client`). Terraform already has `BEDROCK_REGION=us-east-1` in both task definitions.

**What to do in each source repo:**

1. Copy `shared/anthropic_client.py` from bess-platform into the repo (or re-implement it — it's ~100 lines).
2. Find all `Anthropic(api_key=...)` calls and replace with `make_client(api_key)`.
3. Ensure `requirements.txt` / `Dockerfile` has `anthropic[bedrock]>=0.40`.
4. Push a new image, then force-redeploy the ECS service.

Until this is done, these two services will fail if `ANTHROPIC_API_KEY` is not set (they'll get an empty key and the Bedrock path won't activate because the factory isn't there yet).

---

## 9. ph-market and po-market

Both `apps/ph-market/` and `apps/po-market/` are **empty directories** — no code yet. When you build these apps, use `make_client()` from the start rather than direct `Anthropic(api_key=...)`.

---

## 10. ib-platform

ib-platform is a **separate repo** (`~/repo/ETRM/ib-platform`) that runs locally on the MacBook, not on ECS. The migration pattern is the same but uses a local copy of the factory.

### Step 1 — Create the factory

Create `libs/anthropic_client.py` (copy from bess-platform's `shared/anthropic_client.py`, change the module docstring):

```python
# libs/anthropic_client.py
# Same content as bess-platform/shared/anthropic_client.py
# Usage: from libs.anthropic_client import make_client, is_llm_available
```

### Step 2 — Set env var locally

Add to `config/.env`:
```
BEDROCK_REGION=us-east-1
```

The `claude-bedrock` AWS profile already has Bedrock credentials. With this set, all `make_client()` calls will use Bedrock instead of the personal API key.

### Step 3 — Migrate each file

**`services/knowledge/expert_memory.py`** — 3 instantiations (lines 99, 170, 222)

```python
# Before:
import anthropic
client = anthropic.Anthropic(api_key=api_key)   # appears 3 times

# After (add import at top, replace all 3):
from libs.anthropic_client import make_client as _make_anthropic_client
client = _make_anthropic_client(api_key)
```

**`services/knowledge/daily_briefing.py`** — 1 instantiation (line 61)

```python
# Before:
import anthropic
client = anthropic.Anthropic(api_key=api_key)

# After:
from libs.anthropic_client import make_client as _make_anthropic_client
client = _make_anthropic_client(api_key)
```

**`services/news/scorer.py`** — 1 instantiation (line 113)

```python
# Before:
import anthropic
client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))

# After:
from libs.anthropic_client import make_client as _make_anthropic_client
client = _make_anthropic_client(os.environ.get("ANTHROPIC_API_KEY", ""))
```

**`apps/news/tabs/digest.py`** — 1 instantiation (line 52)

```python
# Before:
import anthropic
client = anthropic.Anthropic(api_key=api_key)

# After:
from libs.anthropic_client import make_client as _make_anthropic_client
client = _make_anthropic_client(api_key)
```

**`apps/portfolio/tabs/advisor_pretrade.py`** — 1 instantiation (line 82)

```python
# Before:
import anthropic
client = anthropic.Anthropic(api_key=api_key)

# After:
from libs.anthropic_client import make_client as _make_anthropic_client
client = _make_anthropic_client(api_key)
```

### Step 4 — Install Bedrock extras

```bash
source .venv/bin/activate
pip install "anthropic[bedrock]>=0.40"
```

Or update `requirements.txt`: change `anthropic>=0.40` → `anthropic[bedrock]>=0.40`.

### Step 5 — Smoke test

```bash
python -c "
from libs.anthropic_client import make_client
c = make_client()
r = c.messages.create(model='claude-haiku-4-5-20251001', max_tokens=10, messages=[{'role':'user','content':'ping'}])
print(r.content[0].text)
"
```

Should print without error, hitting Bedrock via the `claude-bedrock` AWS profile.

---

## 11. ECS redeployment (after each bess-platform app migration)

After committing code changes for an app, force-redeploy its ECS service to pick up the `BEDROCK_REGION` env var (Terraform `lifecycle { ignore_changes }` blocks automatic propagation):

```bash
# Replace <service-name> with the actual ECS service name
aws ecs update-service \
  --cluster bess-platform \
  --service bess-platform-<service-name>-svc \
  --force-new-deployment

# Service names:
#   spot-market      → bess-platform-spot-markets-svc (verify exact name)
#   gb-market        → bess-platform-gb-market-svc (verify exact name)
#   bess-map         → bess-platform-bess-map-svc
#   mengxi-dashboard → bess-platform-mengxi-dashboard-svc (verify exact name)
#   deal-structurer  → bess-platform-deal-structurer-svc (verify exact name)
#   crystal-ball     → bess-platform-crystal-ball-svc (verify exact name)
#   crystal-ball-client → bess-platform-crystal-ball-client-svc (verify exact name)

# List actual service names:
aws ecs list-services --cluster bess-platform --output text
```

---

## 12. Verification checklist per app

After migrating and redeploying each app, confirm:

- [ ] App starts without error (`/health` or `/_stcore/health` returns 200)
- [ ] LLM feature works in the UI (e.g. generate a summary, run the agent)
- [ ] CloudWatch logs show no `AuthenticationError` or `invalid_api_key`
- [ ] If Bedrock: logs show `Bedrock region=us-east-1` (from `logger.debug` in factory)
- [ ] `ANTHROPIC_API_KEY` can be removed from ECS task definition (or left as empty string) once Bedrock is confirmed working

---

## 13. Recommended order

Highest risk first (things that will break daily ops), then easiest:

1. **hermes** — **DO THIS FIRST.** The startup crash on line 687 means hermes will fail to start entirely when `ANTHROPIC_API_KEY` is removed from ECS. Only 4 lines to change. All underlying modules already use `make_client`. Blocks all daily scheduled jobs (news screener, KB digest, market reports, etc.).
2. **deal-structurer** — 1 file, 1 call site, already has `[bedrock]` in requirements
3. **bess-map** — 1 file, 1 call site
4. **mengxi-dashboard** — 1 file, 1 call site
5. **gb-market** — 2 files, 2 call sites
6. **ib-platform** — 5 files, 7 call sites (local MacBook, no ECS redeploy needed)
7. **spot-market** — 2 files, 6 call sites (most complex — verify each call site context)
8. **crystal-ball / crystal-ball-client** — separate repos, coordinate separately
