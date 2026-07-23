# Bedrock Migration Guide — All Apps
**Date:** 2026-07-19 (updated 2026-07-20 with lessons from crystal-ball migration)  
**Status:** Services layer done. Apps layer partially done — see status table.

---

## ⚠️ Lessons Learned (crystal-ball migration, 2026-07-20)

Read this before touching anything else.

### 1. Do NOT use `us.anthropic.*` cross-region inference profiles

The original guide said to use `us.anthropic.claude-sonnet-4-6`. This is wrong for this account. Those profiles require an **AWS Marketplace subscription** that cannot be auto-completed:

```
PermissionDeniedError 403: Model access is denied — IAM role not authorized to perform
aws-marketplace:ViewSubscriptions, aws-marketplace:Subscribe
```

Even after adding the IAM permissions, the subscription still fails with "cannot be completed at this time."

**Fix: use `global.anthropic.*` inference profiles instead.** These are available in ap-southeast-1 and require no Marketplace subscription.

### 2. Use `BEDROCK_REGION=ap-southeast-1`, not `us-east-1`

All ECS services run in ap-southeast-1. Setting `BEDROCK_REGION=us-east-1` routes all Bedrock calls cross-region unnecessarily. Use `ap-southeast-1` — `global.anthropic.*` profiles are available there and route optimally.

### 3. BEDROCK_REGION is NOT pre-set in task definitions

Despite what an earlier version of this guide said, `BEDROCK_REGION` was NOT already in the crystal-ball task definitions. **You must add it explicitly** when registering the new task definition. Check every app's task def before assuming it's there.

### 4. Submitting the use case form

The "Model access" page in the AWS Bedrock console has been retired. If you get a 404 "use case details" error:
- Go to **Bedrock console → Model catalog** in ap-southeast-1
- Find any Anthropic model → open its page → submit the use case form
- The form is account-level: submitting once unlocks all Claude models
- Wait ~15 minutes after submission

With `global.anthropic.*` profiles this error was not encountered — but it may appear on first use in a fresh account.

### 5. Correct model IDs (confirmed working, ap-southeast-1)

| Direct API model string | Bedrock model ID | Type |
|---|---|---|
| `claude-sonnet-4-6` | `global.anthropic.claude-sonnet-4-6` | global inference profile |
| `claude-opus-4-6` | `global.anthropic.claude-opus-4-6-v1` | global inference profile |
| `claude-haiku-4-5` / `claude-haiku-4-5-20251001` | `global.anthropic.claude-haiku-4-5-20251001-v1:0` | global inference profile |
| `claude-sonnet-4-5-20250929` | `global.anthropic.claude-sonnet-4-5-20250929-v1:0` | global inference profile |
| `claude-3-5-haiku-20241022` | `anthropic.claude-3-5-haiku-20241022-v1:0` | on-demand |
| `claude-3-opus-20240229` | `anthropic.claude-3-opus-20240229-v1:0` | on-demand |

These are already in `shared/anthropic_client.py`. Call sites do not need to change model strings.

---

## How the factory works

`shared/anthropic_client.py` (already in bess-platform):

```python
from shared.anthropic_client import make_client, is_llm_available

client = make_client(api_key)          # api_key can be None
client.messages.create(model="claude-sonnet-4-6", ...)
```

- If `BEDROCK_REGION` env var is set → uses `AnthropicBedrock` + IAM role, ignores `api_key`
- Otherwise → uses `Anthropic(api_key=api_key or ANTHROPIC_API_KEY)`
- Model strings (e.g. `"claude-sonnet-4-6"`) are auto-mapped to Bedrock IDs
- Env var `BEDROCK_MODEL_<ID>` overrides the static map — change model IDs without rebuilding (e.g. `BEDROCK_MODEL_CLAUDE_SONNET_4_6=global.anthropic.claude-sonnet-4-6`)

---

## Migration pattern (same for every file)

### Before
```python
import anthropic
api_key = os.environ.get("ANTHROPIC_API_KEY", "")
client = anthropic.Anthropic(api_key=api_key)
```

### After
```python
from shared.anthropic_client import make_client as _make_anthropic_client
api_key = os.environ.get("ANTHROPIC_API_KEY", "")
client = _make_anthropic_client(api_key)
```

### For guards that block when key is absent

```python
# Before
if not api_key:
    return "ANTHROPIC_API_KEY not set", ...

# After
from shared.anthropic_client import is_llm_available
if not is_llm_available(api_key):
    return "No LLM configured (set ANTHROPIC_API_KEY or BEDROCK_REGION)", ...
```

The rest of the code (`.messages.create(...)`, model strings, streaming) is unchanged.

---

## Status overview

| App | Repo | Files to change | Done? |
|---|---|---|---|
| crystal-ball | Crystal-Ball repo | `apps/fortune-teller/app.py` (×5) | ✅ v37/td:59 |
| crystal-ball-client | Crystal-Ball repo | `apps/crystal-ball-client/app.py` (×2) | ✅ v22/td:25 |
| hermes | bess-platform | `services/hermes/app.py` (×4) | ✅ |
| spot-market | bess-platform | `apps/spot-market/app.py` (×5), `spot_report.py` (×1) | ✅ |
| gb-market | bess-platform | `apps/gb-market/app.py` (×1), `daily_report.py` (×1) | ✅ |
| bess-map | bess-platform | `apps/bess-map/app.py` (×1) | ✅ |
| mengxi-dashboard | bess-platform | `apps/mengxi-dashboard/app.py` (×1) | ✅ |
| deal-structurer | bess-platform | `apps/deal_structurer/strategist.py` (×1) | ✅ v8/td:9 |
| ph-market | bess-platform | empty dir — no code yet | N/A |
| po-market | bess-platform | empty dir — no code yet | N/A |
| ib-platform | ib-platform repo | 5 files, 7 call sites | No |

**Already done (services layer):** 37 files under `services/` — commits `ae66e2c` + `2ac42a1`.

---

## Per-app changes

### ECS task definition — required for every app

For each app, add `BEDROCK_REGION=ap-southeast-1` to the task definition environment when registering the new revision. **Do not assume it is already there.**

```bash
# Fetch current task def
MSYS_NO_PATHCONV=1 aws ecs describe-task-definition \
  --task-definition <family>:<rev> \
  --region ap-southeast-1 \
  --query 'taskDefinition' --output json > C:/tmp/td.json

# Strip read-only fields + add BEDROCK_REGION + update image (Python):
py -3 -c "
import json
with open('C:/tmp/td.json') as f: td = json.load(f)
for k in ['taskDefinitionArn','revision','status','requiresAttributes','compatibilities','registeredAt','registeredBy']:
    td.pop(k, None)
env = td['containerDefinitions'][0]['environment']
# Add BEDROCK_REGION if not already present
if not any(e['name'] == 'BEDROCK_REGION' for e in env):
    env.append({'name': 'BEDROCK_REGION', 'value': 'ap-southeast-1'})
else:
    next(e for e in env if e['name'] == 'BEDROCK_REGION')['value'] = 'ap-southeast-1'
# Update image tag
td['containerDefinitions'][0]['image'] = td['containerDefinitions'][0]['image'].replace(':vOLD', ':vNEW')
with open('C:/tmp/td-new.json', 'w') as f: json.dump(td, f, indent=2)
print('done')
"

MSYS_NO_PATHCONV=1 aws ecs register-task-definition \
  --cli-input-json file://C:/tmp/td-new.json --region ap-southeast-1

MSYS_NO_PATHCONV=1 aws ecs update-service \
  --cluster bess-platform-cluster --service <svc> \
  --task-definition <family>:<new-rev> \
  --force-new-deployment --region ap-southeast-1
```

### IAM role

The ECS task role `bess-platform-task-role` already has a `bess-platform-task-bedrock` inline policy with `bedrock:InvokeModel` and `bedrock:InvokeModelWithResponseStream`. As of 2026-07-20 it also has `aws-marketplace:ViewSubscriptions/Subscribe/Unsubscribe` (added during crystal-ball migration — harmless, safe to leave).

No IAM changes needed for other apps.

---

### 1. hermes

**File:** `services/hermes/app.py` only.

All underlying modules already use `make_client`. Only the FastAPI entrypoint still has raw `ANTHROPIC_API_KEY` references.

**Issue A — Startup crash (line ~687)**
```python
# Before:
agent = HermesAgent(tasks=tasks, anthropic_api_key=os.environ["ANTHROPIC_API_KEY"], ...)

# After:
agent = HermesAgent(tasks=tasks, anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY", ""), ...)
```

**Issue B — Three endpoint guards (lines ~1147, ~1192, ~1205)**
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

Add `from shared.anthropic_client import is_llm_available` once at the top of `app.py`.

**ECS:** cluster `bess-platform-cluster`, service `bess-platform-hermes-svc`.

---

### 3. spot-market

**Files:** `apps/spot-market/app.py` (×5), `apps/spot-market/spot_report.py` (×1)

Add at top of each file:
```python
from shared.anthropic_client import make_client as _make_anthropic_client, is_llm_available
```

Replace every `anthropic.Anthropic(api_key=X)` with `_make_anthropic_client(X)`.

In `spot_report.py`, also update the `if not api_key:` guard:
```python
# Before:
if not api_key:
    logger.warning("ANTHROPIC_API_KEY not set — skipping AI commentary")

# After:
if not is_llm_available(api_key):
    logger.warning("No LLM available — skipping AI commentary")
```

**Dockerfile:** `apps/spot-market/Dockerfile` already has `"anthropic[bedrock]>=0.40"` — no change.

**ECS:** service name — verify with `aws ecs list-services --cluster bess-platform-cluster`.

---

### 4. gb-market

**Files:** `apps/gb-market/app.py` (line ~140), `apps/gb-market/daily_report.py` (line ~223)

```python
# app.py — Before (lines 18, 139–140):
import anthropic
_ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
_client = anthropic.Anthropic(api_key=_ANTHROPIC_KEY)

# After:
from shared.anthropic_client import make_client as _make_anthropic_client
_ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
_client = _make_anthropic_client(_ANTHROPIC_KEY)
```

```python
# daily_report.py — Before:
import anthropic as _anthropic
client = _anthropic.Anthropic(api_key=api_key)

# After:
from shared.anthropic_client import make_client as _make_anthropic_client
client = _make_anthropic_client(api_key)
```

**Dockerfile:** `apps/gb-market/Dockerfile` has `"anthropic>=0.40"` — change to `"anthropic[bedrock]>=0.40"`.

---

### 5. bess-map

**File:** `apps/bess-map/app.py` (line ~3853)

```python
# Before:
import anthropic as _ant
_ant_client = _ant.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))

# After:
from shared.anthropic_client import make_client as _make_anthropic_client
_ant_client = _make_anthropic_client(os.environ.get("ANTHROPIC_API_KEY", ""))
```

**Dockerfile + requirements.txt:** change `anthropic>=0.40` → `anthropic[bedrock]>=0.40` in both.

---

### 6. mengxi-dashboard

**File:** `apps/mengxi-dashboard/app.py` (line ~1570)

```python
# Before:
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
    st.error("No LLM configured — Trader agent unavailable.")
    ...
_trader_client = _make_anthropic_client(_TRADER_API_KEY)
```

**requirements.txt:** `anthropic>=0.40` → `anthropic[bedrock]>=0.40`.

---

### 7. deal-structurer

**File:** `apps/deal_structurer/strategist.py` (line ~38)

```python
# Before:
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

**Dockerfile + requirements.txt:** already have `"anthropic[bedrock]>=0.40"` — no change.

---

### 8. ph-market and po-market

Both `apps/ph-market/` and `apps/po-market/` are empty — no code yet. When you build these apps, use `make_client()` from the start:

```python
from shared.anthropic_client import make_client as _make_anthropic_client, is_llm_available
```

---

### 9. ib-platform (separate repo)

ib-platform runs locally on the MacBook, not on ECS. Create a local copy of the factory:

```bash
cp shared/anthropic_client.py ~/repo/ETRM/ib-platform/libs/anthropic_client.py
```

Update the docstring to say `from libs.anthropic_client import make_client`. Then migrate 5 files:

| File | Call sites |
|---|---|
| `services/knowledge/expert_memory.py` | 3 |
| `services/knowledge/daily_briefing.py` | 1 |
| `services/news/scorer.py` | 1 |
| `apps/news/tabs/digest.py` | 1 |
| `apps/portfolio/tabs/advisor_pretrade.py` | 1 |

Pattern is the same — replace `anthropic.Anthropic(api_key=X)` with `_make_anthropic_client(X)`.

Set env var locally: add `BEDROCK_REGION=ap-southeast-1` to `config/.env`.

---

## Deployment workflow (quick reference)

```bash
# 1. Build image
cd /path/to/repo
docker build -f apps/<app>/Dockerfile -t <image>:vN .
docker tag <image>:vN 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/<image>:vN

# 2. ECR login + push
MSYS_NO_PATHCONV=1 aws ecr get-login-password --region ap-southeast-1 \
  | docker login --username AWS --password-stdin \
    319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/<image>:vN

# 3. Register new task def + update service (see "ECS task definition" section above)
```

---

## Verification checklist per app

After migrating and redeploying:

- [ ] App starts (`/_stcore/health` or `/health` returns 200)
- [ ] LLM feature works in the UI (no `AuthenticationError`, no `PermissionDeniedError`)
- [ ] CloudWatch logs show no `invalid_api_key` errors
- [ ] Task definition has `BEDROCK_REGION=ap-southeast-1` (verify via `describe-task-definition`)
- [ ] `ANTHROPIC_API_KEY` can be removed from task def once Bedrock confirmed working (or left as empty string)

---

## Recommended order

1. **hermes** — startup crash on `os.environ["ANTHROPIC_API_KEY"]` blocks ALL scheduled jobs. Only 4 lines to change.
2. **deal-structurer** — 1 file, 1 call site
3. **bess-map** — 1 file, 1 call site
4. **mengxi-dashboard** — 1 file, 1 call site
5. **gb-market** — 2 files, 2 call sites
6. **spot-market** — 2 files, 6 call sites (most complex)
7. **ib-platform** — local MacBook, no ECS redeploy
