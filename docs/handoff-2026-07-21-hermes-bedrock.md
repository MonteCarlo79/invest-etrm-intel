# Hermes Bedrock Migration Handoff — 2026-07-21

## Context for the new Claude session

You are continuing work on the **bess-platform** project at:
`C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`

Active branch: `feat/deal-structurer-bedrock-migration`
GitHub: `https://github.com/MonteCarlo79/invest-etrm-intel`
ECS cluster: `bess-platform-cluster` in `ap-southeast-1`
ECR registry: `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com`
Bedrock region (ECS env var): `BEDROCK_REGION=us-east-1`

---

## What was accomplished in this session

### Core problem solved
Hermes service was crashing on startup and all LLM calls were failing after a Bedrock migration commit (`ae66e2c`) that introduced two classes of bugs.

### Bug 1: IndentationError in 9 files (FIXED ✅)
Commit `ae66e2c` placed `from shared.anthropic_client import make_client as _make_anthropic_client` at column 0 (module level) inside function bodies. Fixed in commit `3550f00` and `d64c926`.

Files fixed:
- `services/hermes/agent.py`
- `services/hermes/conversation_memory.py`
- `services/hermes/report_drafter.py`
- `services/hermes/internet_agent.py` ← fixed last (missed in initial batch)
- `services/hermes/market_agent_bridge.py`
- `services/knowledge_pool/knowledge_docs.py`
- `services/knowledge_pool/jizhi_extractor.py`
- `services/intl_market_common/headless_agent.py`
- `services/bess_map/headless_agent.py`
- `services/mengxi_trading/headless_agent.py`
- `services/gb_knowledge/headless_agent.py`

### Bug 2: Wrong Bedrock model IDs (FIXED ✅)
The migration used `us.anthropic.*` cross-region inference profiles which require an **AWS Marketplace subscription** that this account does not have. Fixed in commit `b7ac37b` by switching to `global.anthropic.*` profiles.

**Key lesson**: Use `global.anthropic.*` inference profiles, NOT `us.anthropic.*`.

### Final model map (`shared/anthropic_client.py`) — current state
```python
_BEDROCK_MODEL_MAP = {
    "claude-sonnet-4-6":          "global.anthropic.claude-sonnet-4-6",
    "claude-opus-4-6":            "global.anthropic.claude-opus-4-6-v1",
    "claude-haiku-4-5":           "global.anthropic.claude-haiku-4-5-20251001-v1:0",
    "claude-haiku-4-5-20251001":  "global.anthropic.claude-haiku-4-5-20251001-v1:0",
    "claude-sonnet-4-5-20250929": "global.anthropic.claude-sonnet-4-5-20250929-v1:0",
    "claude-3-5-haiku-20241022":  "anthropic.claude-3-5-haiku-20241022-v1:0",
    "claude-3-opus-20240229":     "anthropic.claude-3-opus-20240229-v1:0",
}
```

Note: `claude-3-5-sonnet-20241022` was removed — EOL in Bedrock us-east-1 as of 2026-07.

### Bug 3: Tool-use blocked even on global profiles (PARTIALLY MITIGATED ✅)
This Bedrock account still returns 404 "Model use case details have not been submitted" for tool use on all models **except** `global.anthropic.claude-sonnet-4-6`.

**Workaround applied**: All tool-use callers in Hermes now hardcode `claude-sonnet-4-6`:
- `services/hermes/bayesian_agent.py` — Bayesian analysis (uses tools)
- `services/hermes/internet_agent.py` — web research (uses tools)
- `services/hermes/market_agent_bridge.py` — market queries (uses tools)
- `services/hermes/market_report.py` — structured report tool calls

**Haiku replaced with Sonnet 4.6** (haiku-4-5 also blocked on this account):
- `services/hermes/scheduler.py` — email summary
- `services/hermes/agent.py` — article summary, file digest
- `services/hermes/capacity_etl.py`, `capacity_manual_etl.py`, `capacity_screener.py`
- `services/hermes/capcomp_manual_etl.py`, `capcomp_screener.py`
- `services/hermes/chart_utils.py`, `conversation_memory.py`
- `services/hermes/market_classifier.py`, `news_screener.py`
- `services/hermes/sysopfee_etl.py`, `sysopfee_screener.py`

**Permanent fix needed**: Submit the Anthropic use-case form in AWS Bedrock Console →
Bedrock → Model access → Anthropic → fill out the use-case form. Once approved, haiku
can be restored (cheaper for lightweight calls) and any model can use tool use.

### ECS Security Group fix (FIXED ✅)
ALB had no inbound rule for TCP 8000 (Hermes FastAPI port) to the ECS tasks SG.
Added manually and in `infra/terraform/main.tf`.

### Hermes agent: Claude tier fallback (FIXED ✅)
`_call_llm(preferred="claude")` previously had no fallback. Now:
```python
order = [_try_claude, _try_gpt, _try_deepseek]
```

---

## Current Hermes deployment state

- **Service**: `bess-platform-hermes-svc` on `bess-platform-cluster` (ap-southeast-1)
- **Status**: Running, rolloutState=COMPLETED
- **ECR**: `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest`
- **Dockerfile**: `apps/hermes-service/Dockerfile`
- **All models**: `claude-sonnet-4-6` → `global.anthropic.claude-sonnet-4-6`

### Deploy command (for future changes)
```bash
cd "C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform"
docker build -f apps/hermes-service/Dockerfile -t bess-platform/hermes:latest .
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker tag bess-platform/hermes:latest 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
aws ecs update-service --cluster bess-platform-cluster --service bess-platform-hermes-svc --force-new-deployment --region ap-southeast-1
```

### Check deployment status
```bash
aws ecs describe-services --cluster bess-platform-cluster --services bess-platform-hermes-svc --region ap-southeast-1 --query "services[0].deployments[*].{status:status,running:runningCount,rolloutState:rolloutState}" --output json
```

### Read CloudWatch logs
```bash
MSYS_NO_PATHCONV=1 aws logs filter-log-events \
  --log-group-name /ecs/bess-platform \
  --log-stream-name-prefix hermes \
  --start-time $(date -d '30 minutes ago' +%s000) \
  --region ap-southeast-1 \
  --query "events[*].message" --output text 2>&1 | head -100
```

---

## Pending tasks (not yet done)

### High priority
1. **Submit Anthropic use-case form** (user action in AWS Console)
   - Go to: AWS Console → Bedrock → Model access → Anthropic
   - Fill out the use-case form for this account
   - Once approved, can restore `claude-haiku-4-5-20251001` for lightweight calls
   - This is the root fix; current workaround wastes Sonnet on trivial tasks

2. **Fix same IndentationError + haiku bugs in other services** (not yet deployed):
   - `services/knowledge_pool/jizhi_extractor.py:384,475` — uses `claude-haiku-4-5-20251001` with tool use → will fail
   - `services/knowledge_pool/knowledge_docs.py:446` — uses `claude-haiku-4-5-20251001`
   - `services/intl_market_common/app_template.py:748,1128`
   - `services/intl_market_common/daily_report_template.py:303`
   - `services/exchange_reports/ingestor.py:175`
   - These services have their own Dockerfiles/deployments — need separate builds

### Medium priority
3. **DB: Fix bad thermal_mw data**
   - `province_installed_monthly` table: 冀南 has `thermal_mw=2812` which is wrong (should be ~28120 or similar)
   - Query: `SELECT * FROM province_installed_monthly WHERE province='冀南' ORDER BY year_month DESC LIMIT 5;`

4. **DB: Add/fix thermal generation data**
   - `thermal_gen_100gwh` missing for: 湖北, 湖南, 广西, and other provinces
   - 江西, 重庆: current values are estimated — replace with official NEA stats

5. **Bedrock migration for remaining apps** (each needs own build+deploy):
   - `apps/spot-market` — pending
   - `apps/bess-map` — pending
   - `apps/mengxi-dashboard` — pending
   - `apps/deal-structurer` — pending
   - `apps/gb-market` — has local changes (Dockerfile, app.py, daily_report.py) not yet committed

### Notes on gb-market
`git status` shows `apps/gb-market/` has unstaged changes. Read these before doing anything.

---

## Key file locations

| Purpose | Path |
|---------|------|
| Bedrock client factory | `shared/anthropic_client.py` |
| Main Hermes agent | `services/hermes/agent.py` |
| Bayesian analysis | `services/hermes/bayesian_agent.py` |
| Email summary | `services/hermes/scheduler.py` |
| Internet/web agent | `services/hermes/internet_agent.py` |
| Market query bridge | `services/hermes/market_agent_bridge.py` |
| Report tool calls | `services/hermes/market_report.py` |
| Hermes Dockerfile | `apps/hermes-service/Dockerfile` |
| Terraform infra | `infra/terraform/main.tf` |
| Bedrock guide | `docs/BEDROCK_MIGRATION_GUIDE.md` |

---

## AWS account facts

| Item | Value |
|------|-------|
| Account ID | `319383842493` |
| ECS cluster | `bess-platform-cluster` (ap-southeast-1) |
| ECR registry | `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com` |
| Bedrock region | `us-east-1` (set as `BEDROCK_REGION` env var in ECS tasks) |
| IAM user | `terraform-admin` |
| ALB SG | `sg-...` — allows 8000 (Hermes) and 8500-8530 (Streamlit) inbound from ALB |

**Note on Windows**: Use `MSYS_NO_PATHCONV=1` before AWS CLI calls with `/`-prefixed paths
(e.g., log group names) to prevent Git Bash from mangling them as Windows paths.
