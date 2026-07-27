# Spot-Market Handoff — 2026-07-28

> **For a new Claude session:** Read this document in full before making any changes.
> Working directory: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`
> Branch: `feat/deal-structurer-bedrock-migration`
> Repo: `https://github.com/MonteCarlo79/invest-etrm-intel`

---

## Deployment State

| Service | Image | ECS Task Def | Cluster | Status |
|---------|-------|--------------|---------|--------|
| spot-markets | `bess-spot-markets:v91` | `bess-platform-spot-markets:124` | `bess-platform-cluster` | ✅ LIVE |
| hermes | `bess-platform-hermes:latest` | `bess-platform-hermes:158` | `bess-platform-cluster` | ✅ LIVE |

**Next version = v92** (rebuild when next code change lands).

---

## What Was Done This Session (2026-07-28)

### Fix: AI Agent `max_tokens` truncation (v91)

**Problem:** When the Spot Market AI Agent hit Claude's `max_tokens=4096` limit mid-response (e.g. for long document requests), the app returned `"Unexpected stop_reason: max_tokens"` — discarding the streamed partial answer entirely.

**Root cause:** `_run_agent_turn` in `apps/spot-market/app.py` lines 3423–3426 treated every non-`tool_use` stop reason as an unexpected error.

**Fix:** Added explicit `max_tokens` handler at line 3423 (`a0874c6`):
```python
if _final.stop_reason == "max_tokens":
    if _status_ph:
        _status_ph.empty()
    if text_placeholder is not None:
        text_placeholder.markdown(streamed_text)
    truncated = streamed_text + "\n\n*(回答因长度限制被截断。请发送「继续」以获取剩余内容。)*"
    return truncated, messages, tool_events
```

**Behaviour after fix:**
- Partial answer is shown in the chat with a Chinese truncation notice
- Conversation history is preserved — "继续" works correctly to continue generation
- The `messages` list already included the partial `_final.content` (line 3414), so continuation was always functionally correct; the fix just surfaces the text

**Deployed:** v91 → `bess-platform-spot-markets:124` → ECS rollout completed, target healthy.

---

## Key File Reference

| File | Purpose | Notes |
|------|---------|-------|
| `apps/spot-market/app.py` | Main Streamlit app (~8240 lines) | All UI tabs, AI agent, scheduler |
| `apps/spot-market/spot_report.py` | AI daily report generator | |
| `apps/spot-market/Dockerfile` | Docker build (v90+: `COPY shared/`, `fastembed>=0.3`) | |
| `services/knowledge_pool/jizhi_extractor.py` | Nova Pro AI extraction for 机制竞价 | |
| `services/hermes/app.py` | `_run_kb_digest` nightly pipeline | Fixed LLM guard in td:158 |
| `shared/anthropic_client.py` | Bedrock-aware client factory | |

### `apps/spot-market/app.py` — key line numbers

| Line | What |
|------|------|
| ~149 | `agent_title` = "Spot Market AI Agent" |
| ~712 | `__conn()` — `@st.cache_resource` DB connection |
| ~723 | `_conn()` — reconnect wrapper |
| ~3044 | Agent system prompt / tool instructions |
| ~3139 | `_AGENT_TOOLS` definitions (`get_spot_prices`, `get_interprov_flow`, `get_market_fundamentals`, `get_bess_pnl`, `search_reference_docs`) |
| ~3383 | `_run_agent_turn()` — main agentic loop |
| **3391/3405** | **`max_tokens=4096`** — both streaming and non-streaming paths |
| **3423** | **`max_tokens` stop_reason handler (THIS SESSION'S FIX)** |
| ~5080 | `tab_forecast` — 价格预测 tab |
| **~5927** | **`_load_forecast_fundamentals` — defined but NOT YET WIRED INTO ARIMA** |

---

## Top Priorities (Open Work)

### 1. Wire `_load_forecast_fundamentals` into ARIMA (highest value — see handoff §4 from 2026-07-25)

`_load_forecast_fundamentals` pulls from `marketdata.spot_fundamentals_hourly`:
- columns: `province`, `datetime`, `load`, `wind`, `solar`, `net_export`
- Currently defined but **not connected** to the PCA+ARIMA or merit-order stack

Goal: ARIMAX with wind/solar/load as exogenous regressors.

**Start here:**
1. Read `_load_forecast_fundamentals` definition at ~line 5927
2. Read the PCA+ARIMA fitting block just below it
3. Note: `spot_prices_hourly` has mixed units — `nanmedian > 5` → divide by 1000 (check at ~line 5927)

**Model architecture quick reference:**
```
PCA + ARIMA
  price matrix: days × 24h from spot_prices_hourly
  SVD → top 4 PCs → ARIMA(1,0,1) per PC, capped at 30 steps
  long horizon (>30d): seasonal mean-reversion, 3%/day decay toward monthly mean

Merit-Order Stack
  RE (0) → Nuclear (25) → Hydro (15) → Coal (coal_price×0.31+18) → Peaking coal

Bayesian Conjugate Gaussian
  posterior_var  = 1/(n0/σ0² + n1/σ1²)
  posterior_mean = posterior_var × (n0×μ0/σ0² + n1×μ1/σ1²)

Horizons: [1, 3, 7, 30, 90, 180, 365, days_to_eoy2027]
```

### 2. Deploy updated hermes image (low urgency)

`services/hermes/app.py` has the `is_llm_available` guard fix committed but the running image (td:158) uses the old code with `ANTHROPIC_API_KEY=bedrock` workaround.

```bash
bash scripts/deploy_hermes.sh
# OR:
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker build -f apps/hermes-service/Dockerfile -t 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest .
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
MSYS_NO_PATHCONV=1 aws ecs update-service --cluster bess-platform-cluster --service bess-platform-hermes-svc --force-new-deployment --region ap-southeast-1
```

### 3. 机制竞价 — Nova Pro prompt tuning (lower priority)

- Model: `apac.amazon.nova-pro-v1:0` (ap-southeast-1, NOT tool use)
- Edit `_BIDS_PROMPT` / `_UPCOMING_PROMPT` in `services/knowledge_pool/jizhi_extractor.py`
- Edge case: multi-page PPTX charts → incomplete JSON output

### 4. KB backlog drain (ongoing)

348 docs pending synthesis. Nightly hermes job drains 30/night. To drain faster:
```powershell
py -c "import requests; r = requests.post('https://bess-platform-alb-1158505371.ap-southeast-1.elb.amazonaws.com/hermes/knowledge/digest', verify=False); print(r.status_code, r.text)"
```
Call repeatedly with ~2 min gap.

---

## Deploy Next Version (v92+)

```powershell
# 1. Build
docker build -f apps/spot-market/Dockerfile `
  -t 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v92 .

# 2. ECR login + push
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v92

# 3. Strip task def + update image tag
MSYS_NO_PATHCONV=1 aws ecs describe-task-definition --task-definition bess-platform-spot-markets:124 --region ap-southeast-1 --query 'taskDefinition' --output json > C:/tmp/td_spot.json

py -3 -c "
import json
with open('C:/tmp/td_spot.json', encoding='utf-8') as f: td = json.load(f)
for k in ['taskDefinitionArn','revision','status','requiresAttributes','compatibilities','registeredAt','registeredBy']:
    td.pop(k, None)
td['containerDefinitions'][0]['image'] = td['containerDefinitions'][0]['image'].replace(':v91', ':v92')
with open('C:/tmp/td_spot_new.json', 'w') as f: json.dump(td, f, indent=2)
print('done')
"

MSYS_NO_PATHCONV=1 aws ecs register-task-definition --cli-input-json file://C:/tmp/td_spot_new.json --region ap-southeast-1 --query 'taskDefinition.revision'

# 4. Update service (replace :NNN with new revision from step 3)
MSYS_NO_PATHCONV=1 aws ecs update-service --cluster bess-platform-cluster --service bess-platform-spot-markets-svc --task-definition bess-platform-spot-markets:NNN --force-new-deployment --region ap-southeast-1
```

**BEDROCK_REGION must be `ap-southeast-1`** — `global.anthropic.*` profiles only work from inside AWS ap-southeast-1.

---

## DB Connection / Key Tables

```python
# Pattern (lines ~712, ~723)
@st.cache_resource
def __conn(): ...   # cache_resource

def _conn():        # reconnect wrapper
    conn = __conn()
    try: conn.cursor().execute("SELECT 1")
    except: __conn.clear(); conn = __conn()
    return conn
```

Key env var: `PGURL` (or `DATABASE_URL`).

| Table | Content |
|-------|---------|
| `marketdata.spot_prices_hourly` | Hourly spot prices (mixed units — nanmedian>5 → /1000) |
| `marketdata.spot_prices_daily` | Daily DA/RT summaries |
| `marketdata.spot_fundamentals_hourly` | Load/wind/solar/net_export per province per hour |
| `staging.jizhi_bids` | 机制竞价 historical bids |
| `staging.jizhi_upcoming` | 机制竞价 upcoming auctions |
| `staging.spot_knowledge_chunks` | KB text chunks (FTS + vector) |
| `staging.kp_expert_insights` | AI-extracted expert insights |

---

## ECR / AWS Quick Reference

| Item | Value |
|------|-------|
| Account | `319383842493` |
| Region | `ap-southeast-1` |
| Spot-markets ECR | `bess-spot-markets` |
| Hermes ECR | `bess-platform-hermes` |
| ECS cluster | `bess-platform-cluster` |
| Spot-markets service | `bess-platform-spot-markets-svc` |
| Hermes service | `bess-platform-hermes-svc` |
| ALB DNS | `bess-platform-alb-1158505371.ap-southeast-1.elb.amazonaws.com` |
| Task role | `bess-platform-task-role` (has `bedrock:InvokeModel`) |
| MSYS prefix | `MSYS_NO_PATHCONV=1 aws ...` (Windows Git Bash) |

---

## Version History (recent)

| Version | Task Def | Change |
|---------|----------|--------|
| v88 | :120 | Pre-Bedrock; 401 errors |
| v89 | — | FAILED — `ModuleNotFoundError: No module named 'shared'` |
| v90 | :123 | Fixed Dockerfile (`COPY shared/`, `fastembed>=0.3`); Bedrock live |
| **v91** | **:124** | **Fix: AI Agent returns partial answer on `max_tokens` instead of error** |
