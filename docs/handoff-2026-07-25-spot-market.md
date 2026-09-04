# Spot-Market Handoff — 2026-07-25

> **For a new Claude session:** Read this document in full before making any changes.
> Working directory: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`
> Branch: `feat/deal-structurer-bedrock-migration`
> Repo: `https://github.com/MonteCarlo79/invest-etrm-intel`

---

## Deployment State

| Service | Image | ECS Task Def | Cluster |
|---|---|---|---|
| spot-markets | `bess-spot-markets:v90` | `bess-platform-spot-markets:123` | `bess-platform-cluster` |
| hermes | `bess-platform-hermes:latest` | `bess-platform-hermes:158` | `bess-platform-cluster` |

**Next version = v91** (no code changes pending for spot-market; rebuild when next feature lands).

---

## What Was Done in the 2026-07-24/25 Sessions

### 1. Fixed 401 AI Agent error (Bedrock migration v88→v90)

Root cause: v88 was built before the Bedrock migration commit; running code used `anthropic.Anthropic(api_key=...)` directly; task def :120 had no `BEDROCK_REGION`; `ANTHROPIC_API_KEY` was invalid.

Fix:
- v89: failed — `ModuleNotFoundError: No module named 'shared'` (Dockerfile missing `COPY shared/`)
- v90: fixed Dockerfile (`COPY shared/` added, `fastembed>=0.3` added); deployed as td:123 with `BEDROCK_REGION=ap-southeast-1`

### 2. Knowledge base — 20 new docs ingested

Ran `ingest_knowledge_bulk.py` across `data/market-fundamentals/`:
- 20 new policy/market docs added (province capacity reports, 南方调频辅助服务规则, 新型能源体系十五五规划, 河北独储收益, 市场调研报告-内蒙, etc.)
- `政策汇编2025.8.17.pdf` still failing (timeout) — retry with `--timeout 600 --workers 1`

### 3. Fixed hermes nightly KB digest

**Bug:** `_run_kb_digest` in `services/hermes/app.py` had `if not api_key:` guard that silently skipped when `ANTHROPIC_API_KEY` was unset.

**Fix applied:**
- Code: changed to `if not _is_llm_available(api_key):` (uses existing `_is_llm_available` import)
- Task def: added `BEDROCK_REGION=ap-southeast-1` + `ANTHROPIC_API_KEY=bedrock` (placeholder so old guard passes) → registered td:158

**Nightly digest schedule:** 18:07 UTC (02:07 Beijing) — runs synthesis + expert insight extraction for up to 30 docs per night.

### 4. KB synthesis triggered manually

Called `POST /hermes/knowledge/digest` to start synthesis for the 348 pending shared docs:

```powershell
py -c "import requests; r = requests.post('https://bess-platform-alb-1158505371.ap-southeast-1.elb.amazonaws.com/hermes/knowledge/digest', verify=False); print(r.status_code, r.text)"
```

Returns `{"status":"started"}` — runs in background. Each call processes 30 docs.

---

## KB Pipeline (full picture)

Synthesis from China (local) is **blocked** — `global.anthropic.*` Bedrock profiles reject calls from non-AWS IPs. Must trigger via hermes HTTP endpoint or ECS task.

```
ingest_knowledge_bulk.py   →   POST /hermes/knowledge/digest   →   kp_expert_insights
   (chunks + FTS index)         (synthesis + digest in ECS)         (AI-extracted insights)
```

### Current KB state
- ~7,026 docs registered; 20 new docs added (synthesis pending — 348 total in queue)
- Nightly hermes job drains 30/night → ~12 nights to clear backlog
- To drain faster: call `/hermes/knowledge/digest` multiple times (wait ~2min between calls)

### Key tables
| Table | Purpose |
|---|---|
| `staging.spot_knowledge_docs` | One row per registered file |
| `staging.spot_knowledge_chunks` | Text chunks (FTS + vector search) |
| `staging.kp_doc_summaries` | Expert synthesis — populated by synthesis step |
| `staging.kp_qa_pairs` | Synthetic Q&A pairs — populated by synthesis step |
| `staging.kp_expert_insights` | Structured insights — populated by digest |

---

## Spot-Market Key Files

| File | Purpose |
|---|---|
| `apps/spot-market/app.py` | Main Streamlit app (~6800 lines, all UI tabs) |
| `apps/spot-market/spot_report.py` | AI daily report generator |
| `apps/spot-market/Dockerfile` | Docker build (v90: added `COPY shared/`, `fastembed>=0.3`) |
| `services/knowledge_pool/jizhi_extractor.py` | Nova Pro AI extraction for 机制竞价 |
| `services/hermes/app.py` | `_run_kb_digest` nightly pipeline (fixed guard, line ~122) |
| `shared/anthropic_client.py` | Bedrock-aware client factory |

---

## DB Connection Pattern

```python
# Two-level cache: __conn() is @st.cache_resource; _conn() wraps with reconnect
@st.cache_resource
def __conn(): ...   # line ~712

def _conn():        # line ~723
    conn = __conn()
    try:
        conn.cursor().execute("SELECT 1")
    except Exception:
        __conn.clear()   # was buggy as _get_conn.clear() — fixed v87
        conn = __conn()
    return conn
```

Key env var: `PGURL` (or `DATABASE_URL`).
Key tables: `marketdata.spot_prices_hourly`, `marketdata.spot_prices_daily`, `marketdata.spot_fundamentals_hourly`, `staging.jizhi_bids`, `staging.jizhi_upcoming`.

---

## Recent Fixes (v86–v90)

| Version | Fix |
|---|---|
| v86 | `_load_price_holdout`: `AND price_col > 0`; backtest guard skips chart when max < 1e-9 |
| v87 | `_conn()`: `_get_conn.clear()` → `__conn.clear()` (NameError caused full app crash) |
| v88 | `_fc_ho_valid` threshold raised to `0.05` ¥/kWh (50 ¥/MWh) |
| v89 | FAILED — `ModuleNotFoundError: No module named 'shared'` |
| v90 | Fixed Dockerfile: added `COPY shared/`, `fastembed>=0.3`; Bedrock migration live |

---

## Open Issues / Next Work

### 1. Build v91 (when next code change lands)

The Dockerfile now includes `fastembed>=0.3` but the ECS container (v90) was built before that line was confirmed. Next rebuild (v91) will enable vector search in production. Until then, FTS fallback is active.

### 2. Deploy hermes with code fix

`services/hermes/app.py` has the `is_llm_available` guard fix committed locally but the running image (td:158) still uses the old code with `ANTHROPIC_API_KEY=bedrock` workaround. Deploy when convenient:

```bash
bash scripts/deploy_hermes.sh
# OR in PowerShell:
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker build -f apps/hermes-service/Dockerfile -t 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest .
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
aws ecs update-service --cluster bess-platform-cluster --service bess-platform-hermes-svc --force-new-deployment --region ap-southeast-1
```

### 3. Backtest always shows "无有效价格数据" (self-healing)

Holdout windows fall in the old-pipeline gap (~pre-2025-09). Will self-heal as `spot_prices_hourly` accumulates new-pipeline rows. No code change needed.

### 4. 价格预测 — wire `_load_forecast_fundamentals` into the models (priority)

`_load_forecast_fundamentals` is defined but **not yet connected** to PCA+ARIMA or merit-order stack:
- `_load_forecast_fundamentals` pulls from `marketdata.spot_fundamentals_hourly` (province, datetime, load, wind, solar, net_export)
- Goal: ARIMAX with wind/solar/load as exogenous regressors, or capacity factor input to merit-order
- Start by reading `_load_forecast_fundamentals` definition and ARIMA fitting code (~line 5927)
- Unit note: `spot_prices_hourly` has mixed units — `nanmedian > 5` → divide by 1000 at ~line 5927

### 5. 机制竞价 — Nova Pro prompt tuning (lower priority)

- Model: `apac.amazon.nova-pro-v1:0` (ap-southeast-1, NOT tool use)
- `_BIDS_PROMPT` / `_UPCOMING_PROMPT` in `services/knowledge_pool/jizhi_extractor.py`
- Edge case: multi-page PPTX charts → incomplete JSON output

### 6. ib-platform Bedrock migration (separate repo, MacBook)

See `docs/BEDROCK_MIGRATION_GUIDE.md` §9.

---

## 价格预测 Model Architecture (quick reference)

```
tab_forecast: ~line 5080
_load_forecast_fundamentals: defined, NOT wired in

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

---

## Deploy Next Version (v91+)

```powershell
# 1. Build
docker build -f apps/spot-market/Dockerfile `
  -t 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v91 .

# 2. ECR login + push
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v91

# 3. Strip task def + inject BEDROCK_REGION + update image tag
aws ecs describe-task-definition --task-definition bess-platform-spot-markets:123 --region ap-southeast-1 --query 'taskDefinition' --output json > C:/tmp/td_spot.json

py -3 -c "
import json
with open('C:/tmp/td_spot.json', encoding='utf-16') as f: td = json.load(f)
for k in ['taskDefinitionArn','revision','status','requiresAttributes','compatibilities','registeredAt','registeredBy']:
    td.pop(k, None)
env = td['containerDefinitions'][0]['environment']
if not any(e['name'] == 'BEDROCK_REGION' for e in env):
    env.append({'name': 'BEDROCK_REGION', 'value': 'ap-southeast-1'})
else:
    next(e for e in env if e['name'] == 'BEDROCK_REGION')['value'] = 'ap-southeast-1'
td['containerDefinitions'][0]['image'] = td['containerDefinitions'][0]['image'].replace(':v90', ':v91')
with open('C:/tmp/td_spot_new.json', 'w') as f: json.dump(td, f, indent=2)
print('done')
"

aws ecs register-task-definition --cli-input-json file://C:/tmp/td_spot_new.json --region ap-southeast-1 --query 'taskDefinition.revision'

# 4. Update service (replace :NNN with new revision)
aws ecs update-service --cluster bess-platform-cluster --service bess-platform-spot-markets-svc --task-definition bess-platform-spot-markets:NNN --force-new-deployment --region ap-southeast-1
```

**BEDROCK_REGION must be `ap-southeast-1`** — `global.anthropic.*` profiles only work from inside AWS ap-southeast-1.

---

## ECR / AWS Quick Reference

| Item | Value |
|---|---|
| Account | `319383842493` |
| Region | `ap-southeast-1` |
| Spot-markets ECR repo | `bess-spot-markets` |
| Hermes ECR repo | `bess-platform-hermes` |
| ECS cluster | `bess-platform-cluster` |
| Spot-markets service | `bess-platform-spot-markets-svc` |
| Hermes service | `bess-platform-hermes-svc` |
| ALB DNS | `bess-platform-alb-1158505371.ap-southeast-1.elb.amazonaws.com` |
| Task role | `bess-platform-task-role` (has `bedrock:InvokeModel` + `bedrock:InvokeModelWithResponseStream`) |

---

## Bedrock Client Pattern

```python
from shared.anthropic_client import make_client as _make_anthropic_client, is_llm_available

api_key = os.environ.get("ANTHROPIC_API_KEY", "")
client = _make_anthropic_client(api_key)   # uses Bedrock when BEDROCK_REGION is set

if not is_llm_available(api_key):
    st.warning("No LLM configured (set ANTHROPIC_API_KEY or BEDROCK_REGION)")
```

Model strings (`"claude-sonnet-4-6"`, `"claude-haiku-4-5-20251001"`) auto-map to Bedrock IDs.

**IMPORTANT:** `global.anthropic.*` Bedrock inference profiles reject calls from China-based IPs (even via VPN to ap-southeast-1). All LLM work must run inside ECS or be triggered via the hermes HTTP endpoint.

---

## Trigger KB Digest from Local Machine

```powershell
# Single batch (30 docs)
py -c "import requests; r = requests.post('https://bess-platform-alb-1158505371.ap-southeast-1.elb.amazonaws.com/hermes/knowledge/digest', verify=False); print(r.status_code, r.text)"

# Drain backlog (N batches, 2min apart)
1..10 | ForEach-Object {
    py -c "import requests; r = requests.post('https://bess-platform-alb-1158505371.ap-southeast-1.elb.amazonaws.com/hermes/knowledge/digest', verify=False); print('Batch $_:', r.status_code, r.text)"
    if (`$_ -lt 10) { Start-Sleep 120 }
}
```
