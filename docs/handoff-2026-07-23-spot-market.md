# Spot-Market Handoff — 2026-07-23

> **For a new Claude session:** Read this document in full before making any changes.
> Working directory: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`
> Branch: `feat/deal-structurer-bedrock-migration`
> Repo: `https://github.com/MonteCarlo79/invest-etrm-intel`

---

## Deployment State

| Service | Image | ECS Task Def | Cluster |
|---|---|---|---|
| spot-markets | `bess-spot-markets:v88` | `bess-platform-spot-markets:120` | `bess-platform-cluster` |
| hermes | `bess-platform-hermes:latest` | `bess-platform-hermes:156` | `bess-platform-cluster` |

---

## What Was Done in the 2026-07-23 Session

### Bedrock migration — all bess-platform apps now done

`shared/anthropic_client.py` was already updated (global.anthropic.* model IDs, env var override).  
All app-layer files were verified and the one remaining gap was fixed:

| App | File | Change |
|---|---|---|
| bess-map | `apps/bess-map/app.py` | Added `is_llm_available` to import; changed `if not os.environ.get("ANTHROPIC_API_KEY")` → `if not _is_llm_available(...)` at line ~3856; updated EN/ZH `"agent_no_key"` strings |
| hermes | `services/hermes/app.py` | Already done (startup crash fix + endpoint guards) — verified |
| spot-market | `apps/spot-market/app.py` + `spot_report.py` | Already done — verified |
| gb-market | `apps/gb-market/app.py` + `daily_report.py` + `Dockerfile` | Already done — verified |
| mengxi-dashboard | `apps/mengxi-dashboard/app.py` + `requirements.txt` | Already done — verified |
| deal-structurer | `apps/deal_structurer/strategist.py` | Already done (v8/td:9) |

**Remaining: `ib-platform`** (separate repo, local MacBook only — see `docs/BEDROCK_MIGRATION_GUIDE.md` §9).  
`docs/BEDROCK_MIGRATION_GUIDE.md` status table updated to mark all bess-platform apps ✅.

### Important: no new spot-market version deployed

All Bedrock migration work for spot-market was already in the source code from a previous session.
The running image is still **v88**. The next code change to spot-market will become **v89**.

---

## Spot-Market Key Files

| File | Purpose |
|---|---|
| `apps/spot-market/app.py` | Main Streamlit app (~6800 lines, all UI tabs) |
| `apps/spot-market/spot_report.py` | AI daily report generator |
| `apps/spot-market/Dockerfile` | Docker build |
| `services/knowledge_pool/jizhi_extractor.py` | Nova Pro AI extraction for 机制竞价 |
| `shared/anthropic_client.py` | Bedrock-aware client factory (already correct) |
| `docs/handoff-2026-07-16-spot-market.md` | Full prior context: DB pattern, tab structure, v86–v88 fixes |

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

## Recent Fixes (v86–v88)

| Version | Fix |
|---|---|
| v86 | `_load_price_holdout`: `AND price_col > 0`; backtest guard skips chart when max < 1e-9 |
| v87 | `_conn()`: `_get_conn.clear()` → `__conn.clear()` (NameError caused full app crash) |
| v88 | `_fc_ho_valid` threshold raised to `0.05` ¥/kWh (50 ¥/MWh) — `> 0` alone passed garbage near-zero values → 100,000%+ MAPE |

---

## Open Issues / Next Work

### 1. Backtest always shows "无有效价格数据" (self-healing — no action needed now)
Holdout windows currently fall in the old-pipeline gap (~pre-2025-09). Will self-heal as `spot_prices_hourly` accumulates new-pipeline rows (¥/MWh range 200–600). No code change needed.

### 2. 价格预测 — wire `_load_forecast_fundamentals` into the models (priority)

`_load_forecast_fundamentals` is defined but **not yet connected** to the PCA+ARIMA or merit-order stack models. The 价格预测 tab (~line 5080) uses:
- **PCA + ARIMA** — price matrix only, no fundamental drivers
- **Merit-Order Stack** — static cost stack (RE 0, Nuclear 25, Hydro 15, Coal coal_price×0.31+18)
- **Bayesian Conjugate Gaussian** — price distribution only

To wire in fundamentals:
- `_load_forecast_fundamentals` pulls from `marketdata.spot_fundamentals_hourly` (province, datetime, load, wind, solar, net_export)
- Goal: use wind/solar/load as exogenous regressors in the ARIMA component (ARIMAX), or as a capacity factor input to the merit-order stack
- Start by reading `_load_forecast_fundamentals` definition and the ARIMA fitting code (~line 5927) before any changes

**Unit note:** `spot_prices_hourly` has mixed units. Training data: `nanmedian > 5` → divide by 1000. This normalisation happens at ~line 5927 — keep it.

### 3. 机制竞价 — Nova Pro prompt tuning for edge-case PPTX (lower priority)

- Model: `apac.amazon.nova-pro-v1:0` (Amazon Nova Pro, ap-southeast-1)
- Plain-text generation (NOT tool use — tool use causes `ModelErrorException` on large files)
- `_BIDS_PROMPT` and `_UPCOMING_PROMPT` are in `services/knowledge_pool/jizhi_extractor.py`
- Edge case: multi-page charts in PPTX → JSON output may be incomplete or malformed
- `_parse_json_from_text(raw, key)` handles fenced ```json blocks and bare JSON
- If tuning prompts: test with a known edge-case file before redeploying hermes

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

## Deploy Next Version (v89+)

```bash
# 1. Build
docker build -f apps/spot-market/Dockerfile \
  -t 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v89 .

# 2. ECR login + push
MSYS_NO_PATHCONV=1 aws ecr get-login-password --region ap-southeast-1 \
  | docker login --username AWS --password-stdin \
    319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v89

# 3. Strip task def + inject BEDROCK_REGION + update image tag
MSYS_NO_PATHCONV=1 aws ecs describe-task-definition \
  --task-definition bess-platform-spot-markets:120 \
  --region ap-southeast-1 \
  --query 'taskDefinition' --output json > C:/tmp/td_spot.json

py -3 -c "
import json
with open('C:/tmp/td_spot.json') as f: td = json.load(f)
for k in ['taskDefinitionArn','revision','status','requiresAttributes','compatibilities','registeredAt','registeredBy']:
    td.pop(k, None)
env = td['containerDefinitions'][0]['environment']
if not any(e['name'] == 'BEDROCK_REGION' for e in env):
    env.append({'name': 'BEDROCK_REGION', 'value': 'ap-southeast-1'})
else:
    next(e for e in env if e['name'] == 'BEDROCK_REGION')['value'] = 'ap-southeast-1'
td['containerDefinitions'][0]['image'] = td['containerDefinitions'][0]['image'].replace(':v88', ':v89')
with open('C:/tmp/td_spot_new.json', 'w') as f: json.dump(td, f, indent=2)
print('done')
"

MSYS_NO_PATHCONV=1 aws ecs register-task-definition \
  --cli-input-json file://C:/tmp/td_spot_new.json --region ap-southeast-1

# 4. Update service (replace :NNN with new revision from previous command)
MSYS_NO_PATHCONV=1 aws ecs update-service \
  --cluster bess-platform-cluster \
  --service bess-platform-spot-markets-svc \
  --task-definition bess-platform-spot-markets:NNN \
  --force-new-deployment --region ap-southeast-1
```

**BEDROCK_REGION must be `ap-southeast-1`** — verified working with `global.anthropic.*` profiles.  
**Do NOT use `us.anthropic.*`** — requires AWS Marketplace subscription that cannot be auto-completed.

---

## ECR / AWS Quick Reference

| Item | Value |
|---|---|
| Account | `319383842493` |
| Region | `ap-southeast-1` |
| Spot-markets ECR repo | `bess-spot-markets` |
| ECS cluster | `bess-platform-cluster` |
| Spot-markets service | `bess-platform-spot-markets-svc` |
| Task role | `bess-platform-task-role` (has `bedrock:InvokeModel` + `bedrock:InvokeModelWithResponseStream`) |

---

## Bedrock Client Pattern (already in codebase)

```python
# spot-market/app.py already uses this — no change needed
from shared.anthropic_client import make_client as _make_anthropic_client, is_llm_available

api_key = os.environ.get("ANTHROPIC_API_KEY", "")
client = _make_anthropic_client(api_key)   # uses Bedrock when BEDROCK_REGION is set

if not is_llm_available(api_key):
    st.warning("No LLM configured (set ANTHROPIC_API_KEY or BEDROCK_REGION)")
```

Model strings (`"claude-sonnet-4-6"`, `"claude-haiku-4-5-20251001"`) auto-map to Bedrock IDs — call sites unchanged.
