# Spot-Market Handoff — 2026-07-16

> **For a new Claude session:** Read this document in full before making any changes to the spot-market app.
> Working directory: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`
> Branch: `cost-optimisation`
> Repo: `https://github.com/MonteCarlo79/invest-etrm-intel`

---

## Deployment State

| Service | Image | ECS Task Def | Cluster |
|---|---|---|---|
| spot-markets | `bess-spot-markets:v88` | `bess-platform-spot-markets:120` | `bess-platform-cluster` |
| hermes | `bess-platform-hermes:latest` | `bess-platform-hermes:156` | `bess-platform-cluster` |

**How to deploy a new spot-market version** (Windows — no jq/node/python3, use `py`):
```bash
# 1. Build
docker build -f apps/spot-market/Dockerfile -t 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:vNN .

# 2. Push
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:vNN

# 3. Create new task def JSON (use py, not python3)
aws ecs describe-task-definition --task-definition bess-platform-spot-markets --query 'taskDefinition' --output json > /tmp/td_spot.json
TMPFILE=$(cygpath -w /tmp/td_spot.json)
py -c "
import json, re
with open(r'$TMPFILE') as f: data = json.load(f)
for k in ['taskDefinitionArn','revision','status','requiresAttributes','compatibilities','registeredAt','registeredBy']: data.pop(k, None)
for c in data.get('containerDefinitions', []): c['image'] = re.sub(r':v\d+$', ':vNN', c['image'])
outfile = r'$TMPFILE'.replace('td_spot.json', 'td_spot_new.json')
with open(outfile, 'w', encoding='utf-8') as f: json.dump(data, f)
"

# 4. Register + update service
NEW_REV=$(aws ecs register-task-definition --cli-input-json "file://$(cygpath -w /tmp/td_spot_new.json)" --query 'taskDefinition.revision' --output text)
aws ecs update-service --cluster bess-platform-cluster --service bess-platform-spot-markets-svc --task-definition bess-platform-spot-markets:$NEW_REV --force-new-deployment
```

---

## Key Files

| File | Purpose |
|---|---|
| `apps/spot-market/app.py` | Main Streamlit app — all UI tabs |
| `apps/spot-market/Dockerfile` | Docker build |
| `services/knowledge_pool/jizhi_extractor.py` | AI extraction + DB persistence for 机制竞价 |
| `services/hermes/app.py` | Hermes bot (Feishu webhook handler) |

---

## App Structure (tabs in order)

```
Overview | DA-RT Spread | Heatmap | Intraday Analysis | Province Deep-Dive
Distributions | Geo Map | Inter-Provincial Flow | Market Fundamentals
Strategist | News Sources | Library | 机制竞价 | 供需结构 | 价格预测 | Data Management
```

Tab variable names (~line 430):
```python
tab_overview, tab_dart, tab_heatmap, tab_intraday, tab_province,
tab_dist, tab_geo, tab_interprov, tab_fund, tab_strat,
tab_news, tab_library, tab_jizhi, tab_supply, tab_forecast, tab_mgmt
```

---

## DB Connection Pattern

```python
# Two-level: __conn() is @st.cache_resource; _conn() wraps it with reconnect
@st.cache_resource
def __conn(): ...  # line ~712

def _conn():       # line ~723
    conn = __conn()
    try:
        conn.cursor().execute("SELECT 1")
    except Exception:
        __conn.clear()   # NOTE: was buggy as _get_conn.clear() — fixed v87
        conn = __conn()
    return conn
```

Key env var: `PGURL` (or `DATABASE_URL`). Default: `postgresql://postgres:root@127.0.0.1:5433/marketdata`.

Key tables:
- `marketdata.spot_prices_daily` — `province_cn`, `da_price`, `rt_price`
- `marketdata.spot_prices_hourly` — `province`, `datetime`, `da_price`, `rt_price`
- `marketdata.spot_fundamentals_hourly` — `province`, `datetime`, `load`, `wind`, `solar`, `net_export`
- `staging.jizhi_bids` — mechanism bidding results
- `staging.jizhi_upcoming` — upcoming bid calendar

---

## 机制竞价 Tab (`with tab_jizhi:` ~line 4705)

### Database tables (auto-created via `ensure_tables()`):
- `staging.jizhi_bids` — completed bid results (UNIQUE on province, year, batch, tech_type)
- `staging.jizhi_bid_winners` — 中标清单 (FK to jizhi_bids)
- `staging.jizhi_upcoming` — upcoming bid calendar

### Sub-tabs:
1. **历史结果** — filterable table + bar chart of cleared prices. Loaded via `_load_jizhi_bids(_pg)` (ttl=300s).
2. **即将竞价** — upcoming bid calendar countdown. Loaded via `_load_jizhi_upcoming(_pg)`.
3. **上传&录入** — file upload → AI extraction → preview → save to DB.
   - Uses `st.session_state["jz_extracted_records"]` to avoid re-extraction on button click
   - After save: `_load_jizhi_bids.clear()` + `st.rerun()` to refresh 历史结果 immediately

### `jizhi_extractor.py` — AI extraction:
- **Model**: `apac.amazon.nova-pro-v1:0` (Amazon Nova Pro via APAC cross-region Bedrock, `ap-southeast-1`)
- **Approach**: Plain text generation (NO tool use — causes `ModelErrorException` on large files)
- **JSON parsing**: `_parse_json_from_text(raw, key)` — handles fenced ```json blocks and bare JSON
- **Why not Anthropic**: `apac.anthropic.claude-3-haiku-20240307-v1:0` requires AWS use-case form not submitted

### batch values:
- `存量` = grid-connected before 2025-05-31
- `增量_2025-12` / `增量_2026-12` / `增量_2027-12` = commissioned before respective year-end

---

## 价格预测 Tab (`with tab_forecast:` ~line 5080)

Three hybrid model components:

### 1. PCA + ARIMA
- Price matrix: days × 24h from `marketdata.spot_prices_hourly`
- SVD → top 4 PCs → ARIMA(1,0,1) per PC, capped at 30 steps
- Long-horizon (>30 days): seasonal mean-reversion at 3%/day decay toward monthly historical mean
- Province selector: only provinces with ≥30 days data

### 2. Merit-Order Stack Model
- RE (0) → Nuclear (25) → Hydro (15) → Coal (coal_price×0.31+18) → Peaking coal

### 3. Bayesian Conjugate Gaussian
- `posterior_var = 1/(n0/σ0² + n1/σ1²)`, `posterior_mean = posterior_var × (n0×μ0/σ0² + n1×μ1/σ1²)`

### Unit normalisation (IMPORTANT):
`spot_prices_hourly` has mixed units across time: new pipeline = ¥/MWh, old pipeline = ¥/kWh.
- Training data: `nanmedian > 5` → divide by 1000 to convert to ¥/kWh (~line 5927)
- Bayesian raw: `median > 5` → divide by 1000
- Holdout: `max() > 5` → divide by 1000

### Backtest / holdout validation:
- Holdout window = 14 days before training start
- `_load_price_holdout` filters `AND {price_col} IS NOT NULL AND {price_col} > 0` (line ~980)
- Guard: `_fc_ho_valid = _fc_ho_mean.max() >= 0.05` (¥/kWh = 50 ¥/MWh minimum; line ~6681)
  - If False: shows "验证期无有效价格数据" and skips broken metrics/chart
  - **Why 0.05?** Old pipeline stored tiny non-zero garbage values (< 0.001 ¥/kWh) that pass `> 0` but cause nonsensical 100,000%+ MAPE. Real China spot prices are ≥ 0.1 ¥/kWh.

### Forecast horizons:
```python
_fc_days_eoy2027 = max(1, (pd.Timestamp('2027-12-31') - pd.Timestamp.today().normalize()).days)
_fc_horizon_opts = [1, 3, 7, 30, 90, 180, 365, _fc_days_eoy2027]
```

---

## Fixes Applied This Session (v86–v88)

| Version | Fix |
|---|---|
| v86 | `_load_price_holdout`: add `AND {price_col} > 0`; backtest guard `max < 1e-9` → skip chart |
| v87 | `_conn()`: `_get_conn.clear()` → `__conn.clear()` (NameError on reconnect caused full app crash) |
| v88 | Raised `_fc_ho_valid` threshold to `0.05` ¥/kWh — `> 0` filter insufficient for garbage near-zero data |

---

## ECR / AWS Details

- Account: `319383842493`
- Region: `ap-southeast-1`
- Spot-markets ECR repo: `bess-spot-markets`
- Hermes ECR repo: `bess-platform-hermes`
- ECS cluster: `bess-platform-cluster`
- Spot-markets service: `bess-platform-spot-markets-svc`
- Hermes service: `bess-platform-hermes-svc`

---

## Known Issues / Potential Follow-ups

1. **Backtest always shows "无有效价格数据"** for any province whose holdout window falls before ~2025-09 (old ingestion pipeline gap). This will self-heal as the DB accumulates more fresh `spot_prices_hourly` rows ingested by the new pipeline (values in ¥/MWh range 200–600).

2. **价格预测 forecast quality** — PCA+ARIMA shape is plausible (solar suppression midday, dual peaks visible for Shandong). However the model has no fundamental drivers (wind/solar/load not yet wired into forecast). `_load_forecast_fundamentals` is defined but may need tuning per province.

3. **机制竞价 extraction quality** — Nova Pro plain-text JSON output is less structured than tool-use; edge cases like multi-page charts in PPTX may need prompt tuning in `_BIDS_PROMPT` / `_UPCOMING_PROMPT` in `jizhi_extractor.py`.

4. **即将竞价** — Hermes internet scan (`_run_jizhi_scan`) runs every 6 hours via APScheduler. Check `staging.jizhi_upcoming` for scan results.

5. **`infra/terraform/terraform.tfvars`** — `image_spot_markets` still shows an old tag (`v31`). This file is NOT used for deployment. Do not confuse it with the actual running version.
