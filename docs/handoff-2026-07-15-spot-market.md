# Spot-Market Handoff — 2026-07-15

> **For a new Claude session:** Read this document in full before making any changes to the spot-market app.
> Working directory: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`
> Branch: `cost-optimisation`
> Repo: `https://github.com/MonteCarlo79/invest-etrm-intel`

---

## Deployment State

| Service | Image | ECS Task Def | Cluster |
|---|---|---|---|
| spot-markets | `bess-spot-markets:v86` | `bess-platform-spot-markets:118` | `bess-platform-cluster` |
| hermes | `bess-platform-hermes:latest` | `bess-platform-hermes:156` | `bess-platform-cluster` |

**How to deploy a new spot-market version** (Windows — no jq/node/python3, use `py`):
```bash
# 1. Build
docker build -f apps/spot-market/Dockerfile -t 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:vNN .

# 2. Push
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:vNN

# 3. Create new task def JSON (use py, not python3)
aws ecs describe-task-definition --task-definition bess-platform-spot-markets:CURRENT_REV --query 'taskDefinition' --output json > /tmp/td_spot.json
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

Tab variable names (defined at top of app.py near line ~430):
```python
tab_overview, tab_dart, tab_heatmap, tab_intraday, tab_province,
tab_dist, tab_geo, tab_interprov, tab_fund, tab_strat,
tab_news, tab_library, tab_jizhi, tab_supply, tab_forecast, tab_mgmt
```

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
- **Approach**: Plain text generation (NO tool use — tool use caused `ModelErrorException` on large files)
- **JSON parsing**: `_parse_json_from_text(raw, key)` — handles fenced ```json blocks and bare JSON
- **Why not Anthropic**: `apac.anthropic.claude-3-haiku-20240307-v1:0` requires AWS use-case form not submitted
- **Why no tool use**: `ModelErrorException: Model produced invalid sequence as part of ToolUse` on large PPTX

### batch values (normalisation):
- `存量` = grid-connected before 2025-05-31
- `增量_2025-12` = commissioned before 2025-12-31
- `增量_2026-12` = commissioned before 2026-12-31
- `增量_2027-12` = commissioned before 2027-12-31

---

## 价格预测 Tab (`with tab_forecast:` ~line 5080+)

Three hybrid model components shown as sub-tabs:

### 1. PCA + ARIMA
- Price matrix: days × 24h from `marketdata.spot_prices_hourly`
- SVD decomposition → top 4 principal components
- ARIMA(1,0,1) per PC, capped at 30 steps
- Long-horizon (>30 days): seasonal mean-reversion at 3%/day decay toward monthly historical mean
- Province selector populated from `_load_hourly_price_provinces(_conn)` (only provinces with ≥30 days data)

### 2. Merit-Order Stack Model
- RE (0 ¥/MWh) → Nuclear (25) → Hydro (15) → Coal (coal_price×0.31+18) → Peaking coal → Peak premium
- Nuclear capacity override dict for coastal provinces:
  `{"江苏":8500, "福建":10000, "广东":20000, "浙江":6600, "辽宁":6700, "海南":2200, "山东":2500, "广西":2200}`

### 3. Bayesian Conjugate Gaussian
- Prior from historical price distribution
- Posterior update: `posterior_var = 1/(n0/σ0² + n1/σ1²)`, `posterior_mean = posterior_var × (n0×μ0/σ0² + n1×μ1/σ1²)`

### Forecast horizons:
```python
_fc_days_eoy2027 = max(1, (pd.Timestamp('2027-12-31') - pd.Timestamp.today().normalize()).days)
_fc_horizon_opts = [1, 3, 7, 30, 90, 180, 365, _fc_days_eoy2027]
```

---

## Database Connection Pattern

```python
# Connection function is _conn() — NOT get_conn()
from apps.spot_market.app import _conn  # (inside app, just call _conn())
pg_url = os.environ.get("PGURL") or os.environ.get("DATABASE_URL") or "postgresql://postgres:root@127.0.0.1:5433/marketdata"
```

Key tables:
- `marketdata.spot_prices_daily` — `province_cn` (Chinese), `da_price`, `rt_price`
- `marketdata.spot_prices_hourly` — `province` (Chinese), `datetime`, `da_price`, `rt_price`
- `marketdata.spot_fundamentals_hourly` — `province`, `datetime`, `load`, `wind`, `solar`, `net_export`
- `staging.jizhi_bids` — mechanism bidding results
- `staging.jizhi_upcoming` — upcoming bid calendar

---

## Geo Map Fix (EOH tab)

In the EOH geo map, the adcode→Chinese name lookup uses:
```python
_ADCODE_TO_NAME = {110000:"北京", 130000:"河北", 140000:"山西", ...}  # ~line 2100
```
Labels rendered as `f"{name}\n{eoh_lbl}"` so province name shows above EOH value.

Bar chart: negative EOH bars have no number label:
```python
text=[f"{v:,}" if v >= 0 else "" for v in _eoh_sorted['eoh']]
```

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

1. **价格预测 tab** — the Bayesian and stack models are scaffolded but the fundamentals loader (`_load_forecast_fundamentals`) may need tuning depending on data availability per province.
   - **Backtest actual price = 0 (FIXED v86)**: Old ingestion pipeline stored `da_price = 0` (not NULL) for missing periods. Fix: `_load_price_holdout` now filters `AND {price_col} > 0`. If all holdout values are still zero, backtest section shows "验证期无有效价格数据" and skips metrics/chart.
   - **Unit mismatch (FIXED v84/v85)**: Recent data in ¥/MWh, older data in ¥/kWh. Fix: `nanmedian > 5` → divide by 1000 applied to training, Bayesian, and holdout arrays.
2. **机制竞价 extraction quality** — Nova Pro plain-text JSON output is less structured than tool-use; edge cases like multi-page charts in PPTX may need prompt tuning in `_BIDS_PROMPT` / `_UPCOMING_PROMPT` in `jizhi_extractor.py`.
3. **即将竞价** — the Hermes internet scan (`_run_jizhi_scan`) runs every 6 hours via APScheduler in `services/hermes/app.py`. Check `staging.jizhi_upcoming` for scan results.
4. **`infra/terraform/terraform.tfvars`** — `image_spot_markets` still shows an old tag (`v31`). This file is NOT used for spot-markets deployment (ECS task def is updated directly). Do not confuse it with the actual running version.
