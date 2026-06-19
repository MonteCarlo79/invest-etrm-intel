# BESS Map App — Handoff 2026-06-19

## Current Deployment

| Item | Value |
|------|-------|
| ECR image | `bess-map:v48` |
| Task definition | `bess-platform-bess-map:70` |
| ECS service | `bess-platform-bess-map-svc` (cluster `bess-platform-cluster`) |
| Status | ✅ Running — PRIMARY, running=1, desired=1 |
| URL | `https://www.pjh-etrm.ai/bess-map/` |
| Branch | `cost-optimisation` (pushed to `origin/cost-optimisation`) |

## App Structure — 8 Tabs

```
tab_ranking  | Tab 1: Province Ranking
tab_geo      | Tab 2: Geo Map
tab_pca      | Tab 3: Price Profile PCA
tab_demand   | Tab 4: BESS Demand Analysis      ← most recent work
tab_dispatch | Tab 5: Dispatch & Economics
tab_irr      | Tab 6: IRR Calculator
tab_mgmt     | Tab 7: Data Management
tab_agent    | Tab 8: Agent
```
Defined at `apps/bess-map/app.py:1352`.

## What Was Built / Fixed (Recent Sessions)

### v48 — BESS Installed Capacity in Demand Analysis Tab (2026-06-19)
Commit `3a9eacd` — `apps/bess-map/app.py`

**Section ② (FR Capacity Requirement table):**
- Added "BESS Installed (万kW)" column pulled from `province_fundamentals.储能.value`
- `_storage_wkw = (_cap_yr.get("储能", {}) or {}).get("value") or 0.0`

**Section ③ (comparison chart):**
- Added green bar for existing BESS capacity (万kW → MW: `_storage_mw = _storage_wkw * 10`)
- Added "Gap (Demand−Existing, MW)" column: `_gap_mw = max(_recommended - _storage_mw, 0.0)`

### v48 — FR Capacity Rules Upgraded to 4-Tuple (earlier)
`_FR_RULES` format: `(desc, pct_load, pct_renew_inst, floor_mw)`
Formula: `FR_mw = max(floor_mw, peak_load_mw × pct_load + renew_installed_mw × pct_renew_inst)`

Confirmed rules (sourced from official provincial FM market docs):
- Shaanxi: 2.5%×load + 10%×wind (Art.17)
- Jiangxi/Hubei/Chongqing: 2–5%×load (Huazhong)
- Yunnan: 450MW floor + 0.6%×load + 0.6%×renew
- South Grid (广东/广西/贵州/海南): ~1.5%×load + 1.5%×renew
- Default `_FR_DEFAULT`: `("National avg", 0.05, 0.10, 0.0)`

Co-location ratios (配储比例) intentionally NOT used — abolished by No. 136 policy.

### Province Fundamentals DB Pipeline
- Table: `marketdata.province_fundamentals` — 32 provinces × 2024/2025
  - Columns: wind/solar/thermal/hydro/nuclear/storage cap + gen + peak load
- Ingest script: `scripts/ingest_province_fundamentals.py`
- `app.py` calls `load_province_data_from_db(dsn)` using `PGURL` env var (not Excel at runtime)
- Dockerfile: `COPY services/market_fundamentals/` added so DB-backed loader is available

### Data Management Tab — Missing-Dates Column
Commit `ecb8aca` — coverage table now shows "Missing Dates" column, not just % coverage.

### 4 Bug Fixes
Commit `10de9b6`:
1. Pandas crash on empty data
2. Wrong cycles table column mapping
3. IRR model filter not applied correctly
4. O(n²) coverage query → rewritten with date_series CTE

## Key Files

| File | Purpose |
|------|---------|
| `apps/bess-map/app.py` | Main Streamlit app (all 8 tabs, ~2600 lines) |
| `apps/bess-map/Dockerfile` | Build context: copies `bess_map/`, `market_fundamentals/`, `shared/`, `auth/` |
| `services/market_fundamentals/loader.py` | `load_province_data_from_db(dsn)` + Excel loader |
| `scripts/ingest_province_fundamentals.py` | CLI to upsert province_fundamentals from Excel to DB |
| `services/bess_map/headless_agent.py` | Headless agent for Tab 8 (Agent) |

## How to Build and Deploy

### Build & Push
```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform"

# 1. ECR login
aws ecr get-login-password --region ap-southeast-1 \
  | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com

# 2. Build (from repo root — Dockerfile uses multi-service COPY)
docker build -f apps/bess-map/Dockerfile -t bess-map:v49 .

# 3. Tag + push
docker tag bess-map:v49 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:v49
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:v49
```

### Deploy (ECS direct — Terraform ignores container_definitions)
```bash
# Export current task def
PYTHONUTF8=1 aws ecs describe-task-definition \
  --task-definition bess-platform-bess-map \
  --region ap-southeast-1 --query taskDefinition \
  --output json > /tmp/bm_td.json

# Patch image with Python
python "C:/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe" -c "
import json
td = json.load(open('/tmp/bm_td.json'))
for k in ['taskDefinitionArn','revision','status','requiresAttributes','compatibilities','registeredAt','registeredBy']:
    td.pop(k, None)
td['containerDefinitions'][0]['image'] = '319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:v49'
json.dump(td, open('/tmp/bm_td_new.json','w'), ensure_ascii=False)
print('done')
"

# Register new task def
PYTHONUTF8=1 aws ecs register-task-definition \
  --cli-input-json file:///tmp/bm_td_new.json \
  --region ap-southeast-1 \
  --query "taskDefinition.{family:family,revision:revision}" --output table

# Update service (replace :71 with actual new revision)
aws ecs update-service \
  --cluster bess-platform-cluster \
  --service bess-platform-bess-map-svc \
  --task-definition bess-platform-bess-map:71 \
  --region ap-southeast-1 --output table

# Monitor
aws ecs describe-services \
  --cluster bess-platform-cluster \
  --services bess-platform-bess-map-svc \
  --region ap-southeast-1 \
  --query "services[0].deployments[*].{status:status,taskDef:taskDefinition,running:runningCount}" \
  --output table
```

## Local Run
```bash
# Via docker-compose (pinned to v48 in docker-compose.local.yml):
docker-compose -f docker-compose.local.yml up bess-map

# Or directly:
PYTHONPATH=. streamlit run apps/bess-map/app.py --server.port 8503
```

## DB Connection

```
Host: bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com
Port: 5432
DB:   marketdata
User: postgres
Schema: public (bess_capture_daily, spot_prices_hourly, etc.)
        audit   (province_progress)
```

Key tables used by bess-map:
| Table | Purpose |
|-------|---------|
| `bess_capture_daily` | Strategy P&L / dispatch results |
| `spot_prices_hourly` | DA/RT prices per province |
| `province_fundamentals` | Wind/solar/storage installed cap + peak load |
| `data_ops_log` | Ingestion run log (Data Management tab) |
| `hermes_memory` | Agent tab persistent memory |

## Re-ingest Province Fundamentals

Run this after updating the market fundamentals Excel file:
```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform"
python scripts/ingest_province_fundamentals.py
```
The script reads `services/market_fundamentals/loader.py::load_province_data()` (local Excel), then upserts all rows into `marketdata.province_fundamentals`.

Excel file location (local): set `EXCEL_DIR` in `services/market_fundamentals/loader.py` — defaults to `C:\Users\dipeng.chen\OneDrive\ETRM\data\`.

## Adding / Updating FR Rules

Edit `_FR_RULES` in `apps/bess-map/app.py:889`:
```python
_FR_RULES: dict[str, tuple[str, float, float, float]] = {
    # province: (description, pct_load, pct_renew_installed, floor_mw)
    "山东": ("Shandong FM rules", 0.10, 0.05, 0.0),
    ...
}
```
Source documents: provincial FM market rule PDFs in the `知识库` or each province's `2-政策/` folder in the KB.

## AWS Info

| Item | Value |
|------|-------|
| Account | `319383842493` |
| Region | `ap-southeast-1` |
| Cluster | `bess-platform-cluster` |
| ECR repo | `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map` |
| Task family | `bess-platform-bess-map` |
| Service | `bess-platform-bess-map-svc` |

## Pending / Known Issues

None currently open for bess-map. Next logical improvements:
- Expand `_FR_RULES` to cover more provinces (most still use `_FR_DEFAULT`)
- Add 2026 province_fundamentals data when available (currently 2024/2025 only)
- Headless agent (`services/bess_map/headless_agent.py`) is new — not yet wired into Agent tab
