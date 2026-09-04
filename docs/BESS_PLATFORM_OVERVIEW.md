# BESS Platform — Overview & Operations Reference

**Last updated:** 2026-05-08  
**Branch:** `cost-optimisation`  
**Deployed at:** `https://www.pjh-etrm.ai`

---

## 1. Architecture

The platform runs on AWS ECS Fargate behind a single ALB, all proxied through an HTTPS listener with path-based routing. Cognito handles authentication (Hosted UI, OAuth2 code flow). All services share one RDS PostgreSQL instance (`marketdata` schema).

```
ALB (bess-platform-alb)  →  ECS Cluster (bess-platform-cluster)
  /bess-map/*              →  bess-map:v37        port 8503  (Pillar 2 cockpit)
  /spot-markets/*          →  bess-spot-markets:v18  port 8505
  /inner-mongolia/*        →  bess-inner-mongolia    port 8501
  /bess-uploader/*         →  bess-uploader          port 8502  (retired, replaced by bess-map Data Mgmt)
  /portal/*                →  portal                 port 80
  /model-catalogue/*       →  bess-model-catalogue   port 8506
  /options-cockpit/*       →  bess-options-cockpit   port 8507
  /pnl-attribution/*       →  bess-pnl-attribution   port 8509
  Agents (AI):
    /strategy-agent/*
    /portfolio-agent/*
    /execution-agent/*
    /dev-agent/*
    /trading-performance-agent/*
```

**RDS:** `bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata`  
**S3 uploads bucket:** `bess-uploader-data-chen-singp-2026`

---

## 2. Pillars

### Pillar 1 — Spot Market Monitor (`apps/spot-market`, `:v18`)

URL: `https://www.pjh-etrm.ai/spot-markets/`

Multi-province China spot electricity market dashboard. Tabs:

| Tab | Content |
|-----|---------|
| Overview | DA vs RT price trends, province selector |
| Spread | DA–RT spread analysis |
| Heatmap | Province × time price heatmap |
| **Intraday Analysis** | Hourly price shape, spread ranking, price duration curve, hour×province heatmap |
| Province Detail | Single-province deep dive |
| Distribution | Price distribution histograms |
| Geo Map | Choropleth of annual BESS revenue by province |
| Inter-Provincial | Cross-province spread & correlation |
| **Fundamentals** | Load, bidding space, wind/solar WAP vs RT price (requires `spot_fundamentals_hourly` data) |
| Agent | Claude-powered analyst with DB tool access |
| Management | (internal) |

**Key data loader dependencies:**
- `marketdata.spot_prices_hourly` — 15-min prices aggregated to hourly (¥/kWh)
- `marketdata.spot_fundamentals_hourly` — load, renewable, bidding space (MW), wind, solar
- `services/spot_ingest/` — ingest pipeline for spot price Excel files
- `services/market_fundamentals/` — market context data

### Pillar 2 — BESS Investment Cockpit (`apps/bess-map`, `:v37`)

URL: `https://www.pjh-etrm.ai/bess-map/`

Tabs:

| Tab | Content |
|-----|---------|
| Province Ranking | LP-optimal annual revenue ranking by province (2h/4h), capture rate, intraday spread |
| Geo Map | Choropleth of annual BESS revenue, payback zone colouring |
| Dispatch & Economics | Monthly avg daily revenue (¥/MWh_cap/day), capture rate, hourly dispatch detail |
| IRR Calculator | Full equity IRR / NPV / payback; sensitivity table (capex × revenue multiplier) |
| Data Management | S3 upload → ingestion → capture pipeline → fundamentals ingest; DB coverage table |
| Agent | Claude claude-sonnet-4-6 agent with `get_bess_economics`, `get_dispatch_detail`, `get_irr_estimate` tools |

### Pillar 3 — Inner Mongolia Operations (`apps/bess-inner-mongolia`)

Daily ops for 4 IM BESS assets. Managed separately; see `docs/openclaw/`.

---

## 3. Data Pipelines

### 3a. Spot Price Ingestion

**Input:** Province Excel files (`各省现货价格及边界数据/<province>.xlsx`)  
**Script:** `services/bess_map/run_all_provinces.py` → `run_one_province.py`  
**Output:** `marketdata.spot_prices_hourly` (`province`, `datetime`, `rt_price`, `da_price`)

**Price unit:** ¥/kWh (Yuan per kilowatt-hour) — this is the raw unit from source files.

Trigger via bess-map Data Management tab → "Run Ingestion" button, or directly:

```bash
python services/bess_map/run_all_provinces.py \
  --indir /tmp/bess_uploads \
  --auto-cols --upload-db --env none \
  --schema marketdata --continue-on-error
```

Progress tracked in `audit.province_progress` (prevents re-ingesting unchanged files).

### 3b. Capture Pipeline

**Script:** `services/bess_map/run_capture_pipeline.py`  
**Purpose:** For each province × duration, runs LP dispatch optimisation and forecast-based "capture" simulation.

**Output tables:**
- `marketdata.spot_dispatch_hourly_theoretical` — theoretical LP dispatch (charge_mw, discharge_mw, soc_mwh)
- `marketdata.spot_dispatch_hourly_rt_forecast` — forecast-based dispatch
- `marketdata.bess_capture_daily` — daily `theoretical_profit_per_mwh_day`, `realized_profit_per_mwh_day`, `capture_rate`

**Units in DB:** `theoretical_profit_per_mwh_day` is in **¥/MWh** (corrected in v37 — see §6).

Trigger via bess-map Data Management tab → "Run Capture Pipeline" (select province from multiselect, choose duration), or:

```bash
python services/bess_map/run_capture_pipeline.py \
  --env none --schema marketdata \
  --duration-h 4 --province-list 山东 \
  --force --force-theoretical
```

`--force` recomputes capture from scratch. `--force-theoretical` recomputes LP dispatch.

### 3c. Fundamentals Ingestion

**Script:** `services/bess_map/run_fundamentals_ingest.py`  
**Input:** Same province Excel files (contains load, renewable, bidding space, wind, solar columns)  
**Output:** `marketdata.spot_fundamentals_hourly`

15-min intervals are aggregated to hourly means before upsert.

```bash
python services/bess_map/run_fundamentals_ingest.py \
  --indir data/market-fundamentals/各省现货价格及边界数据 \
  --env none --schema marketdata --continue-on-error
```

### 3d. Mengxi Scheduled Jobs

EventBridge Scheduler triggers `bess-trading-jobs:v20260405-1` for:
- TT loader (TT App Key/Secret configured in tfvars)
- Mengxi P&L refresh

---

## 4. Database Schema (key tables)

```
marketdata
├── spot_prices_hourly          province, datetime, rt_price [¥/kWh], da_price [¥/kWh]
├── spot_fundamentals_hourly    province, datetime, load_mw, bidding_space_mw, wind_mw, solar_mw, ...
├── spot_dispatch_hourly_theoretical   province, datetime, duration_h, charge_mw, discharge_mw, soc_mwh
├── spot_dispatch_hourly_rt_forecast   province, datetime, model, duration_h, charge_mw, dispatch_grid_mw
├── bess_capture_daily          province, date, model, duration_h, theoretical_profit_per_mwh_day [¥/MWh],
│                               realized_profit_per_mwh_day [¥/MWh], capture_rate
├── bess_daily                  province, date, price_type, duration_h, profit [¥], profit_per_mwh_day [¥/MWh]
└── bess_monthly                aggregate of bess_daily by month

audit
├── processed_files             file_name, processed_at
└── province_progress           province, duration_h, last_ts  (skip-logic for ingestion)
```

---

## 5. Build & Deploy

### Build sequence

```powershell
# ECR login
aws ecr get-login-password --region ap-southeast-1 | `
  docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com

# Build & push bess-map
docker build -f apps/bess-map/Dockerfile -t 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:vNN .
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:vNN

# Build & push spot-markets
docker build -f apps/spot-market/Dockerfile -t 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:vNN .
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:vNN
```

Update `infra/terraform/terraform.tfvars` with the new version tags, then:

```powershell
cd infra/terraform
terraform apply -auto-approve
```

### Current image versions

| Service | Image tag | Status |
|---------|-----------|--------|
| bess-map | v37 | deployed (2026-05-08) |
| bess-spot-markets | v18 | deployed |
| bess-inner-mongolia | v52 | deployed |
| bess-inner-pipeline | v16 | deployed |
| bess-trading-jobs | v20260405-1 | scheduled |
| pnl-attribution | v4 | deployed |
| model-catalogue | v1 | suspended (desired_count=0) |
| bess-uploader | v20 | suspended (desired_count=0) |

### Cognito

Hosted UI domain: configured in Terraform. Callback URLs:
- `https://pjh-etrm.ai/oauth2/idpresponse`
- `https://www.pjh-etrm.ai/oauth2/idpresponse`

Default redirect: `https://www.pjh-etrm.ai/oauth2/idpresponse`

---

## 6. Key Bug Fixes & Known Issues

### v37 — ×1000 units fix (2026-05-08)

**Problem:** Prices in `spot_prices_hourly` are in ¥/kWh. The LP profit formula
`Σ price × MW × dt` yields `¥/kWh × MWh` units, not ¥. Dividing by energy capacity
(`e_cap = power_mw × duration_h MWh`) gave `profit_per_mwh_day` in ¥/kWh — 1000× smaller
than the ¥/MWh the charts expected. Charts showed ~0.3 instead of ~300.

**Fix:** `run_capture_pipeline.py` now multiplies both `theo_profit_by_day` and realized `pnl`
by 1000 before writing to DB.

**Action required:** Re-run capture pipeline with `--force --force-theoretical` for all
provinces to replace the incorrect historical values.

```bash
# Fix all provinces, both durations
for DUR in 2 4; do
  python services/bess_map/run_capture_pipeline.py \
    --env none --schema marketdata \
    --duration-h $DUR \
    --force --force-theoretical --continue-on-error
done
```

### v36 — Province name mismatch fix (2026-05-07)

**Problem:** Data Management "Run Capture Pipeline" used a free-text input. User typed "shandong"
(English) but DB stores "山东" (Chinese). Pipeline skipped all provinces.

**Fix:** Province selector changed to `st.multiselect` loaded from `marketdata.spot_prices_hourly`.

### v34 — Blank page / infinite loading fix (2026-05-06)

**Problem:** `_ensure_memory_table()` was called at module level before `st.tabs()`. psycopg2
has no connect timeout by default — ECS task hung indefinitely waiting for the DB, rendering
a blank page.

**Fix:** Moved `_ensure_memory_table()` inside `with tab_agent:` so tabs render first. Also
added `connect_args={"connect_timeout": 10}` to the SQLAlchemy engine.

### v35 — psycopg import fix (2026-05-06)

**Problem:** `run_all_provinces.py` used `import psycopg` (psycopg3) but Dockerfile only
installs `psycopg2-binary`.

**Fix:** Changed to `import psycopg2 as psycopg`.

---

## 7. Local Development

```bash
# Set env vars from config
set -a && source config/.env && set +a   # or: cp config/.env.example config/.env

# bess-map (Pillar 2)
streamlit run apps/bess-map/app.py --server.port 8503

# spot-market (Pillar 1)
streamlit run apps/spot-market/app.py --server.port 8505
```

Local DB: `postgresql://postgres:root@127.0.0.1:5433/marketdata` (default fallback in both apps).

---

## 8. Environment Variables (ECS task definitions)

| Variable | Used by | Notes |
|----------|---------|-------|
| `PGURL` | all services | Full postgres DSN with `?sslmode=require` |
| `ANTHROPIC_API_KEY` | bess-map, spot-market, agents | claude-sonnet-4-6 |
| `S3_BUCKET` | bess-map | `bess-uploader-data-chen-singp-2026` |
| `COGNITO_DOMAIN` | portal | Cognito Hosted UI |
| `TT_APP_KEY` / `TT_APP_SECRET` | trading-jobs | TT API credentials |
| `DB_HOST` | trading-jobs | RDS hostname (no port) |
| `SHOW_AWS_DEBUG` | bess-map | `false` in prod |

---

## 9. Operational Runbook — Adding a New Province

1. Obtain province Excel file (`<省名>.xlsx`) with columns: `日期`, `时点`, RT price, DA price.
2. Upload via bess-map → Data Management → "Upload Files" (S3).
3. Click **Run Ingestion** → wait for streaming log → verify `[OK] <province>` lines.
4. Click **Run Capture Pipeline** → select province from multiselect → choose duration(s) → Run.
5. Check DB Coverage table: `last_hourly` and `last_capture` should be up to date.
6. Navigate to Province Ranking tab → province appears in ranking.

To ingest fundamentals (load / bidding space / wind / solar) from the same file:
- Click **Run Fundamentals Ingest** in Data Management.
- Fundamentals appear in spot-market → Fundamentals tab (bidding space scatter + WAP charts).

---

## 10. Repository

**Repo:** `MonteCarlo79/invest-etrm-intel` (private)  
**Branch:** `cost-optimisation`  
**ECR account:** `319383842493` (ap-southeast-1)  
**ACM cert:** `arn:aws:acm:ap-southeast-1:319383842493:certificate/8a7d08c9-d008-48a0-a1d4-25e125bd0ab8`
