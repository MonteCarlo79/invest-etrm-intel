# Mengxi BESS Trading Operations — Knowledge Document

**App:** `apps/mengxi-dashboard`  
**Pillar:** 3 — Asset Operations & Portfolio Optimisation  
**URL:** `https://www.pjh-etrm.ai/mengxi-dashboard`  
**Port:** 8505 (ECS/production); 8511 for local `streamlit run` only  
**ECR repo:** `bess-mengxi-dashboard`  
**Current image:** `bess-mengxi-dashboard:v7` (deployed 2026-05-13 23:17 SGT, digest sha256:c22f5a96e2a4)  
**Next image:** `bess-mengxi-dashboard:v8` (8-tab province-level restructuring — pending build & deploy)  
**Ingestion image:** `bess-mengxi-ingestion:v19` (deployed 2026-05-08)

---

## Purpose

Province-level trading management app for Inner Mongolia (Mengxi). Combines:
1. Market fundamentals (prices, wind/solar, load)
2. Market-wide BESS ranking (delegated to inner_pipeline ECS task)
3. Our 4-asset BESS portfolio deep-dive (P&L waterfall, daily ops, strategy)
4. Wind farm performance ranking (inline query)
5. Options pricing, data management, Trader AI agent

**BESS P&L waterfall** — break down BESS revenue into attribution components:


```
PF Unrestricted       ← LP on actual RT prices, no grid constraints (true ceiling)
  ↓ grid_restriction_loss    = PF_unrestricted − PF_grid_feasible
PF Grid-Feasible      ← LP on actual RT prices, within grid constraints
  ↓ forecast_error_loss      = PF_grid_feasible − Forecast_Optimal
Forecast Optimal      ← LP on forecast prices
  ↓ nomination_loss          = Forecast_Optimal − Nomination_PnL
Nomination P&L        ← 申报曲线 × nodal_price (ops Excel)
  ↓ market_clearing_loss     = Nomination − Trading_Cleared
Trading Cleared       ← cleared_energy_mwh_15min × cleared_price (md_id_cleared_energy)
  ↓ execution_loss           = Trading_Cleared − Actual_Cleared
Actual Cleared        ← 实际充放曲线 × nodal_price (ops Excel)
```

---

## Assets

4 Inner Mongolia BESS stations:

| asset_code   | Display name              |
|-------------|---------------------------|
| suyou        | 景蓝乌尔图 (SuYou)         |
| hangjinqi    | 悦杭独贵 (HangJinQi)       |
| siziwangqi   | 景通四益堂储 (SiZiWangQi)  |
| gushanliang  | 裕昭沙子坝 (GuShanLiang)   |

---

## Tabs (v8 — 8-tab province layout)

| Tab | Purpose | Key data source |
|-----|---------|-----------------|
| Market Fundamentals | Provincial RT prices, wind/solar, load, bidding space | `public.hist_mengxi_*_15min` |
| BESS Market Ranking | All-BESS arbitrage ranking via async ECS pipeline (`inner_pipeline`) | `marketdata.inner_mongolia_bess_results`, `marketdata.inner_mongolia_nodal_clusters` |
| Our BESS Portfolio → P&L Waterfall | 5-step P&L cascade + dispatch chart | `reports.bess_asset_daily_attribution`, `marketdata.md_id_cleared_energy`, `marketdata.ops_bess_dispatch_15min`, `canon.nodal_rt_price_15min` |
| Our BESS Portfolio → Daily Ops | 4-asset daily strategy comparison + LP benchmark | `reports.bess_strategy_daily_*`, `marketdata.ops_bess_dispatch_15min` |
| Our BESS Portfolio → Strategy Comparison | Multi-day YTD strategy analysis + report export | `reports.bess_asset_daily_attribution` |
| Options Pricing | Kirk/Margrabe spread call strip valuation | `canon.nodal_rt_price_15min` |
| Wind Farm Ranking | All wind farm generation + revenue ranking (inline query) | `marketdata.md_id_cleared_energy` |
| Wind Farm Trading | Placeholder — future wind dispatch management | — |
| Data Management | Table freshness, coverage, pipeline logs, manual upload, Shanxi nodal download | `marketdata.data_quality_status`, `marketdata.md_load_log`, `marketdata.md_shanxi_nodal_price_96` |
| Trader | Claude agent (sonnet-4-6) for P&L attribution + dispatch analysis + KB search + GB benchmark | `reports.bess_asset_daily_attribution`, `marketdata.ops_bess_dispatch_15min`, `marketdata.agent_memory`, `staging.spot_knowledge_docs` |

---

## Data Sources & Ingestion

### Mengxi market data (Ingestion tables)

| DB table | Content | Date column | Lag |
|----------|---------|-------------|-----|
| `marketdata.md_id_cleared_energy` | Intra-day cleared energy + price per unit | `data_date` | 0d |
| `marketdata.md_rt_nodal_price` | RT nodal prices per node | `data_date` | 0d |
| `marketdata.md_da_cleared_energy` | Day-ahead cleared energy per unit | `data_date` | +1d |
| `marketdata.md_rt_total_cleared_energy` | RT total cleared energy | `data_date` | 0d |
| `marketdata.md_id_fuel_summary` | Intra-day fuel summary | `data_date` | 0d |
| `marketdata.md_da_fuel_summary` | Day-ahead fuel summary | `data_date` | +1d |
| `marketdata.md_avg_bid_price` | Average bid prices | `data_date` | 0d |
| `marketdata.md_settlement_ref_price` | Settlement reference price | `data_date` | 0d |

**Ingestion pipeline:** `bess-marketdata-ingestion/providers/mengxi/`  
**Schedule:** EventBridge cron `0 12 * * ? *` (UTC = 20:00 CST) — moved from 08:30 CST because API returns empty files in the morning (data published in the afternoon). Defined in `infra/terraform/trading-bess-mengxi/schedules.tf`. Downstream schedules (tt-province-loader 09:10 CST, pnl-refresh 10:10 CST) use prior-night's ingest — acceptable given MARKET_LAG_DAYS=1 built-in lag.  
**Source API:** `https://app-portal-cn-ft.enos-iot.com/mengxi-data-sync/v1/api/details/6.52`  
**Min file size:** 3–4 MB (files below threshold skipped as corrupted)  
**Modes:** `daily` (latest available), `reconcile` (gap-fill window), `remediation` (targeted dates)

**Alert webhook:** `ALERT_WEBHOOK_URL` env var — fires on:
- DB connectivity timeout (original)
- Failed or partial_success loads (added 2026-05-08)
- `is_complete=FALSE` in data_quality_status (added 2026-05-08)
- Weekdays with no quality record at all — download failure (added 2026-05-08)
- Pipeline crash (added 2026-05-08)

### Ops dispatch data

| DB table | Content | Date column |
|----------|---------|-------------|
| `marketdata.ops_bess_dispatch_15min` | Nominated (申报) + actual (实际) dispatch per asset | `data_date` |

**Ingestion:** `services/ops_ingestion/inner_mongolia/` — Excel upload from ops files  
**Key columns:** `asset_code`, `interval_start`, `interval_end`, `data_date`, `nominated_dispatch_mw`, `actual_dispatch_mw`, `nodal_price_excel`

### Canon prices

| DB table | Content | Date column |
|----------|---------|-------------|
| `canon.nodal_rt_price_15min` | 15-min RT nodal cleared price per asset (UNION view) | `time` |

**Populated by:** ETL from `md_id_cleared_energy.cleared_price` → `canon.nodal_rt_price_15min_id_cleared` → view  
**Run:** Manual or via MCP tool `fill_canon_nodal_prices`  
**Backfill completed:** 2026-03-01 → 2026-04-23 (as of 2026-04-24)

### Attribution reports

| DB table | Content | Date column |
|----------|---------|-------------|
| `reports.bess_asset_daily_attribution` | Pre-computed daily P&L per step per asset | `trade_date` |

**Populated by:** Daily strategy analysis pipeline (`libs/decision_models/workflows/daily_strategy_report.py`)  
**Run:** Manual via "Run daily analysis" button in Daily Ops tab, or scheduled pipeline

---

## Data Management Tab — How to Read It

### Table Freshness
- 🟢 ≤2 days stale = current (MARKET_LAG_DAYS=1, so yesterday's data is expected)
- 🟡 3–7 days = slightly behind
- 🔴 >7 days or "No data" = problem

**Ops/Canon/Reports showing "No data" is expected** if those pipelines haven't run on this deployment yet. They are not fed by the mengxi ingestion ECS task.

### Ingestion Coverage (Missing Dates)
The authoritative completeness check. Compares weekday calendar since a configurable start date against `DISTINCT data_date` in the selected table. Any weekday not in the table = missing. Does not depend on `data_quality_status` existing.

**Green = 0 missing weekdays. Any missing dates need manual remediation:**
```bash
# Trigger ECS remediation for specific dates
# Set EXACT_DATES env var and run bess-mengxi-remediation task
```

### Pipeline Quality Log
Populated only after the new ingestion pipeline image (v19+) runs. Shows `is_complete`, interval coverage (96 expected per day), file size, and error notes per day.

### Load Log
Populated only after the new pipeline runs. Shows success/partial/failed per file with error details.

### Manual File Upload & Ingest (Section 5)
For dates the automated pipeline can't download (upstream API returning HTTP 500/504), files can be downloaded manually and ingested via the UI:

1. Download the missing file from the Enos portal and rename it `YYYY-MM-DD.xlsx` (e.g. `2026-03-13.xlsx`)
2. Go to **Data Management → Manual File Upload & Ingest**
3. Upload one or more files (multi-select supported)
4. Leave **Force reload** checked (default) — this deletes existing partial data for those dates before inserting
5. Click **Ingest files** — per-file progress and sheet-level results are shown inline
6. Click **Refresh now** to confirm the dates are now covered in the Missing Dates section

**How it works:** Uses `services/mengxi_ingestion/loader.py` — identical parse/upsert/quality-log logic as the ECS pipeline but accepts raw bytes. Updates both `marketdata.md_load_log` and `marketdata.data_quality_status` so the Pipeline Quality Log reflects the manual load.

---

## Deployment

```bash
# Build from repo root (Dockerfile copies libs/ services/ apps/mengxi-dashboard/)
$env:DOCKER_BUILDKIT="0"
docker build -f apps/mengxi-dashboard/Dockerfile -t bess-mengxi-dashboard:v<N> .

docker tag bess-mengxi-dashboard:v<N> \
  319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-mengxi-dashboard:v<N>
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-mengxi-dashboard:v<N>

# Update infra/terraform/terraform.tfvars:
#   image_mengxi_dashboard = "...bess-mengxi-dashboard:v<N>"
cd infra/terraform && terraform apply
```

**Dockerfile notes:**
- Build context = repo root (not the app directory)
- Uses Tsinghua pip mirror (`pypi.tuna.tsinghua.edu.cn`) — required on China network
- `--timeout 300 --retries 5` for poor connectivity

---

## Key Code Locations

| Component | Path |
|-----------|------|
| Main app | `apps/mengxi-dashboard/app.py` |
| BESS Market Ranking tab | `apps/mengxi-dashboard/bess_market_tab.py` |
| Wind Farm Ranking tab | `apps/mengxi-dashboard/wind_farm_tab.py` |
| Inner-Mongolia helpers (shared) | `apps/mengxi-dashboard/inner_mongolia_helpers.py` |
| P&L Waterfall page | `libs/decision_models/adapters/app/dispatch_pnl_page.py` |
| Daily Ops page | `libs/decision_models/adapters/app/daily_ops_page.py` |
| Strategy Comparison page | `libs/decision_models/adapters/app/strategy_comparison_page.py` |
| Options Cockpit page | `libs/decision_models/adapters/app/cockpit_page.py` |
| Attribution page | `libs/decision_models/adapters/app/attribution_page.py` |
| Ingestion orchestrator | `bess-marketdata-ingestion/providers/mengxi/run_pipeline.py` |
| Excel loader | `bess-marketdata-ingestion/providers/mengxi/load_excel_to_marketdata.py` |
| Batch downloader | `bess-marketdata-ingestion/providers/mengxi/batch_downloader.py` |
| Manual upload loader (shared) | `services/mengxi_ingestion/loader.py` |
| Ingestion infra (schedule) | `infra/terraform/trading-bess-mengxi/schedules.tf` |
| **Fengxing nodal price client** | `services/fengxing/nodal_price.py` |
| **Fengxing download script** | `scripts/download_shanxi_nodal.py` |

---

## Shanxi Nodal Prices (Fengxing API)

**DB table:** `marketdata.md_shanxi_nodal_price_96`  
**PK:** `(node_name, metric_time, time_order_96)`  
**API endpoint:** `POST https://lingfeng-saas.tradingthink.cn/api/base/metrics/data/query`  
**Auth:** `X-API-KEY-SECRET` header — key in `FENGXING_API_KEY` env var  
**Metric:** `avg_node_price`  
**Columns requested:** `market_name`, `node_name`, `metric_time`, `time_order_96`  
  ⚠️ Do NOT include `avg_node_price` in `columns` — it is returned automatically as the metric and causes error 10000 "metric repeated" if listed again.

**IP whitelist:** API requires the caller's public IP to be whitelisted. ECS tasks have dynamic IPs — run download locally.  
**Local outgoing IP:** `138.113.14.246` (confirmed 2026-05-13)  
**Download workflow (local → RDS):**
```powershell
# Step 1: download to CSV (fast, no DB round-trips)
py scripts/download_shanxi_nodal.py --start 2026-01-01 --end 2026-05-12 --csv-only

# Step 2: upload CSV to RDS
py scripts/download_shanxi_nodal.py --from-csv shanxi_nodal_2026-01-01_2026-05-12.csv
```

**ECS env vars required:** `ANTHROPIC_API_KEY`, `FENGXING_API_KEY` — already wired in `main.tf` as of 2026-05-13.
