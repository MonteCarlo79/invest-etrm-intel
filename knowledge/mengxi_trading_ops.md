# Mengxi BESS Trading Operations — Knowledge Document

**App:** `apps/mengxi-dashboard`  
**Pillar:** 3 — Asset Operations & Portfolio Optimisation  
**URL:** `https://www.pjh-etrm.ai/mengxi-dashboard`  
**Port:** 8511  
**ECR repo:** `bess-mengxi-dashboard`  
**Current image:** `bess-mengxi-dashboard:v6` (deployed 2026-05-12)  
**Ingestion image:** `bess-mengxi-ingestion:v19` (deployed 2026-05-08)

---

## Purpose

Break down BESS revenue into P&L attribution components. Starting from the theoretical perfect-foresight upper bound, each step reveals where value is being lost:

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

## Tabs

| Tab | Purpose | Key data source |
|-----|---------|-----------------|
| Market Data | Provincial RT prices, wind/solar, load, bidding space | `public.hist_mengxi_*_15min` |
| Dispatch & P&L Waterfall | Hero tab — 5-step P&L cascade + dispatch chart | `reports.bess_asset_daily_attribution`, `marketdata.md_id_cleared_energy`, `marketdata.ops_bess_dispatch_15min`, `canon.nodal_rt_price_15min` |
| Daily Ops | 4-asset daily strategy comparison + LP benchmark | `reports.bess_strategy_daily_*`, `marketdata.ops_bess_dispatch_15min` |
| Strategy Comparison | Multi-day YTD strategy analysis + report export | `reports.bess_asset_daily_attribution` |
| Options Cockpit | Kirk/Margrabe spread call strip valuation | `canon.nodal_rt_price_15min` |
| Data Management | Table freshness, missing dates coverage, pipeline logs | `marketdata.data_quality_status`, `marketdata.md_load_log` |

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
**Schedule:** EventBridge cron `0 20 * * ? *` (UTC, = 04:00 CST) → Lambda `bess-mengxi-launcher` → ECS task `bess-mengxi-reconcile`  
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
| P&L Waterfall page | `libs/decision_models/adapters/app/dispatch_pnl_page.py` |
| Daily Ops page | `libs/decision_models/adapters/app/daily_ops_page.py` |
| Strategy Comparison page | `libs/decision_models/adapters/app/strategy_comparison_page.py` |
| Options Cockpit page | `libs/decision_models/adapters/app/cockpit_page.py` |
| Attribution page | `libs/decision_models/adapters/app/attribution_page.py` |
| Ingestion orchestrator | `bess-marketdata-ingestion/providers/mengxi/run_pipeline.py` |
| Excel loader | `bess-marketdata-ingestion/providers/mengxi/load_excel_to_marketdata.py` |
| Batch downloader | `bess-marketdata-ingestion/providers/mengxi/batch_downloader.py` |
| Manual upload loader (shared) | `services/mengxi_ingestion/loader.py` |
| Ingestion infra | `infra/terraform/mengxi-ingestion/` |
