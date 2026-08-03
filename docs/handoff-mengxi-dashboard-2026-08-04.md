# Mengxi Dashboard — Handoff Note (2026-08-04)

Branch: `feat/deal-structurer-bedrock-migration`
Commit: `018aff7`

---

## What Was Done This Session

### 1. Removed Dead Code
- Deleted `services/enos/` entirely (`nodal_client.py`, `__init__.py`)
  — ENOS REST endpoints `/grid/node/name` and `/grid/node/price` return 404
- Deleted `bess-marketdata-ingestion/providers/mengxi/enos_nodal_ingestor.py`
  — depended on the broken ENOS client; `md_rt_nodal_price` is already kept
  fresh by the `bess-inner-mongolia` ECS Excel pipeline (separate service,
  desired_count=1, still running)

### 2. Fixed 503 OOM Crash on mengxi-dashboard
**File:** `infra/terraform/main.tf`

ECS task definition bumped:
```
cpu    = "256" → "512"
memory = "512" → "2048"
```
**Status: committed, NOT yet applied.**
Run `terraform apply` to deploy:
```bash
cd infra/terraform
terraform plan   # verify only the task definition changes
terraform apply
```

### 3. Fixed wind_farm_tab.py Connection Drop
**File:** `apps/mengxi-dashboard/wind_farm_tab.py`

Root cause: query fetched all raw 15-min rows for all plants × 30 days =
millions of rows → RDS drops connection.

Fix: push `SUM / MAX / COUNT DISTINCT` aggregation into SQL `GROUP BY plant_name`,
return one row per plant (~hundreds of rows), then classify asset type in Python.

The tab now shows **all plant types** (not just wind) — user confirmed this is
the intent.

### 4. Expanded Province List
**File:** `apps/mengxi-dashboard/app.py` line ~841

`_ALL_MARKETS` expanded from 12 to 18 provinces:
```python
_ALL_MARKETS = [
    "蒙西", "山西", "山东", "陕西", "湖南", "浙江", "云南", "贵州",
    "广东", "广西", "海南", "甘肃",
    "安徽", "江西", "河北南网", "湖北", "辽宁", "黑龙江",
]
```

### 5. LingFeng ODS API Client
**Files:**
- `services/lingfeng/api_client.py` — HTTP client for LingFeng ODS nodal price endpoint
- `services/fengxing/nodal_price.py` — `download_and_upsert(province, date, engine)` function
- `services/lingfeng/ingest_province_clearing.py` — province clearing price ingestion

**API endpoint:** `POST /api/open/v1/ods/data/query`
**Key:** `LINGFENG_API_KEY` in `config/.env` — **currently still placeholder `YOUR_LINGFENG_API_KEY_HERE`**

### 6. Historical CSV Backfill Script
**File:** `scripts/ingest_nodal_csvs.py`

Bulk-ingests ~306 historical nodal CSV files from `data/nodal/<province>/`
into `marketdata.md_shanxi_nodal_price_96` using PostgreSQL COPY protocol.

```bash
# Run from repo root (resume is default — skips already-done files)
py scripts/ingest_nodal_csvs.py

# Optional filters
py scripts/ingest_nodal_csvs.py --province 云南 安徽
py scripts/ingest_nodal_csvs.py --since 2026-01
py scripts/ingest_nodal_csvs.py --no-resume   # reprocess all
```

Progress is tracked in `scripts/.ingest_nodal_done` (one `province/YYYY-MM` per line).

---

## Pending / Still To Do

### P0 — Unblocked, do first

| Task | Action |
|------|--------|
| **Run terraform apply** | `cd infra/terraform && terraform apply` — deploys the 2048 MB ECS fix |
| **Re-deploy mengxi-dashboard** | Force new ECS deployment after terraform apply |
| **Complete CSV backfill** | Run `py scripts/ingest_nodal_csvs.py` until all 306 files done |

### P1 — Needs LINGFENG_API_KEY first

| Task | Action |
|------|--------|
| **Add API key to config/.env** | `LINGFENG_API_KEY=<real_key>` |
| **Test Download → DB** | In the dashboard Section 7, download one province/month directly to RDS |
| **Fill historical gaps** | After CSV backfill, use `download_and_upsert()` to patch missing dates |
| **Schedule daily nodal price ingestion** | No cron/ECS scheduled task exists yet for `services/fengxing/nodal_price.py` |

---

## Key File Map

```
apps/mengxi-dashboard/
  app.py                        — main Streamlit app; Section 7=Download→DB, Section 8=CSV backfill
  wind_farm_tab.py              — wind farm ranking tab (fixed: SQL GROUP BY aggregation)

services/fengxing/
  nodal_price.py                — download_and_upsert(province, date, engine); init_table()

services/lingfeng/
  api_client.py                 — LingFeng ODS HTTP client
  ingest_province_clearing.py   — province-level clearing price ingestion

scripts/
  ingest_nodal_csvs.py          — one-time CSV backfill via PostgreSQL COPY
  .ingest_nodal_done            — resume log (province/YYYY-MM per line)

infra/terraform/
  main.tf                       — ECS task definitions; mengxi-dashboard at 512CPU/2048MB

data/nodal/<province>/
  <name>_YYYY-MM.csv            — historical nodal price CSVs (~306 files, OneDrive path)
```

## Key DB Tables

| Table | Schema | Description |
|-------|--------|-------------|
| `md_shanxi_nodal_price_96` | `marketdata` | 15-min nodal prices; PK `(node_name, metric_time, time_order_96)` |
| `md_id_cleared_energy` | `marketdata` | 15-min intraday cleared energy for all plants |
| `md_rt_nodal_price` | `marketdata` | Real-time nodal price (written by bess-inner-mongolia ECS, not this service) |

---

## Known Issues / Gotchas

1. **CSV backfill may hang** — files are in OneDrive path. If `reading CSV ...` hangs,
   it means Windows is downloading the file from cloud. Fix: in Windows Explorer,
   right-click `data/nodal/` → "Always keep on this device" before running the script.

2. **Database lock from interrupted run** — if the script was killed mid-transaction,
   a stale lock may block new runs. Check with:
   ```sql
   SELECT pid, state, query, wait_event_type FROM pg_stat_activity
   WHERE state != 'idle' AND query LIKE '%stg_nodal%';
   -- Kill if needed:
   SELECT pg_terminate_backend(<pid>);
   ```

3. **wind_farm_tab shows all plants** — `infer_asset_type()` classifies by
   `dispatch_unit_name` keywords. If you want wind-only filtering, the asset_type
   filter `df[df["asset_type"] == "wind"]` is in `wind_farm_tab.py:54`.

4. **Terraform state** — infra is managed in `infra/terraform/`. Always `terraform plan`
   before `terraform apply`.
