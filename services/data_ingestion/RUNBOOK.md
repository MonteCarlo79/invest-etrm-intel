# Data Ingestion Runbook

Three collectors share a single Docker image (`bess-data-ingestion`) and a set of ops control tables in the `ops` schema.

## Collectors

| Collector | Script | Target tables | ECS schedule (UTC) |
|---|---|---|---|
| `enos_market` | `enos_market_collector.py` | `marketdata.md_*` | 20:05 (04:05 SGT) |
| `tt_api` | `tt_api_collector.py` | `public.hist_*` | 01:15 (09:15 SGT) |
| `lingfeng` | `lingfeng_export_collector.py` | `public.spot_prices_hourly` | manual / on-demand |
| `freshness_monitor` | `freshness_monitor.py` | reads all, writes `ops.ingestion_gap_queue` | 03:00 (11:00 SGT) |

---

## Environment Variables

| Variable | Required by | Notes |
|---|---|---|
| `PGURL` | all | PostgreSQL DSN — preferred connection method. Used by all collectors including `column_to_matrix_all` (after fix). |
| `DB_DSN` | all | Alternative DSN; takes precedence over `PGURL` if both set |
| `APP_KEY` / `APP_SECRET` | `tt_api` | TT DAAS Poseidon SDK credentials |
| `LINGFENG_BASE_URL` | `lingfeng` | Base URL of portal; leave empty to skip |
| `LINGFENG_USERNAME` / `LINGFENG_PASSWORD` | `lingfeng` | Portal login |
| `LINGFENG_PROVINCE_LIST` | `lingfeng` | Comma-separated province names |
| `S3_BUCKET` | `lingfeng` | Raw landing bucket (optional; skip upload if unset) |
| `ECS_CLUSTER` / `PRIVATE_SUBNETS` / `TASK_SECURITY_GROUPS` | `freshness_monitor` | Only needed when `ECS_DISPATCH=true` |

### DB connection precedence (column_to_matrix_all)

`column_to_matrix_all._db_engine()` resolves its DB connection in this order:

1. `DB_DSN` env var (explicit DSN)
2. `PGURL` env var (ECS common_env — **standard ECS path**)
3. `DB_USER` / `DB_PASSWORD` / `DB_HOST` / `DB_PORT` / `DB_NAME` (local dev fallback)

A startup log line (never containing secrets) shows which mode was resolved:
```
[column_to_matrix_all] db connection mode=pgurl host=bess-platform-pg.xxxx.rds.amazonaws.com
```

In ECS, only `PGURL` needs to be set (it is injected via `common_env` in the task definition).
Local dev can use either `PGURL` or the five discrete vars.

---

## CLI Recipes

> Replace `$PGURL` with the full DSN, e.g.:
> `postgresql://postgres:pass@bess-platform-pg.xxxx.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require`

### Daily run (standard)
```bash
RUN_MODE=daily PGURL=$PGURL python services/data_ingestion/tt_api_collector.py
```

### Dry run — log only, no DB writes, no subprocess
```bash
DRY_RUN=true RUN_MODE=daily PGURL=$PGURL python services/data_ingestion/enos_market_collector.py
# Inserts a 'skipped' row in ops.ingestion_job_runs — safe to run anytime.
```

### Reconcile a date range (e.g. 2026-03-12 to today)
```bash
# EnOS market data
RUN_MODE=reconcile START_DATE=2026-03-12 END_DATE=2026-04-10 \
  PGURL=$PGURL \
  python services/data_ingestion/enos_market_collector.py

# TT API data — also requires DB_* vars for column_to_matrix_all
RUN_MODE=reconcile START_DATE=2026-03-12 END_DATE=2026-04-10 \
  PGURL=$PGURL \
  APP_KEY=$TT_APP_KEY APP_SECRET=$TT_APP_SECRET \
  DB_USER=postgres DB_PASSWORD=$DB_PASSWORD \
  DB_HOST=bess-platform-pg.xxxx.ap-southeast-1.rds.amazonaws.com DB_NAME=marketdata \
  python services/data_ingestion/tt_api_collector.py
```

### Backfill specific markets only (tt_api)
```bash
# DATASET_FILTER is comma-separated keys from MARKET_MAP in column_to_matrix_all.py
# e.g. Mengxi_SuYou, Shandong_BinZhou, Anhui_DingYuan
RUN_MODE=backfill START_DATE=2026-01-01 END_DATE=2026-03-31 \
  DATASET_FILTER=Shandong_BinZhou,Mengxi_SuYou \
  PGURL=$PGURL APP_KEY=$TT_APP_KEY APP_SECRET=$TT_APP_SECRET \
  DB_USER=postgres DB_PASSWORD=$DB_PASSWORD \
  DB_HOST=bess-platform-pg.xxxx.ap-southeast-1.rds.amazonaws.com DB_NAME=marketdata \
  python services/data_ingestion/tt_api_collector.py
```

### Run freshness monitor (gap detection only, no ECS dispatch)
```bash
PGURL=$PGURL python services/data_ingestion/freshness_monitor.py
# Prints JSON log lines for each gap found; inserts into ops.ingestion_gap_queue.
```

### Run freshness monitor + auto-dispatch remediation tasks
```bash
PGURL=$PGURL \
  ECS_DISPATCH=true \
  ECS_CLUSTER=bess-platform-cluster \
  PRIVATE_SUBNETS="subnet-aaa111,subnet-bbb222" \
  TASK_SECURITY_GROUPS="sg-xxxxxxxx" \
  ENOS_MARKET_TASK_DEF=bess-platform-enos-market-collector \
  TT_API_TASK_DEF=bess-platform-tt-api-collector \
  python services/data_ingestion/freshness_monitor.py
```

---

## Monitoring Queries

### Check recent job runs
```sql
SELECT id, collector, run_mode, start_date, end_date, status, rows_written,
       started_at, finished_at,
       EXTRACT(epoch FROM finished_at - started_at)::int AS duration_sec
FROM ops.ingestion_job_runs
ORDER BY started_at DESC
LIMIT 20;
```

### Check pending gaps
```sql
SELECT dataset, collector, gap_start, gap_end, detected_at, status
FROM ops.ingestion_gap_queue
WHERE status = 'pending'
ORDER BY detected_at;
```

### Check dataset freshness
```sql
SELECT ds.collector, ds.dataset, ds.last_success_at, ds.last_date_seen,
       ds.failure_count, ef.max_lag_days
FROM ops.ingestion_dataset_status ds
JOIN ops.ingestion_expected_freshness ef USING (dataset)
ORDER BY ds.last_success_at NULLS FIRST;
```

### Mark a gap as suppressed (skip remediation)
```sql
UPDATE ops.ingestion_gap_queue
SET status = 'suppressed', notes = 'national holiday — no data expected'
WHERE dataset = 'public.hist_mengxi_suyou_clear'
  AND gap_start = '2026-01-01';
```

---

## Deploy DDL
```bash
psql $PGURL -f db/ddl/ops/ingestion_control.sql
```

## Smoke Tests
```bash
pytest services/data_ingestion/tests/test_smoke.py -v
```

## Terraform
```bash
cp infra/terraform/data-ingestion/terraform.tfvars.example \
   infra/terraform/data-ingestion/terraform.tfvars
# edit terraform.tfvars with real values
terraform -chdir=infra/terraform/data-ingestion init
terraform -chdir=infra/terraform/data-ingestion plan
terraform -chdir=infra/terraform/data-ingestion apply
```

---

## Secrets Management (Production)

Credentials are **never** passed as plain environment variables in production. They are stored in AWS Secrets Manager and injected via the ECS task definition `secrets` array.

| Secret path | Used by |
|---|---|
| `bess/pgurl` | all |
| `bess/tt-app-key` | `tt_api` |
| `bess/tt-app-secret` | `tt_api` |
| `bess/lingfeng` (JSON: `username`, `password`) | `lingfeng` |

---

## Out of Scope / Deferred

- **Lingfeng URL and selectors**: `LINGFENG_BASE_URL` env var + Playwright selectors need to be confirmed once the portal URL is provided. The skeleton is in place.
- **ECR build pipeline**: Use the same push flow as `bess-mengxi-ingestion:v*`.
- **`column_to_matrix_all.py` cleanup**: The duplicate `MARKET_MAP` block at lines 88–161 can be removed in a follow-up PR once confirmed safe.
