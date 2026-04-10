# Data Ingestion — Implementation Verification Report

Generated: 2026-04-10

---

## A) Collector Status

| Collector | Status | Notes |
|---|---|---|
| `enos_market` | **Runnable** | Delegates to `bess-marketdata-ingestion/providers/mengxi/run_pipeline.py` via subprocess. Full RunContext, control table updates, structured logging. |
| `tt_api` | **Runnable** | Calls `province_misc_to_db_v2.main()` then `column_to_matrix_all.Column_to_Matrix()`. Lazy import ensures env vars captured correctly. `column_to_matrix_all._db_engine()` now resolves DB_DSN > PGURL > discrete vars — PGURL from ECS common_env is sufficient. |
| `lingfeng` | **Skeleton only — NOT runnable** | `LINGFENG_BASE_URL` not yet known. Will fail-fast with `SystemExit("LINGFENG_BASE_URL is required")` until configured. Playwright selectors (`[name=username]`, `[data-action=export]`) are placeholders. |
| `freshness_monitor` | **Runnable** | Gap detection fully implemented. ECS dispatch (`ECS_DISPATCH=true`) works once task def names are live in ECS. |

---

## B) Which Parts Are Fully Implemented

- `shared/context.py` — RunContext, env + argparse resolution, daily/reconcile/backfill modes
- `shared/db.py` — `get_engine()`, `upsert_staging()`, `delete_append()`
- `shared/logging.py` — structured JSON logger with secret masking; all log calls use `json.dumps()`
- `shared/control.py` — `start_run()`, `finish_run()`, `update_dataset_status()`, `queue_gap()` (idempotent via ON CONFLICT DO NOTHING)
- `shared/s3.py` — `upload_to_landing()`, `file_hash()` (skips upload if `S3_BUCKET` unset)
- `db/ddl/ops/ingestion_control.sql` — 4 ops tables, 11 freshness seed rows
- `infra/terraform/data-ingestion/` — full standalone module (ECR, 4 task defs, 3 EventBridge rules, 4 log groups)
- `enos_market_collector.py` — fully runnable
- `tt_api_collector.py` — fully runnable
- `freshness_monitor.py` — fully runnable

## C) What Is a Stub / Skeleton

- `lingfeng_export_collector.py` — skeleton; direct HTTP probe URLs are guesses, Playwright selectors are placeholders; activate once `LINGFENG_BASE_URL` is confirmed
- `column_to_matrix_all.py` — copied verbatim from local; uses `DB_USER`/`DB_PASSWORD`/`DB_HOST`/`DB_PORT`/`DB_NAME` env vars (NOT PGURL) for its own direct psycopg2 connection

---

## D) Environment Variables

### All collectors
| Variable | Required | Notes |
|---|---|---|
| `PGURL` | Yes | PostgreSQL DSN: `postgresql://user:pass@host:5432/db?sslmode=require` |
| `DB_DSN` | Alternative to PGURL | Both accepted; PGURL takes precedence in shared/db.py |
| `RUN_MODE` | No | `daily` (default) \| `reconcile` \| `backfill` |
| `START_DATE` | For reconcile/backfill | ISO date, e.g. `2026-03-10` |
| `END_DATE` | For reconcile/backfill | ISO date, defaults to yesterday |
| `DRY_RUN` | No | `true` → log only, no DB writes, no subprocess launch |
| `LOG_LEVEL` | No | `INFO` (default) \| `DEBUG` \| `WARNING` |

### `enos_market_collector.py` only
| Variable | Notes |
|---|---|
| `DB_SCHEMA` | Set to `marketdata` (ECS task def hardcodes this) |

### `tt_api_collector.py` only
| Variable | Required | Notes |
|---|---|---|
| `APP_KEY` | Yes | TT DAAS Poseidon SDK credential |
| `APP_SECRET` | Yes | TT DAAS Poseidon SDK credential |
| `MARKET_LIST` | No | Comma-separated markets (ECS task def: `Mengxi,Anhui,Shandong,Jiangsu`) |
| `DB_LOOKBACK_DAYS` | No | Default 2; overrides lookback for daily mode |
| `RUN_INHOUSE_WIND` | No | `true` to include in-house wind data |
| `HIST_START_DATE` | Set automatically | Do NOT set manually; collector sets from RunContext |
| `HIST_END_DATE` | Set automatically | Same as above |
| `FULL_HISTORY` | Set automatically | Always `false` for daily/reconcile; only `true` for one-off backfills |

### `column_to_matrix_all.py` (called by tt_api_collector)

`_db_engine()` resolves connection in this order:

| Priority | Source | Notes |
|---|---|---|
| 1 | `DB_DSN` | Full DSN string |
| 2 | `PGURL` | **Standard ECS path** — already in `common_env` |
| 3 | `DB_USER` / `DB_PASSWORD` / `DB_HOST` / `DB_PORT` / `DB_NAME` | Local dev fallback only |

In ECS: only `PGURL` is needed (set via `common_env`). No `DB_*` vars required.

### `lingfeng_export_collector.py` only
| Variable | Required | Notes |
|---|---|---|
| `LINGFENG_BASE_URL` | Yes (when active) | Fail-fast if unset |
| `LINGFENG_USERNAME` | Yes (when active) | Portal login |
| `LINGFENG_PASSWORD` | Yes (when active) | Portal login |
| `LINGFENG_PROVINCE_LIST` | Yes (when active) | Comma-separated, e.g. `山东,安徽` |
| `S3_BUCKET` | No | Raw landing bucket; skip upload if unset |

### `freshness_monitor.py` only
| Variable | Required | Notes |
|---|---|---|
| `ECS_DISPATCH` | No | `true` to auto-dispatch reconcile tasks for gaps found |
| `ECS_CLUSTER` | If ECS_DISPATCH=true | e.g. `bess-platform-cluster` |
| `PRIVATE_SUBNETS` | If ECS_DISPATCH=true | Comma-separated subnet IDs |
| `TASK_SECURITY_GROUPS` | If ECS_DISPATCH=true | Comma-separated SG IDs |
| `ENOS_MARKET_TASK_DEF` | If ECS_DISPATCH=true | e.g. `bess-platform-enos-market-collector` |
| `TT_API_TASK_DEF` | If ECS_DISPATCH=true | e.g. `bess-platform-tt-api-collector` |
| `LINGFENG_TASK_DEF` | If ECS_DISPATCH=true | e.g. `bess-platform-lingfeng-collector` |

---

## E) Local Run Commands

### Prerequisites
```bash
cd /path/to/bess-platform
pip install -r services/data_ingestion/requirements.txt
# For playwright (lingfeng only, when ready):
playwright install chromium

# Apply DDL (once):
psql $PGURL -f db/ddl/ops/ingestion_control.sql
```

### EnOS market collector
```bash
# Daily (yesterday's data):
PGURL="postgresql://..." RUN_MODE=daily \
  python services/data_ingestion/enos_market_collector.py

# Dry run (no subprocess, just control table record):
DRY_RUN=true RUN_MODE=daily PGURL="postgresql://..." \
  python services/data_ingestion/enos_market_collector.py

# Reconcile a date range:
RUN_MODE=reconcile START_DATE=2026-03-12 END_DATE=2026-04-09 \
  PGURL="postgresql://..." \
  python services/data_ingestion/enos_market_collector.py
```

### TT API collector
```bash
# Daily:
PGURL="postgresql://..." \
  APP_KEY="..." APP_SECRET="..." \
  DB_USER=postgres DB_PASSWORD="..." DB_HOST="rds-host.rds.amazonaws.com" DB_NAME=marketdata \
  RUN_MODE=daily \
  python services/data_ingestion/tt_api_collector.py

# Backfill specific markets only:
RUN_MODE=backfill START_DATE=2026-01-01 END_DATE=2026-03-31 \
  DATASET_FILTER="Shandong_BinZhou,Mengxi_SuYou" \
  PGURL="postgresql://..." APP_KEY="..." APP_SECRET="..." \
  DB_USER=postgres DB_PASSWORD="..." DB_HOST="rds-host" DB_NAME=marketdata \
  python services/data_ingestion/tt_api_collector.py
```

### Freshness monitor (gap detection only)
```bash
PGURL="postgresql://..." python services/data_ingestion/freshness_monitor.py
```

### Freshness monitor + ECS dispatch
```bash
PGURL="postgresql://..." \
  ECS_DISPATCH=true ECS_CLUSTER=bess-platform-cluster \
  PRIVATE_SUBNETS="subnet-aaa111,subnet-bbb222" \
  TASK_SECURITY_GROUPS="sg-xxxxxxxx" \
  ENOS_MARKET_TASK_DEF="bess-platform-enos-market-collector" \
  TT_API_TASK_DEF="bess-platform-tt-api-collector" \
  python services/data_ingestion/freshness_monitor.py
```

### Smoke tests
```bash
pytest services/data_ingestion/tests/test_smoke.py -v
```

---

## F) Terraform Apply Steps

### First deploy (standalone — no root stack changes)

```bash
# 1. Copy and fill in real values
cp infra/terraform/data-ingestion/terraform.tfvars.example \
   infra/terraform/data-ingestion/terraform.tfvars
# Edit with: ecs_cluster_arn, private_subnet_ids, task_security_group_id,
#            ecs_execution_role_arn, ecs_task_role_arn, events_invoke_ecs_role_arn,
#            container_image (use placeholder until first build), db_dsn, tt_app_key, tt_app_secret

# 2. Init and plan
terraform -chdir=infra/terraform/data-ingestion init
terraform -chdir=infra/terraform/data-ingestion plan

# 3. Apply (creates ECR repo, log groups, task defs, EventBridge rules)
terraform -chdir=infra/terraform/data-ingestion apply

# 4. Build and push the Docker image
ECR_URL=$(terraform -chdir=infra/terraform/data-ingestion output -raw ecr_repository_url)
aws ecr get-login-password --region ap-southeast-1 | \
  docker login --username AWS --password-stdin $ECR_URL
docker build -t bess-data-ingestion -f services/data_ingestion/Dockerfile .
docker tag bess-data-ingestion:latest $ECR_URL:latest
docker push $ECR_URL:latest

# 5. Update terraform.tfvars: set container_image = "$ECR_URL:latest"
# 6. Re-apply to update ECS task definitions with the real image URI
terraform -chdir=infra/terraform/data-ingestion apply
```

### Wire into root stack (when ready)

Add to `infra/terraform/main.tf`:
```hcl
module "data_ingestion" {
  source                     = "./data-ingestion"
  name                       = var.name
  region                     = var.region
  ecs_cluster_arn            = aws_ecs_cluster.this.arn
  ecs_cluster_name           = aws_ecs_cluster.this.name
  private_subnet_ids         = var.private_subnet_ids
  task_security_group_id     = aws_security_group.ecs_tasks.id
  ecs_execution_role_arn     = aws_iam_role.task_execution.arn
  ecs_task_role_arn          = aws_iam_role.task_role.arn
  events_invoke_ecs_role_arn = aws_iam_role.eventbridge_ecs.arn
  container_image            = var.image_data_ingestion
  db_dsn                     = "postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.pg.address}:5432/${var.db_name}?sslmode=require"
  tt_app_key                 = var.tt_app_key
  tt_app_secret              = var.tt_app_secret
  s3_bucket                  = aws_s3_bucket.uploads.bucket
}
```

Add to `infra/terraform/variables.tf`:
```hcl
variable "image_data_ingestion" { type = string }
variable "tt_app_key"           { type = string; sensitive = true }
variable "tt_app_secret"        { type = string; sensitive = true }
```

---

## G) Rollback Steps

### Remove EventBridge schedules (stop triggering tasks)
```bash
# Quickest way to stop automated runs without destroying state:
terraform -chdir=infra/terraform/data-ingestion apply \
  -var='enos_market_schedule_enabled=false' ...
# OR manually disable in AWS Console: EventBridge → Rules → Disable
```

### Full rollback (destroy module)
```bash
terraform -chdir=infra/terraform/data-ingestion destroy
# Note: ECR images are NOT deleted by terraform destroy (lifecycle policy keeps last 10).
# Delete manually if needed: aws ecr delete-repository --repository-name bess-data-ingestion --force
```

### Rollback ops DDL
```sql
-- Only run if you want to remove the ops control tables entirely.
-- These tables do NOT touch any existing pipeline tables.
DROP TABLE IF EXISTS ops.ingestion_gap_queue;
DROP TABLE IF EXISTS ops.ingestion_expected_freshness;
DROP TABLE IF EXISTS ops.ingestion_dataset_status;
DROP TABLE IF EXISTS ops.ingestion_job_runs;
```

### Remove just the data-ingestion service (keep existing pipelines)
The data-ingestion module is fully standalone. Destroying it does NOT affect:
- `trading-bess-mengxi/schedules.tf` and its ECS tasks
- `mengxi-ingestion/` module
- `bess-marketdata-ingestion/` pipeline
- Any existing `public.hist_*` or `marketdata.md_*` tables

---

## H) Known Gaps / Deferred Work

| Item | Status | Action needed |
|---|---|---|
| `column_to_matrix_all.py` uses `DB_USER/DB_PASSWORD/DB_HOST` (not PGURL) | Known | ECS task def for tt_api must set both PGURL and DB_* vars, or add a PGURL-parsing wrapper |
| Lingfeng `LINGFENG_BASE_URL` unknown | Deferred | Set env var + confirm Playwright selectors once portal URL provided |
| ECR image build pipeline | Deferred | Use same push flow as `bess-mengxi-ingestion:v*` |
| `column_to_matrix_all.py` duplicate MARKET_MAP block (lines 88–161) | Deferred | Clean up in follow-up PR once confirmed safe |
