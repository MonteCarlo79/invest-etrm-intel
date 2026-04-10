# Deployment Readiness — Go-Live Status

Generated: 2026-04-10  
Updated: 2026-04-10 (live execution pass + Phase 2 deployment complete)  
Method: live runs against production RDS via SSH tunnel (i-0196ded69f4366656); Docker build + ECR push; terraform apply completed

---

## 1. EnOS Collector — Simulated Run

### Command
```bash
RUN_MODE=reconcile START_DATE=2026-04-07 END_DATE=2026-04-09 \
  PGURL="postgresql://postgres:REDACTED@bess-platform-pg.xxxx.rds.amazonaws.com:5432/marketdata?sslmode=require" \
  python services/data_ingestion/enos_market_collector.py
```

### Execution trace (derived from source)

`enos_market_collector.run()` does the following in order:
1. Calls `start_run(collector="enos_market", mode="reconcile", start_date=2026-04-07, end_date=2026-04-09)` → inserts row into `ops.ingestion_job_runs`, returns run_id
2. Sets `env["RUN_MODE"]="reconcile"`, `env["START_DATE"]="2026-04-07"`, `env["END_DATE"]="2026-04-09"`
3. Launches subprocess: `python bess-marketdata-ingestion/providers/mengxi/run_pipeline.py`
4. `run_pipeline.py` calls `batch_downloader.py` (downloads Excel files for each date), then `load_excel_to_marketdata.py` (parses and upserts)
5. On subprocess success: calls `finish_run(run_id, "success")` and `update_dataset_status("enos_market", "marketdata.md_rt_nodal_price", last_date=2026-04-09)`

### Target tables written (from `batch_downloader.py:55-63`)

| Table | Source lag |
|---|---|
| `marketdata.md_rt_nodal_price` | today |
| `marketdata.md_rt_total_cleared_energy` | today |
| `marketdata.md_da_cleared_energy` | yesterday |
| `marketdata.md_da_fuel_summary` | yesterday |
| `marketdata.md_avg_bid_price` | today |
| `marketdata.md_id_cleared_energy` | today |
| `marketdata.md_id_fuel_summary` | today |
| `marketdata.md_settlement_ref_price` | today |

### Expected ops.ingestion_job_runs row

```sql
 id | collector    | run_mode   | start_date  | end_date    | dry_run | status  | rows_written | error_message | started_at           | finished_at
----+--------------+------------+-------------+-------------+---------+---------+--------------+---------------+----------------------+---------------------
  1 | enos_market  | reconcile  | 2026-04-07  | 2026-04-09  | f       | success | NULL         | NULL          | 2026-04-10 ...       | 2026-04-10 ...
```

### Expected ops.ingestion_dataset_status row

```sql
 collector    | dataset                         | last_run_at | last_success_at | last_date_seen | failure_count
--------------+---------------------------------+-------------+-----------------+----------------+---------------
 enos_market  | marketdata.md_rt_nodal_price    | now()       | now()           | 2026-04-09     | 0
```

### Known gaps in this simulation

| Gap | Severity | Notes |
|---|---|---|
| `rows_written = NULL` | Medium | Subprocess returns no row count; wrapper has no way to capture it. The field will always be NULL for enos_market. |
| Only `md_rt_nodal_price` updated in dataset_status | Medium | `enos_market_collector.py:51` calls `update_dataset_status` for one table only. All 8 target tables should be updated. |
| `min_ts`/`max_ts` not captured | Low | Freshness monitor derives max date by querying target tables directly — not a blocker. |

---

## 2. Freshness Monitor — Simulated Run

### Command
```bash
PGURL="postgresql://..." python services/data_ingestion/freshness_monitor.py
```

### Execution trace

`check_freshness()` reads all 11 rows from `ops.ingestion_expected_freshness`:

```
enos_market (7 tables, date_col=data_date, max_lag=2):
  marketdata.md_da_cleared_energy
  marketdata.md_da_fuel_summary
  marketdata.md_id_cleared_energy
  marketdata.md_id_fuel_summary
  marketdata.md_rt_nodal_price
  marketdata.md_rt_total_cleared_energy
  marketdata.md_settlement_ref_price

tt_api (4 tables, date_col=date, max_lag=3):
  public.hist_mengxi_suyou_clear
  public.hist_mengxi_wulate_clear
  public.hist_shandong_binzhou_clear
  public.hist_anhui_dingyuan_clear
```

For each dataset, runs: `SELECT MAX(date_col::date) FROM {dataset}`

**Scenario**: assume all tables last populated on 2026-04-06 (3 days ago). Today = 2026-04-10.

- enos_market lag = 4 > 2 → gap flagged: gap_start=2026-04-07, gap_end=2026-04-09
- tt_api lag = 4 > 3 → gap flagged: gap_start=2026-04-07, gap_end=2026-04-09

### Gaps inserted into ops.ingestion_gap_queue

```sql
INSERT INTO ops.ingestion_gap_queue (dataset, collector, gap_start, gap_end)
-- executed once per stale dataset; ON CONFLICT (dataset, gap_start, gap_end, status) DO NOTHING

 id | dataset                              | collector    | gap_start   | gap_end     | status
----+--------------------------------------+--------------+-------------+-------------+---------
  1 | marketdata.md_da_cleared_energy      | enos_market  | 2026-04-07  | 2026-04-09  | pending
  2 | marketdata.md_da_fuel_summary        | enos_market  | 2026-04-07  | 2026-04-09  | pending
  ...
  8 | public.hist_mengxi_suyou_clear       | tt_api       | 2026-04-07  | 2026-04-09  | pending
  ...
 11 | public.hist_anhui_dingyuan_clear     | tt_api       | 2026-04-07  | 2026-04-09  | pending
```

### Idempotency proof

Running the monitor a second time with same table max dates:

```sql
INSERT INTO ops.ingestion_gap_queue (dataset, collector, gap_start, gap_end)
VALUES ('marketdata.md_da_cleared_energy', 'enos_market', '2026-04-07', '2026-04-09')
ON CONFLICT (dataset, gap_start, gap_end, status) DO NOTHING;
-- status defaults to 'pending'
-- UNIQUE (dataset, gap_start, gap_end, status) = ('md_da_cleared_energy', '2026-04-07', '2026-04-09', 'pending')
-- → conflict → DO NOTHING → no duplicate inserted ✓
```

**Edge case**: if a gap was resolved and a NEW gap appears for the exact same (dataset, gap_start, gap_end), it CAN be re-inserted because the existing row has `status='resolved'` (different value in the unique key). This is correct behavior — a resolved gap that reappears is a new event.

---

## 3. TT API Collector — Truth Table

| Dimension | Fact |
|---|---|
| **Status** | PARTIALLY RUNNABLE — `province_misc_to_db_v2.py` step works; `column_to_matrix_all.py` step will FAIL in ECS due to DB connection mismatch (see below) |
| **province_misc_to_db_v2 import timing** | SAFE in current code. `os.environ["HIST_START_DATE"]` and `os.environ["HIST_END_DATE"]` are set at lines 49-50 of `tt_api_collector.py` BEFORE the lazy `from services.loader.province_misc_to_db_v2 import main` at line 55. The module-level `HIST_START_DATE = os.getenv("HIST_START_DATE")` (province_misc_to_db_v2.py:114) runs at import time, which is after the env vars are set. Works for single in-process call. Fragile only if the module was already imported earlier in the same process. |
| **province_misc_to_db_v2 DB connection** | Uses `DB_DSN = os.getenv("DB_DSN") or os.getenv("PGURL")` (line 94) in `_db_engine()` (line 323-325). ECS task has PGURL in common_env → **WORKS**. |
| **column_to_matrix_all DB connection** | `_db_engine()` at line 267-270 constructs URL ONLY from `DB_DEFAULTS` (DB_USER/DB_PASSWORD/DB_HOST/DB_PORT/DB_NAME). Does NOT check PGURL or DB_DSN. Default is `postgres:root@localhost:5433/marketdata`. ECS task def does NOT set these vars. → **WILL FAIL in ECS** with connection refused to localhost. |
| **MARKET_LIST value in ECS task** | Set to `"Mengxi,Anhui,Shandong,Jiangsu"`. province_misc_to_db_v2's `_selected_markets()` reads this and calls `Column_to_Matrix("", "Mengxi")` etc. These are province-alias mode calls — valid per the docstring. However the province_misc_to_db_v2 `MARKET_MAP` does not contain a "Mengxi" key; Column_to_Matrix handles unknown keys via a code path that needs verification. |
| **Env vars required** | PGURL or DB_DSN (for province_misc_to_db_v2), APP_KEY, APP_SECRET, HIST_START_DATE + HIST_END_DATE (set automatically), FULL_HISTORY=false (set automatically) |
| **Env vars MISSING from ECS task def** | DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME — all required by column_to_matrix_all._db_engine() |
| **Exact local command** | `PGURL="postgresql://..." APP_KEY="..." APP_SECRET="..." DB_USER=postgres DB_PASSWORD="..." DB_HOST="rds-host" DB_PORT=5432 DB_NAME=marketdata RUN_MODE=daily python services/data_ingestion/tt_api_collector.py` |
| **Expected target tables (province_misc_to_db_v2)** | `public.hist_*_15min`, `public.hist_*_dayahead_15min` — intra-day 15-min price tables per province/node |
| **Expected target tables (column_to_matrix_all)** | `public.hist_{market}_clear`, `public.hist_{market}_forecast`, `public.hist_{market}`, `public.hist_{market}_clear_dayahead_15min`, `public.hist_{market}_forecast_dayahead_15min`, `public.hist_{market}_clear_dayahead`, `public.hist_{market}_forecast_dayahead` — for each of 7 markets in MARKET_MAP |
| **Known failure modes** | (1) column_to_matrix_all DB connection fail in ECS — localhost default; (2) poseidon SDK `SystemExit` if APP_KEY/APP_SECRET missing; (3) province_misc_to_db_v2 may raise RuntimeError if any market fails (failures dict check at line 913); (4) MARKET_LIST="Mengxi,..." not matching MARKET_MAP keys in province_misc_to_db_v2 |

---

## 4. DDL Review — ingestion_control.sql

### ops.ingestion_job_runs

| Check | Result |
|---|---|
| Primary key | `id BIGSERIAL` ✓ |
| Unique constraint | None — multiple runs per (collector, date_range) are allowed ✓ (correct, this is an audit log) |
| Status field | `status TEXT DEFAULT 'running'` → values: running / success / failed / skipped ✓ |
| Timestamps | `started_at TIMESTAMPTZ NOT NULL DEFAULT now()`, `finished_at TIMESTAMPTZ` (nullable, set on finish) ✓ |
| Retry/attempt counter | **ABSENT** — no `attempt_number` column. If a failed run is retried, a new row is inserted with no link to the failed one. Acceptable for v1. |
| Index for monitoring | **ABSENT** — no index on `(collector, started_at DESC)`. The `ORDER BY started_at DESC LIMIT 20` monitoring query will do a full scan. Acceptable for small table; add index if table grows. |

### ops.ingestion_dataset_status

| Check | Result |
|---|---|
| Primary key | `(collector, dataset)` composite ✓ |
| Idempotency | `ON CONFLICT ... DO UPDATE SET ...` — upserts correctly ✓ |
| `failure_count` reset | `CASE WHEN :ok THEN 0 ELSE failure_count + 1 END` — resets on success ✓ |
| `last_success_at` preserved on failure | `CASE WHEN :ok THEN now() ELSE ingestion_dataset_status.last_success_at END` ✓ |
| Index | No extra index needed beyond PK. |

### ops.ingestion_expected_freshness

| Check | Result |
|---|---|
| Primary key | `dataset TEXT PRIMARY KEY` ✓ |
| Seed data | 11 rows (7 enos_market, 4 tt_api) with `ON CONFLICT DO NOTHING` ✓ |
| `active` flag | Present — monitor respects `WHERE active = TRUE` ✓ |

### ops.ingestion_gap_queue

| Check | Result |
|---|---|
| Primary key | `id BIGSERIAL` ✓ |
| Unique constraint | `UNIQUE (dataset, gap_start, gap_end, status)` — prevents duplicate pending gaps ✓ |
| Status values | `pending / dispatched / resolved / suppressed` — status field defined in comment only, no CHECK constraint |
| Idempotency on insert | `ON CONFLICT ... DO NOTHING` in `queue_gap()` ✓ |
| `dispatched_at`, `resolved_at` | Present and nullable ✓ |
| Retry/attempt counter | **ABSENT** — no `dispatch_attempts` column. If a dispatched ECS task fails silently, the gap stays in `dispatched` state with no automatic retry. Manual reset required. |
| Index on `status` | **ABSENT** — monitoring query `WHERE status='pending'` will full-scan. Add: `CREATE INDEX ON ops.ingestion_gap_queue (status, detected_at)` for production. |
| Index on `(dataset, status)` | **ABSENT** — freshness monitor's `queue_gap()` uses ON CONFLICT which is covered by the unique index, but queries filtering by dataset+status have no dedicated index. |

**Missing CHECK constraint:**
```sql
-- Recommended addition (not blocking go-live):
ALTER TABLE ops.ingestion_gap_queue
  ADD CONSTRAINT chk_status CHECK (status IN ('pending','dispatched','resolved','suppressed'));
ALTER TABLE ops.ingestion_job_runs
  ADD CONSTRAINT chk_status CHECK (status IN ('running','success','failed','skipped'));
```

---

## 5. Terraform Review — infra/terraform/data-ingestion/main.tf

### Task family → EventBridge rule mapping

| Task family | EventBridge rule | Schedule (UTC) | Schedule (SGT) |
|---|---|---|---|
| `bess-platform-enos-market-collector` | `bess-platform-enos-market-daily` | `cron(5 20 * * ? *)` | 04:05 |
| `bess-platform-tt-api-collector` | `bess-platform-tt-api-daily` | `cron(55 0 * * ? *)` | 08:55 |
| `bess-platform-freshness-monitor` | `bess-platform-freshness-monitor-daily` | `cron(0 3 * * ? *)` | 11:00 |
| `bess-platform-lingfeng-collector` | **No schedule** | — | manual only |

### Does the remediation runner have its own schedule?

Yes. `freshness_monitor` IS the remediation runner. It has its own EventBridge rule (`bess-platform-freshness-monitor-daily`) at 11:00 SGT. It runs gap detection AND dispatches ECS reconcile tasks in the same execution (when `ECS_DISPATCH=true`, which is hardcoded in the task def).

### Root-module inputs required before `terraform apply`

All variables in `variables.tf` that have NO default and must come from the root stack:

| Variable | Root source |
|---|---|
| `ecs_cluster_arn` | `aws_ecs_cluster.this.arn` |
| `ecs_cluster_name` | `aws_ecs_cluster.this.name` |
| `private_subnet_ids` | `var.private_subnet_ids` |
| `task_security_group_id` | `aws_security_group.ecs_tasks.id` |
| `ecs_execution_role_arn` | `aws_iam_role.task_execution.arn` |
| `ecs_task_role_arn` | `aws_iam_role.task_role.arn` |
| `events_invoke_ecs_role_arn` | `aws_iam_role.eventbridge_ecs.arn` |
| `container_image` | ECR push output; set placeholder first |
| `db_dsn` | Construct from `var.db_username`, `var.db_password`, `aws_db_instance.pg.address`, `var.db_name` |
| `tt_app_key` | New `var.tt_app_key` in root variables.tf |
| `tt_app_secret` | New `var.tt_app_secret` in root variables.tf |

Variables with defaults (OK to omit): `name` (bess-platform), `region` (ap-southeast-1), `lingfeng_base_url` (""), `lingfeng_username` (""), `lingfeng_password` (""), `lingfeng_province_list` (""), `s3_bucket` ("")

### Log retention explicitly set?

Yes:
- `enos_market`, `tt_api`, `lingfeng` log groups: `retention_in_days = 30` ✓
- `freshness` log group: `retention_in_days = 14` ✓

---

## 6. GO_LIVE_STATUS.md

### In scope for go-live

- `enos_market_collector` — scheduled daily at 04:05 SGT
- `freshness_monitor` — scheduled daily at 11:00 SGT (gap detection only; ECS dispatch needs IAM verification)
- ops control tables DDL (`ingestion_job_runs`, `ingestion_dataset_status`, `ingestion_expected_freshness`, `ingestion_gap_queue`)
- Terraform module deployment (ECR, log groups, task definitions, EventBridge rules)

### Out of scope for go-live

- `lingfeng_export_collector` — **BLOCKED** (LINGFENG_BASE_URL unknown, Playwright selectors unverified)
- ECS gap dispatch from freshness_monitor — enable manually after IAM verification (see P5)

### Preconditions

| # | Precondition | Owner | Status |
|---|---|---|---|
| P1 | `db/ddl/ops/ingestion_control.sql` applied to production RDS | DBA | **DONE** — 4 tables created, 11 seed rows (2026-04-10) |
| P2 | ECR image built and pushed to `bess-data-ingestion:latest` | DevOps | **DONE** — `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-data-ingestion:latest` (2026-04-10) |
| P3 | `terraform.tfvars` populated with real values | DevOps | **DONE** — cluster ARN, subnets, SG, IAM role ARNs, DB DSN, TT creds (2026-04-10) |
| P4 | `terraform apply` completed successfully | DevOps | **DONE** — 16 resources created; task defs at `:2` with real image. ECR, 4 task defs, 3 EventBridge rules, 4 log groups (2026-04-10) |
| P5 | `aws_iam_role.task_role` has `ecs:RunTask` + `iam:PassRole` on execution/task roles. Enable ECS_DISPATCH by changing task def env var to `"true"` and redeploying. | Infra | **Not verified** — ECS_DISPATCH=false in all task defs; safe until verified |
| P6 | ~~**tt_api only**: DB_USER/DB_PASSWORD/DB_HOST added to task def~~ | Dev | **RESOLVED** — `column_to_matrix_all._db_engine()` resolves `DB_DSN > PGURL > discrete vars`. Live run confirmed: `db connection mode=pgurl host=127.0.0.1` |
| P7 | **tt_api only**: Verify `MARKET_LIST=Mengxi,Anhui,Shandong,Jiangsu` resolves correctly in `province_misc_to_db_v2._selected_markets()` | Dev | **PARTIALLY VERIFIED** — live run with `DATASET_FILTER=Shandong_BinZhou` succeeded; `hist_shandong_binzhou_*` updated. Full 4-market ECS run pending. |

### Validation passed ✓

| Check | Evidence |
|---|---|
| DDL idempotent (IF NOT EXISTS, ON CONFLICT DO NOTHING) | Code review: `ingestion_control.sql` ✓ |
| gap_queue idempotency on duplicate insert | `ON CONFLICT (dataset, gap_start, gap_end, status) DO NOTHING` in `control.py:62` ✓ |
| Secrets not logged | `_SecretFilter` in `shared/logging.py`; all log calls use `json.dumps({})` ✓ |
| Terraform uses existing IAM roles (no new roles created) | `var.ecs_execution_role_arn`, `var.ecs_task_role_arn`, `var.events_invoke_ecs_role_arn` passed in ✓ |
| Terraform does not touch `trading-bess-mengxi/` or `mengxi-ingestion/` | Code review ✓ |
| enos_market_collector dry-run path safe | `finish_run(run_id, "skipped")` before any subprocess call ✓ |
| freshness_monitor gap detection logic correct | `gap_start = MAX(date)+1`, `gap_end = today-1`; handles NULL (empty table) with warning ✓ |
| EventBridge rules use existing `events_invoke_ecs_role_arn` | matches `trading-bess-mengxi` convention ✓ |
| `assign_public_ip = false` on all tasks | main.tf lines 299, 313, 327 ✓ |
| Log groups named `/ecs/${var.name}/...` | matches `trading-bess-mengxi` convention ✓ |

### Validation passed after live execution ✓ (updated 2026-04-10)

| Check | Evidence |
|---|---|
| `column_to_matrix_all._db_engine()` reads PGURL | Live log: `[column_to_matrix_all] db connection mode=pgurl host=127.0.0.1` ✓ |
| `enos_market_collector` updates all 8 target tables in dataset_status | `enos_market_collector.py` iterates `ENOS_TARGET_TABLES` (8 tables) in both success and failure paths ✓ |
| Real collector run executed | EnOS reconcile 2026-04-09: 267,493 rows across 8 tables. TT API (Shandong_BinZhou): `hist_shandong_binzhou_*` updated. `ops.ingestion_job_runs` status='success' for both ✓ |
| ops table rows written (enos_market) | `rows_written=267493` in `ops.ingestion_job_runs` (SUM across 8 tables for the date) ✓ |
| Freshness monitor gap detection | 4 TT gaps detected and queued in `ops.ingestion_gap_queue`. Idempotency proven (second run produced 0 new rows) ✓ |

### Validation still pending ✗

| Check | Finding | Fix required |
|---|---|---|
| `tt_api_collector` rows_written is 1 per market (not actual row count) | `rows_written=1` in ops reflects 1 matrix execution per market, not 15-min rows. Misleading but not a blocker for monitoring. | Document or post-go-live fix. |
| No index on `ops.ingestion_gap_queue(status)` | Low — full scan on status='pending'. Acceptable until table exceeds ~10k rows. | `CREATE INDEX CONCURRENTLY ON ops.ingestion_gap_queue(status, detected_at)` post go-live. |
| `task_role_arn` ECS RunTask permissions unverified | freshness_monitor `dispatch_gaps()` has no try/except — will fail the task if `ecs:RunTask` / `iam:PassRole` missing. ECS_DISPATCH=false by default, so safe for now. | Verify IAM before enabling ECS_DISPATCH (see P5). |

### Rollback steps

1. **Disable EventBridge rules** (stop all automated triggers, no data destroyed):
   ```bash
   aws events disable-rule --name bess-platform-enos-market-daily --region ap-southeast-1
   aws events disable-rule --name bess-platform-tt-api-daily --region ap-southeast-1
   aws events disable-rule --name bess-platform-freshness-monitor-daily --region ap-southeast-1
   ```

2. **Destroy Terraform module** (removes ECS task defs, rules, log groups; does NOT touch existing pipelines):
   ```bash
   terraform -chdir=infra/terraform/data-ingestion destroy
   ```

3. **Remove ops control tables** (only if DDL rollback needed; does not affect any marketdata/public tables):
   ```sql
   DROP TABLE IF EXISTS ops.ingestion_gap_queue;
   DROP TABLE IF EXISTS ops.ingestion_expected_freshness;
   DROP TABLE IF EXISTS ops.ingestion_dataset_status;
   DROP TABLE IF EXISTS ops.ingestion_job_runs;
   ```

4. **Delete ECR images** (optional — lifecycle policy retains last 10 regardless):
   ```bash
   aws ecr delete-repository --repository-name bess-data-ingestion --force --region ap-southeast-1
   ```

### Recommended rollout order

| Phase | Action | Gate | Status |
|---|---|---|---|
| 1 | Apply DDL: `psql $PGURL -f db/ddl/ops/ingestion_control.sql` | 4 tables created, 11 seed rows present | **DONE** 2026-04-10 |
| 2 | Build + push Docker image; `terraform apply` (standalone module) | 16 resources created, `terraform plan` 0 changes to existing | **DONE** 2026-04-10 — task defs `:2`, ECR `bess-data-ingestion:latest` |
| 3 | Dry-run enos_market: `DRY_RUN=true RUN_MODE=daily PGURL=... python services/data_ingestion/enos_market_collector.py` | ops.ingestion_job_runs has 1 row status='skipped' | **DONE** 2026-04-10 |
| 4 | Live reconcile enos_market 1 day: `RUN_MODE=reconcile START_DATE=2026-04-09 END_DATE=2026-04-09 PGURL=...` | ops.ingestion_job_runs status='success'; rows_written > 0; all 8 tables dataset_status | **DONE** 2026-04-10 — 267,493 rows, 8 tables updated |
| 5 | Enable enos_market EventBridge rule; monitor 2 days | Daily rows in ops.ingestion_job_runs | **NEXT ACTION** |
| 6 | Live run tt_api locally with PGURL only: confirm startup log shows `mode=pgurl` | ops.ingestion_job_runs status='success' for tt_api | **DONE** 2026-04-10 — `mode=pgurl` confirmed; `hist_shandong_binzhou_*` updated |
| 7 | Enable tt_api EventBridge rule | Monitor 2 days; verify public.hist_* tables updated | Pending phase 5 |
| 8 | Enable freshness_monitor EventBridge rule (ECS_DISPATCH=false by default) | Gaps appear in ops.ingestion_gap_queue; no ECS tasks launched | Pending phase 7 |
| 9 | Verify IAM: `aws iam simulate-principal-policy` for ecs:RunTask + iam:PassRole on task_role. Then set ECS_DISPATCH=true in task def and redeploy. | Structured dispatch_error logs absent; gap_queue rows move to status='dispatched' | Pending phase 8 |
| 10 | Lingfeng — defer until LINGFENG_BASE_URL confirmed | Out of scope | Deferred |
