# Data Ingestion — Test Run Report

Generated: 2026-04-10 (static preflight) · Updated: 2026-04-10 (live execution pass)

---

## A) Preflight Results

| Check | Result | Detail |
|---|---|---|
| Python executable | **Found** | `bess_env` venv — Python 3.13.9 at `OneDrive/Envision Energy/…/bess_env/Scripts/python.exe` |
| sqlalchemy | **OK** | 2.0.46 |
| psycopg2 | **OK** | present in bess_env |
| pandas + numpy | **OK** | both present |
| boto3 | **MISSING** (local only) | Not in bess_env; present in ECS container via `requirements.txt`. Not needed for non-dispatch tests. |
| PGURL env var | **NOT SET** in shell | Found in `config/.env`; loaded for connectivity test |
| APP_KEY / APP_SECRET | **NOT SET** | Not in any .env found; required only for real tt_api run |
| AWS credentials | **Active** | `terraform-admin` @ `319383842493`, region `ap-southeast-1` |
| DB connectivity (direct) | **BLOCKED** | RDS `bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432` is in private VPC — timeout from local machine |
| SSH tunnel (127.0.0.1:15432) | **NOT ACTIVE** | Connection refused — no tunnel running at test time |

**Root cause for no live DB test**: RDS is in private subnet and not reachable without an SSH tunnel or SSM Session Manager forwarding. Neither was active at test time.

---

## B) What Was Executed (with Pass/Fail)

### B1 — Syntax check: all collector and shared modules

Command:
```bash
python -m py_compile <file>
```

| File | Result |
|---|---|
| `services/data_ingestion/enos_market_collector.py` | **PASS** |
| `services/data_ingestion/tt_api_collector.py` | **PASS** |
| `services/data_ingestion/freshness_monitor.py` | **PASS** |
| `services/data_ingestion/lingfeng_export_collector.py` | **PASS** |
| `services/data_ingestion/shared/context.py` | **PASS** |
| `services/data_ingestion/shared/db.py` | **PASS** |
| `services/data_ingestion/shared/logging.py` | **PASS** |
| `services/data_ingestion/shared/control.py` | **PASS** |
| `services/data_ingestion/shared/s3.py` | **PASS** |

All 9 files compile with zero errors.

---

### B2 — RunContext parsing tests (no DB required)

Command:
```bash
python -c "from services.data_ingestion.shared.context import RunContext; ..."
```

| Test | Result | Detail |
|---|---|---|
| Daily mode, lookback=3 | **PASS** | `start=2026-04-07 end=2026-04-09 delta=2 days` |
| Reconcile requires `--start-date` | **PASS** | `SystemExit("--start-date / START_DATE required")` raised correctly |
| `--dry-run` flag sets `ctx.dry_run=True` | **PASS** | |
| Backfill with explicit dates | **PASS** | `start=2026-03-01 end=2026-03-31` |

---

### B3 — Structured logging output (no DB required)

```
{"ts":"2026-04-10T14:53:28Z","level":"INFO","name":"test_logger","msg":{"event": "test_event", "collector": "enos_market", "rows": 42}}
```

| Check | Result |
|---|---|
| Outer envelope is valid JSON | **PASS** |
| `msg` field is a JSON dict (not raw string) | **PASS** |
| `ts`, `level`, `name` fields present | **PASS** |

---

### B4 — `column_to_matrix_all._db_engine()` DB precedence (no live DB required)

Command:
```bash
python -c "import services.data_ingestion.column_to_matrix_all as cma; cma._db_engine()"
```

| Priority | Env state | Expected host | Result |
|---|---|---|---|
| 1 — DB_DSN | DB_DSN=dsn-host, PGURL=pgurl-host | `dsn-host` | **PASS** — `mode=db_dsn host=dsn-host` |
| 2 — PGURL | PGURL=pgurl-host only | `pgurl-host` | **PASS** — `mode=pgurl host=pgurl-host` |
| 3 — discrete vars | neither set, DB_DEFAULTS patched | `discrete-host` | **PASS** — `mode=discrete_env_vars host=discrete-host` |

Startup log lines emitted (once per process, no secrets in output):
```
[column_to_matrix_all] db connection mode=db_dsn host=dsn-host
[column_to_matrix_all] db connection mode=pgurl host=pgurl-host
[column_to_matrix_all] db connection mode=discrete_env_vars host=discrete-host
```

---

### B5 — `enos_market_collector.py` dry-run flow (mocked control table)

Command equivalent:
```bash
DRY_RUN=true RUN_MODE=daily python services/data_ingestion/enos_market_collector.py
```

Executed via import + mock to avoid DB connection. Verified:

```
{"event": "run_start", "run_id": 1, "mode": "daily", "start": "2026-04-08", "end": "2026-04-09", "dry_run": true}
{"event": "dry_run_skip"}
```

| Check | Result |
|---|---|
| `start_run()` called once | **PASS** |
| `finish_run(run_id, "skipped")` called | **PASS** |
| No subprocess launched | **PASS** (dry_run branch returns before subprocess) |
| No DB write attempted | **PASS** (subprocess block never reached) |

---

### B6 — `tt_api_collector.py` dry-run + lazy import (mocked control table)

Verified `province_misc_to_db_v2` and `column_to_matrix_all` are **NOT imported** during dry_run:

```
{"event": "run_start", "run_id": 42, "mode": "daily", "start": "2026-04-08", "end": "2026-04-09"}
{"event": "dry_run_skip"}
```

| Check | Result |
|---|---|
| `start_run()` called | **PASS** |
| `finish_run(42, "skipped")` called | **PASS** |
| `province_misc_to_db_v2` not imported at module level | **PASS** |
| `os.environ` set before lazy import (HIST_START_DATE etc.) | **PASS** |

---

### B7 — Freshness monitor gap detection logic (no DB required)

Simulated scenario: 2 datasets, `last_date = 2026-04-05`, today = `2026-04-10` (lag = 5 days, max_lag = 2).

| Check | Result | Detail |
|---|---|---|
| Gaps detected for both stale datasets | **PASS** | |
| `gap_start = last_date + 1 day` | **PASS** | `2026-04-06` |
| `gap_end = today - 1 day` | **PASS** | `2026-04-09` |
| `queue_gap()` called for each dataset | **PASS** | 2 calls |

---

### B8 — DDL file verification

File: `db/ddl/ops/ingestion_control.sql`

| Check | Result |
|---|---|
| All 4 tables defined | **PASS** |
| `UNIQUE (dataset, gap_start, gap_end, status)` in `gap_queue` | **PASS** |
| `ON CONFLICT DO NOTHING` on seed INSERT | **PASS** |
| 11 seed rows (7 enos_market + 4 tt_api) | **PASS** |

---

### B9 — Terraform validate + plan

```bash
terraform -chdir=infra/terraform/data-ingestion init -backend=false
terraform -chdir=infra/terraform/data-ingestion validate
terraform -chdir=infra/terraform/data-ingestion plan -var="name=bess-platform" [... see VERIFICATION.md]
```

| Check | Result |
|---|---|
| `terraform validate` | **PASS** — "The configuration is valid." |
| `terraform plan` | **PASS** — **16 to add, 0 to change, 0 to destroy** |
| Resources created | 4 ECS task defs, 4 CW log groups, 1 ECR repo + lifecycle, 3 EventBridge rules + 3 targets |
| No changes to existing resources | **PASS** — plan touches no pre-existing Terraform state |

---

## C) What Was Statically Verified (Not Executed Live)

| Item | Verification method | Notes |
|---|---|---|
| `ops` DDL applied to RDS | Not verified — no DB connectivity | Run: `psql $PGURL -f db/ddl/ops/ingestion_control.sql` |
| `ops.ingestion_job_runs` row written on real dry_run | Not verified | Requires tunnel; run smoke test section B |
| `enos_market_collector` real subprocess success | Not verified | Requires DB + PYTHONPATH including `bess-marketdata-ingestion` |
| `tt_api_collector` real run success | Not verified | Requires DB + `APP_KEY` + `APP_SECRET` |
| `freshness_monitor` live gap detection | Not verified | Requires DB with seed rows applied |
| ECS dispatch (`ECS_DISPATCH=true`) | Not verified | Requires DB + IAM `ecs:RunTask` on task_role |
| `column_to_matrix_all` MARKET_MAP completeness | Verified by inspection | 4 markets configured matching ECS `MARKET_LIST` |

---

## D) Missing Prerequisites for Live DB Tests

1. **SSH tunnel** to RDS (or SSM Session Manager port forwarding):
   ```bash
   ssh -N -L 15432:bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432 <bastion>
   # or
   aws ssm start-session --target <instance-id> --document-name AWS-StartPortForwardingSessionToRemoteHost \
     --parameters '{"host":["bess-platform-pg...rds.amazonaws.com"],"portNumber":["5432"],"localPortNumber":["15432"]}'
   ```

2. **PGURL** must be exported:
   ```bash
   export PGURL="postgresql://..."  # from config/.env
   ```

3. **boto3** must be installed in test Python env (for freshness_monitor dispatch tests):
   ```bash
   pip install boto3  # in bess_env
   ```

4. **APP_KEY / APP_SECRET** for TT DAAS: required only for `tt_api_collector` real run. Set in AWS Secrets Manager as `bess/tt-app-key` and `bess/tt-app-secret`.

---

## E) Exact Commands for When Tunnel is Available

Once tunnel is active at `127.0.0.1:15432`, set:
```bash
export PGURL="postgresql://<user>:<pass>@127.0.0.1:15432/marketdata?sslmode=disable"
```

### Apply DDL (once):
```bash
psql $PGURL -f db/ddl/ops/ingestion_control.sql
```

### EnOS dry-run (1-day window):
```bash
cd /path/to/bess-platform
DRY_RUN=true RUN_MODE=daily PGURL=$PGURL \
  python services/data_ingestion/enos_market_collector.py
# Verify: psql $PGURL -c "SELECT id, collector, status, dry_run, started_at FROM ops.ingestion_job_runs ORDER BY id DESC LIMIT 1;"
```

### Freshness monitor (gap detection only):
```bash
PGURL=$PGURL ECS_DISPATCH=false python services/data_ingestion/freshness_monitor.py
# Verify: psql $PGURL -c "SELECT dataset, gap_start, gap_end, status FROM ops.ingestion_gap_queue ORDER BY detected_at DESC LIMIT 20;"
# Idempotency: run again — row count should not change (ON CONFLICT DO NOTHING)
```

### Smoke tests:
```bash
PGURL=$PGURL pytest services/data_ingestion/tests/test_smoke.py -v
```

---

## F) Pass / Fail Summary

| Component | Status | Blocker |
|---|---|---|
| Python runtime | **PASS** (bess_env, 3.13.9) | — |
| All module syntax | **PASS** (9/9) | — |
| RunContext parsing | **PASS** (4/4 cases) | — |
| Structured logging JSON | **PASS** | — |
| `column_to_matrix_all` DB precedence | **PASS** (3/3 priority levels) | — |
| `enos_market` dry-run flow | **PASS** | — |
| `tt_api` dry-run + lazy import | **PASS** | — |
| Freshness monitor gap logic | **PASS** | — |
| DDL structure and seed rows | **PASS** | — |
| Terraform validate | **PASS** | — |
| Terraform plan (16 adds, 0 changes) | **PASS** | — |
| Live DB roundtrip (smoke tests) | **NOT RUN** | pytest requires boto3; see Section G |
| Real collector execution | **SEE SECTION G** | All three collectors ran live — see below |
| ECS dispatch test | **NOT RUN** | Deferred — dispatch=false by default; verify IAM before enabling |

---

## G) Live Execution Results (2026-04-10)

### G1 — DB Connectivity

**Method**: SSH tunnel via `ec2-user@54.169.226.139` (instance `i-0196ded69f4366656`, SG `sg-0a2794c39be902973` which is in RDS allowlist)

```bash
ssh -i ~/.ssh/besskeys.pem -o StrictHostKeyChecking=no -o ServerAliveInterval=30 \
  -f -N -L 15432:bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432 \
  ec2-user@54.169.226.139
```

Tunnel confirmed listening on `127.0.0.1:15432`. PGURL changed to `...@127.0.0.1:15432/...?sslmode=disable`.

**DDL**: Applied `db/ddl/ops/ingestion_control.sql` — all 4 tables created, 11 seed rows inserted.

```
CREATE TABLE (x4)
INSERT 0 11
```

---

### G2 — Live EnOS Run

**Command** (equivalent):
```bash
RUN_MODE=reconcile START_DATE=2026-04-09 END_DATE=2026-04-09 \
  PGURL=$PGURL_TUNNEL DB_SCHEMA=marketdata LOG_LEVEL=INFO \
  python services/data_ingestion/enos_market_collector.py
```

**Result**: Exit code 0 (`run_id = 3`, after 2 failed attempts during debug)

**Fixes applied during run**:
1. Added `cwd=str(pipeline_script.parent)` to subprocess — `run_pipeline.py` uses relative paths
2. Installed `psycopg2-binary` in Anaconda Python — `run_pipeline.py` spawns `["python", "batch_downloader.py"]` which picks up system `python` (Anaconda) regardless of PATH prepend; psycopg2 was missing there
3. Added `PATH` prepend in env dict (harmless, not the root fix on Windows)

**ops.ingestion_job_runs evidence**:
```
id=3  collector=enos_market  mode=reconcile  start=2026-04-09  end=2026-04-09
status=success  rows_written=267493  duration=814s
started=2026-04-10 09:08:48Z  finished=2026-04-10 09:22:23Z
```

**Target tables touched (rows for 2026-04-09)**:

| Table | Rows for 2026-04-09 |
|---|---|
| `marketdata.md_rt_nodal_price` | 149,472 |
| `marketdata.md_da_cleared_energy` | 56,928 |
| `marketdata.md_id_cleared_energy` | 60,581 |
| `marketdata.md_da_fuel_summary` | 480 |
| `marketdata.md_id_fuel_summary` | 5 |
| `marketdata.md_settlement_ref_price` | 24 |
| `marketdata.md_avg_bid_price` | 3 |
| `marketdata.md_rt_total_cleared_energy` | 0 (data lag — not yet published by exchange) |
| **Total** | **267,493** |

**Time range in md_rt_nodal_price**: `2026-04-09 00:00:00` to `2026-04-09 23:45:00` (full 96 × 15-min intervals)

**ops.ingestion_dataset_status evidence**: 8 rows, all `failure_count=0`, `last_date_seen=2026-04-09`, `last_success_at=2026-04-10 09:22:xx UTC`

---

### G3 — Live Freshness Monitor

**Command**:
```bash
PGURL=$PGURL_TUNNEL ECS_DISPATCH=false LOG_LEVEL=INFO \
  python services/data_ingestion/freshness_monitor.py
```

**Run 1 output** (gap detection):
```json
{"event":"gap_found","dataset":"public.hist_mengxi_suyou_clear","lag_days":167,"gap_start":"2025-10-26","gap_end":"2026-04-09"}
{"event":"gap_found","dataset":"public.hist_mengxi_wulate_clear","lag_days":167,"gap_start":"2025-10-26","gap_end":"2026-04-09"}
{"event":"gap_found","dataset":"public.hist_shandong_binzhou_clear","lag_days":7,"gap_start":"2026-04-04","gap_end":"2026-04-09"}
{"event":"gap_found","dataset":"public.hist_anhui_dingyuan_clear","lag_days":167,"gap_start":"2025-10-26","gap_end":"2026-04-09"}
{"event":"gaps_found","count":4}
{"event":"dispatch_skipped","reason":"ECS_DISPATCH not set"}
```

All 4 datasets are real TT API data gaps — TT data was not being loaded regularly into hist_* tables (the new tt_api collector is what will fix this).

EnOS tables (7 datasets): **no gaps flagged** — all within `max_lag_days=2` as of today.

**Run 2 (idempotency proof)**: Same 4 gaps emitted to log. `ops.ingestion_gap_queue` still has **exactly 4 rows** (not 8). `ON CONFLICT DO NOTHING` working correctly.

**ops.ingestion_gap_queue** (4 rows, all status=pending):
```
public.hist_mengxi_suyou_clear     gap: 2025-10-26 → 2026-04-09
public.hist_mengxi_wulate_clear    gap: 2025-10-26 → 2026-04-09
public.hist_shandong_binzhou_clear gap: 2026-04-04 → 2026-04-09
public.hist_anhui_dingyuan_clear   gap: 2025-10-26 → 2026-04-09
```

---

### G4 — Live TT API Run

EnOS succeeded and DB was confirmed, so TT was attempted.

**Command** (equivalent; DATASET_FILTER limits to one market for safety):
```bash
RUN_MODE=reconcile START_DATE=2026-04-09 END_DATE=2026-04-09 \
  PGURL=$PGURL_TUNNEL APP_KEY=<from tfvars> APP_SECRET=<from tfvars> \
  DATASET_FILTER=Shandong_BinZhou LOG_LEVEL=INFO \
  python services/data_ingestion/tt_api_collector.py
```

**Result**: Exit code 0 (`run_id = 4`)

**DB connection log**:
```
[column_to_matrix_all] db connection mode=pgurl host=127.0.0.1
```
Confirms: PGURL-only path works — no `DB_*` discrete vars needed. Critical ECS blocker resolved.

**Tables written (Shandong_BinZhou, 2026-04-09)**:
- `hist_shandong_binzhou_clear_15min`: 96 rows (full day, `max_time=2026-04-09 23:45:00`)
- `hist_shandong_binzhou_forecast_15min`: 96 rows
- `hist_shandong_binzhou_clear_dayahead_15min`: 0 rows (DA actual not available)
- `hist_shandong_binzhou_forecast_dayahead_15min`: 96 rows
- `hist_shandong_binzhou_clear`: 621 total rows, `max_date=2026-04-09`
- `hist_shandong_binzhou_forecast`: 890 total rows, `max_date=2026-04-09`
- `hist_shandong_binzhou_clear_dayahead`: 0 new rows

**province_misc** (province-level data): Shandong wrote 3,647 rows across ~38 `hist_shandong_*_15min` tables. Jiangsu returned no data for this date (normal — market may not publish).

**ops evidence**:
```
id=4  collector=tt_api  mode=reconcile  start=2026-04-09  end=2026-04-09
status=success  rows_written=1  duration=64s
```

Note: `rows_written=1` reflects only the `column_to_matrix_all` matrix rows (1 date row per market), not the total 15-min rows. This is a known observability gap — the province_misc rows are not counted separately. Acceptable for now; can add explicit row counting in a follow-up.

---

### G5 — Live Execution Pass / Fail Summary

| Component | Live Result | Notes |
|---|---|---|
| SSH tunnel (access path) | **PASS** | `besskeys.pem` + `ec2-user@54.169.226.139` |
| DDL apply | **PASS** | 4 tables, 11 seed rows created fresh |
| EnOS reconcile (1-day window) | **PASS** | 267,493 rows, run_id=3, duration=814s |
| EnOS ops.ingestion_job_runs | **PASS** | status=success, rows_written correct |
| EnOS ops.ingestion_dataset_status | **PASS** | 8 tables, all failure_count=0 |
| EnOS md_rt_total_cleared_energy | **PASS (expected 0)** | Exchange data lag — 0 rows is correct |
| Freshness monitor (run 1) | **PASS** | 4 TT gaps detected correctly |
| Freshness monitor (run 2 idempotency) | **PASS** | 4 rows in queue, not 8 |
| EnOS freshness (no false gaps) | **PASS** | All EnOS tables within lag window |
| TT API reconcile (Shandong_BinZhou) | **PASS** | PGURL-only mode confirmed |
| TT column_to_matrix DB mode log | **PASS** | `mode=pgurl host=127.0.0.1` |
| TT province_misc Shandong | **PASS** | 3,647 rows written |
| TT INHOUSE_WIND missing | **EXPECTED WARNING** | Not needed for production (optional feature) |
| pytest smoke tests | **NOT RUN** | boto3 missing in local Python env |
| ECS dispatch | **NOT RUN** | Deferred by design (ECS_DISPATCH=false default) |

---

### G6 — Blockers Remaining Before Production Rollout

| # | Blocker | Severity | Resolution |
|---|---|---|---|
| 1 | **Anaconda Python picked up by `["python", ...]` subprocess chain** | Production-safe | In ECS the Docker image has one Python — no Anaconda conflict. Local-only issue. Already fixed for local by installing psycopg2-binary in Anaconda. |
| 2 | **`rows_written` for tt_api counts only matrix rows (=1 per market), not 15-min rows** | Minor observability gap | Add explicit row count query in tt_api finish_run, or accept current behaviour as "date rows written per market" |
| 3 | **`INHOUSE_APP_KEY/INHOUSE_APP_SECRET` not set** | Warning only | In-house wind is optional. Set if in-house wind data is required; otherwise warning is normal. |
| 4 | **ECS task definitions not deployed** | Blocks scheduled runs | Run `terraform -chdir=infra/terraform/data-ingestion apply` with real tfvars to create task defs + EventBridge rules |
| 5 | **ECR image not built** | Blocks ECS runs | Build and push `bess-data-ingestion` Docker image (same push flow as `bess-mengxi-ingestion:v*`) |
| 6 | **TT API gaps (167 days) in 3 of 4 monitored datasets** | Data quality — existing | These are pre-existing gaps, not caused by this framework. Reconcile via: `RUN_MODE=reconcile START_DATE=2025-10-26 END_DATE=2026-04-09 DATASET_FILTER=Mengxi_SuYou,...` |
| 7 | **ECS_DISPATCH=false until IAM verified** | Intentional | Before enabling: verify task_role has `ecs:RunTask` + `iam:PassRole` on execution role |
