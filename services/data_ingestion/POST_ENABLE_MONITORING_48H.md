# Post-Enable Monitoring — EnOS Daily Rule (48 Hours)

Generated: 2026-04-10  
Calibrated: 2026-04-10 — DB evidence from 1 successful manual reconcile run (run_id=3, 2026-04-09, rows_written=267,493); ECS scheduled run evidence pending (first fire: 2026-04-11 04:05 SGT)  
See also: `CALIBRATION_NOTES.md` for full evidence trail  
Applies to: first 48 hours after enabling `bess-platform-enos-market-daily` EventBridge rule

---

## 1. Scope and Guardrails

### In scope for this monitoring pass

| Resource | Name |
|---|---|
| EventBridge rule | `bess-platform-enos-market-daily` |
| Schedule | `cron(5 20 * * ? *)` — **04:05 SGT / 20:05 UTC** daily |
| ECS task family | `bess-platform-enos-market-collector` |
| Container name | `enos-market-collector` |
| CloudWatch log group | `/ecs/bess-platform/enos-market-collector` |
| Target image | `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-data-ingestion:latest` |
| Verified task def revision | `:2` (as of 2026-04-10 deployment) |

### Out of scope — do NOT enable during this window

| Rule | Name | Status |
|---|---|---|
| TT API schedule | `bess-platform-tt-api-daily` | **Disabled — keep disabled** |
| Freshness monitor | `bess-platform-freshness-monitor-daily` | **Disabled — keep disabled** |
| Lingfeng automation | N/A | Skeleton only — not ready |
| ECS auto-dispatch | `ECS_DISPATCH=false` in freshness-monitor task def | **Keep false** |

This is a controlled rollout of a single collector. No other automated scheduling changes should be made during this 48-hour window.

---

## 2. Immediate Post-Enable Checks (within 10 minutes of enabling rule)

### 2a. Confirm rule is enabled

```bash
aws events describe-rule \
  --name bess-platform-enos-market-daily \
  --region ap-southeast-1 \
  --query '{Name:Name,State:State,ScheduleExpression:ScheduleExpression}'
```

Expected output:
```json
{
  "Name": "bess-platform-enos-market-daily",
  "State": "ENABLED",
  "ScheduleExpression": "cron(5 20 * * ? *)"
}
```

Failure: `State: DISABLED` — rule enable did not persist. Re-run enable command.

### 2b. Confirm EventBridge target and task definition

```bash
aws events list-targets-by-rule \
  --rule bess-platform-enos-market-daily \
  --region ap-southeast-1 \
  --query 'Targets[*].{Id:Id,Arn:Arn,TaskDef:EcsParameters.TaskDefinitionArn}'
```

**Verified output (2026-04-10):**
```json
[{
  "Id": "enos-market-collector",
  "Arn": "arn:aws:ecs:ap-southeast-1:319383842493:cluster/bess-platform-cluster",
  "TaskDef": "arn:aws:ecs:ap-southeast-1:319383842493:task-definition/bess-platform-enos-market-collector:2"
}]
```

If `TaskDef` shows `:1` instead of `:2`, the second `terraform apply` did not propagate. Re-run `terraform apply -chdir=infra/terraform/data-ingestion`.

### 2c. Confirm active task definition revision and image

```bash
aws ecs describe-task-definition \
  --task-definition bess-platform-enos-market-collector \
  --region ap-southeast-1 \
  --query 'taskDefinition.{Revision:revision,Image:containerDefinitions[0].image,Command:containerDefinitions[0].command,Cpu:cpu,Memory:memory}'
```

**Verified output (2026-04-10):**
```json
{
  "Revision": 2,
  "Image": "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-data-ingestion:latest",
  "Command": ["python", "services/data_ingestion/enos_market_collector.py"],
  "Cpu": "512",
  "Memory": "1024"
}
```

Current `:latest` digest: `sha256:adf3c30c96923909b0bee63cb15d2deb472a940080a7726fb9fb4b100b195289` (pushed 2026-04-10 18:34 SGT, ~168 MB)

Failure: image shows `:placeholder` or wrong revision. Do not proceed until resolved.

### 2d. Confirm CloudWatch log group exists

```bash
# NOTE: On Windows Git Bash, leading '/' is mangled. Use PowerShell for log group commands:
powershell -Command "aws logs describe-log-streams --log-group-name '/ecs/bess-platform/enos-market-collector' --region ap-southeast-1"
```

**Verified (2026-04-10):** Log group `/ecs/bess-platform/enos-market-collector` exists, `retentionInDays=30`, 0 streams (no ECS runs yet). Once the first run fires, a stream named `ecs/enos-market-collector/<TASK_ID>` will appear.

### 2e. Checklist before first scheduled run

- [ ] Rule state = `ENABLED`
- [ ] Target `TaskDefinitionArn` ends in `:2` (not `:1` or `:placeholder`)
- [ ] Image is `bess-data-ingestion:latest` (not `placeholder`)
- [ ] Log group `/ecs/bess-platform/enos-market-collector` exists
- [ ] Informed team that 04:05 SGT run is expected next morning

---

## 3. First-Run Checklist

### Trigger timing

| Timezone | Time |
|---|---|
| **UTC** | 20:05 |
| **SGT (UTC+8)** | **04:05** |

The rule fires once per day at the above time. The first scheduled run after enabling will be the **next calendar occurrence** of 20:05 UTC.

> **Calibration status:** Rule was enabled 2026-04-10. First scheduled ECS trigger = **2026-04-11 04:05 SGT**. No ECS run has yet occurred. Timing observations in this section are pre-live estimates. Update after first ECS run fires.

### Observed baseline (from 1 successful manual run — 2026-04-10)

| Field | Observed value | Source |
|---|---|---|
| Run type | manual reconcile (not scheduled) | `ops.ingestion_job_runs` run_id=3 |
| Date range covered | 2026-04-09 → 2026-04-09 (1 day) | same |
| Duration (local Windows Python) | **13 min 34 sec** (17:08:48 → 17:22:23 SGT) | same |
| rows_written | **267,493** | same |
| ECS task launch latency | *not yet calibrated* (no ECS runs) | — |
| CloudWatch log events observed | *not yet calibrated* (local run only) | — |

**ECS run duration estimate:** 13–20 min (pre-live estimate; local run was 13:34 on Windows; Fargate Linux may be faster or similar). **Label: pre-live estimate.**

### 3a. Watch for task launch (within 2 minutes of trigger time)

```bash
# List recent tasks for the family — check for a RUNNING or STOPPED task
aws ecs list-tasks \
  --cluster bess-platform-cluster \
  --family bess-platform-enos-market-collector \
  --region ap-southeast-1

# If task ID is returned:
aws ecs describe-tasks \
  --cluster bess-platform-cluster \
  --tasks <TASK_ID> \
  --region ap-southeast-1 \
  --query 'tasks[*].{Status:lastStatus,StopCode:stopCode,StoppedReason:stoppedReason,StartedAt:startedAt}'
```

Normal sequence: `PROVISIONING → PENDING → RUNNING → STOPPED` (task is short-lived, completes in ~5–15 minutes).

### 3b. Tail CloudWatch logs during run

```bash
# Stream live logs (requires awslogs or CLI log tail)
aws logs tail /ecs/bess-platform/enos-market-collector \
  --follow \
  --region ap-southeast-1

# Alternatively, get the log stream name from the task ID and fetch directly:
aws logs get-log-events \
  --log-group-name /ecs/bess-platform/enos-market-collector \
  --log-stream-name ecs/enos-market-collector/<TASK_ID> \
  --region ap-southeast-1 \
  --query 'events[*].message' \
  --output text
```

### 3c. Successful startup log pattern

All log lines are JSON. Expected events in order:

```
{"event": "run_start", "run_id": <N>, "mode": "daily", "start": "<yesterday>", "end": "<yesterday>", "dry_run": false}
{"event": "pipeline_ok", "script": "run_pipeline.py"}
{"event": "rows_counted", "total": <N>, "tables": 8}
```

`run_id` is the integer primary key from `ops.ingestion_job_runs`. The expected `total` for a single trading day based on the 2026-04-09 calibration run:

| Table | Approximate rows (single day) |
|---|---|
| `md_rt_nodal_price` | ~149,000 (dominant — 1,557 nodes × 96 intervals) |
| `md_da_cleared_energy` | ~57,000 |
| `md_id_cleared_energy` | ~61,000 |
| `md_da_fuel_summary` | ~480 |
| `md_settlement_ref_price` | ~24 (1 per hour) |
| `md_id_fuel_summary` | ~5 |
| `md_avg_bid_price` | ~3 |
| `md_rt_total_cleared_energy` | **~0** (2-day structural lag — 0 rows for yesterday is NORMAL) |
| **Total** | **~267,000 (calibrated from 2026-04-09)** |

> **Operator decision rule:** If `rows_written` is reported as < 100,000, the pipeline likely ran partially or hit the 2-day lag case where most tables had no data. Investigate but do not treat as automatic failure.  
> If `rows_written < 50,000`, treat as a warning and check per-table max dates.  
> If `rows_written = 0`, treat as a hard failure.

### 3d. Failure log patterns

| Pattern | Meaning |
|---|---|
| `{"event": "pipeline_error", "error": "..."}` | `run_pipeline.py` subprocess failed; check error text |
| Container exits with code 1 and no `pipeline_ok` log | Import error or DB connection failure before subprocess |
| No log lines at all after task launch | Image pull failure or IAM permission issue |
| `{"event": "dry_run_skip"}` | `DRY_RUN=true` was set in the task def — should not happen in production |

### 3d. Observed failure signatures (from pre-ECS test runs)

Two failed runs were recorded before the successful one (run_ids 1 and 2):

| Observed pattern | Duration | error_message in DB |
|---|---|---|
| `subprocess.CalledProcessError` — `run_pipeline.py` non-zero exit | ~6–8 sec | `"Command '[...run_pipeline.py]' returned non-zero exit status 1."` |

These fast-failure runs (< 10 sec) are the signature of a subprocess-level crash — either a Python module import error or a path/CWD issue. In ECS, these will appear as container exits with non-zero code and a very short runtime.

### 3e. DB evidence to check after run (within 30 minutes of expected run end)

**Step 1 — Confirm job run row (calibrated query):**
```sql
SELECT id, collector, run_mode, start_date, end_date, dry_run,
       status, rows_written, error_message,
       started_at AT TIME ZONE 'Asia/Singapore' AS started_sgt,
       finished_at AT TIME ZONE 'Asia/Singapore' AS finished_sgt,
       EXTRACT(EPOCH FROM (finished_at - started_at)) / 60 AS duration_min
FROM ops.ingestion_job_runs
WHERE collector = 'enos_market'
ORDER BY started_at DESC
LIMIT 5;
```

**Calibrated expectation:** `status='success'`, `rows_written` ≈ 200,000–300,000 (single-day range based on 2026-04-09 observation of 267,493), `error_message IS NULL`, `duration_min` ≈ 13–20 (pre-live ECS estimate). Note: only 1 successful run has been observed; treat range as provisional until 3+ runs are recorded.

**Operator decision rule:**
- `rows_written >= 200,000` and `status='success'` → normal
- `rows_written` between 50,000 and 199,999 → investigate (partial write or low-activity day)
- `rows_written < 50,000` or `rows_written IS NULL` with `status='success'` → anomaly, check per-table max dates
- `duration_min < 2` and `status='failed'` → fast failure, likely import/subprocess error
- `duration_min > 45` → stall or hang in subprocess

**Step 2 — Confirm all 8 dataset_status entries updated:**
```sql
SELECT dataset,
       last_run_at AT TIME ZONE 'Asia/Singapore' AS last_run_sgt,
       last_success_at AT TIME ZONE 'Asia/Singapore' AS last_success_sgt,
       last_date_seen, failure_count
FROM ops.ingestion_dataset_status
WHERE collector = 'enos_market'
ORDER BY dataset;
```

**Calibrated expectation (from 2026-04-10 run):** 8 rows present (all 8 target tables), all with `failure_count = 0`. After each daily run, `last_date_seen` should advance by 1 for all 8 rows. All `last_success_at` timestamps will fall within a 15-second window of each other (the `update_dataset_status` calls are sequential, ~2 sec apart per table based on observed timing).

**Operator decision rule:** If fewer than 8 rows are returned, `update_dataset_status` was not called for all tables — this is a collector code bug. If any row has `failure_count > 0`, the most recent run failed for that table specifically.

**Step 3 — Check per-table max dates and row counts (calibrated):**
```sql
SELECT 'md_rt_nodal_price'          AS tbl, MAX(data_date) AS max_date,
       COUNT(*) FILTER (WHERE data_date = CURRENT_DATE - 1) AS yesterday_rows
FROM marketdata.md_rt_nodal_price
UNION ALL
SELECT 'md_rt_total_cleared_energy', MAX(data_date),
       COUNT(*) FILTER (WHERE data_date = CURRENT_DATE - 1)
FROM marketdata.md_rt_total_cleared_energy
UNION ALL
SELECT 'md_da_cleared_energy',       MAX(data_date),
       COUNT(*) FILTER (WHERE data_date = CURRENT_DATE - 1)
FROM marketdata.md_da_cleared_energy
UNION ALL
SELECT 'md_da_fuel_summary',         MAX(data_date),
       COUNT(*) FILTER (WHERE data_date = CURRENT_DATE - 1)
FROM marketdata.md_da_fuel_summary
UNION ALL
SELECT 'md_avg_bid_price',           MAX(data_date),
       COUNT(*) FILTER (WHERE data_date = CURRENT_DATE - 1)
FROM marketdata.md_avg_bid_price
UNION ALL
SELECT 'md_id_cleared_energy',       MAX(data_date),
       COUNT(*) FILTER (WHERE data_date = CURRENT_DATE - 1)
FROM marketdata.md_id_cleared_energy
UNION ALL
SELECT 'md_id_fuel_summary',         MAX(data_date),
       COUNT(*) FILTER (WHERE data_date = CURRENT_DATE - 1)
FROM marketdata.md_id_fuel_summary
UNION ALL
SELECT 'md_settlement_ref_price',    MAX(data_date),
       COUNT(*) FILTER (WHERE data_date = CURRENT_DATE - 1)
FROM marketdata.md_settlement_ref_price
ORDER BY yesterday_rows DESC;
```

**Calibrated expected output interpretation:**

| Table | `yesterday_rows` expected | `max_date` expected | Notes |
|---|---|---|---|
| `md_rt_nodal_price` | ~149,000 | yesterday | Dominant table |
| `md_id_cleared_energy` | ~61,000 | yesterday | |
| `md_da_cleared_energy` | ~57,000 | **today** | DA data leads by 1 day — max_date = CURRENT_DATE is NORMAL |
| `md_da_fuel_summary` | ~480 | **today** | Same DA lead pattern |
| `md_settlement_ref_price` | ~24 | yesterday | 1 per hour |
| `md_id_fuel_summary` | ~5 | yesterday | Summary table; low count is normal |
| `md_avg_bid_price` | ~3 | yesterday | Summary table; low count is normal |
| `md_rt_total_cleared_energy` | **0** | yesterday-2 | **Structural 2-day lag — 0 rows for yesterday is ALWAYS NORMAL** |

**Operator decision rule:** If 7/8 tables have `yesterday_rows > 0` but `md_rt_total_cleared_energy` shows 0 — this is **not a partial write failure**; it is the confirmed structural lag pattern. If any other table shows 0 when it previously had data, treat as a partial write and investigate.

---

## 4. 24-Hour Monitoring Checklist

Repeat after each scheduled run (one run per day). Target run window: 04:05–04:25 SGT.

### After each run

- [ ] **ECS task launched?** (`aws ecs list-tasks --family bess-platform-enos-market-collector`)
- [ ] **Task exited cleanly?** (`stopCode = EssentialContainerExited`, exit code 0)
- [ ] **Log has `pipeline_ok` event?** (no `pipeline_error`)
- [ ] **`ops.ingestion_job_runs` new row?** `status = 'success'`, `finished_at` populated
- [ ] **`rows_written` plausible?** Non-null, not zero, roughly consistent with prior runs (±30% of baseline)
- [ ] **All 8 `dataset_status` rows updated?** `last_success_at` within 30 minutes of run time
- [ ] **`last_date_seen` advanced by 1 day?** (daily mode collects yesterday; each day it should increment)

### Metrics to track across runs

| Metric | Expected behavior | Calibration status |
|---|---|---|
| `rows_written` | ~200,000–300,000 for a full trading day | **Calibrated:** 267,493 for 2026-04-09 (1 data point) |
| `last_date_seen` in dataset_status | Advances by 1 each day — must never go backwards | **Calibrated:** confirmed for all 8 tables after 2026-04-09 run |
| `failure_count` in dataset_status | Must stay 0; any increment = failure on most recent run | **Calibrated:** all 8 at 0 |
| Task duration (started_at to finished_at) | 13–20 min (pre-live ECS estimate based on 13:34 local run) | **Pre-live estimate** — update after first ECS run |
| Number of rows in `ops.ingestion_job_runs` | Exactly 1 new row per day (no duplicate triggers) | Structural (EventBridge trigger guarantees 1) |
| `md_rt_total_cleared_energy.yesterday_rows` | **0 is normal** (2-day structural lag confirmed) | **Calibrated** |
| DA table `max_date` (`md_da_cleared_energy`, `md_da_fuel_summary`) | **max_date = CURRENT_DATE** (1 day ahead of other tables) | **Calibrated** |

### Operator decision rules (calibrated)

- **If `rows_written` falls below 150,000:** investigate. At 267k baseline, a drop to 150k suggests at least `md_rt_nodal_price` or `md_id_cleared_energy` had a partial write. Check per-table union query. *(Threshold is provisional — 1 data point only)*
- **If 7/8 tables have `yesterday_rows > 0` but `md_rt_total_cleared_energy` shows 0:** do NOT treat as a partial write. This is the confirmed structural lag. Check only if `md_rt_total_cleared_energy.max_date` is more than 3 days old.
- **If `md_da_cleared_energy` or `md_da_fuel_summary` max_date equals yesterday instead of today:** the DA forecast for tomorrow was not yet published by the source. This may indicate an EnOS source delay. Not an immediate failure but worth noting.
- **If the same `max_date` persists across 2 consecutive successful runs for any non-DA, non-rt_total table:** escalate as stale-write anomaly. The run reported success but the underlying data did not advance.
- **If task launches more than 10 minutes after 04:05 SGT:** *(pre-live estimate — verify after first ECS run)* check ECS capacity and EventBridge delivery logs.
- **If `duration_min < 2` and `status='failed'`:** this is the confirmed fast-failure signature (subprocess crash). Error message in `ops.ingestion_job_runs.error_message` will contain `returned non-zero exit status`.

### Anomalies requiring same-day investigation

- `rows_written = 0` or `rows_written IS NULL` with `status='success'`
- `failure_count > 0` for any of the 8 dataset_status rows
- `last_date_seen` did not advance for any non-DA, non-rt_total table
- 2 or more rows in `ops.ingestion_job_runs` for the same SGT calendar day
- Task `duration_min < 2` and `status='failed'` (fast failure = not transient)

---

## 5. 48-Hour Decision Gate

### Criteria for "EnOS stable — proceed with rollout"

All of the following must be true after 2 consecutive successful daily runs:

- [ ] 2 rows in `ops.ingestion_job_runs` where `collector = 'enos_market'` and `status = 'success'`
- [ ] Both rows have `rows_written > 0`
- [ ] Both rows have `finished_at IS NOT NULL` (runs completed, not stuck)
- [ ] `failure_count = 0` for all 8 rows in `ops.ingestion_dataset_status` for `enos_market`
- [ ] `last_date_seen` in `ops.ingestion_dataset_status` reflects last 2 trading days for all 8 tables
- [ ] `MAX(data_date)` in `marketdata.md_rt_nodal_price` and `marketdata.md_da_cleared_energy` matches expected recent dates
- [ ] No `pipeline_error` events in CloudWatch logs
- [ ] No ECS task stopped with non-zero exit code

If all satisfied: **proceed to Phase 7 (enable TT API rule)** per GO_LIVE_STATUS.md.

### Criteria for "needs investigation — hold rollout"

Any of the following triggers a hold (do not disable rule yet, investigate first):

- `rows_written` significantly lower than baseline (< 50% of live-test value ~267k for a full day)
- One run succeeded but the second failed
- Some (but not all) dataset_status entries updated — partial table write
- Task duration unexpectedly long (> 45 min) — possible stall in subprocess

### Criteria for "disable rule now — rollback"

Disable the EventBridge rule immediately if any of the following are true:

- **2 consecutive failures** (`status = 'failed'` in job_runs for 2 days)
- **DB errors in logs** — DSN misconfiguration, authentication failures, or RDS unreachable
- **Unexpected data in marketdata schema** — tables truncated, wrong date ranges inserted, duplicate rows inconsistent with prior upsert behavior
- **ECS task launch loop** — more than 1 task visible simultaneously for the same family (EventBridge misconfiguration)
- **`error_message` contains `subprocess.CalledProcessError`** and the same error repeats across runs (not transient)

---

## 6. Exact SQL Checks

### Latest job runs for enos_market

```sql
SELECT id, run_mode, start_date, end_date, dry_run,
       status, rows_written, error_message,
       started_at AT TIME ZONE 'Asia/Singapore' AS started_sgt,
       finished_at AT TIME ZONE 'Asia/Singapore' AS finished_sgt,
       EXTRACT(EPOCH FROM (finished_at - started_at)) / 60 AS duration_minutes
FROM ops.ingestion_job_runs
WHERE collector = 'enos_market'
ORDER BY started_at DESC
LIMIT 10;
```

### All 8 dataset_status rows for EnOS

```sql
SELECT dataset, last_run_at AT TIME ZONE 'Asia/Singapore' AS last_run_sgt,
       last_success_at AT TIME ZONE 'Asia/Singapore' AS last_success_sgt,
       last_date_seen, failure_count, updated_at AT TIME ZONE 'Asia/Singapore' AS updated_sgt
FROM ops.ingestion_dataset_status
WHERE collector = 'enos_market'
ORDER BY dataset;
```

Expected 8 rows:
- `marketdata.md_avg_bid_price`
- `marketdata.md_da_cleared_energy`
- `marketdata.md_da_fuel_summary`
- `marketdata.md_id_cleared_energy`
- `marketdata.md_id_fuel_summary`
- `marketdata.md_rt_nodal_price`
- `marketdata.md_rt_total_cleared_energy`
- `marketdata.md_settlement_ref_price`

### MAX(data_date) in all 8 EnOS target tables

```sql
SELECT 'md_rt_nodal_price'          AS tbl, MAX(data_date) AS max_date FROM marketdata.md_rt_nodal_price
UNION ALL
SELECT 'md_rt_total_cleared_energy' AS tbl, MAX(data_date) AS max_date FROM marketdata.md_rt_total_cleared_energy
UNION ALL
SELECT 'md_da_cleared_energy'       AS tbl, MAX(data_date) AS max_date FROM marketdata.md_da_cleared_energy
UNION ALL
SELECT 'md_da_fuel_summary'         AS tbl, MAX(data_date) AS max_date FROM marketdata.md_da_fuel_summary
UNION ALL
SELECT 'md_avg_bid_price'           AS tbl, MAX(data_date) AS max_date FROM marketdata.md_avg_bid_price
UNION ALL
SELECT 'md_id_cleared_energy'       AS tbl, MAX(data_date) AS max_date FROM marketdata.md_id_cleared_energy
UNION ALL
SELECT 'md_id_fuel_summary'         AS tbl, MAX(data_date) AS max_date FROM marketdata.md_id_fuel_summary
UNION ALL
SELECT 'md_settlement_ref_price'    AS tbl, MAX(data_date) AS max_date FROM marketdata.md_settlement_ref_price
ORDER BY max_date DESC, tbl;
```

**Calibrated expected state after a successful daily run:**
- 6 tables: `max_date = yesterday` (CURRENT_DATE - 1)
- 2 DA tables (`md_da_cleared_energy`, `md_da_fuel_summary`): `max_date = today` (CURRENT_DATE) — **this is normal**
- `md_rt_total_cleared_energy`: `max_date = CURRENT_DATE - 2` or older — **this is normal** (2-day structural lag)

A non-DA, non-rt_total table with `max_date` older than yesterday indicates a partial write for that table.

### Detecting stale tables across consecutive runs

Run this after each of the 2 daily runs and compare output. If any `max_date` is unchanged between Day 1 and Day 2 checks, that table did not receive new data:

```sql
-- Save output with date for comparison
SELECT CURRENT_DATE AS check_date,
       'md_rt_nodal_price' AS tbl, MAX(data_date) AS max_date,
       COUNT(*) FILTER (WHERE data_date = CURRENT_DATE - 1) AS yesterday_rows
FROM marketdata.md_rt_nodal_price
UNION ALL
SELECT CURRENT_DATE, 'md_da_cleared_energy', MAX(data_date),
       COUNT(*) FILTER (WHERE data_date = CURRENT_DATE - 1)
FROM marketdata.md_da_cleared_energy
-- repeat for other 6 tables as needed
;
```

### Checking rows_written is not zero or missing

```sql
SELECT id, started_at, status, rows_written,
  CASE
    WHEN rows_written IS NULL   THEN 'MISSING'
    WHEN rows_written = 0       THEN 'ZERO — investigate'
    WHEN rows_written < 10000   THEN 'LOW — verify source'
    ELSE 'ok'
  END AS rows_flag
FROM ops.ingestion_job_runs
WHERE collector = 'enos_market'
ORDER BY started_at DESC
LIMIT 5;
```

Note: `rows_written` in `ops.ingestion_job_runs` for `enos_market` is the SUM of `COUNT(*) WHERE data_date BETWEEN start AND end` across all 8 target tables, queried after the subprocess completes. For a single-day daily run the expected range is approximately 50,000–400,000 rows depending on market activity.

### Detect duplicate triggered runs on the same day

```sql
SELECT DATE(started_at AT TIME ZONE 'Asia/Singapore') AS run_date,
       COUNT(*) AS run_count
FROM ops.ingestion_job_runs
WHERE collector = 'enos_market'
  AND started_at > NOW() - INTERVAL '7 days'
GROUP BY 1
ORDER BY 1 DESC;
```

Any `run_count > 1` for the same `run_date` indicates a duplicate trigger. Investigate EventBridge rule configuration.

---

## 7. Exact AWS CLI Checks

### Describe rule

```bash
aws events describe-rule \
  --name bess-platform-enos-market-daily \
  --region ap-southeast-1
```

### List targets for rule

```bash
aws events list-targets-by-rule \
  --rule bess-platform-enos-market-daily \
  --region ap-southeast-1
```

### Describe current task definition

```bash
# Latest active revision (no revision suffix = latest ACTIVE)
aws ecs describe-task-definition \
  --task-definition bess-platform-enos-market-collector \
  --region ap-southeast-1

# Specific revision (revision 2 is current as of 2026-04-10):
aws ecs describe-task-definition \
  --task-definition bess-platform-enos-market-collector:2 \
  --region ap-southeast-1
```

### List recent tasks for the ECS family

```bash
# Running tasks:
aws ecs list-tasks \
  --cluster bess-platform-cluster \
  --family bess-platform-enos-market-collector \
  --desired-status RUNNING \
  --region ap-southeast-1

# Stopped tasks (last run evidence):
aws ecs list-tasks \
  --cluster bess-platform-cluster \
  --family bess-platform-enos-market-collector \
  --desired-status STOPPED \
  --region ap-southeast-1
```

### Describe a stopped task for diagnostics

```bash
# Replace <TASK_ID> with the ID from list-tasks output
aws ecs describe-tasks \
  --cluster bess-platform-cluster \
  --tasks <TASK_ID> \
  --region ap-southeast-1 \
  --query 'tasks[*].{Status:lastStatus,StopCode:stopCode,StoppedReason:stoppedReason,ExitCode:containers[0].exitCode,StartedAt:startedAt,StoppedAt:stoppedAt}'
```

Normal stopped task: `stopCode = EssentialContainerExited`, `ExitCode = 0`.  
Failure exit codes: any non-zero value; `StoppedReason` will contain the error type.

### Tail CloudWatch logs

```bash
# Live tail (requires AWS CLI v2):
# NOTE: On Windows Git Bash, the leading '/' is path-mangled. Use PowerShell:
powershell -Command "aws logs tail '/ecs/bess-platform/enos-market-collector' --follow --since 2h --region ap-southeast-1"

# List log streams (PowerShell):
powershell -Command "aws logs describe-log-streams --log-group-name '/ecs/bess-platform/enos-market-collector' --order-by LastEventTime --descending --limit 5 --region ap-southeast-1"

# Fetch all events from a specific stream (PowerShell):
# Stream name pattern: ecs/enos-market-collector/<TASK_ID>
powershell -Command "aws logs get-log-events --log-group-name '/ecs/bess-platform/enos-market-collector' --log-stream-name 'ecs/enos-market-collector/<TASK_ID>' --region ap-southeast-1 --query 'events[*].message' --output text"
```

> **Windows path mangling note (verified 2026-04-10):** On Git Bash for Windows, `aws logs` commands with log group names starting with `/ecs/` are corrupted to `C:/Program Files/Git/ecs/...`. Always use `powershell -Command "..."` wrapper on Windows for CloudWatch log commands.

### Check ECR image digest (verify :latest is up to date)

```bash
aws ecr describe-images \
  --repository-name bess-data-ingestion \
  --image-ids imageTag=latest \
  --region ap-southeast-1 \
  --query 'imageDetails[*].{Tag:imageTags[0],Digest:imageDigest,PushedAt:imagePushedAt}'
```

Expected digest: `sha256:adf3c30c96923909b0bee63cb15d2deb472a940080a7726fb9fb4b100b195289` (as pushed 2026-04-10). If digest changed unexpectedly, a new image was pushed — verify it was intentional.

---

## 8. Failure Matrix

| Symptom | Likely cause | Where to verify | Immediate action | Escalation threshold |
|---|---|---|---|---|
| EventBridge rule enabled but no ECS task launched within 5 min of trigger | `events_invoke_ecs_role_arn` lacks `ecs:RunTask` or `iam:PassRole`; or rule has no target | `aws events list-targets-by-rule`; CloudTrail for EventBridge failures | Check IAM policy on `bess-platform-eventbridge-ecs` role; re-verify targets | Trigger manual run via `aws ecs run-task` to isolate rule vs task def issue |
| ECS task launches but exits immediately (< 60 sec), exit code non-zero | Image pull failure, entrypoint crash, missing Python module | `aws ecs describe-tasks` → `stoppedReason`; CloudWatch log stream | Check log stream for Python traceback; verify ECR image pull permissions on execution role | If 2 consecutive exits < 60 sec: disable rule |
| `ModuleNotFoundError` or `ImportError` in logs | Missing package in Docker image, or wrong PYTHONPATH | CloudWatch log stream | Rebuild and push image with corrected requirements.txt; re-deploy task def | Disable rule until image fixed |
| DB connection / auth failure in logs (`psycopg2.OperationalError`, `could not connect to server`) | Wrong PGURL, RDS unreachable from task SG, or SSL mismatch | CloudWatch logs; verify `sg-08576f2bea0274a81` allows outbound to RDS port 5432 | Verify DB_DSN / PGURL env var in task def; verify SG rules | Disable rule; test connection from EC2 instance in same SG |
| `subprocess.CalledProcessError` in logs (pipeline_error event) | `run_pipeline.py` or `batch_downloader.py` failed in the EnOS subprocess | CloudWatch logs (stderr from subprocess captured by wrapper) | Check the full error text in `ops.ingestion_job_runs.error_message`; run manually to reproduce | 2 consecutive subprocess failures → disable rule |
| `rows_written = 0` in ops.ingestion_job_runs | EnOS source returned empty data (market holiday? API quota?) or date range produced no rows | `ops.ingestion_job_runs`; check `MAX(data_date)` in target tables | Verify if a market holiday applies; run reconcile for the date manually | If 0 rows for 2 consecutive non-holiday days → investigate pipeline |
| Some (not all) target tables updated | Partial write in EnOS pipeline; one of the 8 load steps failed | `ops.ingestion_dataset_status` — compare `last_date_seen` across all 8 tables; check `MAX(data_date)` union query | Identify which table is stale; run reconcile targeting that date | If same table consistently stale → bug in `load_excel_to_marketdata.py` for that sheet |
| `rows_written` is non-null but `last_date_seen` in dataset_status did not advance | `update_dataset_status` called with wrong `last_date` value, or `last_date_seen` upsert collided | Compare `end_date` in job_runs vs `last_date_seen` in dataset_status | Manual inspection; run `SELECT * FROM ops.ingestion_dataset_status WHERE collector = 'enos_market'` | If discrepancy persists 2 runs → code issue in collector |
| Repeated same `MAX(data_date)` across consecutive daily runs | Daily run is collecting old dates; `START_DATE`/`END_DATE` env not set correctly for daily mode | Task def env vars: `RUN_MODE=daily` (no explicit dates — RunContext computes yesterday automatically) | Verify `RUN_MODE=daily` and no `START_DATE` override in task def | Disable rule if data_date stuck |
| Log JSON malformed (log lines not valid JSON) | `log_format` string in `shared/logging.py` wrapping a dict incorrectly, or subprocess stdout not JSON | Raw log text in CloudWatch | Informational only — does not affect DB writes; fix log format in follow-up | Not a rollback trigger unless it prevents parsing for alerts |
| Wrong image revision targeted | EventBridge target still pointing to `:1` task def after second `terraform apply` | `aws events list-targets-by-rule` → `TaskDefinitionArn` suffix | Re-run `terraform apply -chdir=infra/terraform/data-ingestion` | Disable rule until task def revision is correct |
| `DRY_RUN=true` in task def by mistake | Env var accidentally set; task writes `dry_run_skip` log and exits 0 | `ops.ingestion_job_runs.dry_run = true`; log shows `dry_run_skip` event | Remove `DRY_RUN` override from task def; re-run `terraform apply` | Not urgent but means no data was written |

---

## 9. Rollback Procedure

### Step 1 — Capture minimum evidence before rollback

```bash
# Save last 3 job run rows
psql "$PGURL" -c "
SELECT id, run_mode, start_date, end_date, status, rows_written,
       error_message, started_at, finished_at
FROM ops.ingestion_job_runs
WHERE collector = 'enos_market'
ORDER BY started_at DESC LIMIT 3;" > /tmp/enos_job_runs_prerollback.txt

# Save dataset status snapshot
psql "$PGURL" -c "
SELECT * FROM ops.ingestion_dataset_status WHERE collector = 'enos_market'
ORDER BY dataset;" >> /tmp/enos_job_runs_prerollback.txt

# Save last task metadata
aws ecs list-tasks \
  --cluster bess-platform-cluster \
  --family bess-platform-enos-market-collector \
  --desired-status STOPPED \
  --region ap-southeast-1 > /tmp/ecs_stopped_tasks.txt
```

### Step 2 — Disable the EventBridge rule

```bash
aws events disable-rule \
  --name bess-platform-enos-market-daily \
  --region ap-southeast-1
```

### Step 3 — Confirm no further scheduled launches

```bash
# Rule must show State: DISABLED
aws events describe-rule \
  --name bess-platform-enos-market-daily \
  --region ap-southeast-1 \
  --query '{Name:Name,State:State}'
```

Wait until any currently-running ECS task finishes (do not kill a running task — it will self-terminate on success/failure).

```bash
# Confirm no running tasks remain
aws ecs list-tasks \
  --cluster bess-platform-cluster \
  --family bess-platform-enos-market-collector \
  --desired-status RUNNING \
  --region ap-southeast-1
```

Expected: `taskArns: []` (empty list).

### Step 4 — Save CloudWatch logs

```bash
# Get the most recent log stream name from the describe-log-streams command,
# then export the full stream. Replace <STREAM_NAME> with actual stream.
aws logs get-log-events \
  --log-group-name /ecs/bess-platform/enos-market-collector \
  --log-stream-name <STREAM_NAME> \
  --region ap-southeast-1 \
  --query 'events[*].message' \
  --output text > /tmp/enos_market_collector_last_run.log
```

### Step 5 — Preserve Terraform state (no destroy needed)

The module is already deployed. The rule being disabled is sufficient to stop automated triggers. Do NOT run `terraform destroy` unless full decommission is required. Task definitions, log groups, and the ECR repo remain available for re-enable.

### Re-enable command (when ready to retry)

```bash
aws events enable-rule \
  --name bess-platform-enos-market-daily \
  --region ap-southeast-1
```

---

## 10. Decision Note for Later Phases

### Gate for enabling `freshness_monitor` scheduling

The following must all be true:

1. `bess-platform-enos-market-daily` has produced **2 consecutive successful daily runs** (`status = 'success'` in `ops.ingestion_job_runs`)
2. All 8 EnOS tables have `failure_count = 0` and `last_date_seen` advancing daily in `ops.ingestion_dataset_status`
3. **No open anomalies** from the 48-hour checklist above

The freshness monitor will read `ops.ingestion_expected_freshness` (7 EnOS rows + 4 TT rows) and potentially queue TT gaps even before TT scheduling is enabled. This is acceptable — queued gaps with `status='pending'` and `ECS_DISPATCH=false` are harmless.

> Note: `marketdata.md_avg_bid_price` appears in `ENOS_TARGET_TABLES` in `enos_market_collector.py` but is **not** in the `ops.ingestion_expected_freshness` seed data (7 rows seeded, not 8). The freshness monitor will not check this table. Add it manually if needed:
> ```sql
> INSERT INTO ops.ingestion_expected_freshness (dataset, collector, date_column, max_lag_days)
> VALUES ('marketdata.md_avg_bid_price', 'enos_market', 'data_date', 2)
> ON CONFLICT (dataset) DO NOTHING;
> ```

### Gate for attempting a real TT scheduled rollout

1. EnOS gate satisfied (above)
2. `bess-platform-tt-api-daily` task definition reviewed — confirm `MARKET_LIST=Mengxi,Anhui,Shandong,Jiangsu` maps correctly to `province_misc_to_db_v2._selected_markets()` (partially verified in live test with `Shandong_BinZhou`; full 4-market validation pending ECS run)
3. `PGURL`-only DB connection confirmed working in ECS environment (confirmed locally; ECS uses same common_env)
4. No IAM issues with the task role accessing TT DAAS SDK endpoints (outbound HTTPS to Poseidon API)

---

## 11. What Good Looks Like — End-to-End Healthy Run

This section defines the complete expected sequence for a healthy scheduled EnOS daily run. Use it as the reference baseline for operator judgement.

```
04:05:00 SGT   EventBridge fires cron(5 20 * * ? *)
04:05:xx SGT   ECS task PROVISIONING (verify: aws ecs list-tasks --desired-status RUNNING)
04:06:xx SGT   ECS task RUNNING; container starts
               CloudWatch log stream created: ecs/enos-market-collector/<TASK_ID>

               Log events (JSON, in order):
               {"event":"run_start","run_id":<N>,"mode":"daily",
                "start":"<yesterday>","end":"<yesterday>","dry_run":false}
               [subprocess: run_pipeline.py runs batch_downloader.py + load_excel_to_marketdata.py]
               {"event":"pipeline_ok","script":"run_pipeline.py"}
               {"event":"rows_counted","total":~267000,"tables":8}

04:19–04:26    ECS task STOPPED
               stopCode = EssentialContainerExited
               exitCode = 0

               DB state:
               ops.ingestion_job_runs:
                 status='success', rows_written≈267000,
                 duration≈13-20 min, error_message IS NULL

               ops.ingestion_dataset_status (8 rows):
                 all failure_count=0, all last_date_seen=yesterday

               Per-table max_date:
                 md_rt_nodal_price       → yesterday
                 md_id_cleared_energy    → yesterday
                 md_da_cleared_energy    → TODAY (normal — DA lead)
                 md_da_fuel_summary      → TODAY (normal — DA lead)
                 md_settlement_ref_price → yesterday
                 md_id_fuel_summary      → yesterday
                 md_avg_bid_price        → yesterday
                 md_rt_total_cleared_energy → yesterday-2 or older (normal — 2-day lag)
```

> **Calibration note:** Task launch latency (04:05:00 → RUNNING) and exact duration are pre-live ECS estimates. Update this section after the first ECS Fargate run completes.

---

## 12. Known Current Limits

These limits are active as of 2026-04-10. Do not widen scope without explicit review.

| Limit | Detail |
|---|---|
| TT scheduled rollout **not approved** | `bess-platform-tt-api-daily` rule is DISABLED. Full 4-market validation pending ECS run. Do not enable without satisfying the gate in Section 10. |
| Lingfeng **out of scope** | `LINGFENG_BASE_URL` unknown; Playwright selectors unverified. Collector exits immediately with `SystemExit` if run without the env var. Rule has no schedule. |
| freshness_monitor scheduling **gated** | `bess-platform-freshness-monitor-daily` is DISABLED. Enable only after 2 consecutive EnOS successes (Section 10 gate). |
| ECS_DISPATCH **false** | `ECS_DISPATCH=false` in freshness-monitor task def. IAM policy for `bess-platform-task-role` (`ecs:RunTask` + `iam:PassRole`) not yet verified. Do not set to true until IAM simulation passes. |
| `rows_written` is post-query count | `enos_market_collector._count_rows_written()` queries all 8 tables AFTER the subprocess exits. If DB becomes unreachable between subprocess finish and count query, `rows_written` will be NULL even if data was written. This is a known observability gap. |
| Thresholds provisional (1 data point) | `rows_written` range (200k–300k), run duration (13–20 min), and per-table row counts are based on a single day (2026-04-09). Do not treat these as tight bounds until 5+ runs are observed. Update `CALIBRATION_NOTES.md` after each ECS run. |
| `md_avg_bid_price` not in freshness seed | This table is in `ENOS_TARGET_TABLES` (written by collector) but not in `ops.ingestion_expected_freshness`. Freshness monitor will not detect gaps for this table. Add manually if freshness alerting is needed. |
| CloudWatch log commands require PowerShell on Windows | Git Bash mangles log group names starting with `/ecs/`. All CloudWatch commands must be wrapped in `powershell -Command "..."` on Windows machines. |

---

## Quick Reference

### Key names (all ap-southeast-1)

| Resource | Value |
|---|---|
| EventBridge rule | `bess-platform-enos-market-daily` |
| ECS cluster | `bess-platform-cluster` |
| Task family | `bess-platform-enos-market-collector` |
| Container name | `enos-market-collector` |
| Log group | `/ecs/bess-platform/enos-market-collector` |
| ECR repo | `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-data-ingestion` |
| Current task def | `bess-platform-enos-market-collector:2` |
| Schedule | `cron(5 20 * * ? *)` = 04:05 SGT / 20:05 UTC |

### Enable / disable one-liners

```bash
# Enable
aws events enable-rule  --name bess-platform-enos-market-daily --region ap-southeast-1

# Disable
aws events disable-rule --name bess-platform-enos-market-daily --region ap-southeast-1
```
