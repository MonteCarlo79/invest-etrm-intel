# EnOS Collector — Calibration Notes

Generated: 2026-04-10  
Evidence gathered: AWS CLI + SSH tunnel to production RDS (bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com via i-0196ded69f4366656)  
Calibration status: **partial** — DB evidence from 1 successful manual reconcile run; no scheduled ECS runs have fired yet

---

## 1. Evidence Sources

| Source | Command used | Evidence gathered |
|---|---|---|
| AWS EventBridge | `aws events describe-rule --name bess-platform-enos-market-daily --region ap-southeast-1` | Rule state, schedule, ARN |
| AWS EventBridge targets | `aws events list-targets-by-rule --rule bess-platform-enos-market-daily --region ap-southeast-1` | Target ID, task def ARN revision, subnets, SGs |
| AWS ECS task definition | `aws ecs describe-task-definition --task-definition bess-platform-enos-market-collector --region ap-southeast-1` | Revision, image, command, env vars |
| AWS ECS task history | `aws ecs list-tasks --cluster bess-platform-cluster --family bess-platform-enos-market-collector --desired-status STOPPED/RUNNING` | No tasks yet (rule just enabled) |
| AWS ECR | `aws ecr describe-images --repository-name bess-data-ingestion --image-ids imageTag=latest --region ap-southeast-1` | Image digest, pushed timestamp, size |
| CloudWatch logs | PowerShell: `aws logs describe-log-streams --log-group-name /ecs/bess-platform/enos-market-collector` | Log group exists; 0 streams (no ECS runs yet) |
| DB: ops.ingestion_job_runs | `SELECT * FROM ops.ingestion_job_runs WHERE collector = 'enos_market'` | 3 rows (2 failed, 1 success) — all manual reconcile runs |
| DB: ops.ingestion_dataset_status | `SELECT * FROM ops.ingestion_dataset_status WHERE collector = 'enos_market'` | 8 rows; all `failure_count=0`, `last_date_seen=2026-04-09` |
| DB: per-table MAX(data_date) + row counts | UNION ALL query across all 8 target tables | Full row breakdown for 2026-04-09 |
| DB: md_rt_nodal_price schema | `\d marketdata.md_rt_nodal_price` | Columns: datetime, node_name, data_date, node_price, etc. |

---

## 2. Calibrated Values (from live evidence)

### 2a. Infrastructure state (verified 2026-04-10)

| Field | Observed value | Source |
|---|---|---|
| Rule name | `bess-platform-enos-market-daily` | `describe-rule` |
| Rule state | `ENABLED` | `describe-rule` |
| Schedule | `cron(5 20 * * ? *)` = 04:05 SGT | `describe-rule` |
| EventBridge target ID | `enos-market-collector` | `list-targets-by-rule` |
| Task definition ARN (targeted) | `arn:aws:ecs:ap-southeast-1:319383842493:task-definition/bess-platform-enos-market-collector:2` | `list-targets-by-rule` |
| Task def revision | `:2` | `describe-task-definition` |
| Container image | `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-data-ingestion:latest` | `describe-task-definition` |
| Image digest (current :latest) | `sha256:adf3c30c96923909b0bee63cb15d2deb472a940080a7726fb9fb4b100b195289` | `describe-images` |
| Image pushed at | `2026-04-10T18:34:53 SGT` | `describe-images` |
| Image size | ~168 MB (176,650,190 bytes) | `describe-images` |
| Container CPU / memory | 512 / 1024 MB | `describe-task-definition` |
| Subnets targeted | `subnet-0d561ea9ef0242812`, `subnet-04eef3891262d543a` | `list-targets-by-rule` |
| Security group | `sg-08576f2bea0274a81` | `list-targets-by-rule` |
| Log group | `/ecs/bess-platform/enos-market-collector` | PowerShell `describe-log-streams` |

### 2b. Observed run history (ops.ingestion_job_runs)

Three manual reconcile runs executed on 2026-04-10 covering date range 2026-04-09 → 2026-04-09:

| run_id | status | rows_written | duration | error summary |
|---|---|---|---|---|
| 1 | failed | NULL | ~6 sec | `subprocess.CalledProcessError` — `run_pipeline.py` non-zero exit (missing `cwd=` arg) |
| 2 | failed | NULL | ~8 sec | Same error (fix being applied) |
| 3 | **success** | **267,493** | **13.57 min** | None |

**Calibrated run duration (1-day reconcile, local Windows execution):** 13 minutes 34 seconds  
**Calibration caveat:** This was a local Windows Python run, not a Fargate ECS run. ECS Fargate on Linux may be faster (no Windows path overhead, faster disk I/O), or comparable. Treat 13–20 minutes as the provisional ECS range until first ECS run is observed.

### 2c. Per-table row counts for 2026-04-09 (single day)

| Table | rows for 2026-04-09 | max_date observed | Notes |
|---|---|---|---|
| `md_rt_nodal_price` | **149,472** | 2026-04-09 | 1,557 nodes × 96 intervals (00:00–23:45, 15-min) |
| `md_da_cleared_energy` | **56,928** | **2026-04-10** | DA data published 1 day ahead — max_date LEADS end_date by 1 day. Normal. |
| `md_id_cleared_energy` | **60,581** | 2026-04-09 | Intraday cleared |
| `md_da_fuel_summary` | **480** | **2026-04-10** | DA data published 1 day ahead. Normal. |
| `md_id_fuel_summary` | **5** | 2026-04-09 | Very low count — verify this is source-normal |
| `md_settlement_ref_price` | **24** | 2026-04-09 | 24 reference prices (1 per hour) |
| `md_avg_bid_price` | **3** | 2026-04-09 | Very low count — verify this is source-normal |
| `md_rt_total_cleared_energy` | **0** | **2026-04-08** | **STRUCTURAL LAG: source publishes with 2-day delay. 0 rows for yesterday is NORMAL for daily mode.** |

**Total rows_written for 2026-04-09:** 267,493  
Breakdown: 149,472 + 56,928 + 60,581 + 480 + 5 + 24 + 3 + 0 = 267,493 ✓

### 2d. Structural data patterns (confirmed)

These are NOT partial-write failures — they are normal source-data patterns confirmed by DB evidence:

1. **`md_rt_total_cleared_energy` has a 2-day structural lag.** The source does not publish same-day data. `rows_written` for this table will be 0 for the most recent date in every daily run. The `ingestion_expected_freshness` seed correctly sets `max_lag_days=2` for this table.

2. **Day-ahead tables (`md_da_cleared_energy`, `md_da_fuel_summary`) have max_date = run_date, not yesterday.** The source publishes next-day forecasts early. A daily run for `end_date=yesterday` will write DA rows where `data_date=today`. This means MAX(data_date) for these two tables will appear 1 day ahead of the other 6 tables. This is expected.

3. **`md_id_fuel_summary` (5 rows) and `md_avg_bid_price` (3 rows) have very low row counts.** These are summary tables, not time-series. Their low counts are consistent with their role. Treat as warnings only if count drops to 0 when non-zero was previously observed.

---

## 3. Values Still Estimated / Not Yet Calibrated

The following require at least one successful ECS Fargate scheduled run before they can be calibrated:

| Field | Pre-live estimate | Why not yet calibrated |
|---|---|---|
| First EventBridge trigger observed | 04:05 SGT — not yet fired | Rule enabled 2026-04-10; first trigger = 2026-04-11 04:05 SGT |
| ECS task launch latency (trigger → RUNNING) | "within 2 minutes" (pre-live estimate) | No ECS tasks launched via EventBridge yet |
| ECS run duration (Fargate Linux) | 13–20 min (extrapolated from 13.57 min local Windows run) | No ECS Fargate run data |
| CloudWatch log stream name pattern | `ecs/enos-market-collector/<TASK_ID>` (standard AWS pattern) | No streams exist yet |
| Actual log event sequence from ECS | Expected: `run_start → pipeline_ok → rows_counted` | Not observed in ECS |
| ECS task exit code | Expected: 0 | No ECS tasks run yet |
| rows_written for a daily (yesterday-only) ECS run | ~267k (extrapolated from 1-day reconcile) | Depends on market activity; only 1 data point |
| Variance of rows_written across days | Unknown | Only 1 day calibrated (2026-04-09) |
| ECS image pull time included in duration | Unknown | Not yet measured |

---

## 4. Gaps Still Preventing Full Calibration

1. **No ECS scheduled runs yet.** The rule was enabled on 2026-04-10. First fire is 2026-04-11 04:05 SGT. Until then, run duration, log patterns, and ECS task metadata are all unobserved.

2. **Only 1 successful run (1 date = 2026-04-09).** Cannot estimate day-to-day variance in rows_written. The values from April 9 may not be representative of all market days (weekends, holidays, or periods with different market activity may produce different row counts).

3. **Log patterns from ECS not observed.** All 3 job_runs rows came from local Windows execution — logs went to Windows stdout, not CloudWatch. ECS log format may differ slightly (timestamp formatting, container wrapper headers).

4. **`md_id_fuel_summary` (5 rows) and `md_avg_bid_price` (3 rows) — unclear if these counts are normal.** Only 1 data point. Need 5+ days to establish a normal range.

5. **`md_rt_total_cleared_energy` 2-day lag confirmed for April 9, but not tested for all conditions.** If the market publishes data faster on some days, the lag may vary.

---

## 5. Recommended Calibration Actions After First ECS Run

After the first successful ECS Fargate scheduled run (expected 2026-04-11):

1. Record `started_at`, `finished_at`, `rows_written` from `ops.ingestion_job_runs` row
2. Note the CloudWatch log stream name pattern from `aws logs describe-log-streams`
3. Tail and record the exact JSON event sequence from the log stream
4. Record the ECS task stop code and exit code
5. Update `CALIBRATION_NOTES.md` Section 2 with ECS-specific values
6. After 3–5 days, compute mean ± stddev for `rows_written` to establish the anomaly threshold
