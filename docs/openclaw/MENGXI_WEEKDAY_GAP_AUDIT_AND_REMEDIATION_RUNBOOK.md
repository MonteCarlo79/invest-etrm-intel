# Mengxi Weekday Gap Audit and Remediation Runbook

## Scope

This runbook covers:

1. reproducible weekday gap audit for Mengxi marketdata ingestion
2. targeted remediation by exact missing source-file dates
3. recurring remediation path distinct from normal daily ingestion

This is additive to existing drift-cleanup runbooks and does not replace them.

## Root Cause Context

- The scheduled launcher currently runs `bess-mengxi-reconcile` with `DEFAULT_START_DATE=2026-03-12`.
- By design, this does not scan H2 2025 (`2025-07-01` onward), so those gaps are out-of-window.
- Reconcile success can still coexist with historical gaps if they are outside the active reconcile window.
- Partial sheet/table failures can also leave holes if run status is treated as full success.

## Code Paths

- downloader + missing-date logic:
  - `bess-marketdata-ingestion/providers/mengxi/batch_downloader.py`
- load + per-file status + data-quality logic:
  - `bess-marketdata-ingestion/providers/mengxi/load_excel_to_marketdata.py`
- pipeline modes (`daily`, `reconcile`, `remediation`):
  - `bess-marketdata-ingestion/providers/mengxi/run_pipeline.py`
- reproducible DB audit DDL:
  - `db/ddl/ops/mengxi_weekday_gap_audit.sql`
- recurring remediation infra:
  - `infra/terraform/mengxi-ingestion/main.tf`
  - `infra/terraform/mengxi-ingestion/variables.tf`

## 1) Run the Reproducible Weekday Gap Audit

Apply DDL once:

```sql
\i db/ddl/ops/mengxi_weekday_gap_audit.sql
```

Audit query:

```sql
SELECT *
FROM ops.mengxi_weekday_gap_audit
ORDER BY source_file_date, table_name;
```

Summary query:

```sql
SELECT
  table_name,
  count(*) AS missing_days,
  min(source_file_date) AS first_missing_source_date,
  max(source_file_date) AS last_missing_source_date
FROM ops.mengxi_weekday_gap_audit
GROUP BY table_name
ORDER BY missing_days DESC, table_name;
```

Exact-date remediation candidate list:

```sql
SELECT DISTINCT source_file_date
FROM ops.mengxi_weekday_gap_audit
ORDER BY source_file_date;
```

## 2) Trigger Targeted Remediation for Exact Dates

`run_pipeline.py` now supports `RUN_MODE=remediation`:

- finds exact missing dates from DB gap audit logic
- batches exact dates (`REMEDIATION_BATCH_SIZE`)
- downloads exact source files
- loads each chunk with retry

Example ECS/container environment:

```text
RUN_MODE=remediation
START_DATE=2025-07-01
END_DATE=2025-12-31
REMEDIATION_BATCH_SIZE=7
FORCE_RELOAD=true
```

For emergency one-off exact list, use downloader env directly:

```text
EXACT_DATES=["2025-09-03","2025-09-04","2025-09-05"]
RUN_MODE=reconcile
```

## 3) Recurring Remediation Path (Separate from Daily Ingestion)

Terraform now defines a separate recurring remediation path:

- ECS task definition: `bess-mengxi-remediation`
- EventBridge rule: `bess-mengxi-remediation-weekly`
- target type: direct ECS task run
- mode: `RUN_MODE=remediation`

This path is separate from `bess-mengxi-daily-ingestion` + launcher flow and can run on a different cadence/window.

## 4) Post-Run Verification

Check load outcomes:

```sql
SELECT file_date, status, message, loaded_at
FROM marketdata.md_load_log
WHERE file_date BETWEEN DATE '2025-07-01' AND DATE '2025-12-31'
ORDER BY loaded_at DESC
LIMIT 200;
```

Check quality status:

```sql
SELECT data_date, is_complete, interval_coverage, notes, check_time
FROM marketdata.data_quality_status
WHERE province = 'mengxi'
  AND data_date BETWEEN DATE '2025-07-01' AND DATE '2025-12-31'
ORDER BY data_date DESC;
```

Re-run gap audit summary and confirm counts drop:

```sql
SELECT table_name, count(*) AS missing_days
FROM ops.mengxi_weekday_gap_audit
GROUP BY table_name
ORDER BY missing_days DESC, table_name;
```
