# MENGXI AGENT 4 MVP VALIDATION RUNBOOK (SANITIZED)

## Purpose

Validate the Agent 4 proactive reliability MVP for the Mengxi ingestion flow without exposing secrets in commands or documentation.

This runbook verifies:
1. DDL applies cleanly
2. the sentinel can run from an environment with DB + CloudWatch access
3. `ops.mengxi_agent4_status` is written
4. dedupe state is written
5. known `db_connect_timeout` classification works
6. trust state lands as expected

---

## Preconditions

Run these steps only from an environment that has both:
- TCP reachability to the target RDS/Postgres endpoint on port `5432`
- AWS credentials with CloudWatch Logs read/write access

Examples:
- bastion / jump host inside the VPC
- ECS Exec shell in a task with correct network reachability
- other VPC-reachable operational shell

If your current desktop/host cannot reach the DB, do not use it for validation.

---

## Secret-handling rule

Do **not** paste passwords into command lines or documentation.

Provide DB credentials via one of these:
- an existing `PGURL` environment variable already set in your shell
- a secret manager retrieval step outside this document
- a secure shell profile / temporary env export performed manually

Required DB env input:
- `PGURL` or equivalent connection details supplied securely by the operator

---

## 1. Common setup

### Required environment variables
PowerShell:

```powershell
$env:AWS_REGION = "ap-southeast-1"
$env:DB_SCHEMA = "marketdata"
$env:OPS_SCHEMA = "ops"
$env:PROVINCE = "mengxi"
$env:PIPELINE_NAME = "bess-mengxi-ingestion"
$env:MENGXI_LOG_GROUP = "/ecs/bess-mengxi-ingestion"
$env:MARKET_LAG_DAYS = "1"
$env:AGENT4_LOOKBACK_HOURS = "48"
$env:AGENT4_STREAM_SCAN_LIMIT = "20"
$env:AGENT4_EVENT_SCAN_LIMIT = "300"
$env:ALERT_DEDUP_HOURS = "6"
```

Optional only if you want to validate end-to-end alert delivery/dedupe:

```powershell
$env:ALERT_WEBHOOK_URL = "<reachable-webhook-url>"
$env:ALERT_CONTEXT = "agent4-mengxi-validation"
```

### DB connection requirement
Before continuing, ensure `PGURL` is already set securely in the shell.

Example check:

```powershell
if (-not $env:PGURL) { throw "PGURL is not set. Load it securely before continuing." }
```

### Optional `psql` helper variables
Adjust path if needed:

```powershell
$Psql = "C:\Program Files\PostgreSQL\18\bin\psql.exe"
```

If you want to use `psql` directly and your client accepts connection URIs, use `PGURL` via env rather than embedding secrets.

---

## 2. Prove the DDL applies cleanly

Check before:

```powershell
& $Psql "$env:PGURL" -t -A -F "|" -c "
SELECT current_database(), current_user;
SELECT to_regclass('ops.mengxi_agent4_status');
SELECT to_regclass('ops.mengxi_agent4_alert_state');
"
```

Apply:

```powershell
& $Psql "$env:PGURL" -v ON_ERROR_STOP=1 -f "C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\db\ddl\ops\mengxi_agent4_reliability.sql"
```

Check after:

```powershell
& $Psql "$env:PGURL" -t -A -F "|" -c "
SELECT to_regclass('ops.mengxi_agent4_status') AS status_table;
SELECT to_regclass('ops.mengxi_agent4_alert_state') AS alert_table;

SELECT schemaname, tablename, indexname
FROM pg_indexes
WHERE schemaname = 'ops'
 AND tablename IN ('mengxi_agent4_status', 'mengxi_agent4_alert_state')
ORDER BY schemaname, tablename, indexname;
"
```

Expected:
- `ops.mengxi_agent4_status`
- `ops.mengxi_agent4_alert_state`
- index `idx_mengxi_agent4_alert_state_active`

---

## 3. Prove the sentinel can run with DB + CloudWatch access

### Option A — run with local Python

```powershell
python C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\services\ops\mengxi_failure_sentinel.py
```

### Option B — run with Docker

```powershell
docker run --rm `
 -v C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform:/work `
 -w /work `
 -e AWS_REGION `
 -e PGURL `
 -e DB_SCHEMA `
 -e OPS_SCHEMA `
 -e PROVINCE `
 -e PIPELINE_NAME `
 -e MENGXI_LOG_GROUP `
 -e MARKET_LAG_DAYS `
 -e AGENT4_LOOKBACK_HOURS `
 -e AGENT4_STREAM_SCAN_LIMIT `
 -e AGENT4_EVENT_SCAN_LIMIT `
 -e ALERT_DEDUP_HOURS `
 -e ALERT_WEBHOOK_URL `
 -e ALERT_CONTEXT `
 -v $HOME\.aws:/root/.aws:ro `
 python:3.11-slim `
 sh -lc "pip install --quiet boto3 sqlalchemy psycopg2-binary && python services/ops/mengxi_failure_sentinel.py"
```

---

## 4. Prove `ops.mengxi_agent4_status` gets written

```powershell
& $Psql "$env:PGURL" -x -c "
SELECT
 pipeline_name,
 province,
 trust_state,
 last_run_status,
 failure_class,
 latest_success_file_date,
 latest_quality_date,
 expected_file_date,
 freshness_lag_days,
 source_log_group,
 source_log_stream,
 source_event_time,
 updated_at
FROM ops.mengxi_agent4_status
WHERE pipeline_name = 'bess-mengxi-ingestion';
"
```

Expected:
- one row for `bess-mengxi-ingestion`

---

## 5. Prove dedupe state gets written

```powershell
& $Psql "$env:PGURL" -x -c "
SELECT
 incident_key,
 pipeline_name,
 failure_class,
 trust_state,
 occurrence_count,
 first_observed_at,
 last_observed_at,
 last_alert_sent_at,
 resolved_at
FROM ops.mengxi_agent4_alert_state
WHERE pipeline_name = 'bess-mengxi-ingestion'
ORDER BY last_observed_at DESC
LIMIT 5;
"
```

Important:
- if `ALERT_WEBHOOK_URL` is empty or unreachable, `last_alert_sent_at` may remain `NULL`
- for end-to-end alert dedupe proof, use a reachable webhook sink

---

## 6. Isolated validation for known `db_connect_timeout` classification

Use a temporary CloudWatch log group so the test does not depend on whatever the latest production logs happen to be.

Create temp log group and stream:

```powershell
$ValidationLogGroup = "/codex/agent4-mengxi-validation"
$ValidationStream = "db-timeout-repro"

aws logs create-log-group --region $env:AWS_REGION --log-group-name $ValidationLogGroup 2>$null
aws logs create-log-stream --region $env:AWS_REGION --log-group-name $ValidationLogGroup --log-stream-name $ValidationStream 2>$null
```

Replay known failure strings:

```powershell
$now = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$events = @(
 @{ timestamp = $now - 3000; message = "Checking DB connectivity (attempt 10)..." },
 @{ timestamp = $now - 2000; message = "DB connection failed: connection to server at `"bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com`" (172.31.23.207), port 5432 failed: timeout expired" },
 @{ timestamp = $now - 1000; message = "RuntimeError: Database not reachable" }
) | ConvertTo-Json -Compress

aws logs put-log-events `
 --region $env:AWS_REGION `
 --log-group-name $ValidationLogGroup `
 --log-stream-name $ValidationStream `
 --log-events "$events"
```

Run sentinel against validation log group with a separate pipeline name:

```powershell
$env:PIPELINE_NAME = "bess-mengxi-ingestion-validation"
$env:MENGXI_LOG_GROUP = $ValidationLogGroup
$env:AGENT4_LOOKBACK_HOURS = "1"
$env:AGENT4_STREAM_SCAN_LIMIT = "5"

python C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\services\ops\mengxi_failure_sentinel.py
```

Or Docker form:

```powershell
docker run --rm `
 -v C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform:/work `
 -w /work `
 -e AWS_REGION `
 -e PGURL `
 -e DB_SCHEMA `
 -e OPS_SCHEMA `
 -e PROVINCE `
 -e PIPELINE_NAME `
 -e MENGXI_LOG_GROUP `
 -e MARKET_LAG_DAYS `
 -e AGENT4_LOOKBACK_HOURS `
 -e AGENT4_STREAM_SCAN_LIMIT `
 -e AGENT4_EVENT_SCAN_LIMIT `
 -e ALERT_DEDUP_HOURS `
 -e ALERT_WEBHOOK_URL `
 -e ALERT_CONTEXT `
 -v $HOME\.aws:/root/.aws:ro `
 python:3.11-slim `
 sh -lc "pip install --quiet boto3 sqlalchemy psycopg2-binary && python services/ops/mengxi_failure_sentinel.py"
```

---

## 7. Prove `db_connect_timeout` and `unsafe_to_trust` land correctly

```powershell
& $Psql "$env:PGURL" -x -c "
SELECT
 pipeline_name,
 trust_state,
 last_run_status,
 failure_class,
 evidence_summary,
 heuristic_summary,
 recommended_action,
 source_log_group,
 source_log_stream,
 source_event_time
FROM ops.mengxi_agent4_status
WHERE pipeline_name = 'bess-mengxi-ingestion-validation';
"
```

Expected:
- `trust_state = unsafe_to_trust`
- `last_run_status = failed`
- `failure_class = db_connect_timeout`

---

## 8. Prove dedupe behavior

Run the validation sentinel a second time immediately with the same env:

```powershell
python C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\services\ops\mengxi_failure_sentinel.py
```

Then query:

```powershell
& $Psql "$env:PGURL" -x -c "
SELECT
 incident_key,
 pipeline_name,
 failure_class,
 trust_state,
 occurrence_count,
 first_observed_at,
 last_observed_at,
 last_alert_sent_at,
 resolved_at
FROM ops.mengxi_agent4_alert_state
WHERE pipeline_name = 'bess-mengxi-ingestion-validation';
"
```

Expected:
- same `incident_key`
- `occurrence_count` increments
- `last_observed_at` updates
- `last_alert_sent_at` remains unchanged on second run if inside the dedupe window
- if a reachable webhook sink was used, only one delivery should be observed there

---

## 9. Optional cleanup

```powershell
aws logs delete-log-group --region $env:AWS_REGION --log-group-name /codex/agent4-mengxi-validation

& $Psql "$env:PGURL" -c "
DELETE FROM ops.mengxi_agent4_status
WHERE pipeline_name = 'bess-mengxi-ingestion-validation';

DELETE FROM ops.mengxi_agent4_alert_state
WHERE pipeline_name = 'bess-mengxi-ingestion-validation';
"
```

---

## Interpretation notes

### Steps 2–5 validate
1. DDL applies
2. sentinel runs with DB + CloudWatch access
3. `ops.mengxi_agent4_status` is written
4. dedupe state is written

### Steps 6–8 validate
5. known `db_connect_timeout` classification
6. `unsafe_to_trust` trust-state for that incident

---

## Known limitations

- This runbook assumes the execution environment already has secure access to the DB secret via `PGURL`.
- It does not itself solve infra reachability problems.
- For the known Mengxi DB timeout / SG-drift incident class, the operational runbook remains:
  - rerun `terraform apply`
  - in `bess-platform/infra/terraform/mengxi-ingestion/`
