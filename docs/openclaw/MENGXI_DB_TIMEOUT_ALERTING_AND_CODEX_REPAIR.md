# MENGXI DB TIMEOUT ALERTING AND CODEX REPAIR

## Purpose

This note is the operational handoff for the recurring failure:

- ECS/Fargate Mengxi ingestion/reconciliation task starts
- Python `run_pipeline.py` retries DB connectivity
- connection to RDS Postgres on `5432` times out
- task eventually raises `RuntimeError: Database not reachable`

This is primarily a **Platform Reliability, Data Quality & Control Agent** issue under `FOUR_AGENTS_OPERATIONS.md`.

Observed evidence:
- the failure is a network reachability timeout, not a SQL syntax error
- host in logs: `bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com`
- resolved IP in failure: `172.31.23.207`
- task retries 10 times at 5s connect timeout + 10s delay and still cannot connect

## Most likely root-cause buckets for Codex / operator to inspect

Treat these as hypotheses, not facts.

### 1. ECS task subnet / route mismatch
The Lambda launcher uses:
- `subnets = private_subnet_ids`
- `assignPublicIp = ENABLED`

That combination is odd and may not match the intended network design.

Codex/operator should inspect:
- whether those subnets can route to the RDS ENI privately
- whether the task is actually landing in the same VPC as the RDS instance
- whether the named `private_subnet_ids` are truly private and associated with the correct route tables

### 2. Security group path incomplete or attached to wrong RDS SG
Terraform creates:
- ECS task SG with all egress allowed
- ingress rule on `var.rds_security_group_id` from ECS task SG on `5432`

Codex/operator should verify:
- `var.rds_security_group_id` is the SG actually attached to the DB instance
- no additional RDS SG blocks the path
- ECS ENI really uses `ecs-mengxi-ingestion-sg`

### 3. NACL or VPC-level filtering
If SGs look right and timeout persists, check:
- subnet NACLs for both ECS and RDS subnets
- ephemeral return traffic allowances

### 4. Wrong DB endpoint / cross-environment mismatch
Confirm the PG URL points at the intended environment:
- prod vs dev RDS
- same VPC/account/region assumptions
- stale endpoint copied into Terraform vars

### 5. RDS availability / failover / maintenance window
Less likely given repeated timeout pattern, but still verify:
- RDS instance status
- whether DB was rebooting/failing over during job run
- CloudWatch / RDS events around `2026-04-08 04:00 SGT`

## Existing code and infra touchpoints

### Pipeline entrypoint
- `bess-marketdata-ingestion/providers/mengxi/run_pipeline.py`

Current behavior:
- checks DB reachability before doing useful work
- raises a generic `RuntimeError("Database not reachable")`
- does not emit a dedicated alert payload

### Container
- `bess-marketdata-ingestion/providers/mengxi/Dockerfile`

### Terraform module
- `infra/terraform/mengxi-ingestion/main.tf`
- `infra/terraform/mengxi-ingestion/variables.tf`
- `infra/terraform/mengxi-ingestion/outputs.tf`
- `infra/terraform/mengxi-ingestion/lambda_function.py`

Current launcher behavior:
- EventBridge invokes Lambda
- Lambda launches Fargate reconcile task
- no explicit failure notification path is defined in this module

## Minimum useful implementation

### Goal
Send a message when this class of failure happens, without redesigning the whole pipeline.

### Preferred narrow implementation
Use the **Platform Reliability / OpenClaw orchestration path** and add an explicit failure notifier.

Recommended small implementation package:

1. Add optional alert webhook env var(s) to the Mengxi ECS task definition, for example:
   - `ALERT_WEBHOOK_URL`
   - `ALERT_CHANNEL` or `ALERT_CONTEXT`

2. Update `run_pipeline.py` so that on terminal DB reachability failure it sends a structured alert before raising.

3. Keep the alert payload simple and evidence-based:
   - pipeline name: `bess-mengxi-ingestion`
   - run mode
   - start/end date if present
   - error class: `db_connect_timeout`
   - DB host extracted from DSN if possible
   - retry counts and timeout settings
   - UTC timestamp

4. Do not bury business logic in the alerting path.

### Alternative infra-first implementation
If Codex prefers not to put alerting in the ingestion container, implement AWS-native monitoring instead:
- CloudWatch metric filter on `Database not reachable` or `timeout expired`
- CloudWatch alarm
- SNS / webhook / downstream notification target

This is operationally cleaner, but requires a notification target and slightly more infra plumbing.

## Suggested task split

### OpenClaw / ops side
- wire the alert route
- validate schedule, task definition, SG target, and launch path
- verify whether the issue is infra/networking vs code

### Codex side
- implement narrow code changes for alerting and diagnostics
- optionally improve error classification in `run_pipeline.py`
- optionally add a tiny helper for DSN host extraction / structured logging

## Codex implementation checklist

1. Inspect repo conventions near the Mengxi ingestion module before editing.
2. Keep scope additive.
3. Do not redesign the entire scheduling stack.
4. Prefer one helper file at most if needed.
5. Preserve current run behavior when alerting env vars are absent.
6. Emit clear observed facts only.

## Concrete candidate changes for Codex

### Option A — in-container alerting
Files likely to edit:
- `bess-marketdata-ingestion/providers/mengxi/run_pipeline.py`
- `infra/terraform/mengxi-ingestion/main.tf`
- `infra/terraform/mengxi-ingestion/variables.tf`

Suggested behavior:
- add helper `send_alert(text_or_payload)`
- only send when `ALERT_WEBHOOK_URL` is present
- wrap terminal DB failure path
- keep retries unchanged initially

### Option B — CloudWatch alarm path
Files likely to edit:
- `infra/terraform/mengxi-ingestion/main.tf`
- `infra/terraform/mengxi-ingestion/variables.tf`
- maybe `outputs.tf`

Suggested behavior:
- metric filter on log group `/ecs/bess-mengxi-ingestion`
- alarm on 1+ matching events in period
- route to SNS / webhook integration already used elsewhere if available

## Exact failure signature to match

Observed strings from logs:
- `DB connection failed:`
- `timeout expired`
- `RuntimeError: Database not reachable`

## What the alert message should say

Recommended message content:

> Mengxi ingestion alert: DB connectivity timeout. The ECS task could not reach Postgres on port 5432 after repeated retries. This is likely an infra/network reachability issue rather than a SQL/query error. Check ECS subnet/VPC placement, RDS SG attachment, NACLs, and the configured PGURL endpoint.

## Recommended place for future repair notes

Store future Codex handoff / repair instructions here:
- `invest-etrm-intel/docs/openclaw/`

Suggested filenames:
- `MENGXI_DB_TIMEOUT_ALERTING_AND_CODEX_REPAIR.md`
- `MENGXI_NETWORK_CONNECTIVITY_RUNBOOK.md`
- `MENGXI_ALERTING_IMPLEMENTATION_NOTES.md`

## Evidence labeling

- **Observed:** repeated `psycopg2.OperationalError` timeout to RDS host/port 5432
- **Heuristic inference:** likely VPC/subnet/SG/NACL/access-path issue rather than loader logic bug
