# MENGXI PROACTIVE FAILURE SENTINEL SPEC

## Purpose

Define a narrow proactive operational skill for the Mengxi ingestion flow.

This is not a general autonomous agent.
It is a focused **Platform Reliability, Data Quality & Control** sentinel whose job is to detect ingestion failures early, classify them, alert clearly, and route repair work safely.

It should support the 4-agent operating model without creating branch chaos or autonomous repo rewrites.

---

## Why this skill exists

Current observed problem:
- the `bess-mengxi-ingestion` / reconciliation workflow can fail in ECS/Fargate
- example observed failure: repeated Postgres connection timeout to RDS on port 5432
- when this happens, downstream trading / portfolio / strategy outputs may become stale or unsafe

The proactive sentinel exists to:
1. detect failures quickly
2. classify the likely failure class
3. notify with useful evidence
4. mark downstream outputs as unsafe when appropriate
5. route implementation-heavy repair work to Codex rather than improvising broad edits

---

## Scope

### In scope
- monitor Mengxi ingestion job health
- monitor terminal failure signatures
- classify failures into operational buckets
- monitor freshness of expected outputs
- send concise alerts
- produce a repair handoff note when needed
- recommend safe rerun vs escalation

### Out of scope
- large repo rewrites
- direct production schema redesign
- speculative business conclusions
- autonomous infrastructure redesign
- silent branch switching
- overlapping coding with Codex on the same active branch

---

## Primary operating principle

The sentinel should be **small, deterministic, and evidence-first**.

Every conclusion must be labeled as:
- observed
- proxy-based
- heuristic inference

It must not present a guess as fact.

---

## Target system placement

This sentinel belongs under the logic of:
- **Agent 4: Platform Reliability, Data Quality & Control**

It exists to support the other 3 agents by preventing them from consuming untrustworthy data.

Operational order:
1. sentinel checks health/freshness
2. if unhealthy, alert + quarantine recommendation
3. if healthy, downstream analytics can proceed

---

## Detection targets

### 1. Job failure detection

Monitor for:
- ECS task launch failure
- ECS task stop with non-zero exit
- Lambda launcher failure
- repeated runtime exceptions inside `run_pipeline.py`

### 2. Log-signature detection

Initial failure signatures to classify:

#### DB connectivity timeout
Observed patterns:
- `DB connection failed:`
- `timeout expired`
- `RuntimeError: Database not reachable`

Classification:
- `db_connect_timeout`

Likely interpretation:
- infra/network path issue between ECS task and RDS

#### Source download failure
Possible patterns:
- request timeout
- HTTP error
- batch downloader exception

Classification:
- `source_download_failure`

#### Parsing / extraction failure
Possible patterns:
- malformed Excel
- schema mismatch
- parser exception

Classification:
- `parse_or_extract_failure`

#### DB load failure
Possible patterns:
- SQL error
- constraint violation
- missing table/column

Classification:
- `db_load_failure`

#### Unknown terminal failure
Anything unclassified.

Classification:
- `unknown_terminal_failure`

---

## Freshness targets

The sentinel should also check whether expected outputs are fresh enough.

Initial checks:
- latest successful ingestion date
- whether target reporting tables were updated for the expected date window
- whether the reconcile period completed as intended

If ingestion failed or output date is stale beyond threshold, downstream outputs should be marked:
- **unsafe_to_trust**

---

## Alerting behavior

### Alert message requirements
Every alert should include:
- pipeline name
- job/run mode
- failure class
- short evidence summary
- trust label
- recommended next action
- link/path to repair instructions if available

### Example alert for current DB timeout case

> Mengxi ingestion alert: DB connectivity timeout. Observed repeated Postgres timeout to RDS on port 5432 from ECS task. Heuristic inference: likely infra/network reachability issue rather than SQL logic bug. Recommended action: inspect ECS subnet/VPC placement, RDS SG attachment, NACLs, and configured PGURL. See `docs/openclaw/MENGXI_DB_TIMEOUT_ALERTING_AND_CODEX_REPAIR.md`.

### Delivery options
Preferred order:
1. webhook / chat message
2. `ops.alerts` row in database when available
3. generated markdown handoff note when coding-heavy repair is needed

---

## Recommended trigger model

### Option A — event-driven
Trigger when:
- Lambda launcher returns failure
- ECS task stops unsuccessfully
- CloudWatch log pattern matches terminal failure

Preferred for production.

### Option B — periodic health sweep
Run every N minutes to check:
- last run status
- freshness lag
- latest alerts

Useful as a safety net.

### Best pattern
Use both:
- event-driven for fast failure alerts
- periodic sweep for missed/stale-state detection

---

## Recommended implementation shape

### Layer 1 — Detection
Possible sources:
- CloudWatch log group `/ecs/bess-mengxi-ingestion`
- Lambda launcher result
- ECS task status
- DB freshness query

### Layer 2 — Classification
Map raw failures into a small enum:
- `db_connect_timeout`
- `source_download_failure`
- `parse_or_extract_failure`
- `db_load_failure`
- `unknown_terminal_failure`

### Layer 3 — Response
For each failure class, define:
- alert text
- severity
- whether safe rerun is recommended
- whether downstream data should be quarantined
- whether Codex handoff should be created/updated

---

## Response policy

### `db_connect_timeout`
Severity:
- high

Trust label:
- observed failure + heuristic infra diagnosis

Recommended action:
- inspect VPC/subnet/SG/NACL/endpoint path
- do not assume loader code bug first

Downstream policy:
- mark dependent outputs unsafe if run did not complete

Codex handoff:
- yes, if recurring or unresolved

### `source_download_failure`
Severity:
- medium/high depending on recurrence

Recommended action:
- inspect source website availability, throttling, auth/session logic, request timings

Downstream policy:
- unsafe if target day not ingested

### `parse_or_extract_failure`
Severity:
- medium/high

Recommended action:
- inspect file format drift and parser assumptions

Codex handoff:
- likely yes

### `db_load_failure`
Severity:
- high

Recommended action:
- inspect table compatibility, schema drift, constraint violations, and loader SQL assumptions

Codex handoff:
- yes

### `unknown_terminal_failure`
Severity:
- high

Recommended action:
- capture logs and classify before rerun loops

---

## Interaction with the 4 agents

### Agent 4 — Reliability/Data Quality/Control
Owns the sentinel.

### Agent 3 — Trading/Dispatch/Execution
Must consume sentinel status before publishing monetisation conclusions.
If sentinel says data is stale/unsafe, Agent 3 should say so explicitly.

### Agent 2 — Portfolio/Risk
Should avoid firm P&L explain conclusions when ingestion for relevant dates is stale or incomplete.

### Agent 1 — Strategy
Should not treat stale operational outputs as fresh market evidence.

---

## Codex handoff rule

If repair work looks implementation-heavy, the sentinel should not improvise broad code changes.
It should instead create/update a narrow handoff note for Codex.

Recommended handoff location:
- `invest-etrm-intel/docs/openclaw/`

Examples:
- `MENGXI_DB_TIMEOUT_ALERTING_AND_CODEX_REPAIR.md`
- `MENGXI_NETWORK_CONNECTIVITY_RUNBOOK.md`
- `MENGXI_FAILURE_CLASSIFICATION_NOTES.md`

---

## Suggested file/module shape for implementation

This is a recommended direction, not a mandatory exact layout.

### Infra / monitoring
- Terraform for log metric filters / alarms / webhook wiring
- or a small OpenClaw/ops-side scheduled checker

### Service logic
- `services/ops/mengxi_failure_sentinel.py`
- `services/ops/mengxi_freshness_check.py`

### Reporting / persistence
- `ops.alerts`
- `ops.job_runs`
- `ops.data_freshness_status`

### Docs / runbooks
- `docs/openclaw/MENGXI_PROACTIVE_FAILURE_SENTINEL_SPEC.md`
- `docs/openclaw/MENGXI_DB_TIMEOUT_ALERTING_AND_CODEX_REPAIR.md`

---

## Minimal viable version

If implementing quickly, start with this narrow MVP:

1. detect terminal DB timeout failure
2. send a webhook alert
3. include run metadata + repair link
4. write/update a markdown handoff note for Codex
5. optionally track a simple stale/not-stale flag

That alone would materially improve ops.

---

## Good behavior rules

The sentinel should:
- be concise
- avoid duplicate spam
- avoid repeated alerts for the same unresolved incident unless state changes
- distinguish observed evidence from inference
- prefer a repair recommendation over blind rerun loops

The sentinel should not:
- open-endedly modify production code
- rewrite Terraform broadly
- claim certainty without evidence
- generate noisy alerts every few minutes for the same failure

---

## Success criteria

This proactive skill is successful if:
- ingestion failures are noticed quickly
- alerts are specific and useful
- downstream users know when data is unsafe
- Codex receives a clear repair package when coding is needed
- operators spend less time manually reading raw logs

---

## Initial recommendation

Implement this sentinel first before building more ambitious proactive behavior for the other 3 agents.

Reason:
- if reliability/data quality is weak, strategy/trading/portfolio agents will produce confident nonsense on stale or broken inputs.

This sentinel should be the first operational proactive skill in the 4-agent system.
