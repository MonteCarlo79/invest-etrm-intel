# CODEX TASK — AGENT 4 PROACTIVE MVP

## Purpose

Implement the smallest useful **Agent 4: Platform Reliability, Data Quality & Control** proactive MVP for the Mengxi / BESS operating loop.

This task should create a narrow, production-conscious reliability layer that:
- detects important ingestion failures
- classifies them into a few useful categories
- checks whether data is stale/unsafe
- emits a clear operational status that downstream agents can respect

This is not a request to build a giant autonomous agent system.

---

## Why this task matters

The 4-agent operating model only works if the system knows when its own data is broken or stale.

Right now the highest-value proactive capability is under Agent 4.
Without it:
- Trading / Dispatch can produce false confidence on stale data
- Portfolio / Risk can explain incomplete P&L
- Strategy can overinterpret weak evidence

So the first live proactive agent should be a **reliability/freshness sentinel**.

---

## References

Read these first:
- `docs/openclaw/FOUR_AGENT_PROACTIVE_OPERATING_PLAN.md`
- `docs/openclaw/MENGXI_PROACTIVE_FAILURE_SENTINEL_SPEC.md`
- `docs/openclaw/MENGXI_DB_TIMEOUT_ALERTING_AND_CODEX_REPAIR.md`
- `docs/openclaw/MENGXI_ECS_RDS_CONNECTIVITY_REPAIR_BRIEF.md`

---

## Scope

### In scope
Build the smallest useful operational MVP that can:
1. detect terminal Mengxi ingestion failures
2. classify failures into a small enum
3. check a simple freshness/trust state
4. expose a status usable by downstream agents/services
5. avoid noisy duplicate alerts

### Out of scope
- broad platform redesign
- building all 4 agents at once
- replacing existing schedules
- full enterprise monitoring framework
- complex ML/anomaly systems
- uncontrolled repo-wide refactor

---

## MVP target behavior

### A. Failure classification
Support at least these failure classes:
- `db_connect_timeout`
- `source_download_failure`
- `parse_or_extract_failure`
- `db_load_failure`
- `unknown_terminal_failure`

It is acceptable if initial classification is simple pattern matching with clear logic.

### B. Freshness / trust state
Produce a minimal health state for the relevant Mengxi workflow, such as:
- `healthy`
- `degraded`
- `unsafe_to_trust`

This can be implemented simply at first.

Example logic:
- latest run succeeded recently and expected date landed → `healthy`
- non-terminal warning / mild lag → `degraded`
- terminal failure or stale beyond threshold → `unsafe_to_trust`

### C. Alert deduplication
Do not spam repeated identical alerts every sweep.
At minimum, suppress duplicate alerts for the same unresolved incident signature within a reasonable window.

### D. Downstream-consumable status
Expose a small structured output that downstream agent code can read.
This could be:
- a DB row
- a JSON file/state file
- a small service function result
- a table/view if already consistent with repo patterns

Keep it simple and additive.

---

## Preferred implementation shape

Use repo conventions and inspect nearby modules first.

A reasonable additive shape could be:

### Service modules
- `services/ops/mengxi_failure_sentinel.py`
- `services/ops/mengxi_freshness_check.py`
- optionally a tiny shared helper for incident fingerprinting / classification

### Persistence / state
Prefer one of these, in order of practicality:
1. existing/additive ops tables if already available or easy to add safely
2. a lightweight structured state output that OpenClaw/ops code can read

### Docs
If you introduce a new operator-facing command or entrypoint, document it briefly under:
- `docs/openclaw/`

---

## Required design rules

### 1. Keep scope narrow
This should be an MVP.
Do not use this task to redesign all monitoring.

### 2. Evidence-first
Any generated summary/status should distinguish:
- observed
- proxy-based
- heuristic inference

### 3. Additive only
Preserve existing jobs and schedules.
Wrap or monitor them; do not replace them outright.

### 4. ECS-friendly / stateless where possible
Avoid desktop-only assumptions.
Do not rely on local manual state as the system of record unless clearly temporary and documented.

### 5. Respect known runbook
For the specific known Mengxi DB timeout / SG-drift pattern, the operational runbook now is:
- rerun `terraform apply`
- in `bess-platform/infra/terraform/mengxi-ingestion/`

Do not hardcode this as a magical universal fix, but allow the failure classifier / operator summary to reference it when the pattern matches.

---

## Suggested implementation plan

### Step 1 — inspect current repo patterns
Inspect:
- any existing ops / monitoring / alert patterns
- current Mengxi ingestion module behavior
- whether `ops.*` tables already exist or are partially scaffolded

### Step 2 — implement failure classification
Use terminal log/error strings initially.
Example patterns:
- `RuntimeError: Database not reachable` + `timeout expired` → `db_connect_timeout`
- downloader/request errors → `source_download_failure`
- parser/schema errors → `parse_or_extract_failure`
- SQL/load exceptions → `db_load_failure`
- else → `unknown_terminal_failure`

### Step 3 — implement freshness check
Define a simple freshness rule for Mengxi outputs.
Use the smallest reliable indicator available in the repo/database.
If there is ambiguity, document it rather than inventing business logic.

### Step 4 — implement deduplicated alert/state emission
Emit a structured result containing at least:
- workflow name
- state
- failure class if any
- observed summary
- recommended next action
- updated timestamp

### Step 5 — expose one runnable entrypoint
Provide one clean entrypoint Codex can describe and ops can call.
For example:
- a Python script/module entrypoint
- a small ECS-friendly command
- or a callable service wrapper

---

## Acceptance criteria

The MVP is successful if it can do all of the following:

1. recognize a terminal Mengxi DB timeout incident as `db_connect_timeout`
2. produce a trust state of at least `unsafe_to_trust` for that incident
3. emit or persist a structured operational status
4. avoid duplicate-alert spam for the same unresolved incident
5. provide a concise next-action recommendation
6. remain additive and branch-safe

---

## Nice-to-have, not required

- writing status into `ops.alerts` / `ops.job_runs` / `ops.data_freshness_status`
- CloudWatch integration helpers
- richer incident fingerprints
- severity levels beyond the core state model
- hooks for Agent 3/2/1 consumers

These are good follow-ups, not MVP blockers.

---

## Deliverables required from Codex

When done, report:
- files changed
- implementation summary
- assumptions made
- any unresolved data-policy/freshness ambiguities
- checks run
- what still needs operational validation on AWS

---

## What not to do

- do not build all 4 agents in this task
- do not invent a large agent framework
- do not widen into unrelated infra cleanup
- do not claim stale data is healthy
- do not hide heuristic guesses as observed fact

---

## Preferred outcome

At the end of this task, the platform should have the beginnings of a real Agent 4 operating layer:
- small
- useful
- evidence-based
- easy to extend later

That is the right first step for the 4-agent system.
