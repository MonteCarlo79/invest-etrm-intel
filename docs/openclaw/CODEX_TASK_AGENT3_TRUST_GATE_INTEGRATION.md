# CODEX TASK — AGENT 3 TRUST GATE INTEGRATION

## Purpose

Implement the next narrow step in the 4-agent operating model:
make **Agent 3 (Trading, Dispatch & Execution)** consume the validated **Agent 4 (Platform Reliability, Data Quality & Control)** trust state before publishing meaningful downstream execution conclusions.

This task should convert Agent 4 from a standalone sentinel into an operational gate for Trading / Dispatch outputs.

---

## Why this task matters

Agent 4 is now validated as an MVP.
That means the next practical step is to ensure downstream analysis does not present stale or broken data as trustworthy.

Agent 3 is the right first downstream consumer because it sits closest to ingestion freshness and realised-vs-theoretical operational analysis.

---

## References

Read these first:
- `docs/openclaw/FOUR_AGENT_PROACTIVE_OPERATING_PLAN.md`
- `docs/openclaw/MENGXI_PROACTIVE_FAILURE_SENTINEL_SPEC.md`
- `docs/openclaw/CODEX_TASK_AGENT4_PROACTIVE_MVP.md`
- `docs/openclaw/MENGXI_AGENT4_MVP_VALIDATION_RUNBOOK_SANITIZED.md`

Context:
- Agent 4 proactive MVP has already been implemented and validated
- it persists status in `ops.mengxi_agent4_status`
- trust states include:
  - `healthy`
  - `degraded`
  - `unsafe_to_trust`

---

## Scope

### In scope
Build the smallest useful integration so that Agent 3 checks Agent 4 trust state before presenting a meaningful execution/trading conclusion.

### Out of scope
- integrating all 4 agents at once
- dashboard redesign
- broad refactors of trading logic
- replacing existing execution/report generation paths
- adding a large rules engine

---

## Required behavior

### 1. Agent 3 must read Agent 4 trust state
Agent 3 should read from the validated Agent 4 status source:
- `ops.mengxi_agent4_status`

At minimum it should consume:
- `pipeline_name`
- `trust_state`
- `failure_class`
- `updated_at`
- any summary/recommended-action fields that help explain the gate decision

### 2. Gate behavior by trust state

#### `healthy`
- Agent 3 may proceed normally

#### `degraded`
- Agent 3 may proceed, but must clearly caveat the output
- output should explicitly say reliability/freshness is degraded

#### `unsafe_to_trust`
- Agent 3 must not present a normal execution/trading conclusion as if it were trustworthy
- it should instead:
  - block the normal output path, or
  - return a sharply limited output that says the data is unsafe

Keep this implementation minimal and consistent with existing repo patterns.

### 3. Evidence labeling remains mandatory
Any resulting output should still distinguish:
- observed
- proxy-based
- heuristic inference

### 4. Do not invent a fake healthy default
If Agent 4 status is missing or unreadable, do not silently assume healthy.
Prefer a cautious fallback such as degraded/unknown, with explicit explanation.

---

## Preferred implementation approach

Use the smallest additive integration point near current Agent 3 / trading execution output logic.

Expected pattern:
1. add a small helper/service to read current Agent 4 status
2. call it before downstream output is rendered/published
3. branch behavior based on `trust_state`
4. keep the normal path intact when `healthy`

Possible implementation locations depend on current repo conventions, but likely near:
- `services/trading/...`
- `apps/trading/...`
- or whichever current Mengxi execution output path is the narrowest trustworthy interception point

Inspect first; do not guess.

---

## Design rules

### 1. Additive only
Do not rewrite trading/dispatch logic.
Insert a gate, not a new framework.

### 2. Narrowest integration point wins
Prefer one clean interception point rather than sprinkling trust checks everywhere.

### 3. Honest fallback behavior
If Agent 4 status is unavailable, do not silently continue with high confidence.

### 4. No UI theatrics
This is an operational reliability gate, not a “smart assistant” layer.
Keep messages plain and useful.

---

## Suggested implementation plan

### Step 1 — inspect current Agent 3 output path
Identify the narrowest code path where Trading / Dispatch conclusions are assembled or surfaced.

### Step 2 — add a small Agent 4 status reader
Read the latest status for Mengxi from `ops.mengxi_agent4_status`.

### Step 3 — implement trust-gate branching
Behavior:
- `healthy` → unchanged normal path
- `degraded` → normal path + visible caveat/warning
- `unsafe_to_trust` → block or sharply constrain normal output
- missing status → cautious fallback

### Step 4 — keep output explicit
For degraded/unsafe cases, include a short explanation like:
- current Mengxi data reliability state is degraded
- current Mengxi data is unsafe to trust due to recent reliability failure

### Step 5 — run the narrowest relevant checks
Whatever minimal checks/tests are practical in this repo, run them and report results.

---

## Acceptance criteria

This task is successful if:
1. Agent 3 reads `ops.mengxi_agent4_status`
2. `healthy` preserves normal behavior
3. `degraded` visibly caveats output
4. `unsafe_to_trust` prevents a normal-trust execution conclusion
5. missing/unreadable Agent 4 status does not silently become healthy
6. changes remain narrow and additive

---

## Deliverables required from Codex

When done, report:
- files changed
- the exact integration point chosen
- how trust-state branching works
- assumptions made
- checks run
- what still needs AWS/runtime validation

---

## What not to do

- do not integrate Agents 1, 2, and 3 all at once
- do not redesign trading dashboards wholesale
- do not create a sprawling agent orchestration layer
- do not hide data reliability problems behind normal-looking output

---

## Preferred outcome

At the end of this task, the system should have the first real downstream trust gate:
- Agent 4 detects reliability issues
- Agent 3 respects that status before presenting conclusions

That is the next concrete step toward a real 4-agent operating loop.
