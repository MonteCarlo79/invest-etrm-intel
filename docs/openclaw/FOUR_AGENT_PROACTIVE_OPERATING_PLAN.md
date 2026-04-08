# FOUR AGENT PROACTIVE OPERATING PLAN

## Purpose

This document defines the practical operating plan for standing up the 4-agent model in a way that is useful, controlled, and compatible with the existing `bess-platform` repo.

It is intentionally operational.
It is not a greenfield multi-agent redesign.

---

## Core operating principle

Use **one orchestration shell** and **four logical business agents**.

### Orchestration shell
- OpenClaw

### Logical agents
1. Market Strategy & Investment Intelligence
2. Enterprise Portfolio, Risk & Capital Allocation
3. Trading, Dispatch & Execution
4. Platform Reliability, Data Quality & Control

The agents should not all write code or mutate infra independently.
They should operate through shared data/reporting objects and clear escalation paths.

---

## Why proactive behavior is needed

The platform is only trustworthy if it detects problems before downstream summaries become misleading.

A proactive operating layer is therefore needed to:
- detect pipeline/job failures early
- detect stale or incomplete data
- block or label unsafe downstream conclusions
- route repairs to Codex or an operator
- reduce manual log-watching

The first proactive capability should be built under:
- **Agent 4: Platform Reliability, Data Quality & Control**

---

## Operating roles

### OpenClaw
Use for:
- orchestration
- monitoring
- scheduling
- alert summarisation
- repair handoff creation
- branch-safe operational work

Do not use as an unrestricted coding engine for large repo rewrites.

### Codex Desktop
Use for:
- implementation-heavy fixes
- service modules
- Terraform patches
- additive code changes
- branch-owned repair work

### GPT / Claude Code
Use for:
- architecture
- business logic framing
- high-stakes review
- ambiguous design decisions

---

## Shared truth objects

The 4-agent operating loop should rely on shared state, not chat memory.

Preferred backbone objects:
- `ops.job_runs`
- `ops.alerts`
- `ops.data_freshness_status`
- `agent.agent_summaries`
- `agent.findings`
- `reports.generated_reports`
- `reports.bess_asset_daily_scenario_pnl`
- `reports.bess_asset_daily_attribution`

If these are not fully implemented yet, start with lightweight substitutes and add proper tables incrementally.

---

## Rollout order

### Phase 1 — Agent 4 first
Build the proactive reliability/data-quality layer first.

Reason:
- without it, the other 3 agents may produce confident nonsense on stale or broken inputs

### Phase 2 — Agent 3 next
Once ingestion and freshness checks exist, operationalise Trading / Dispatch / Execution outputs.

### Phase 3 — Agent 2
Build Portfolio / Risk summaries on top of trustworthy realised-vs-plan and attribution outputs.

### Phase 4 — Agent 1
Only then scale Strategy / Investment Intelligence on top of validated market and operational evidence.

---

## Agent-by-agent operating plan

# Agent 4 — Platform Reliability, Data Quality & Control

## Mission
Keep the platform operationally trustworthy.

## Inputs
- ECS / Lambda / scheduled job status
- CloudWatch logs
- job exit status
- DB freshness checks
- row-count/completeness checks
- known failure signatures

## Outputs
- failure alerts
- rerun recommendations
- quarantine recommendations
- data freshness flags
- operational exception summaries
- repair briefs for Codex when coding-heavy work is needed

## Proactive behavior
This is the first proactive agent.

Initial proactive responsibilities:
- detect ingestion/job failures quickly
- classify failures into a small set of categories
- avoid duplicate alert spam
- mark downstream outputs unsafe when needed
- route known incident patterns to known runbooks

## Current known runbook
For the known Mengxi ingestion DB timeout pattern tied to ECS↔RDS SG drift:
- first remediation step:
  - rerun `terraform apply`
  - in `bess-platform/infra/terraform/mengxi-ingestion/`

That should be treated as an operational runbook action for this specific known failure class.

## MVP implementation target
- event-driven failure detection and/or periodic health sweep
- simple stale/not-stale status
- concise alert delivery
- repair note generation

---

# Agent 3 — Trading, Dispatch & Execution

## Mission
Explain and improve monetisation performance.

## Inputs
- refreshed market and dispatch data
- realised outcome data
- scenario tables
- settlement / reconciliation outputs
- Agent 4 trust/health status

## Outputs
- realised-vs-theoretical capture summaries
- monetisation leakage diagnosis
- strategy comparison notes
- rerun/challenge recommendations

## Proactive behavior
This agent should only be mildly proactive at first.

It may:
- publish daily execution summaries
- flag unusual realised-vs-theoretical gaps
- recommend strategy/backtest review

It must not act as if data is reliable when Agent 4 says otherwise.

## Hard rule
If Agent 4 marks relevant data stale or unsafe:
- Agent 3 must explicitly say the conclusion is unsafe/limited
- Agent 3 must not present a clean monetisation narrative as fact

---

# Agent 2 — Enterprise Portfolio, Risk & Capital Allocation

## Mission
Explain enterprise and asset-level performance, risk concentration, and capital attention shifts.

## Inputs
- daily/periodic P&L outputs
- attribution outputs
- concentration/exposure data
- Agent 3 findings
- Agent 4 trust/health status

## Outputs
- P&L explain summaries
- realised-vs-plan summaries
- concentration/risk notes
- capital allocation framing notes

## Proactive behavior
Later-stage proactive behavior may include:
- daily portfolio health notes
- unusual concentration alerts
- realised-vs-plan divergence alerts

But it should only run on top of reliable upstream data.

## Hard rule
If freshness or ingestion is broken:
- this agent should reduce confidence and say so explicitly

---

# Agent 1 — Market Strategy & Investment Intelligence

## Mission
Identify where value pools and structural opportunities exist.

## Inputs
- market data
- policy / report documents
- opportunity screens
- selected downstream operational learnings
- Agent 4 trust/health status

## Outputs
- opportunity screening memos
- attractiveness rankings
- structural regime-shift notes
- shortlist recommendations

## Proactive behavior
This should be the least proactive at the beginning.

Later it can:
- surface policy changes
- note market regime shifts
- generate periodic opportunity scans

But it must not overfit on stale or weak downstream data.

---

## Shared trust-label rule

Every agent output must distinguish:
- observed
- proxy-based
- heuristic inference

This is mandatory.

---

## Shared branch-control rule

One AI tool writes to one active branch at a time.

### OpenClaw
- orchestration / monitoring / small operational edits
- docs / handoff / validation branches are fine

### Codex
- principal implementation branch owner

### Rule
Do not let multiple tools compete on the same active write branch.

---

## Trigger matrix

### Agent 4 triggers
- on ECS/Lambda/job failure
- on stale freshness threshold breach
- on-demand operator check
- periodic health sweep

### Agent 3 triggers
- after successful ingestion / refresh
- daily scheduled execution summary
- on unusual realised-vs-theoretical variance

### Agent 2 triggers
- after attribution / P&L refresh succeeds
- daily or periodic enterprise summary

### Agent 1 triggers
- on scheduled market scan cadence
- when major regime/event/policy input lands
- after enough validated downstream evidence accumulates

---

## Minimum trust-gating contract

Before Agents 1–3 publish a meaningful conclusion, they should check whether Agent 4 considers the relevant data:
- fresh
- complete enough
- safe to trust

Minimum states:
- `healthy`
- `degraded`
- `unsafe_to_trust`

Behavior:
- `healthy` → normal output
- `degraded` → output allowed but caveated
- `unsafe_to_trust` → block or sharply limit conclusion

---

## Recommended proactive implementation path

### Step 1
Implement the Mengxi proactive failure sentinel under Agent 4.

References:
- `docs/openclaw/MENGXI_PROACTIVE_FAILURE_SENTINEL_SPEC.md`
- `docs/openclaw/MENGXI_DB_TIMEOUT_ALERTING_AND_CODEX_REPAIR.md`
- `docs/openclaw/MENGXI_ECS_RDS_CONNECTIVITY_REPAIR_BRIEF.md`

### Step 2
Add a simple shared health/freshness flag consumed by downstream agents.

### Step 3
Add one daily Trading/Dispatch summary that respects that flag.

### Step 4
Add Portfolio/Risk summary generation on top of validated attribution outputs.

### Step 5
Add Strategy summaries only after the first 3 layers are stable.

---

## Recommended cadence

### Near real-time / event-driven
- Agent 4 failure alerts

### Every 15–30 minutes
- Agent 4 health/freshness sweep

### Daily
- Agent 3 execution summary if healthy
- Agent 2 portfolio/risk summary if healthy enough

### Weekly or slower initially
- Agent 1 strategic opportunity summary

---

## Success criteria

This operating plan is working if:
- failures are caught early
- stale data is detected before misleading summaries go out
- Codex gets clear repair briefs instead of vague requests
- Trading / Portfolio / Strategy outputs explicitly respect data trust levels
- the system behaves like an operating platform, not 4 chatbots improvising in parallel

---

## Immediate next milestone

Build the Agent 4 proactive MVP and make the other 3 agents consume its trust signal.

That is the shortest path to a real 4-agent operating loop.
