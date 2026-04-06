# Codex Desktop Task 01: PnL Attribution Rebuild

## Purpose

This file is a direct handoff brief for local Codex Desktop running on Windows.

Use it when Codex Desktop is the active implementation engine for the `invest-etrm-intel` / `bess-platform` repo.

Read this together with:
- `CODEX_DESKTOP_MASTER_BRIEF.md`
- `CODEX_AGENT.md`
- `PLATFORM_HANDOFF.md`
- `docs/agents/codex/investment_trading_asset_intelligence_sop v2.md`

Follow repo governance and keep changes narrow, additive, production-conscious, and reviewable.

---

## Operating constraints

- inspect first before implementation
- additive changes only
- production-ready
- no broad rewrite
- preserve current repo patterns
- preserve `marketdata` compatibility
- keep changes narrow and reviewable
- one active write branch for Codex only

Recommended branch name:
- `feature/pnl-attribution-rebuild`
  or
- `fix/pnl-attribution-rebuild`

---

## Business objective

Rebuild or tighten the BESS PnL attribution path so that it becomes clearer, more reviewable, and safer for recurring operational use without broad redesign.

The target outcome is a production-conscious attribution flow that:
- preserves current business semantics as closely as practical
- improves structure and maintainability
- supports recurring operational use
- keeps attribution outputs queryable and explainable
- stays compatible with current reporting / marketdata patterns

---

## Required delivery sequence

### Phase 1 — inspect and report first

Before coding, inspect and report:
1. existing relevant files/modules
   - PnL attribution SQL
   - report tables
   - any app/service wiring using attribution outputs
   - scheduling / job / loader paths if relevant
2. recommended implementation approach
3. recommended schema/table/view changes if any
4. exact files to add/modify

Do this inspection before implementation.

### Phase 2 — implement narrow diffs

After inspection, implement the solution with narrow additive diffs only.

---

## Required work packages

## A. Source analysis

Inspect current repo code related to:
- PnL attribution logic
- `db/ddl/reports/bess_pnl_attribution.sql`
- any app/report/service usage of attribution outputs
- any refresh/build scripts already present
- existing reporting conventions that attribution should follow

Document the current attribution flow and identify the minimum safe rebuild scope.

---

## B. Target implementation shape

Produce a recommended rebuild that may include, where justified by repo patterns:
- cleaner SQL structure
- better decomposition into intermediate steps/views/tables
- more explicit field naming
- clearer lineage from raw inputs to attribution outputs
- more explicit treatment of assumptions, allocations, or derived components

Do not redesign unrelated reporting architecture.

---

## C. Output requirements

The rebuilt attribution path should aim to provide:
- consistent and queryable attribution outputs
- explicit component logic where possible
- predictable refresh behavior
- compatibility with current downstream consumers
- clear points for validation and debugging

If extra metadata or lineage columns are needed, add them only if aligned with repo patterns.

---

## D. Operational requirements

The rebuilt flow should support daily operation by non-developers as far as practical.

Required properties:
- understandable refresh path
- clear failure points
- clear test/validation checks
- minimized rerun ambiguity
- compatibility with existing report refresh workflows

---

## E. Required deliverables

Codex should produce:
- modified SQL / DDL / migration files as needed
- supporting service/query helpers if justified
- small runbook or inline documentation if needed
- validation/test plan
- concise explanation of what changed and why

---

## Expected final report from Codex

At completion, provide:
1. files added
2. files modified
3. attribution rebuild summary
4. assumptions
5. runtime risks
6. test/check plan
7. any items still requiring DB/AWS/runtime validation

---

## Suggested execution prompt for local Codex Desktop

> Read `CODEX_DESKTOP_MASTER_BRIEF.md`, `CODEX_AGENT.md`, `PLATFORM_HANDOFF.md`, and `CODEX_DESKTOP_TASK_01_PNL_ATTRIBUTION_REBUILD.md`. First inspect the repo and report the current PnL attribution files/modules, recommended rebuild shape, and exact files to add/modify. Then implement a narrow, additive, production-conscious rebuild of the PnL attribution path while preserving current repo patterns and downstream compatibility. Finish with a concise implementation report.
