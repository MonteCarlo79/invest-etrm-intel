# Codex Desktop Task 03: Uploader + BESS Map Daily Ops

## Purpose

This file is a direct handoff brief for local Codex Desktop running on Windows.

Use it when Codex Desktop is the active implementation engine for the `invest-etrm-intel` / `bess-platform` repo.

Read this together with:
- `CODEX_DESKTOP_MASTER_BRIEF.md`
- `CODEX_AGENT.md`
- `PLATFORM_HANDOFF.md`
- `docs/agents/codex/investment_trading_asset_intelligence_sop v2.md`

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
- `feature/uploader-bess-map-daily-ops`
  or
- `fix/uploader-bess-map-daily-ops`

---

## Business objective

Strengthen the uploader and BESS map daily operations path so it is safer and more usable for recurring operational workflows.

The target outcome is an additive, production-conscious improvement that:
- preserves current uploader and BESS map patterns
- improves daily operating usability
- makes data flow and failures clearer
- supports repeatable daily operational execution

---

## Required delivery sequence

### Phase 1 — inspect and report first

Before coding, inspect and report:
1. existing relevant files/modules
   - `apps/uploader`
   - `services/bess_map`
   - `config/projects/bess_map.yaml`
   - any daily job/runbook/config paths already present
2. recommended implementation approach
3. recommended config/schema/interface changes if any
4. exact files to add/modify

### Phase 2 — implement narrow diffs

After inspection, implement narrow additive diffs only.

---

## Required work packages

## A. Source analysis

Inspect current repo code related to:
- uploader flows
- file intake / validation flows
- BESS map services
- project config related to BESS map
- daily operational paths already present
- any current monitoring/error surfacing patterns

Document how uploader and BESS map currently interact, if they do.

---

## B. Target implementation shape

Recommend and implement only the minimum necessary additive improvements for daily ops, which may include:
- clearer uploader flow wiring
- safer daily operation hooks
- better validation or status surfacing
- more explicit config usage
- better operational logs or result visibility

Do not rewrite the uploader or BESS map architecture broadly.

---

## C. Daily ops requirements

The resulting implementation should support recurring operational use with:
- clear operator steps
- clear failure visibility
- safe rerun behavior where applicable
- compatibility with current uploader and marketdata flows
- minimal disruption to existing usage patterns

---

## D. Required deliverables

Codex should produce, if justified by repo patterns:
- uploader/service/config changes
- small runbook updates or inline documentation
- validation/test plan
- concise ops-oriented summary

---

## Expected final report from Codex

At completion, provide:
1. files added
2. files modified
3. daily ops improvement summary
4. assumptions
5. runtime risks
6. test/check plan
7. any items still requiring AWS/runtime validation

---

## Suggested execution prompt for local Codex Desktop

> Read `CODEX_DESKTOP_MASTER_BRIEF.md`, `CODEX_AGENT.md`, `PLATFORM_HANDOFF.md`, and `CODEX_DESKTOP_TASK_03_UPLOADER_BESS_MAP_DAILY_OPS.md`. First inspect the repo and report the current uploader/BESS map daily ops structure, recommended additive implementation shape, and exact files to add/modify. Then implement narrow, production-conscious improvements for recurring daily operations without broad rewrite. Finish with a concise implementation report.
