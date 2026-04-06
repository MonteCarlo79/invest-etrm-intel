# Codex Desktop Task 04: Spot Reports Ingestion

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
- `feature/spot-reports-ingestion`
  or
- `fix/spot-reports-ingestion`

---

## Business objective

Implement or tighten a spot reports ingestion path that is safe for recurring operational use and aligned with current repo conventions.

The target outcome is an additive ingestion workflow that:
- ingests spot reports from their existing source format/path
- normalizes them into the repo’s current reporting/marketdata patterns as closely as practical
- makes failures and rerun behavior explicit
- avoids broad redesign

---

## Required delivery sequence

### Phase 1 — inspect and report first

Before coding, inspect and report:
1. existing relevant files/modules
   - current spot report readers or loaders if present
   - current reporting ingestion patterns
   - target tables / staging paths / services if present
2. recommended schema / interface approach
3. recommended parser/ingestion architecture
4. exact files to add/modify

### Phase 2 — implement narrow diffs

After inspection, implement narrow additive diffs only.

---

## Required work packages

## A. Source analysis

Inspect current repo code related to:
- report ingestion
- any spot report-specific files/modules/config
- uploader/loader patterns that may be reusable
- marketdata/report persistence patterns
- staging/normalization patterns already used elsewhere

Document the closest existing ingestion pattern that should be reused.

---

## B. Recommended implementation shape

Design an additive ingestion flow that may include:
- raw file/input registration if appropriate
- staging outputs
- normalized outputs
- parser or reader helpers
- clear status and error handling
- rerun-safe behavior

Use the narrowest implementation that matches existing repo patterns.

---

## C. Operational requirements

The resulting ingestion path should support recurring use with:
- explicit parse/ingestion status
- explicit error visibility
- idempotent or clearly rerunnable behavior
- documented operator steps
- minimal disruption to current workflows

---

## D. Required deliverables

Codex should produce, if justified by repo patterns:
- parser/reader modules
- DDL/migration/schema additions if necessary
- ingestion runner or integration wiring
- runbook notes
- test/check plan

---

## Expected final report from Codex

At completion, provide:
1. files added
2. files modified
3. ingestion summary
4. assumptions
5. runtime risks
6. test/check plan
7. any items still requiring DB/AWS/runtime validation

---

## Suggested execution prompt for local Codex Desktop

> Read `CODEX_DESKTOP_MASTER_BRIEF.md`, `CODEX_AGENT.md`, `PLATFORM_HANDOFF.md`, and `CODEX_DESKTOP_TASK_04_SPOT_REPORTS_INGESTION.md`. First inspect the repo and report the current spot-reports-related files/modules, recommended ingestion shape, and exact files to add/modify. Then implement a narrow, additive, production-conscious ingestion path aligned with current repo patterns. Finish with a concise implementation report.
