# Codex Desktop Master Brief

## Purpose

This file is the root handoff brief for local Codex Desktop working on the `invest-etrm-intel` / `bess-platform` repository.

Use it when Codex Desktop is the active implementation engine and AWS OpenClaw is acting only as the orchestrator, reviewer handoff manager, or remote operational shell.

Read this together with:
- `CODEX_AGENT.md`
- `PLATFORM_HANDOFF.md`
- `CODEX_DESKTOP_EXECUTION_ORDER.md`
- the task-specific `CODEX_DESKTOP_TASK_*.md` files
- `docs/agents/codex/investment_trading_asset_intelligence_sop v2.md`

---

## Universal constraints

Apply these constraints to every Codex Desktop task in this pack:

- inspect first before implementation
- additive changes only
- production-ready
- no broad rewrite
- preserve current repo patterns
- preserve `marketdata` compatibility
- keep changes narrow and reviewable
- one AI tool writes to one branch at a time
- do not overlap active write branches with OpenClaw or other tools
- prefer reusable service-layer changes over page-only logic
- keep operational safety and rerun safety explicit

---

## Required execution pattern

For each task:

### Phase 1 — inspect and report first

Before coding, inspect the repo and report:
1. existing relevant files/modules
2. recommended implementation shape
3. recommended schema / interfaces where relevant
4. exact files to add/modify
5. assumptions and ambiguities

### Phase 2 — implement narrow diffs

Only after the inspection summary, implement the task with narrow additive diffs.

### Phase 3 — final report

At completion, provide:
1. files added
2. files modified
3. patch summary
4. assumptions
5. runtime risks
6. test/check plan
7. items still requiring environment or AWS validation

---

## Task pack included here

1. `CODEX_DESKTOP_TASK_01_PNL_ATTRIBUTION_REBUILD.md`
2. `CODEX_DESKTOP_TASK_BESS_SETTLEMENT_INVOICE_INGESTION.md`
3. `CODEX_DESKTOP_TASK_03_UPLOADER_BESS_MAP_DAILY_OPS.md`
4. `CODEX_DESKTOP_TASK_04_SPOT_REPORTS_INGESTION.md`
5. `CODEX_DESKTOP_EXECUTION_ORDER.md`

If task numbering appears incomplete, do not invent hidden tasks. Work only from the files present.

---

## Standard branch discipline

Use a dedicated branch per task or a carefully controlled docs/coordination branch only when the work is documentation-only.

Recommended task branch patterns:
- `feature/...`
- `fix/...`
- `refactor/...`

If a task-specific branch already exists and is clearly the intended target, verify ownership before writing.

---

## Suggested execution prompt for local Codex Desktop

> Read `CODEX_AGENT.md`, `PLATFORM_HANDOFF.md`, `CODEX_DESKTOP_MASTER_BRIEF.md`, `CODEX_DESKTOP_EXECUTION_ORDER.md`, and the current task file you are executing. First inspect the repo and report the relevant files/modules, recommended implementation shape, and exact files to add/modify. Then implement using narrow, additive, production-conscious diffs only. Finish with a concise implementation report.
