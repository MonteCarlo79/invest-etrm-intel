# Codex Desktop Execution Order

## Purpose

This file defines the recommended execution order for the Codex Desktop handoff task pack.

Use it to sequence implementation so that shared dependencies and operational workflows are handled in a sensible order.

Read this together with:
- `CODEX_DESKTOP_MASTER_BRIEF.md`
- `CODEX_AGENT.md`
- `PLATFORM_HANDOFF.md`

---

## Order of execution

## 1. `CODEX_DESKTOP_TASK_01_PNL_ATTRIBUTION_REBUILD.md`

Reason:
- likely touches existing reporting / attribution logic
- may define or clarify reusable marketdata/reporting patterns
- should be understood before downstream daily ops/report workflows are widened

## 2. `CODEX_DESKTOP_TASK_BESS_SETTLEMENT_INVOICE_INGESTION.md`

Reason:
- introduces structured settlement ingestion patterns
- likely affects raw/staging/normalized operational data flows
- may become a feeder into later operational and reporting workflows

## 3. `CODEX_DESKTOP_TASK_03_UPLOADER_BESS_MAP_DAILY_OPS.md`

Reason:
- likely depends on or interacts with uploader and mapped daily operating flows
- should reuse prior ingestion/reporting conventions where practical

## 4. `CODEX_DESKTOP_TASK_04_SPOT_REPORTS_INGESTION.md`

Reason:
- likely another ingestion/reporting workflow that should align with the same operational patterns
- execute after earlier reporting and ingestion conventions are clarified

---

## Execution rule

For each task in order:
1. inspect first
2. report findings
3. implement additive diffs only
4. commit cleanly
5. avoid mixing unrelated changes across tasks

If a blocking dependency is discovered that changes the sequence, document the dependency explicitly before deviating.

---

## Handoff/reporting expectation

At the end of each task, report:
- whether the next task can proceed cleanly
- any shared abstractions introduced
- any unresolved dependency that should affect the next task
