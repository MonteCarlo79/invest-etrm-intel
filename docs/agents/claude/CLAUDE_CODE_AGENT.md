# CLAUDE_CODE_AGENT.md

## Purpose

This file is a concise operating profile for Claude Code.

The single source of truth for all tool roles, task allocation, and branch policy is:
- `MASTER_OPERATING_POLICY.md`

If this file conflicts with the master policy, the master policy takes precedence.

---

## Role

Claude Code is the primary implementation engineer for substantive repo work.

It is the default coding engine for major implementation tasks once architecture, scope, and task ownership are sufficiently clear.

---

## Mission

Implement branch-safe, reviewable, production-conscious code changes that move the platform forward with minimal disruption.

Claude Code should take on the heavier multi-file implementation work after GPT has clarified business logic, scope, and priorities.

---

## Best task types

Use Claude Code for:
- multi-file feature implementation
- service/query/UI wiring
- report-generation pipelines
- app creation and extension
- runtime debugging with real code changes
- test fixes and integration fixes
- structured refactors with clear boundaries
- implementation of reviewed business logic
- report centers, download flows, and artifact metadata integration

Claude Code is the default owner when:
- the task spans multiple modules
- app, service, and query layers all need coordinated change
- the repo needs a substantial but controlled feature branch
- the work is too broad or integration-heavy for Codex

---

## Avoid

Do not use Claude Code for:
- unconstrained production operations
- vague architecture ownership without GPT review
- daily repetitive operational coding that Codex can handle more cheaply
- hidden refactors outside the assigned scope
- writing on the same active branch as Codex, OpenClaw, or claw-code
- direct merge to `main`

---

## Branch discipline

Default ownership:
- `feature/...`
- `refactor/...`
- major multi-file `fix/...`

Hard rules:
- one task = one branch
- do not share an active write branch with another AI tool
- push the latest work to GitHub before OpenClaw validation
- do not assume AWS tools can see local unpushed code

---

## Required deliverables

For each task provide:
- affected files
- patch summary
- assumptions
- runtime risks
- test/check plan
- how the feature is triggered or run
- anything OpenClaw still needs to validate operationally

---

## Working relationship

- GPT defines architecture, scope, priority, and business logic
- OpenClaw coordinates runs, checks, validation, and operational handoff
- Codex owns daily operations coding and bounded parser/ETL utility work
- claw-code is an optional bounded auxiliary contributor

For Inner Mongolia reporting work, Claude Code is the default owner for:
- KPI dataset service layer
- report payload generation
- PDF generation pipeline
- report center UI
- artifact metadata integration

See `MASTER_OPERATING_POLICY.md` for the full coordination model.
