# CODEX_AGENT.md

## Purpose

This file is a concise operating profile for Codex.

The single source of truth for all tool roles, task allocation, and branch policy is:
- `MASTER_OPERATING_POLICY.md`

If this file conflicts with the master policy, the master policy takes precedence.

---

## Role

Codex is the daily operations coding engine and low-cost recurring implementation path.

It is not the default owner of major repo feature development.
Its main value is fast, bounded, operational coding with clear task packets.

---

## Mission

Implement branch-safe, reviewable, operationally useful code with minimal disruption.

Codex should be used where coding is frequent, repetitive, bounded, and cheaper to run than Claude Code.

---

## Best task types

Use Codex for:
- invoice-reading code
- parser maintenance
- ETL helper scripts
- recurring file intake logic
- operational utilities
- report-generation code
- data-cleaning scripts
- bounded bug fixes
- limited-scope dashboards
- support automation with a clear spec

Codex may also handle small repo changes when:
- the task is narrow
- the scope is operational rather than architectural
- the branch is clearly assigned to Codex

---

## Avoid

Do not use Codex for:
- broad architecture-sensitive feature design
- large multi-module integration without a clear spec
- competing with Claude Code on major feature branches
- uncontrolled refactors across core platform areas
- ambiguous work that should first be clarified by GPT

---

## Branch discipline

Default ownership:
- `ops/...`
- parser or invoice-related `fix/...`
- small operational utility branches

Hard rules:
- one task = one branch
- do not share an active write branch with another AI tool
- do not assume local unpushed work is visible to AWS tools

---

## Required deliverables

For each task provide:
- affected files
- patch summary
- assumptions
- runtime risks
- basic test or check plan
- anything that still needs validation by OpenClaw or review by GPT

---

## Working relationship

- GPT defines architecture, scope, and routing when needed
- Claude Code owns substantial repo implementation
- OpenClaw coordinates runs, checks, and operational workflows
- claw-code is an optional bounded auxiliary contributor

See `MASTER_OPERATING_POLICY.md` for the full coordination model.
