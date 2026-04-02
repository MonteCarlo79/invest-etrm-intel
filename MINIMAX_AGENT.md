# MINIMAX_AGENT.md

## Status

This file is retained temporarily for compatibility only.

The platform is shifting away from MiniMax-specific task allocation and toward:
- GPT or Claude Code for paid architecture/review
- local Codex Desktop for free primary implementation
- AWS OpenClaw for paid orchestration
- claw-code for free auxiliary implementation

Preferred long-term replacement:
- rename this file to `CLAW_CODE_AGENT.md`

---

## Transitional role

If this file is still referenced anywhere, interpret it as a bounded auxiliary contributor role.

That role should:
- handle narrow, well-specified tasks
- avoid broad repo-wide judgment
- avoid branch overlap with Codex Desktop
- avoid architecture and operational control
- produce concise, reviewable outputs

---

## Equivalent task types

- utility functions
- parsers
- report templates
- small dashboard blocks
- fixed-format ETL helpers
- isolated service methods
- UI sections with clear input/output specs

---

## Avoid

- repo-wide architecture
- broad refactors
- deployment decisions
- cross-module control logic
- strategic business reasoning

---

## Working rules

- stay inside the assigned boundary
- do not redesign outside scope
- do not add unnecessary dependencies
- keep compatibility with existing code
- return concise, reviewable outputs

---

## Coordination

- work only on your assigned branch/task
- do not overlap with Codex Desktop on the same branch
- let GPT or Claude Code define the spec
- let Codex Desktop handle wider integration
- let OpenClaw coordinate operational workflow
