# MASTER_OPERATING_POLICY.md
# Investment-Trading-Asset Intelligence Platform — Master Operating Policy

## Purpose

This file is the single source of truth for AI-tool operating roles, task allocation, branch ownership, and coordination rules across the investment-trading-asset intelligence platform.

All other governance files should stay consistent with this document.
If any file conflicts with this one, this file takes precedence.

The objective is to keep the platform moving fast without role confusion, overlapping edits, or wasted paid-token usage.

---

## 1. Platform mission

Build a multi-asset power investment-trading-asset intelligence platform that compounds advantage through a closed loop:

1. identify where value pools and monetisable opportunities exist
2. test whether the opportunity is real and durable
3. monetise through trading, dispatch, optimisation, and execution
4. compare realised outcomes versus assumptions
5. improve forecasting, strategy, and capital allocation

This is not a BESS-only dashboard project.
It starts from `bess-platform`, but the target system must support:
- BESS
- wind
- retail power books
- coal-fired assets
- later VPPs and other flexibility assets

---

## 2. Core design principles

All tools and contributors must operate within these principles:

- reuse and extend, not rewrite, unless rewrite is clearly justified
- preserve working modules wherever possible
- prefer additive changes over structural disruption
- keep `marketdata` as a major operational foundation
- keep shared logic asset-neutral where reasonable
- separate app, service, and query responsibilities cleanly
- avoid hidden dependencies and silent architecture drift
- distinguish observed evidence from proxy-based and heuristic inference

---

## 3. Tool operating model

### 3.1 GPT

Role:
- architecture brain
- business-logic controller
- task allocator
- reviewer of realism, evidence quality, and strategic fit

Use GPT for:
- architecture and sequencing
- role allocation across tools
- business logic and analytical meaning
- ambiguity resolution
- operating prompt and spec writing
- review of proposed implementation paths
- final judgement on scope, priority, and integration logic

Do not use GPT for:
- large batches of repetitive edits
- routine coding already well specified
- daily operational coding
- mechanical parser or invoice-processing implementation

---

### 3.2 Claude Code

Role:
- primary implementation engineer for substantive repo work

Claude Code is the default coding engine for major implementation tasks once architecture and scope are sufficiently clear.

Use Claude Code for:
- feature implementation across multiple files
- repo-level debugging with real code changes
- service/query/UI wiring
- analytics page implementation
- test fixes and runtime fixes
- structured refactors with clear boundaries
- implementation of planned business logic
- integration of reviewed code from auxiliary tools

Do not use Claude Code for:
- unconstrained production operations
- vague architecture ownership without GPT review
- daily repetitive operational coding when Codex can do it more cheaply
- writing on the same active branch as another coding tool

Default ownership:
- `feature/...`
- `refactor/...`
- substantial `fix/...` branches involving multi-file repo changes

---

### 3.3 Codex

Role:
- daily operations coding engine
- low-cost recurring implementation path

Codex is not the default owner of major repo feature development.
Its main value is low-cost, high-frequency, bounded implementation for operational workflows.

Use Codex for:
- invoice-reading code
- parser maintenance
- ETL helper scripts
- recurring file intake logic
- small support utilities
- report-generation code
- bounded bug fixes
- data-cleaning scripts
- operational dashboards with limited scope
- clearly specified support automation

Codex may also handle small repo changes when:
- the task is narrow
- the scope is operational rather than architectural
- the branch is clearly assigned to Codex

Do not use Codex for:
- broad architecture-sensitive feature design
- large multi-module integration without a clear spec
- competing with Claude Code on major feature branches
- uncontrolled refactors across core platform areas

Default ownership:
- `ops/...`
- parser or invoice-related `fix/...`
- small operational utility branches

---

### 3.4 AWS OpenClaw

Role:
- orchestration shell
- controlled operator
- build/test/run coordinator
- branch-safe execution layer

OpenClaw is not the default coding engine.
It is the operator that routes work, validates state, runs checks, monitors flows, and coordinates safe execution.

Use OpenClaw for:
- classifying requests into the right tool lane
- repo and branch inspection on AWS
- build/test/run sequencing
- app validation and health checks
- pipeline monitoring
- data freshness and input monitoring
- file intake/routing workflow coordination
- recurring operational summaries
- controlled handoff packaging for Claude Code or Codex

Do not use OpenClaw for:
- uncontrolled large-scale coding
- acting as primary feature implementer by default
- editing the same branch already owned by Claude Code or Codex
- silent branch switching or destructive operational actions

---

### 3.5 claw-code

Role:
- auxiliary bounded contributor
- draft implementation and reference-pattern engine

Use claw-code for:
- helper modules
- draft parsers
- small UI sections
- prompt/governance drafts
- reference-pattern extraction
- alternative implementation sketches

Do not use claw-code for:
- main branch ownership
- architectural decisions
- production operational control
- large uncontrolled rewrites

---

### 3.6 MiniMax

Role:
- compatibility-only legacy lane

MiniMax is no longer a primary task-allocation path.
If referenced in older docs or workflows, interpret it as a temporary legacy placeholder rather than an active strategic role.

Do not route new core work to MiniMax unless there is a specific temporary reason.

---

## 4. Task allocation policy

Use this default decision tree.

### Keep with GPT when:
- business meaning matters more than coding speed
- the task is ambiguous
- multiple implementation paths exist
- role routing is unclear
- a design or scope decision must be made
- outputs need realism review

### Give to Claude Code when:
- the task is substantive repo implementation
- multiple files need coordinated edits
- the architecture is already known or mostly known
- feature logic or runtime debugging requires deep code changes
- a reviewable feature branch is needed

### Give to Codex when:
- the task is recurring operational coding
- the work is parser / invoice / ETL-helper / support-automation oriented
- the scope is bounded and well specified
- cost efficiency matters and the work is not architecture-heavy

### Give to OpenClaw when:
- the task is operational routing or orchestration
- checks, builds, validation, scheduling, or monitoring are required
- a safe operator should inspect repo state before work proceeds
- a coding task should be packaged before handoff to Claude Code or Codex

### Give to claw-code when:
- you want an auxiliary draft path
- the task is bounded, reviewable, and non-authoritative
- you want a quick alternative implementation sketch or helper pattern

### Do not give to MiniMax when:
- the task is new platform work
- the task is a core implementation lane
- the task could be done by Claude Code, Codex, or OpenClaw instead

---

## 5. Branch ownership policy

This is the most important coordination rule.

### Hard rule

One AI coding tool writes to one active branch at a time.

Never allow:
- Claude Code and Codex writing to the same branch concurrently
- OpenClaw and Claude Code editing the same active feature branch concurrently
- claw-code and Codex writing into the same branch without explicit handoff

### Default ownership model

- `feature/...` → Claude Code
- `refactor/...` → Claude Code
- major multi-file `fix/...` → Claude Code
- `ops/...` → Codex
- parser / invoice / recurring support `fix/...` → Codex
- bounded experiment or draft branches → claw-code only if explicitly assigned

### Handoff rule

If branch ownership changes:
1. close or pause the current tool’s write session
2. summarise current state, assumptions, and risks
3. make the receiving tool the explicit new owner
4. avoid parallel edits until the handoff is complete

---

## 6. Daily operating split

To keep cost and throughput aligned, use this steady-state model:

### Claude Code owns
- major repo feature work
- debugging of platform modules
- integration-heavy implementation
- feature completion after GPT has defined direction

### Codex owns
- invoices
- document/file reading pipelines
- parser upkeep
- routine ETL support
- recurring operational support coding
- low-cost utilities and scripts

### OpenClaw owns
- scheduling
- orchestration
- run/check/monitor workflows
- routing work to Claude Code or Codex
- AWS-side operational validation

### GPT owns
- deciding what matters
- deciding who should do what
- reviewing whether outputs are strategically useful and technically sane

---

## 7. Evidence and output discipline

Every analytical, operational, or business output should explicitly distinguish between:
- observed
- proxy-based
- heuristic inference

No agent or coding tool should present an inferred conclusion as an observed fact.

For implementation tasks, the expected delivery summary should include:
- business goal
- scope
- affected files or systems
- branch ownership
- patch summary
- assumptions
- runtime risks
- test/check plan
- remaining operational validation needs

---

## 8. Operational safety rules

All tools must follow these constraints:

- do not merge to `main` automatically
- do not deploy to production without explicit approval
- do not make uncontrolled DB/schema changes
- do not create hidden dependencies
- do not switch branches silently
- do not perform destructive cleanup without explicit instruction
- prefer smallest additive path first
- preserve auditability of actions and outputs

---

## 9. Recommended doc simplification policy

To avoid duplication drift:

- keep this file as the master operating policy
- shorten role files so they reference this document
- keep only role-specific details in:
  - `CODEX_AGENT.md`
  - `OPENCLAW_AGENT.md`
  - `CLAW_CODE_AGENT.md`
  - `GPT_MASTERMIND.md`
- keep business-agent intent in `FOUR_AGENTS_OPERATIONS.md`
- keep platform vision and architecture state in `PLATFORM_HANDOFF.md`
- keep `MINIMAX_AGENT.md` as compatibility-only until it can be retired safely

Suggested reference line for the other docs:

> This file should be interpreted together with `MASTER_OPERATING_POLICY.md`, which is the controlling source for role allocation, branch ownership, and coordination rules.

---

## 10. Current default operating pattern

Use this pattern unless explicitly overridden:

- GPT = architecture / business logic / reviewer / allocator
- Claude Code = main implementation engine
- Codex = daily operations coding engine
- OpenClaw = orchestration shell and controlled operator
- claw-code = auxiliary bounded contributor
- MiniMax = legacy compatibility only

---

## 11. Governing principle

This platform is a closed-loop intelligence system for:
- finding value
- testing whether it is real
- monetising through execution
- explaining realised outcomes
- improving future strategy and capital allocation

Tool usage, branch policy, and agent orchestration should always serve that business loop rather than drift into disconnected coding activity.
