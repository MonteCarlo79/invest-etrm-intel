# PLATFORM_HANDOFF.md
# Investment-Trading-Asset Intelligence System

## 1. Strategic objective

Build a multi-asset power investment-trading-asset intelligence platform that compounds advantage through a closed loop:

1. identify where value pools and opportunities exist across markets, provinces, nodes, products, and asset classes
2. verify whether opportunities are real and durable
3. monetise assets and books through trading, dispatch, and execution
4. validate realised P&L versus assumptions
5. sharpen forecasting, strategy, and future capital allocation

This is not a BESS-only system.

It starts from `bess-platform`, but the target platform must support:
- BESS
- wind farms
- retail trading books
- coal-fired plants
- later VPPs and other flexibility assets

---

## 2. Core business logic

The platform should answer this sequence continuously:

- where are the opportunities?
- which are real enough to invest in or trade?
- how do we monetise them?
- what did we actually realise?
- why was realised P&L different from assumptions?
- how do we improve models, forecasts, and strategies?
- where should future capital and management attention go?

This is the investment-trading flywheel.

---

## 3. Current architecture direction

Preferred philosophy:
- reuse and refactor, not rewrite
- preserve current `bess-platform` with minimal disruption
- wrap existing apps, jobs, and services before deep refactors
- keep `marketdata` as a major operational foundation
- build common shared layers around existing live modules
- keep shared logic asset-neutral where reasonable

Current repo shape includes:
- `apps/portal/`
- `apps/uploader/`
- `apps/bess-inner-mongolia/im/app.py`
- `apps/trading/...`
- `services/bess_map/`
- `services/loader/`
- `services/common/`
- `services/bess-inner-mongolia/`
- `shared/agents/registry.py`
- `auth/`
- `db/`
- `infra/terraform/`

---

## 4. Tool operating model

### GPT or Claude Code
Role:
- paid architect / reviewer / business-logic controller

Use for:
- architecture
- business logic
- cross-module integration planning
- ambiguity resolution
- task allocation across tools
- realism review
- prioritisation
- prompt/spec writing
- final integration control

Do not waste this layer on:
- repetitive boilerplate coding
- low-leverage mechanical edits

Claude Code may be used as an alternative to GPT for this paid architect/reviewer role.
Do not let both tools actively write to the same branch at the same time.

### local Codex Desktop
Role:
- free principal implementation engine

Use for:
- app and service expansion inside the repo
- refactors across many files
- schema/service/UI wiring where architecture is already decided
- test fixes
- implementation on dedicated feature/fix/refactor branches

### AWS OpenClaw
Role:
- paid operations orchestrator and controlled build/run shell

Use for:
- workflow coordination
- scheduling and monitoring the 4 agents
- file intake/routing
- repo inspection on AWS
- build/test/run sequencing
- app validation
- pipeline monitoring
- branch-safe operational control

Do not make OpenClaw the unconstrained primary coder.

### claw-code
Role:
- free auxiliary implementation and reference-pattern engine

Use for:
- bounded draft implementation
- helper modules
- prompt/governance file drafts
- alternative implementation sketches
- reference-pattern extraction from external repos

Do not treat claw-code as the unquestioned source of truth.

---

## 5. Coordination rule

Use this operating pattern:

- GPT or Claude Code = mastermind / architect / reviewer
- local Codex Desktop = principal implementation engine
- AWS OpenClaw = orchestrator / operator / controlled build runner
- claw-code = bounded auxiliary contributor

Rule:
one AI tool = one active write branch at a time

Never let multiple AI tools write to the same branch concurrently.

---

## 6. Current feature work completed

### Mengxi Strategy Diagnostics (v1)

Goal:
Compare Envision-owned BESS assets against top-performing Inner Mongolia BESS assets and diagnose why Envision performance is weaker.

Known files created/changed on `feature/mengxi-strategy-diagnostics`:
- `services/bess_inner_mongolia/__init__.py`
- `services/bess_inner_mongolia/queries.py`
- `services/bess_inner_mongolia/peer_benchmark.py`
- `services/bess_inner_mongolia/strategy_diagnostics.py`
- `apps/bess-inner-mongolia/im/pages/strategy_diagnostics.py`
- `apps/bess-inner-mongolia/im/Dockerfile`

Important note:
- remove this unrelated Dockerfile line if still present:
  - `COPY apps/trading/bess/mengxi/pnl_attribution /apps/apps/trading/bess/mengxi/pnl_attribution`
- preserve this useful line:
  - `COPY services/bess_inner_mongolia /apps/services/bess_inner_mongolia`

---

## 7. What still needs verification right now

Immediate verification tasks:
1. remove the unrelated `pnl_attribution` Dockerfile copy line if not already removed
2. build and run the Inner Mongolia app container
3. check that the new Streamlit page appears correctly under the existing app
4. validate actual DB column availability and page runtime
5. review analytical logic in:
   - `services/bess_inner_mongolia/peer_benchmark.py`
   - `services/bess_inner_mongolia/strategy_diagnostics.py`
6. push the feature branch and open PR after validation

---

## 8. Target business agents for platform operations

### A. Market Strategy & Investment Intelligence Agent
Mission:
- detect value pools and market opportunities
- screen policy, structure, node, province, and asset opportunities
- rank where to investigate, invest, or reallocate

Outputs:
- opportunity screening
- market structure summary
- asset opportunity ranking
- investment intelligence memo

### B. Enterprise Portfolio, Risk & Capital Allocation Agent
Mission:
- explain portfolio P&L and risk
- compare realised versus assumed economics
- support capital allocation decisions

Outputs:
- P&L explain
- risk framing
- scenario summary
- capital allocation note

### C. Trading, Dispatch & Execution Agent
Mission:
- monetise assets and books through execution, trading, and dispatch intelligence
- compare realised results with theoretical opportunity

Outputs:
- execution support
- strategy diagnostics
- backtesting
- settlement reconciliation
- realised-versus-theoretical comparison

### D. Platform Reliability, Data Quality & Control Agent
Mission:
- keep the platform operationally healthy and trustworthy
- monitor jobs, data freshness, quality, and failures
- coordinate safe reruns and operational alerts

Outputs:
- job health
- data quality alerts
- platform status
- safe remediation actions

---

## 9. Evidence discipline

Every analytical insight should explicitly distinguish:
- observed
- proxy-based
- heuristic inference

No output should imply higher confidence than the evidence supports.

---

## 10. Suggested roadmap

### Immediate
- verify Mengxi strategy diagnostics
- clean Dockerfile if needed
- open PR
- establish OpenClaw as the operational shell
- define safe branch workflow across all tools

### Next
- formalise data intake and routing
- make OpenClaw operate the 4 agents on schedule
- define claw-code’s bounded contribution role clearly
- expand repeatable review workflow between GPT/Claude and Codex

### Then expand in this order
1. BESS diagnostics and operations intelligence
2. wind trading/risk/strategy module
3. retail trading book intelligence
4. coal trading/risk module
5. VPP later

---

## 11. Key design constraints for all future work

- minimal disruption to existing BESS platform
- keep `marketdata` compatible
- prefer additive shared services
- do not create unnecessary separate apps/routes too early
- keep SQL out of Streamlit pages where possible
- keep architecture multi-asset even if implementation remains phased
