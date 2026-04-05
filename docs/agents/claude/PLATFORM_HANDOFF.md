# PLATFORM_HANDOFF.md
# Investment-Trading-Asset Intelligence System

## Purpose

This file is a concise handoff and operating-context document for the platform.

The single source of truth for tool roles, task allocation, branch ownership, and coordination rules is:
- `MASTER_OPERATING_POLICY.md`

If this file conflicts with the master policy, the master policy takes precedence.

---

## 1. Strategic objective

Build a multi-asset power investment-trading-asset intelligence platform that compounds advantage through a closed loop:

1. identify value pools and monetisable opportunities
2. test whether the opportunity is real and durable
3. monetise through trading, dispatch, optimisation, and execution
4. compare realised outcomes versus assumptions
5. improve forecasting, strategy, and capital allocation

This is not a BESS-only system.

Target asset scope:
- BESS
- wind
- retail power books
- coal-fired plants
- later VPPs and other flexibility assets

---

## 2. Architecture direction

Working philosophy:
- reuse and refactor, not rewrite
- preserve the existing `bess-platform` with minimal disruption
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

## 3. Tool operating pattern

Refer to `MASTER_OPERATING_POLICY.md` for the full role definitions.

Working summary:
- GPT = architect, allocator, reviewer
- Claude Code = primary implementation engine for substantive repo work
- Codex = daily operations coding engine
- AWS OpenClaw = orchestration shell and controlled execution layer
- claw-code = bounded auxiliary contributor
- MiniMax = compatibility-only legacy lane

Hard rule:
- one AI tool writes to one active branch at a time

---

## 4. Current feature work completed

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

## 5. What still needs verification right now

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

## 6. Business-agent operating intent

The four logical business agents are:
1. Market Strategy & Investment Intelligence Agent
2. Enterprise Portfolio, Risk & Capital Allocation Agent
3. Trading, Dispatch & Execution Agent
4. Platform Reliability, Data Quality & Control Agent

For full definitions and coordination rules, refer to:
- `FOUR_AGENTS_OPERATIONS.md`
- `MASTER_OPERATING_POLICY.md`

---

## 7. Evidence discipline

Every analytical insight should explicitly distinguish:
- observed
- proxy-based
- heuristic inference

No output should imply higher confidence than the evidence supports.

---

## 8. Immediate roadmap

### Immediate
- verify Mengxi strategy diagnostics
- clean Dockerfile if needed
- open PR
- establish OpenClaw as the operational shell
- define safe branch workflow across all tools

### Next
- formalise data intake and routing
- make OpenClaw operate the 4 agents on schedule
- expand repeatable review workflow between GPT and the coding tools

### Then expand in this order
1. BESS diagnostics and operations intelligence
2. wind trading/risk/strategy module
3. retail trading book intelligence
4. coal trading/risk module
5. VPP later

---

## 9. Key constraints

- minimal disruption to existing BESS platform
- keep `marketdata` compatible
- prefer additive shared services
- do not create unnecessary separate apps/routes too early
- keep SQL out of Streamlit pages where possible
- keep architecture multi-asset even if implementation remains phased
