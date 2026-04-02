# Platform Handoff: Investment–Trading–Asset Intelligence System

## 1. Strategic objective

Build a multi-asset power **investment–trading–asset intelligence platform** that compounds advantage through a closed loop:

1. **Where is my money?**  
   Identify where value pools and opportunities exist across markets, provinces, nodes, and asset classes.

2. **Explore and verify asset opportunities**  
   Screen markets, compare assets and strategies, validate whether opportunities are real and durable.

3. **Monetise the assets through trading**  
   Use trading, dispatch, and execution intelligence to realise value from assets and books.

4. **Validate realised P&L vs assumptions**  
   Use backtesting, settlement, and attribution to understand why realised outcomes differ from original assumptions.

5. **Sharpen forecasting and strategies**  
   Improve forecasting, risk controls, and strategy rules so both future trading and future investment decisions improve.

This is not a BESS-only system.  
It started from `bess-platform`, but the target platform must support:
- BESS
- wind farms
- retail trading books
- coal-fired power plants
- later VPPs and other flexibility assets

## 2. Core business logic

The platform should answer this sequence continuously:

- **Where are the opportunities?**
- **Which are real enough to invest in or trade?**
- **How do we monetise them?**
- **What did we actually realise?**
- **Why was realised P&L different from assumptions?**
- **How do we improve models, forecasts, and strategies?**

This is the investment–trading flywheel.

## 3. Current architecture direction

Preferred philosophy:
- reuse and refactor, not rewrite
- preserve current `bess-platform` with minimal disruption
- wrap existing apps/jobs/services before deep refactors
- keep `marketdata` as a major operational foundation
- build common shared layers around existing live modules

Current repo shape (approximate but verified enough for design):
- `apps/portal/`
- `apps/uploader/`
- `apps/bess-inner-mongolia/im/app.py`
- `apps/trading/...`
- `services/bess_map/`
- `services/loader/`
- `services/common/`
- `services/bess-inner-mongolia/` (legacy hyphenated path)
- `shared/agents/registry.py`
- `auth/`
- `db/`
- `infra/terraform/`

## 4. Important current integration decisions

Already done / partially done:
- `province_misc_to_db_v2.py` moved into `services/loader/`
- `column_to_matrix_all.py` moved into `services/common/` and renamed to `focused_assets_data.py`
- `shared/agents/registry.py` updated/clarified for portal + agents
- New feature branch created for Inner Mongolia strategy diagnostics:
  - branch: `feature/mengxi-strategy-diagnostics`
  - commit: `ddbc760`
- New package added:
  - `services/bess_inner_mongolia/`
- New page added:
  - `apps/bess-inner-mongolia/im/pages/strategy_diagnostics.py`

## 5. Current feature work completed

### Mengxi Strategy Diagnostics (v1)
Goal:
Compare Envision-owned BESS assets against top-performing Inner Mongolia BESS assets and diagnose why Envision performance is weaker.

Files created/changed on `feature/mengxi-strategy-diagnostics`:
- `services/bess_inner_mongolia/__init__.py`
- `services/bess_inner_mongolia/queries.py`
- `services/bess_inner_mongolia/peer_benchmark.py`
- `services/bess_inner_mongolia/strategy_diagnostics.py`
- `apps/bess-inner-mongolia/im/pages/strategy_diagnostics.py`
- `apps/bess-inner-mongolia/im/Dockerfile`

Important note:
- One Dockerfile line added by OpenClaw was identified as unnecessary and should be removed if still present:
  - `COPY apps/trading/bess/mengxi/pnl_attribution /apps/apps/trading/bess/mengxi/pnl_attribution`
- The useful Dockerfile addition is:
  - `COPY services/bess_inner_mongolia /apps/services/bess_inner_mongolia`

## 6. What still needs verification right now

Immediate verification tasks:
1. Remove the unrelated `pnl_attribution` Dockerfile copy line if not already removed
2. Build and run the Inner Mongolia app container
3. Check that the new Streamlit page appears correctly under the existing app
4. Validate actual DB column availability and page runtime
5. Review the analytical logic in:
   - `services/bess_inner_mongolia/peer_benchmark.py`
   - `services/bess_inner_mongolia/strategy_diagnostics.py`
6. Push the feature branch and open PR after validation

## 7. Target agent model for platform operations

### A. Market Strategy & Investment Intelligence Agent
Mission:
- detect value pools and market opportunities
- screen policy, structure, node, province, and asset opportunities
- rank where to investigate, invest, or reallocate

Outputs:
- opportunity screening
- market structure summaries
- asset opportunity ranking
- investment intelligence memos

### B. Enterprise Portfolio, Risk & Capital Allocation Agent
Mission:
- explain portfolio P&L and risk
- compare realised vs assumed economics
- support capital allocation decisions

Outputs:
- P&L explain
- risk metrics
- scenario summaries
- capital allocation framing

### C. Trading, Dispatch & Execution Agent
Mission:
- monetise assets and books through execution, trading, and dispatch intelligence
- compare realised results with theoretical opportunity

Outputs:
- execution support
- strategy diagnostics
- backtesting
- settlement reconciliation
- realised-vs-theoretical comparison

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

## 8. Optimal task allocation across tools

### GPT-5.4
Use for:
- master architecture
- business logic design
- cross-module integration planning
- review of analytics logic
- defining agent operating model
- triaging tradeoffs
- preparing prompts/specs for other tools

Do not waste GPT-5.4 on:
- repetitive boilerplate coding
- large-scale file-by-file edits once the design is fixed

### Codex
Use for:
- app and service expansion inside repo
- implementing new modules and pages
- refactors across many files
- schema + service + UI wiring
- PR-oriented code generation

Best use cases:
- new wind module scaffold
- retail risk module
- portal integration work
- service extraction from legacy scripts
- test fixes and repo-wide edits

### OpenClaw + Anthropic
Use as:
- operations orchestrator
- workflow coordinator
- intake/routing controller
- controlled builder that can coordinate Codex
- chat/ops interface for running the 4 agents

Best use cases:
- daily/weekly operational runs
- file intake/routing
- build coordination
- pipeline monitoring
- alerting
- scheduling the 4 agents

Do not make OpenClaw the unconstrained primary coder.

### MiniMax
Use for:
- bounded coding tasks with a clear spec
- repetitive implementation blocks
- UI widgets / helper scripts
- lower-risk parallel coding tasks

Best use cases:
- parser utilities
- report templates
- dashboards with a fixed design
- smaller isolated service methods

## 9. Best coordination model

Use this operating pattern:

- **GPT-5.4** = mastermind / architect / reviewer
- **Codex** = principal implementation engine
- **OpenClaw** = orchestrator / operator / controlled builder
- **MiniMax** = bounded parallel contributor

Rule:
**one AI tool = one branch at a time**

Never let multiple tools write to the same branch concurrently.

## 10. Suggested next roadmap

### Immediate
- verify Mengxi strategy diagnostics feature
- clean Dockerfile if needed
- open PR

### Next
- formalise data intake & routing layer
- make OpenClaw operate the 4 agents on schedule
- add safe branch/PR workflow for auto-expansion tasks

### Then expand apps in this order
1. BESS diagnostics and operations intelligence (continue)
2. wind trading/risk/strategy module
3. retail trading book intelligence
4. coal trading/risk module
5. VPP later

## 11. Key design constraints for all future work

- minimal disruption to existing BESS platform
- keep `marketdata` compatible
- prefer additive shared services
- do not create unnecessary separate apps/routes too early
- keep SQL out of Streamlit pages where possible
- every analytical insight should distinguish:
  - observed
  - proxy-based
  - heuristic inference
