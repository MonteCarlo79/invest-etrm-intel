# FOUR_AGENTS_OPERATIONS.md

## Purpose

This file defines the operating intent of the 4 business agents inside the platform.

The single source of truth for AI-tool operating roles, task allocation, and branch ownership is:
- `MASTER_OPERATING_POLICY.md`

This file focuses on business-agent purpose, not detailed tool governance.
If this file conflicts with the master policy, the master policy takes precedence.

---

## Platform business loop

The platform exists to compound advantage through a closed loop:
1. identify where value pools exist
2. verify whether the opportunity is real and durable
3. monetise through trading, dispatch, and execution
4. explain realised P&L versus assumptions
5. improve forecasting, strategy, and future capital allocation

This is a multi-asset platform, not a BESS-only dashboard.

Current asset order:
- BESS first
- wind next
- retail power books next
- coal after that
- VPP and other flexibility assets later

---

## 1. Market Strategy & Investment Intelligence Agent

### Goal
Identify where the money is.

### Responsibilities
- scan market structure, nodes, provinces, products, and policy signals
- identify monetisable value pools
- compare asset, market, and strategy attractiveness
- surface development and acquisition opportunities
- publish opportunity-screening summaries

### Typical outputs
- market screening memo
- node or province attractiveness ranking
- policy or market-change impact note
- asset opportunity shortlist

---

## 2. Enterprise Portfolio, Risk & Capital Allocation Agent

### Goal
Explain portfolio performance and where capital, risk appetite, or management attention should move.

### Responsibilities
- explain portfolio P&L at enterprise and asset-book level
- compare realised versus expected economics
- frame concentration risk and hidden exposure
- support capital allocation and deallocation decisions
- connect asset performance back to original assumptions

### Typical outputs
- portfolio P&L explain
- realised-versus-plan summary
- concentration or risk memo
- capital allocation framing note

---

## 3. Trading, Dispatch & Execution Agent

### Goal
Monetise assets and books better through trading, dispatch, scheduling, and execution intelligence.

### Responsibilities
- refresh market, position, and realised outcome data
- compare realised versus theoretical value capture
- support strategy comparison and backtesting
- diagnose monetisation leakage
- improve trading and dispatch rules
- support settlement and reconciliation review

### Typical outputs
- strategy comparison output
- realised-versus-theoretical capture report
- monetisation leakage diagnosis
- settlement or reconciliation exception summary

---

## 4. Platform Reliability, Data Quality & Control Agent

### Goal
Keep the platform operationally trustworthy.

### Responsibilities
- monitor jobs, pipelines, apps, and routing flows
- monitor data freshness, completeness, and quality
- alert on failures, missing inputs, and stale outputs
- coordinate safe reruns and non-destructive remediation
- support auditability and evidence tracing

### Typical outputs
- job health summary
- freshness or data quality alert
- rerun recommendation
- operational exception report

---

## Shared output rule

Every agent output must explicitly distinguish whether a conclusion is:
- observed
- proxy-based
- heuristic inference

No strategy claim should be presented as fact unless supported by observed evidence.

---

## Shared coordination rule

The business agents are logical agents.
Execution across AI tools is controlled separately under:
- `MASTER_OPERATING_POLICY.md`

Working summary:
- GPT = architecture, allocation, review
- Claude Code = major implementation
- Codex = daily operations coding
- OpenClaw = orchestration and controlled execution
- claw-code = bounded auxiliary contribution
- MiniMax = legacy-only compatibility lane
