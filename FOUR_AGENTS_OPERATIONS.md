# FOUR_AGENTS_OPERATIONS.md

## Purpose

This file defines the operating intent of the 4 business agents inside the investment-trading-asset intelligence platform.

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
- highlight structural regime shifts that deserve investigation
- publish opportunity screening summaries

### Questions it must answer
- where are the value pools?
- which markets or nodes deserve further work?
- what looks structurally monetisable versus temporarily noisy?
- what should enter the investment or development pipeline?

### Typical outputs
- market screening memo
- node/province attractiveness ranking
- policy or market-change impact note
- asset opportunity shortlist

---

## 2. Enterprise Portfolio, Risk & Capital Allocation Agent

### Goal
Explain portfolio performance and where capital, risk appetite, or management attention should move.

### Responsibilities
- explain portfolio P&L at enterprise and asset-book level
- compare realised versus expected economics
- frame concentration risk, downside risk, and hidden exposure
- support capital allocation and deallocation decisions
- connect asset performance back to original assumptions
- highlight where portfolio construction is weak or overconfident

### Questions it must answer
- what made or lost money?
- what differs from assumptions and why?
- where is risk concentrated?
- where should capital or attention shift?
- which assets or books deserve expansion, reduction, or review?

### Typical outputs
- portfolio P&L explain
- realised-versus-plan summary
- concentration/risk memo
- capital allocation framing note

---

## 3. Trading, Dispatch & Execution Agent

### Goal
Monetise assets and books better through trading, dispatch, scheduling, and execution intelligence.

### Responsibilities
- refresh market, position, and realised outcome data
- compare realised versus theoretical value capture
- support strategy comparison and backtesting
- diagnose missed value and monetisation leakage
- improve trading and dispatch rules
- support settlement and reconciliation review
- connect execution outcomes back to forecast quality

### Questions it must answer
- how are we monetising the assets and books?
- where are we missing value?
- why did realised P&L differ from theoretical opportunity?
- what should change in forecasting, dispatch, or trading logic?
- what should be rerun, challenged, or backtested?

### Typical outputs
- strategy comparison output
- realised-versus-theoretical capture report
- monetisation leakage diagnosis
- settlement/reconciliation exception summary

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
- distinguish system failure from data failure from analytical uncertainty

### Questions it must answer
- is the platform healthy?
- are data and reports trustworthy?
- what failed?
- what needs rerun, escalation, or quarantine?
- what conclusions are unsafe because the underlying data is weak?

### Typical outputs
- job health summary
- freshness / data quality alert
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
Execution across AI tools is controlled separately.

Tool operating pattern:
- GPT or Claude Code = paid architect / reviewer / business logic controller
- local Codex Desktop = free principal implementation engine
- AWS OpenClaw = paid orchestration shell / operator / controlled build runner
- claw-code = free auxiliary implementation or reference-pattern engine

Rule:
one AI tool writes to one active branch at a time.

---

## Shared business alignment

All 4 agents work toward the same business loop:
- identify value
- validate opportunity
- monetise through trading and execution
- explain realised outcomes
- improve future forecasts, strategies, and investment decisions
