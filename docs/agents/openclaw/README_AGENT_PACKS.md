# README_AGENT_PACKS.md

## Purpose

This file defines which governance/context files should be given to each core tool.

The goal is to keep every tool aligned without overloading it with unnecessary role files.

The controlling source for role allocation, branch ownership, and coordination is:
- `MASTER_OPERATING_POLICY.md`

---

## 1. Core shared pack for all tools

Give these files to all four core tools:

- `MASTER_OPERATING_POLICY.md`
- `PLATFORM_HANDOFF.md`
- `investment_trading_asset_intelligence_sop v2.md`

Optional shared context when relevant:
- `FOUR_AGENTS_OPERATIONS.md`

Use the optional file when the task depends heavily on business-agent intent or output framing.

---

## 2. GPT pack

Give GPT:
- `MASTER_OPERATING_POLICY.md`
- `PLATFORM_HANDOFF.md`
- `investment_trading_asset_intelligence_sop v2.md`
- `FOUR_AGENTS_OPERATIONS.md`
- `GPT_MASTERMIND.md`

Use this pack when GPT is doing:
- architecture
- business-logic definition
- task allocation
- realism review
- prompt/spec writing
- KPI meaning review

---

## 3. OpenClaw pack

Give OpenClaw:
- `MASTER_OPERATING_POLICY.md`
- `PLATFORM_HANDOFF.md`
- `investment_trading_asset_intelligence_sop v2.md`
- `FOUR_AGENTS_OPERATIONS.md`
- `OPENCLAW_AGENT.md`

Minimum workable OpenClaw pack:
- `MASTER_OPERATING_POLICY.md`
- `investment_trading_asset_intelligence_sop v2.md`
- `OPENCLAW_AGENT.md`

Use this pack when OpenClaw is doing:
- orchestration
- scheduling
- reruns
- branch-safe validation
- report publication
- operational monitoring
- AWS-side build/test/run coordination

---

## 4. Codex pack

Give Codex:
- `MASTER_OPERATING_POLICY.md`
- `PLATFORM_HANDOFF.md`
- `investment_trading_asset_intelligence_sop v2.md`
- `CODEX_AGENT.md`

Optional:
- `FOUR_AGENTS_OPERATIONS.md`

Use the optional business-agent file only when the coding task directly depends on business output framing.

Use this pack when Codex is doing:
- parser maintenance
- invoice/PDF/Excel reading
- ETL helper scripts
- recurring file intake logic
- support automation
- bounded operational fixes

---

## 5. Claude Code pack

Give Claude Code:
- `MASTER_OPERATING_POLICY.md`
- `PLATFORM_HANDOFF.md`
- `investment_trading_asset_intelligence_sop v2.md`
- `CLAUDE_CODE_AGENT.md`

Optional:
- `FOUR_AGENTS_OPERATIONS.md`

Use this pack when Claude Code is doing:
- major feature implementation
- service/query/UI wiring
- report-generation pipelines
- app creation
- multi-file debugging
- report center integration

---

## 6. claw-code pack

Only give these when using claw-code:
- `MASTER_OPERATING_POLICY.md`
- `PLATFORM_HANDOFF.md`
- `CLAW_CODE_AGENT.md`

Add the SOP only if the draft task directly depends on it.

---

## 7. Files not needed by default

Do not give every role file to every tool by default.

Usually unnecessary unless auditing governance:
- `CODEX_AGENT.md` for GPT or OpenClaw
- `OPENCLAW_AGENT.md` for Codex or Claude Code
- `CLAW_CODE_AGENT.md` for the four core tools
- old or superseded duplicate SOP/task-allocation files

---

## 8. Simplest recommended distribution

### GPT
- `MASTER_OPERATING_POLICY.md`
- `PLATFORM_HANDOFF.md`
- `investment_trading_asset_intelligence_sop v2.md`
- `FOUR_AGENTS_OPERATIONS.md`
- `GPT_MASTERMIND.md`

### OpenClaw
- `MASTER_OPERATING_POLICY.md`
- `PLATFORM_HANDOFF.md`
- `investment_trading_asset_intelligence_sop v2.md`
- `FOUR_AGENTS_OPERATIONS.md`
- `OPENCLAW_AGENT.md`

### Codex
- `MASTER_OPERATING_POLICY.md`
- `PLATFORM_HANDOFF.md`
- `investment_trading_asset_intelligence_sop v2.md`
- `CODEX_AGENT.md`

### Claude Code
- `MASTER_OPERATING_POLICY.md`
- `PLATFORM_HANDOFF.md`
- `investment_trading_asset_intelligence_sop v2.md`
- `CLAUDE_CODE_AGENT.md`

---

## 9. Practical rule

If uncertain, give:
- the master policy
- the platform handoff
- the latest SOP
- the tool's own role file

That is the default safe pack.
