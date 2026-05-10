#!/bin/bash
# SessionStart hook — prints key project reminders at the start of every session.
# Runs automatically when Claude Code starts a session in this repo.

cat <<'EOF'
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  bess-platform — Investment-Trading-Asset Intelligence
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Read before starting:
  CLAUDE.md    — project rules, agent patterns, deploy protocol
  MEMORY.md    — decisions log and session summaries
  ERRORS.md    — failed approaches (avoid repeating them)

4 Agents:
  Strategist   apps/spot-market       Pillar 1 — China spot market
  Quant        apps/bess-map          Pillar 2 — BESS investment economics
  Trader       apps/mengxi-dashboard  Pillar 3 — IM asset trading ops
  Deal Struct  (Pillar 5, TBD)        Investment committee orchestration

Subagents available:  /strategist  /quant  /trader  /deal-structurer
Dev tools:            /code-reviewer  /test-runner
Skills:               /deploy  /session-end  /new-agent

Reminder: ALL deployments require explicit "yes" in the current message.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EOF
