# MacBook Claude Handover
**Date:** 2026-07-19  
**Written by:** Windows laptop Claude Code session (via envisioncn.com gateway)

---

## TL;DR — Where to focus

**MacBook primary project: `ib-platform`**  
Working directory: `~/repo/ETRM/ib-platform`

bess-platform development continues from the Windows company laptop. MacBook only needs to know the bess-platform Bedrock context below, then switch to ib-platform.

---

## Your Claude Code setup (MacBook)

You are running via **AWS Bedrock** (no personal Anthropic credits needed).

`~/.claude/settings.json` should contain:
```json
{
  "env": {
    "CLAUDE_CODE_USE_BEDROCK": "1",
    "AWS_PROFILE": "claude-bedrock",
    "AWS_REGION": "us-east-1"
  }
}
```

This was set up because personal Anthropic API credits ran out and UK card top-ups failed. AWS Bedrock bills through AWS (accepts UK cards). The `claude-bedrock` profile was created via `aws configure --profile claude-bedrock`.

---

## bess-platform — what Windows just did (for your awareness)

You don't need to actively work on bess-platform, but here's what changed in case it comes up:

- **Branch:** `cost-optimisation`
- **Commits `ae66e2c` + `2ac42a1`:** Migrated all 37 ECS service files from `Anthropic(api_key=...)` to a new factory `shared/anthropic_client.py` → `make_client()`. When `BEDROCK_REGION=us-east-1` is set in ECS env, it uses IAM role + Bedrock instead of the personal API key.
- **Terraform** updated: `BEDROCK_REGION=us-east-1` added to all ECS task definitions.
- **Pending:** ECS services need force-redeploy to pick up the new env var (`aws ecs update-service --force-new-deployment`). Windows will handle this.

---

## ib-platform — your primary focus

See: `~/repo/ETRM/ib-platform/docs/handover/MACBOOK_HANDOVER.md`

That document has full context on the current state of ib-platform, open items, and quick-start commands.
