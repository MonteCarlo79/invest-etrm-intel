# MENGXI INGESTION GOVERNANCE POSTMORTEM — 2026-04-09

## Purpose

Document the coordination/governance failure around the Mengxi ingestion incident so the team can avoid repeating it.

This postmortem focuses on operator behavior, handoff quality, and source-of-truth confusion.

---

## Governance failure

The main failure was not just the network timeout itself.
It was the lack of a clean distinction between:

1. local unpushed code
2. GitHub branch state
3. Terraform intended state
4. live deployed AWS state

That confusion slowed diagnosis and made it easier to argue from the wrong layer.

---

## What went wrong

### 1. Local state was treated as if it were shared truth

Operators discussed branch contents before confirming whether the relevant changes or docs were pushed to GitHub.

Guardrail:

- treat GitHub remote as the shared source of truth for collaboration
- if a change is only local, say so explicitly

### 2. Terraform intent was treated as if it proved live deployment

Reviewing `.tf` files or `terraform plan` output is useful, but it does not prove the running ECS task or RDS instance actually matches that intent.

Guardrail:

- never claim deployed state from Terraform code alone
- always verify live AWS attachments and runtime placement

### 3. Console/live AWS observations were not tied back to branch/commit evidence cleanly

Operators could identify live SG IDs but still lacked a lightweight pattern for tying those findings back to:

- the GitHub branch
- the exact Terraform module
- the next repair action

Guardrail:

- every incident update should include branch, commit, Terraform path, and live AWS evidence

### 4. Handoff docs were branch-fragmented

OpenClaw operational docs may exist on a docs branch while Codex is implementing on a different branch.

Guardrail:

- branch + path + commit must be supplied for authoritative handoff docs
- missing local visibility is a sync issue, not evidence the doc is absent

---

## Source-of-truth rules

### Code and docs

- GitHub branch/ref is the team source of truth
- local worktree is only provisional until pushed

### Infrastructure intent

- Terraform code and vars define intended state
- Terraform state/plan help explain expected state

### Deployed reality

- live AWS API results are source of truth for actual deployed state
- ECS task ENI, subnet, SG, RDS SG attachment, and CloudWatch logs outrank assumptions

### Incident closure

An incident is not closed until runtime evidence confirms the repaired path works.

---

## Minimum evidence pack for future incidents

Every Mengxi infra incident update should include:

1. GitHub branch name
2. latest pushed commit SHA
3. exact Terraform path involved
4. exact AWS resource IDs observed live
5. whether evidence is:
   - observed
   - inferred
   - intended only
6. next operator action

---

## Required operating rule

Before saying “fixed,” the operator must be able to answer all of these:

- Which GitHub commit contains the intended change?
- Which Terraform module/path is supposed to own the resource?
- Was `terraform apply` actually run in the intended environment?
- What SG/subnet/endpoint values are attached live right now?
- Did a controlled rerun prove the task now reaches DB successfully?

If any answer is missing, the issue is not fully validated.

---

## Follow-up guardrail docs

- `docs/openclaw/CODEX_TASK_MENGXI_DRIFT_GUARDRAILS_AND_COORDINATION.md`
- `docs/openclaw/MENGXI_TERRAFORM_LIVE_STATE_CHECKLIST.md`
