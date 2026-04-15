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

### Provenance finding

Later evidence clarified that the drift was not abstract or accidental:

- both drift artefacts were created by the AWS root account
- both originated from AWS Console usage
- the rogue ECS service came from ECS Console `Create Service`
- AWS wrapped that service in CloudFormation
- the orphan EventBridge target persisted because Terraform never owned that target ID in state
- the same source IP observed for those artefacts matched the `terraform-admin` path, indicating the same operator/machine

This matters because it turns the lesson from generic “drift can happen” into a concrete governance rule: avoid console-side changes that bypass Terraform ownership unless they are explicitly temporary, recorded, and cleaned up.

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

Additional lesson:

- if a resource was created from the AWS Console and Terraform never imported/owned it in state, Terraform reviews alone will miss it

### 3. Console/live AWS observations were not tied back to branch/commit evidence cleanly

Operators could identify live SG IDs but still lacked a lightweight pattern for tying those findings back to:

- the GitHub branch
- the exact Terraform module
- the next repair action

Guardrail:

- every incident update should include branch, commit, Terraform path, and live AWS evidence
- provenance should be captured when available:
  - creating principal/account
  - creation path (`Terraform`, `AWS Console`, `CloudFormation`, etc.)
  - source IP or equivalent operator trace if available

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
- resources not owned in Terraform state may survive even when they contradict Terraform intent

### Deployed reality

- live AWS API results are source of truth for actual deployed state
- ECS task ENI, subnet, SG, RDS SG attachment, and CloudWatch logs outrank assumptions
- CloudTrail/creation provenance can explain why live reality diverged

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
6. provenance details when available:
   - creating account/principal
   - creation path
   - source IP/operator trace
7. next operator action

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
