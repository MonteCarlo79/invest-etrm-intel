# CODEX TASK — MENGXI DRIFT GUARDRAILS AND COORDINATION

## Purpose

Add lightweight repo-level guardrails so future operators are much less likely to confuse:

- local code
- GitHub code
- Terraform intent
- live AWS deployed state

This is a coordination/control task, not an infra redesign task.

---

## Scope

### In scope

- markdown guardrails
- operator checklists
- explicit source-of-truth rules
- incident update format
- lightweight Codex/OpenClaw coordination rules

### Out of scope

- broad platform framework work
- mandatory CI/CD redesign
- large Terraform refactor
- replacing existing deployment paths

---

## Mandatory source-of-truth order

### 1. Shared code/docs

Use GitHub remote as the team source of truth.

Required behavior:

- do not describe local-only files as shared state
- say whether the relevant branch/commit has been pushed
- prefer branch + commit references in incident notes

### 2. Infra intent

Use Terraform files and vars as intended state only.

Required behavior:

- do not claim a running resource matches Terraform just because the code says so
- do not treat `terraform plan` as proof of deployed fix

### 3. Live deployed state

Use AWS API evidence as source of truth for what is actually running.

Required behavior:

- verify ECS task subnet and SGs from live task/task ENI
- verify RDS-attached SGs from live DB inspection
- verify runtime outcome from CloudWatch logs or controlled rerun

---

## Required incident update template

Every Mengxi infra/runtime incident update should contain these fields:

### GitHub state

- branch:
- pushed commit:
- files/paths intended to own the change:

### Terraform intent

- Terraform module/path:
- intended SG/subnet/endpoint behavior:
- whether `terraform apply` has been run:

### Live AWS state

- ECS task definition:
- ECS task subnet(s):
- ECS task SG(s):
- RDS SG(s):
- DB endpoint:

### Runtime evidence

- latest failing or passing task/run:
- relevant CloudWatch log group/stream:
- status:

### Assessment

- observed facts:
- inferred cause:
- next action:

---

## Required operator checks

Before escalation:

1. confirm the relevant change/docs are pushed to GitHub
2. name the exact branch and commit
3. identify the owning Terraform path
4. distinguish “intended” from “observed live”

Before declaring repair:

1. verify live ECS task SG/subnet
2. verify live RDS-attached SG
3. verify those match the intended path
4. rerun one controlled task
5. confirm the task gets past the failing connectivity step

---

## Codex/OpenClaw coordination rule

### OpenClaw

- may write incident docs on a docs/ops branch
- should include branch + path + commit when handing off
- should label statements as observed vs inferred where possible

### Codex

- should read authoritative docs from the named branch/ref when local branch visibility differs
- should not claim a doc is nonexistent when the real issue is branch visibility
- should report whether the implementation branch has been pushed before treating it as shared truth

---

## Minimal behavior rule for this exact Mengxi class

For a Mengxi ECS-to-RDS connectivity failure that matches the known SG-drift pattern:

1. do not start by changing ingestion code
2. first verify live ECS SG and live RDS-attached SG
3. if drift is confirmed, rerun `terraform apply` in `infra/terraform/mengxi-ingestion/`
4. then re-check live state and rerun one controlled task

---

## Deliverable expectation for future Codex work

When Codex performs a related repair or validation, the final report should include:

- branch
- upstream
- commits pushed
- exact files changed
- whether deploy/apply is still required
- whether live AWS validation was performed

---

## Companion checklist

Use:

- `docs/openclaw/MENGXI_TERRAFORM_LIVE_STATE_CHECKLIST.md`
