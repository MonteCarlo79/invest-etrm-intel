# Repo / Infra Reporting Rule

## Purpose

Make repo and infrastructure investigations easier to review by forcing the basic delivery state to be reported every time.

This is a lightweight process rule.

---

## Mandatory reporting fields

For future repo, Terraform, deployment, AWS-runtime, or operational investigations, the final report must always state explicitly:

- branch
- exact commits
- pushed status
- deploy status

---

## Required meaning

### Branch

Name the working branch used for the investigation or change.

### Exact commits

List the exact commit SHA values relevant to the work.

### Pushed status

State whether those commits are pushed to GitHub.
If only local, say so explicitly.

### Deploy status

State whether any deploy, `terraform apply`, task-definition update, rerun, or other runtime action is still required.

---

## Why this matters

This prevents a common class of confusion:

- code may be changed locally but not pushed
- Terraform may be updated but not applied
- a branch may exist on GitHub but not be deployed
- live AWS state may still differ from both local and GitHub code

---

## Minimum final-report example

- branch: `codex/example-branch`
- exact commits: `abc1234`, `def5678`
- pushed status: pushed to `origin/codex/example-branch`
- deploy status: no deploy required

If deploy is still needed, say that directly.
