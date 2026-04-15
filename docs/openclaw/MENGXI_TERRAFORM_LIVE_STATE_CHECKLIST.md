# MENGXI TERRAFORM ↔ LIVE STATE CHECKLIST

## Purpose

Use this checklist before and after Mengxi infra repairs so operators do not confuse:

- local files
- GitHub branch state
- Terraform intent
- live AWS deployed state

This checklist is lightweight on purpose.

---

## A. GitHub state

- [ ] Confirm the working branch name
- [ ] Confirm the relevant commit is pushed to GitHub
- [ ] Record the exact commit SHA
- [ ] Record the exact file paths intended to own the change

If this is not pushed yet, call it local/provisional only.

---

## B. Terraform intent

- [ ] Record the exact Terraform module path
- [ ] Record the relevant variables/inputs
- [ ] State the intended ECS subnet/SG path
- [ ] State the intended RDS SG path
- [ ] State whether `terraform plan` was run
- [ ] State whether `terraform apply` was actually run

Important:

- `plan` is not proof of deployment
- code review is not proof of deployment

---

## C. Live AWS state

- [ ] Record the live ECS task definition/revision
- [ ] Record the live ECS task subnet ID(s)
- [ ] Record the live ECS task SG ID(s)
- [ ] Record the live RDS SG ID(s)
- [ ] Record the live DB endpoint
- [ ] Record whether live state matches Terraform intent

If this section is incomplete, you do not yet know deployed reality.

---

## D. Runtime evidence

- [ ] Record the failing or passing task/run ID
- [ ] Record the CloudWatch log group/stream
- [ ] Record the exact observed error or success signal
- [ ] Label each statement as observed or inferred

---

## E. Known Mengxi SG-drift rule

If the failure is the known DB timeout / SG-drift pattern:

- [ ] verify the ECS task SG actually used at runtime
- [ ] verify the RDS SG actually attached at runtime
- [ ] compare those values directly
- [ ] if drift is confirmed, rerun `terraform apply` in `infra/terraform/mengxi-ingestion/`
- [ ] rerun one controlled task after apply

Do not call the issue fixed until the rerun passes the DB connectivity step.

---

## F. Incident closure test

Close only if all are true:

- [ ] GitHub branch/commit is known
- [ ] Terraform owner path is known
- [ ] live AWS state has been inspected
- [ ] repaired state matches intent
- [ ] controlled rerun succeeded
- [ ] latest report clearly states whether deploy/apply is still required
