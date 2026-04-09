# MENGXI INGESTION INCIDENT SUMMARY — 2026-04-09

## Purpose

Record the exact failure mode, the validated root cause, and the operational lesson from the April 2026 Mengxi ingestion incident.

This note is intentionally narrow.
It is not a broad platform postmortem.

---

## Incident summary

Observed production-style failure:

- Mengxi ECS/Fargate ingestion task started normally
- `run_pipeline.py` retried DB connectivity
- connection to RDS Postgres on `5432` timed out repeatedly
- the task ended with `RuntimeError: Database not reachable`

Observed endpoint:

- RDS host: `bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com`
- resolved private IP seen in logs: `172.31.23.207`

---

## What was validated

Validated from live AWS inspection:

- ECS task and RDS were in the same VPC
- subnet placement was not proven to be the primary blocker
- the critical mismatch was security-group drift between Terraform intent and live attached state
- both drift artefacts were created outside Terraform ownership via AWS Console usage
- both drift artefacts were attributed to the AWS root account
- the same source IP seen for the console artefacts matched the `terraform-admin` operator path, indicating the same operator/machine was involved

Validated live mismatch:

- launcher/task traffic path used SG `sg-024c9057983f9e0de`
- RDS SG allowed `5432` from `sg-08576f2bea0274a81`

Result:

- the DB ingress rule in live AWS did not match the SG actually used by the failing task path
- the task timed out at the network layer

Validated provenance details:

- the rogue ECS service was created through ECS Console `Create Service`
- AWS wrapped that console-created service in CloudFormation-managed resources
- the orphan EventBridge target persisted because Terraform never owned that target ID in state

---

## Root-cause conclusion

### Observed

- recurring timeout was caused by a live ECS-to-RDS SG path mismatch
- the deployed/live state did not match the intended Terraform connectivity path
- the relevant drift artefacts were console-created rather than Terraform-state-owned
- the artefact provenance pointed to AWS root account activity

### Best conclusion

This was primarily a **Terraform-applied-state / live-console drift problem**, not a loader-code bug.

More specifically:

- a console-created ECS service introduced rogue runtime behavior outside Terraform control
- a non-Terraform-owned EventBridge target remained in place
- both artefacts came from the same operator/machine path indicated by shared source IP evidence

### Important nuance

This does **not** automatically prove the Terraform definitions were perfect.
It does prove that the immediate production incident was explained by live-state drift rather than by ingestion logic.

---

## First operational fix

For this exact known Mengxi SG-drift pattern, the first operational repair step is:

1. rerun `terraform apply`
2. in `infra/terraform/mengxi-ingestion/`
3. then re-verify live ECS task SG and RDS-attached SGs from AWS APIs

---

## What caused confusion

The incident became harder than necessary because operators were mixing four different states:

1. local branch contents
2. GitHub branch contents
3. Terraform-defined intended state
4. live AWS deployed state

The key lesson:

- GitHub is the source of truth for shared code/docs
- Terraform files describe intent
- only live AWS inspection proves deployed reality

---

## Required follow-up behavior

Future operators should not mark this class of issue resolved until they can show:

- the GitHub branch containing the intended infra/config change
- the exact Terraform module/inputs being applied
- the live ECS task subnet/SG actually launched
- the live RDS-attached SGs actually present
- the post-change Mengxi task passing DB connectivity

---

## Related docs

- `docs/openclaw/MENGXI_INGESTION_GOVERNANCE_POSTMORTEM_2026-04-09.md`
- `docs/openclaw/CODEX_TASK_MENGXI_DRIFT_GUARDRAILS_AND_COORDINATION.md`
- `docs/openclaw/MENGXI_TERRAFORM_LIVE_STATE_CHECKLIST.md`
