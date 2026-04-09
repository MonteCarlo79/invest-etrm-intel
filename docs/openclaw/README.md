# OpenClaw Mengxi Index

## Purpose

This is the operator entrypoint for the current Mengxi incident, governance, and coordination docs.

Use this page first before diving into individual runbooks.

---

## Start here

### Incident and lessons

- [MENGXI_INGESTION_INCIDENT_SUMMARY_2026-04-09.md](C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\docs\openclaw\MENGXI_INGESTION_INCIDENT_SUMMARY_2026-04-09.md)
- [MENGXI_INGESTION_GOVERNANCE_POSTMORTEM_2026-04-09.md](C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\docs\openclaw\MENGXI_INGESTION_GOVERNANCE_POSTMORTEM_2026-04-09.md)

### Terraform vs live-state validation

- [MENGXI_TERRAFORM_LIVE_STATE_CHECKLIST.md](C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\docs\openclaw\MENGXI_TERRAFORM_LIVE_STATE_CHECKLIST.md)

### Current Mengxi handoff and coordination

- [CODEX_TASK_MENGXI_DRIFT_GUARDRAILS_AND_COORDINATION.md](C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\docs\openclaw\CODEX_TASK_MENGXI_DRIFT_GUARDRAILS_AND_COORDINATION.md)

---

## Recommended reading order

1. Read the incident summary for the validated failure pattern.
2. Read the governance postmortem for the source-of-truth lessons.
3. Use the Terraform-vs-live checklist during diagnosis and repair.
4. Use the coordination task doc when handing work between operators, OpenClaw, and Codex.

---

## Working rule

For Mengxi infra incidents:

- GitHub branch/commit is the shared code/docs truth
- Terraform describes intended state
- live AWS inspection proves deployed state
- controlled rerun and logs prove runtime recovery
