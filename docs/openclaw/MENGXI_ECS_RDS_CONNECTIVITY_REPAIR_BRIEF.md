# MENGXI ECS ↔ RDS CONNECTIVITY REPAIR BRIEF

## Purpose

This brief is for Codex or an operator to diagnose and repair the underlying ECS/Fargate to RDS connectivity problem affecting the Mengxi ingestion/reconciliation workflow.

This brief is intentionally narrow.
It is not a request to redesign the ingestion system.

---

## Triggering incident

Observed failure pattern from production-style logs:

- ECS task starts normally
- `run_pipeline.py` repeatedly tries DB connectivity
- connection to Postgres endpoint times out on port `5432`
- after repeated retries the job fails with:
  - `psycopg2.OperationalError: ... port 5432 failed: timeout expired`
  - `RuntimeError: Database not reachable`

Observed endpoint from logs:
- host: `bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com`
- resolved IP seen in logs: `172.31.23.207`

---

## What is already done

Already implemented or being implemented separately:
- optional webhook alerting on terminal DB timeout failure
- repair/runbook markdown documenting likely cause buckets
- proactive sentinel spec for future operational monitoring

Those are useful, but they do **not** fix the connectivity problem itself.

---

## Objective

Find and fix the smallest underlying infrastructure/network/configuration issue that prevents the Mengxi ECS task from reaching the intended RDS instance on port `5432`.

Success condition:
- Mengxi ECS task can connect to the correct RDS endpoint reliably
- the job proceeds past `wait_for_db()` without timeout

---

## Constraints

- keep changes minimal
- preserve existing Terraform/ECS/EventBridge pattern
- do not redesign the whole stack
- do not make speculative schema or app changes
- do not silently widen scope into unrelated infra cleanup

---

## Most likely root-cause buckets

Treat these as hypotheses until verified.

### 1. Wrong or misleading subnet strategy
Current launcher code uses:
- `subnets = private_subnet_ids`
- `assignPublicIp = ENABLED`

This may be valid in some setups, but it is suspicious and should be examined carefully.

Questions:
- are these truly the intended private subnets?
- are they in the same VPC as the RDS instance?
- are their route tables correct?
- does enabling public IP here reflect the actual network design, or is it a misconfiguration carried forward?

### 2. Wrong RDS security group reference
Terraform creates an ingress rule on `var.rds_security_group_id` allowing source SG = ECS task SG on `5432`.

Questions:
- is `var.rds_security_group_id` actually attached to the target DB instance?
- does the DB have multiple SGs, with another one effectively controlling traffic?
- is the ECS task ENI actually using the intended ECS SG?

### 3. NACL blocking return traffic
If SGs look correct but timeouts persist, inspect subnet NACLs for both:
- ECS task subnets
- RDS subnets

Make sure ephemeral return traffic is allowed.

### 4. Wrong DB endpoint / wrong environment
Questions:
- is `PGURL` pointing at the right DB/environment?
- is the DB in the expected VPC/account/region?
- was the endpoint copied from a different environment?

### 5. RDS health / failover / maintenance
Less likely but should be verified quickly:
- current DB status
- recent failover/reboot/maintenance events
- whether the DB was actually available at the failure time

---

## Files to inspect first

### Terraform module
- `infra/terraform/mengxi-ingestion/main.tf`
- `infra/terraform/mengxi-ingestion/variables.tf`
- `infra/terraform/mengxi-ingestion/outputs.tf`
- `infra/terraform/mengxi-ingestion/lambda_function.py`

### Ingestion pipeline
- `bess-marketdata-ingestion/providers/mengxi/run_pipeline.py`

### Root/shared infra if referenced by vars
- `infra/terraform/main.tf`
- `infra/terraform/variables.tf`
- `infra/terraform/outputs.tf`
- any tfvars / deployment inputs used for this module

---

## Required verification steps

Codex/operator should produce evidence for each of these, not assumptions.

### A. Verify VPC alignment
Confirm:
- ECS task subnets belong to the same VPC as the RDS instance
- the module variable `vpc_id` matches the actual target environment

Deliverable:
- short note: `same_vpc = yes/no`

### B. Verify actual RDS-attached SGs
Confirm:
- which SG(s) are attached to the DB instance
- whether `var.rds_security_group_id` is one of them
- whether the created ingress rule lands on the right SG

Deliverable:
- exact SG IDs and whether the Terraform rule targets the attached one

### C. Verify actual ECS task ENI SGs/subnets
Confirm for a launched task:
- subnet ID used
- SG(s) attached to task ENI
- whether they match Terraform expectations

Deliverable:
- exact subnet + SG IDs observed at runtime

### D. Verify route tables / reachability assumptions
Confirm:
- subnet route tables for ECS and RDS path are sensible
- no hidden cross-VPC / peering / TGW assumption is missing

Deliverable:
- short explanation of why path should or should not be reachable

### E. Verify NACLs if SG check passes but timeout persists
Confirm:
- ingress/egress on relevant subnets does not block 5432 or return traffic

Deliverable:
- brief NACL conclusion

### F. Verify DB endpoint correctness
Confirm:
- hostname in `PGURL`
- intended environment
- whether endpoint resolves to the expected private IP range

Deliverable:
- endpoint correctness note

---

## Suggested smallest-fix candidates

Only apply the smallest change justified by evidence.

### Candidate fix 1
Correct the `rds_security_group_id` input if it points to the wrong SG.

### Candidate fix 2
Adjust ECS launch subnets to the correct private subnets that have working reachability to RDS.

### Candidate fix 3
Remove or change `assignPublicIp = ENABLED` if it is inconsistent with intended private-only RDS access path.

### Candidate fix 4
Correct mismatched VPC/subnet inputs for the Mengxi module.

### Candidate fix 5
Update route/NACL rules only if evidence shows they are the blocker.

---

## What not to do

- do not rewrite the ingestion app to work around broken infra
- do not add long blind retry loops as a substitute for fixing reachability
- do not broaden into full network refactoring unless explicitly needed
- do not claim the problem is solved just because alerting was added
- do not hardcode temporary DB endpoints in code

---

## Recommended Codex output format

When Codex finishes, it should report:

### 1. Observed root cause
Example:
- wrong RDS SG ID passed into module
- ECS task launched in wrong subnet set
- `assignPublicIp`/subnet combo inconsistent with actual private DB access path

### 2. Files changed
List exact Terraform or config files changed.

### 3. Why the change is sufficient
Short explanation of why this fixes reachability.

### 4. Risks / assumptions
Anything still not fully validated.

### 5. Validation plan
For example:
- terraform plan
- deploy module
- trigger launcher
- verify task reaches DB successfully
- verify pipeline gets past `wait_for_db()`

---

## Preferred validation after repair

Minimal but meaningful:
1. apply infra/config change in the intended environment
2. trigger one controlled Mengxi run
3. verify DB connectivity success message
4. verify pipeline proceeds into downloader/load phases
5. confirm no fresh timeout in CloudWatch logs

---

## Relationship to existing runbooks

Use together with:
- `docs/openclaw/MENGXI_DB_TIMEOUT_ALERTING_AND_CODEX_REPAIR.md`
- `docs/openclaw/MENGXI_PROACTIVE_FAILURE_SENTINEL_SPEC.md`

This brief is specifically for the **root-cause network/connectivity repair** task.

---

## Evidence labeling reminder

Final output must distinguish:
- **Observed** facts from logs/config/runtime inspection
- **Heuristic inference** when concluding likely infra/network cause

Do not present an unverified cause as certain.
