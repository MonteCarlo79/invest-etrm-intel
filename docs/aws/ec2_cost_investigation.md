# EC2 Cost & Operations Investigation (bess-platform)

Date: 2026-04-20  
Operator: Codex (read-only investigation)  
AWS account: `319383842493`  
Primary region with matches: `ap-southeast-1`

## 0) Safety guardrails and plan (before any changes)

This investigation was executed as **read-only**.  
No instance stop/start/resize/terminate, no SG edits, no route/DNS edits, and no detach/delete actions were performed.

Planned phases:
1. Identify exact billed instance families (`t3.xlarge`, `t2.micro`) across all enabled regions.
2. Map each instance to dependencies (LB/DNS/ASG/EIP/RDS/SG/SSM/Terraform references).
3. Pull utilization signals (CloudWatch CPU/network, activity history).
4. Propose risk-ranked cost options with dependency checks + rollback path.

## 1) Exact charged resources found

Cross-region EC2 sweep found exactly **3** matching running instances, all in `ap-southeast-1`.

| Instance ID | Type | State | Launch (UTC) | Platform | Private IP | Public IP | AZ | Subnet | VPC | Security Groups | IAM Profile | Key Pair | EBS | ASG |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `i-078297b9e83f03dc1` | `t3.xlarge` | running | 2026-03-30 13:44:53 | Linux/UNIX | 172.31.30.155 | 54.254.121.148 | ap-southeast-1b | subnet-0d561ea9ef0242812 | vpc-0e44e77436492fc1a | sg-0e9ac24004d8ca851 (`launch-wizard-1`) | none | besskeys | vol-068c35e60ea48dd1f (80 GiB gp3) | none |
| `i-0196ded69f4366656` | `t2.micro` | running | 2026-02-17 01:46:33 | Linux/UNIX | 172.31.31.123 | 54.169.226.139 | ap-southeast-1b | subnet-0d561ea9ef0242812 | vpc-0e44e77436492fc1a | sg-0a2794c39be902973 (`ec2-rds-3`), sg-0a060f8f8d1c62c35 (`bess-platform-rds-sg`) | `arn:aws:iam::319383842493:instance-profile/bess-ec2-role` | besskeys | vol-0a9e87184e5101642 (8 GiB gp3) | none |
| `i-0ccd81f0ce5bd9fa0` | `t2.micro` | running | 2026-02-15 17:01:48 | Linux/UNIX | 172.31.20.75 | 52.77.219.97 | ap-southeast-1b | subnet-0d561ea9ef0242812 | vpc-0e44e77436492fc1a | sg-0ace3a874e8083049 (`ec2-rds-1`), sg-0a060f8f8d1c62c35 (`bess-platform-rds-sg`) | none | none | vol-0f66f0c99fd23e0a1 (8 GiB gp3) | none |

Notes:
- `Name` tags are absent on all three instances.
- No additional `t3.xlarge`/`t2.micro` were found in other enabled regions.

## 2) Purpose and dependency analysis

## 2.1 Shared dependency checks (all 3 instances)

- ALB/NLB target groups: **no attachments**
- Classic ELB: **no attachments**
- Auto Scaling groups: **none**
- Elastic IP associations: **none**
- Route53 records directly referencing these IPs: **none found**
- CloudWatch alarms scoped to these instance IDs: **none found**
- User data: **none**
- API stop/termination protection: **disabled** on all 3

Implication: no evidence they are fleet-managed app nodes; they look like standalone/manual hosts.

## 2.2 Instance-by-instance purpose signal

### A) `i-078297b9e83f03dc1` (`t3.xlarge`)

Evidence:
- Private IP `172.31.30.155` matches repo docs identifying a **Tailscale jump host** path (`docs/knowledge_pool_aws_migration_recon.md:62`, `docs/knowledge_pool_aws_runbook.md:20`, `docs/knowledge_pool_aws_validation.md:141`).
- Security group `launch-wizard-1` has SSH 22 open to `0.0.0.0/0`.
- CloudTrail shows repeated `SendSSHPublicKey` events by `root` in early April.
- Not attached to RDS SG directly.

Assessment:
- Most likely **admin/jump/gateway host** (manual access pattern), not an ECS/ALB production serving node.
- Criticality: **Medium (operational admin access), not confirmed production data-plane critical**.

### B) `i-0196ded69f4366656` (`t2.micro`)

Evidence:
- Attached to `bess-platform-rds-sg` and `ec2-rds-3`; ENI mapping confirms DB-network adjacency.
- Has instance profile `bess-ec2-role`; CloudTrail shows frequent STS `AssumeRole` activity.
- Role has very broad permissions (EC2/IAM/VPC/S3/CloudWatch/ECR full access + ECS exec/session inline policy).

Assessment:
- Likely **active admin/automation or operations helper host** with DB/API access.
- Criticality: **High uncertainty; treat as potentially critical** until in-host workload inventory is verified.

### C) `i-0ccd81f0ce5bd9fa0` (`t2.micro`)

Evidence:
- Attached to `bess-platform-rds-sg` and `ec2-rds-1`; likely DB-access-oriented host.
- No IAM profile, no key pair, no recent CloudTrail lifecycle changes; only historical `SendSSHPublicKey` near creation.
- No clear Terraform ownership or repo references.

Assessment:
- Likely **legacy/manual utility host** or former DB access node.
- Criticality: **Unknown**, but lower confidence of active automation than `i-0196...`.

## 2.3 Terraform/repo ownership signal

- No Terraform EC2 resource patterns found (`aws_instance`, launch templates, ASGs, bastion/jump/tailscale refs absent in `infra/terraform` search).
- Existing Terraform clearly manages RDS/ECS/security patterns (for example `infra/terraform/main.tf:121` for RDS SG naming and ECS/RDS wiring), but not these EC2 hosts.

Assessment:
- These EC2 instances appear **manually created/out-of-band** from current Terraform module set.

## 3) Utilization and right-sizing signal (last ~30 days)

CloudWatch `AWS/EC2` (1-hour period):

| Instance ID | Avg CPU | P95 CPU | Peak CPU | Hours sampled | CPU >5% hours | CPU >10% hours | Avg NetIn (bytes/h) | Avg NetOut (bytes/h) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `i-0196ded69f4366656` | 2.38% | 2.49% | 9.80% | 721 | 0 | 0 | 2,446 | 1,295 |
| `i-0ccd81f0ce5bd9fa0` | 2.37% | 2.66% | 13.65% | 721 | 0 | 0 | 2,677 | 1,185 |
| `i-078297b9e83f03dc1` | 1.45% | 8.18% | 42.55% | 493 | 31 | 20 | 84,759 | 1,126,285 |

Additional observations:
- No `CWAgent` metrics (memory/disk-in-guest telemetry unavailable).
- No `DiskReadOps`/`DiskWriteOps` datapoints in default EC2 metrics.

Interpretation:
- Both `t2.micro` hosts look very lightly loaded from CPU/network perspective.
- `t3.xlarge` is also mostly low CPU, but with intermittent bursts and materially higher network egress.

## 4) Bill-line mapping and confidence

Given latest bill lines provided:
- Linux/UNIX `t3.xlarge`: **444.749 hours**, about **USD 93.93**
- Linux/UNIX `t2.micro`: **888.807 hours**, about **USD 12.98**

Most likely mapping:
- `t3.xlarge` hours map to `i-078297b9e83f03dc1` (the only matching instance type in account sweep).
- `t2.micro` hours map to combined runtime of `i-0196...` + `i-0ccd...` (the only two matching instances).

Confidence:
- Type-to-resource mapping confidence: **High** (only 1 `t3.xlarge`, only 2 `t2.micro` found across enabled regions).
- Direct Cost Explorer resource-ID confirmation: **Not available** (resource-level CE not enabled in payer settings).

## 5) Ranked savings options with dependency checks + rollback

## Option A: No-interruption savings (safest immediate)

1) Add ownership/criticality tags and cost allocation tags to all 3 EC2 instances (no runtime impact).  
2) If these hosts are expected to remain 24/7, evaluate Savings Plan/RI coverage (financial optimization, no runtime interruption).

Estimated savings:
- Tagging itself: $0 direct, but enables accountability and follow-up savings.
- Savings Plan/RI: depends on commitment term; potentially meaningful for always-on baseline.

Risk:
- Operational risk: **Very low**.
- Financial commitment risk (for SP/RI): **Medium** if workload is retired soon.

Rollback:
- Tags can be reverted immediately.
- SP/RI cannot be “rolled back” operationally; treat as financial decision only after 2-4 weeks validation.

## Option B: Low-risk right-sizing (controlled maintenance window)

Candidate:
- `i-078297b9e83f03dc1` (`t3.xlarge`) -> trial `t3.large` (or `t3.medium` if validated).

Why:
- CPU mostly low (avg 1.45%, P95 8.18%), indicating likely overprovisioned compute.

Expected savings:
- Roughly 50%-75% of that line item depending target size.
- Using bill baseline (~$93.93 over 444.749h): approx **$47 to $70** savings for similar usage hours.

Pre-change validation steps (must pass first):
1. Confirm host purpose and active processes (interactive check by operator).
2. Verify no hard dependency on current public IP (no EIP attached; stopping can change IP).
3. Snapshot/AMI baseline.
4. Announce maintenance window + smoke-test checklist (SSH, admin workflows, any jump-routing path).

Rollback:
1. Resize back to `t3.xlarge`.
2. Restart instance.
3. Re-run smoke tests.

## Option C: Schedule / migrate / retire (highest savings, more dependency work)

### C1) `i-0ccd81f0ce5bd9fa0` (`t2.micro`) retirement candidate (after proof)

Why candidate:
- No IAM role, weak recent activity signals, manual/legacy pattern.

Expected savings:
- Up to entire share of `t2.micro` line attributable to this instance (order of magnitude ~half if both run similarly).

Required dependency removal checks first:
1. In-host workload inventory (cron/systemd/docker/tmux jobs).
2. Connection checks to DB and external systems.
3. Confirm no admin workflows rely on this box.
4. Keep EBS snapshot before stop.

Safe execution:
1. Stop for short observation window.
2. Monitor ingestion, dashboards, admin tasks, and DB access for regressions.
3. If stable, keep stopped or retire.

Rollback:
- Start instance immediately from stopped state.

### C2) `i-078...` admin-host schedule (if confirmed non-24/7)

Why:
- If it is jump/admin-only, schedule uptime for business hours/on-demand.

Expected savings:
- Potentially large portion of `t3.xlarge` cost depending off-hours.

Required checks:
1. Verify no batch jobs or tunnels require 24/7 runtime.
2. Verify emergency access path exists (alternate bastion/SSM).

Rollback:
- Disable schedule, return to 24/7.

## 6) Uncertainties (explicit)

1. Resource-level Cost Explorer attribution is unavailable due payer-account CE setting; exact line-item-to-instance ID mapping is inferred from unique type inventory.
2. No SSM-managed inventory on these hosts, so in-guest cron/systemd/job evidence was not directly collectible in this pass.
3. No Name tags/owner tags exist, reducing certainty on business purpose.
4. Could not find direct Terraform ownership of these EC2 nodes; they appear manual/out-of-band.

## 7) Actions taken

- Performed read-only AWS CLI investigation and local evidence capture in `docs/aws/_tmp_*`.
- Created this report: `docs/aws/ec2_cost_investigation.md`.
- No operational changes made to AWS resources.

## Phase 2: Host-level validation and change-ready recommendations

Date: 2026-04-20

## 8) Access-path validation (all three instances)

- SSM managed status: **not managed** (`aws ssm describe-instance-information` returned no instance IDs).
- EC2 Instance Connect Endpoint: **none** in region.
- SSH path:
  - `i-078297b9e83f03dc1` (`t3.xlarge`): SG allows `22/tcp` from `0.0.0.0/0`; CloudTrail shows repeated `SendSSHPublicKey` through 2026-04-05.
  - `i-0ccd81f0ce5bd9fa0` (`t2.micro`): SG allows `22/tcp` from `0.0.0.0/0`; CloudTrail `SendSSHPublicKey` only around 2026-02-15/16.
  - `i-0196ded69f4366656` (`t2.micro`): SG allows `22/tcp` only from `138.113.14.246/32`; no recent `SendSSHPublicKey` events.
- Bastion/jump usage signal:
  - `i-078...` private IP `172.31.30.155` matches repo docs describing a Tailscale jump host (`docs/knowledge_pool_aws_migration_recon.md:62`, `docs/knowledge_pool_aws_runbook.md:20`, `docs/knowledge_pool_aws_validation.md:141`).

## 9) Runtime behavior validation status

Requested host-level checks (processes, docker, systemd, cron, recent logins, listeners, disk usage) were attempted via available safe paths.

What is verified:
- Boot/runtime console logs collected via EC2 console output.
- CloudTrail access/activity patterns collected.
- CloudWatch CPU/network utilization collected.

What remains unverified without host shell access:
- Running process list (`ps`, `top`, `pgrep`)
- Docker/containerd state
- systemd service inventory
- crontab/timers inventory
- shell history / user-level command history
- open listening ports from host perspective (`ss`, `netstat`)
- filesystem occupancy and data retention risks

Additional access needed to complete full host-level proof:
1. One of:
   - SSM managed-instance enablement (role + SSM connectivity), or
   - Approved SSH credential path for each host (for `i-078`/`i-0196` likely `besskeys`; `i-0ccd` has no keypair and likely needs EIC/other access path).
2. Read-only command set on host (`ps`, `systemctl`, `crontab -l`, `ss -lntup`, `df -h`, `last`, `docker ps`).

## 10) Instance-level findings and recommendations

### 10.1 `i-078297b9e83f03dc1` (`t3.xlarge`)

A. Current observed purpose  
Likely admin/jump host (human interactive use), likely the Tailscale jump path.

B. Evidence
- IP matches documented jump-host reference (`172.31.30.155`) in repo docs.
- CloudTrail shows frequent `SendSSHPublicKey` by `root` between 2026-03-31 and 2026-04-05.
- Console output confirms Ubuntu host with `besskeys` authorized and interactive login prompt.
- No ALB/ASG/EIP/Route53 direct dependency; standalone host.
- CPU mostly low (avg 1.45%, P95 8.18%) with occasional spikes; non-trivial network egress.

C. Criticality rating  
`likely needed` (for admin/jump use), but not proven as production data-plane critical.

D. Best cost action  
`downsize` (change-ready recommendation: `t3.xlarge` -> `t3.large` first, then reassess for `t3.medium`).

E. Preconditions before action
1. Confirm no long-running job/container on host (requires host shell access).
2. Confirm alternate emergency admin path exists during maintenance window.
3. Snapshot/AMI before resize.
4. Validate jump-host workflows post-change (SSH login, DB/admin path used by operators).

F. Rollback plan  
Resize back to `t3.xlarge`, reboot if required, rerun jump-host smoke tests.

### 10.2 `i-0196ded69f4366656` (`t2.micro`)

A. Current observed purpose  
Active admin/automation helper with IAM-backed AWS API activity.

B. Evidence
- Attached IAM instance profile `bess-ec2-role` (broad EC2/IAM/VPC/S3/CloudWatch/ECR permissions).
- CloudTrail shows continuous hourly STS `AssumeRole` events up to 2026-04-20 10:40:34 +08:00.
- Security groups grant DB-path adjacency (`ec2-rds-3` + `bess-platform-rds-sg`).
- Inbound SSH restricted to specific source (`138.113.14.246/32`), suggesting controlled operator automation/admin host.

C. Criticality rating  
`critical` until host-level process/job inventory proves otherwise.

D. Best cost action  
`keep as-is` for now; plan `migrate function elsewhere` only after explicit workload extraction.

E. Preconditions before action
1. Host-level inventory of running jobs/services/scripts and IAM API usage purpose.
2. Dependency mapping for any DB admin/ingestion tooling run from this host.
3. Re-home validated jobs to ECS/Lambda/CI runner before stop test.

F. Rollback plan  
If migration attempt causes regressions, revert jobs back to host and retain instance unchanged.

### 10.3 `i-0ccd81f0ce5bd9fa0` (`t2.micro`)

A. Current observed purpose  
Likely legacy/manual DB-access utility host with low current human activity.

B. Evidence
- No IAM profile, no key pair, no Name tag.
- CloudTrail: last `SendSSHPublicKey` activity clustered near launch (2026-02-15/16), no recent evidence.
- Console output states: `no authorized SSH keys fingerprints found for user ec2-user`.
- Low CPU/network profile (avg CPU ~2.37%, P95 2.66%).
- Not attached to LB/ASG/EIP/Route53 direct dependency.

C. Criticality rating  
`likely removable` (with caution due missing in-host visibility).

D. Best cost action  
`stop/retire` via controlled stop-test.

E. Preconditions before action
1. Snapshot root volume.
2. 24-hour pre-check with ops users for any manual dependency.
3. Execute stop during low-risk window.
4. Observe 3-7 days: ingestion, admin scripts, DB access paths, OpenClaw workflows.

F. Rollback plan  
Immediate `StartInstances` on the same instance ID if any issue appears; verify dependent workflows recover.

## 11) Special-focus outcomes

### 11.1 `i-0ccd...` prove/disprove needed
- Evidence leans to low active use (no recent SSH activity, no IAM role, no authorized key evidence in boot log).
- Still not fully proven without host shell checks.
- Change-ready approach: **safe stop-test with snapshot + rollback**.

### 11.2 `i-078...` right-size justification
- Observed workload does not justify `t3.xlarge` from CPU perspective.
- Recommended path: `t3.large` first (lower risk than jumping directly to `t3.medium`), then reassess.

### 11.3 `i-0196...` practical meaning of “active admin/automation helper”
- Practical signal is continuous role-session issuance + DB-adjacent SG + restricted SSH source.
- Most likely breakage risk if removed: admin scripts, AWS control-plane helpers, or DB-access workflows currently executed from host.

## 12) Final decision table

| Instance ID | Current role | Confidence | Recommended action | Estimated savings | Operational risk | Rollback complexity |
|---|---|---|---|---|---|---|
| `i-078297b9e83f03dc1` | Admin/jump host (likely Tailscale jump path) | Medium-High | Downsize to `t3.large` after prechecks | ~USD 45-50/month vs current `t3.xlarge` line (order-of-magnitude from bill baseline) | Medium | Low-Medium |
| `i-0196ded69f4366656` | Active admin/automation helper with IAM role | Medium | Keep as-is now; migrate function only after host-level inventory | 0 immediate (migration later could remove ~USD 6-7/month share) | High if changed prematurely | Medium |
| `i-0ccd81f0ce5bd9fa0` | Legacy/manual DB-access utility (likely low use) | Medium | Controlled stop-test, then retire if no impact | ~USD 6-7/month share of `t2.micro` line | Low-Medium with staged test | Low |

## Phase 3: Execution runbooks for safe cost reduction

Important: This section is planning-only. No changes executed.

## 13) Runbook 1: Controlled stop-test (`i-0ccd81f0ce5bd9fa0`)

Goal: Prove whether host can be safely retired with zero interruption.

### 13.1 Pre-stop checklist

1. Confirm change window and owner-on-call.
2. Notify operators that a reversible stop-test is planned.
3. Confirm no known critical manual dependency (DB tunnel, scripts, ad hoc ops).
4. Confirm baseline health is green:
   - ECS services healthy
   - EventBridge-triggered ingestion succeeding
   - RDS reachable from normal production paths
5. Confirm instance metadata snapshot to file:
   - instance attributes, SGs, subnet/VPC, EBS attachments, route context.

### 13.2 Backup and metadata capture (mandatory)

1. Capture metadata (JSON export):
   - `aws ec2 describe-instances --region ap-southeast-1 --instance-ids i-0ccd81f0ce5bd9fa0`
   - `aws ec2 describe-volumes --region ap-southeast-1 --volume-ids vol-0f66f0c99fd23e0a1`
2. Create EBS snapshot of root volume:
   - `aws ec2 create-snapshot --region ap-southeast-1 --volume-id vol-0f66f0c99fd23e0a1 --description "pre-stop-test i-0ccd81f0ce5bd9fa0 YYYY-MM-DD"`
3. (Optional stronger rollback) Create AMI:
   - `aws ec2 create-image --region ap-southeast-1 --instance-id i-0ccd81f0ce5bd9fa0 --name "pre-stop-test-i-0ccd81f0ce5bd9fa0-YYYYMMDD" --no-reboot`
4. Wait for snapshot/AMI completion before stop.

### 13.3 Pre-stop monitoring baseline (record first)

Record previous 24h:
1. EC2 CPU/Network trends for instance.
2. RDS active connections and error counts.
3. Ingestion pipeline health indicators:
   - latest successful runs for scheduled jobs
   - no sudden fail spike in CloudWatch Logs/alarms.
4. Operator validation:
   - no active SSH session expected on this host.

### 13.4 Stop-test execution steps

1. Stop instance (do not terminate):
   - `aws ec2 stop-instances --region ap-southeast-1 --instance-ids i-0ccd81f0ce5bd9fa0`
2. Confirm stopped:
   - `aws ec2 describe-instances --region ap-southeast-1 --instance-ids i-0ccd81f0ce5bd9fa0 --query "Reservations[].Instances[].State.Name" --output text`
3. Start observation timer immediately.

### 13.5 Post-stop monitoring checks

Check at +15m, +1h, +4h, +24h, then daily during window:
1. Ingestion and scheduled job success rates unchanged.
2. No new DB access failures/timeouts attributable to missing host.
3. No operator-reported breakage in admin workflows.
4. No new critical alarms in related services (ECS, Lambda, RDS, EventBridge-linked jobs).

### 13.6 Observation window recommendation

- Minimum: 72 hours  
- Preferred: 7 days (covers weekday patterns + manual admin cycles)

### 13.7 Rollback procedure

Rollback trigger: any confirmed operational regression linked to host stop.

1. Start instance:
   - `aws ec2 start-instances --region ap-southeast-1 --instance-ids i-0ccd81f0ce5bd9fa0`
2. Confirm running and reachable.
3. Re-run failed workflow(s) and validate recovery.
4. Document incident and defer retirement decision.

Expected rollback time:
- EC2 start + basic validation: typically 3-10 minutes.

### 13.8 Decision criteria for permanent retirement

Retire only if all are true:
1. Full observation window passes with no service/admin/ingestion impact.
2. No owner claims dependency.
3. No hidden scheduled task discovered.
4. Snapshot/AMI retained for agreed retention period.

Retirement step (separate approved change):
- Terminate instance only after explicit sign-off; keep snapshot/AMI for rollback safety.

## 14) Runbook 2: Controlled right-sizing (`i-078297b9e83f03dc1`, `t3.xlarge` -> `t3.large`)

Goal: Reduce cost with reversible, low-risk sizing change.

### 14.1 Pre-change validation

1. Confirm host role owner (likely jump/admin path) is on standby.
2. Confirm no critical session is active.
3. Validate no LB/ASG hard dependencies (already checked).
4. Validate no static-IP dependency:
   - host has no EIP; stop/start may change public IP.
   - confirm users access by DNS/SSM/known procedure, not fixed IP.
5. Validate baseline:
   - recent CPU/network behavior captured
   - known admin tasks tested pre-change.

### 14.2 Backup / rollback preparation

1. Capture instance metadata export:
   - `aws ec2 describe-instances --region ap-southeast-1 --instance-ids i-078297b9e83f03dc1`
2. Snapshot root volume (`vol-068c35e60ea48dd1f`):
   - `aws ec2 create-snapshot --region ap-southeast-1 --volume-id vol-068c35e60ea48dd1f --description "pre-resize i-078297b9e83f03dc1 YYYY-MM-DD"`
3. Optional AMI:
   - `aws ec2 create-image --region ap-southeast-1 --instance-id i-078297b9e83f03dc1 --name "pre-resize-i-078297b9e83f03dc1-YYYYMMDD" --no-reboot`
4. Confirm snapshots complete.

### 14.3 Change steps

1. Stop instance:
   - `aws ec2 stop-instances --region ap-southeast-1 --instance-ids i-078297b9e83f03dc1`
2. Modify type:
   - `aws ec2 modify-instance-attribute --region ap-southeast-1 --instance-id i-078297b9e83f03dc1 --instance-type "{\"Value\":\"t3.large\"}"`
3. Start instance:
   - `aws ec2 start-instances --region ap-southeast-1 --instance-ids i-078297b9e83f03dc1`
4. Confirm running + new type:
   - `aws ec2 describe-instances --region ap-southeast-1 --instance-ids i-078297b9e83f03dc1 --query "Reservations[].Instances[].{State:State.Name,Type:InstanceType,PublicIp:PublicIpAddress,PrivateIp:PrivateIpAddress}" --output table`

### 14.4 Post-change validation

1. Confirm operator SSH/jump workflow works.
2. Confirm any known tunnel/admin scripts still work.
3. Monitor CPU/network for 24-72h for throttling/saturation symptoms.
4. Confirm no new operational alarms or operator complaints.

### 14.5 Rollback steps

Rollback trigger:
- admin/jump workflows degrade, connection instability appears, or resource pressure increases materially.

1. Stop instance.
2. Change type back to `t3.xlarge`.
3. Start instance.
4. Re-validate jump/admin workflows.

Expected rollback time:
- Type revert + restart + validation: typically 10-20 minutes.

### 14.6 Hidden-workload risk statement

If this host is silently running gateway/tunnel/orchestration tasks not visible from current evidence, risks include:
1. Broken admin access path.
2. Failed ad hoc operational scripts.
3. Latency/perf issues in undocumented background jobs.

Mitigation:
- Enforce owner-attended window, pre/post smoke tests, and immediate rollback criteria.

## 15) Hold recommendation: `i-0196ded69f4366656` (no change now)

Recommendation: `hold`.

Reason:
- Continuous STS `AssumeRole` activity indicates ongoing automated/API use.
- Instance has powerful IAM role and DB-adjacent security-group posture.

Evidence gaps to close before any cost action:
1. In-host process/service inventory (what is invoking role usage).
2. Cron/systemd/docker inventory.
3. Exact business workflow mapping (which jobs would break if host removed).
4. Controlled migration target for each discovered function (ECS/Lambda/runner).

Until those gaps are closed, treat this host as potentially critical.

## 16) Approval summary (change-ready)

| Change | Expected savings | Risk | Rollback time |
|---|---:|---|---|
| Stop-test `i-0ccd81f0ce5bd9fa0` (no terminate) | ~USD 6-7/month if retirement confirmed | Low-Medium (with staged observation) | 3-10 minutes |
| Resize `i-078297b9e83f03dc1` `t3.xlarge` -> `t3.large` | ~USD 45-50/month (order-of-magnitude) | Medium (hidden admin workload risk) | 10-20 minutes |
| Keep `i-0196ded69f4366656` unchanged | 0 immediate | Lowest operational risk now | N/A |

## 17) Execution log (approved actions)

Execution date: 2026-04-20 (Asia/Shanghai)

### 17.1 Action 1: Controlled stop-test `i-0ccd81f0ce5bd9fa0`

Status: **Executed** (instance stopped; rollback not required so far)

Timestamps:
- Pre-change baseline captured: ~13:45-13:49 +08:00
- Rollback artifacts initiated: `2026-04-20 13:49:05 +08:00`
- Snapshot completed: ~`13:50:xx +08:00` (`snap-0b61d9d82bae084d6`)
- Stop requested: `2026-04-20 14:02:29 +08:00` (`06:02:29 UTC`)
- Confirmed stopped: `2026-04-20 14:02:xx +08:00`
- Post-stop health recheck completed: `2026-04-20 14:33:30 +08:00`

Rollback protection artifacts:
- Snapshot: `snap-0b61d9d82bae084d6` (completed)
- AMI: `ami-028afe66bb480be33` (created; state was pending when checked)

Pre/post validation summary:
- RDS `bess-platform-pg`: remained `available`
- ECS service counts in `bess-platform-cluster`: unchanged from baseline
- CloudWatch alarms in `ALARM` state: none returned

Operational impact:
- No observed platform impact in immediate/short rechecks.
- Stop-test observation window is **still in progress** (recommended 72h minimum, 7 days preferred).

Rollback needed:
- **No**

Updated monthly savings estimate after this action:
- Immediate run-rate reduction while instance stays stopped: ~USD **6-7/month** (estimate from prior `t2.micro` allocation share).

### 17.2 Action 2: Right-size `i-078297b9e83f03dc1` (`t3.xlarge` -> `t3.large`)

Status: **Executed successfully** (now running as `t3.large`; rollback not required)

Timestamps:
- Rollback artifacts initiated: `2026-04-20 14:36:24 +08:00`
- Snapshot completed: ~`14:5x +08:00` (`snap-083adfde8a0420410`)
- First resize attempt started: `2026-04-20 14:50:38 +08:00`
- First attempt outcome: CLI quoting error on modify step; instance restarted still `t3.xlarge`
- Corrected retry started: `2026-04-20 14:54:14 +08:00`
- Retry outcome: instance running `t3.large` at `2026-04-20 14:54:44 +08:00` (`06:54:44 UTC`)

Rollback protection artifacts:
- Snapshot: `snap-083adfde8a0420410` (completed)
- AMI: `ami-0267cab4efc6c2bc6` (created; state was pending when checked)

Pre/post validation summary:
- Final instance state: `running`, type `t3.large`, private IP unchanged `172.31.30.155`
- RDS `bess-platform-pg`: `available`
- ECS service counts: unchanged vs baseline
- CloudWatch alarms in `ALARM` state: none returned

Operational impact:
- Public IP changed during restart cycles:
  - Before changes: `54.254.121.148`
  - Intermediate after first restart: `13.229.131.211`
  - Final after successful resize: `13.250.37.64`
- This confirms documented risk for workflows that rely on static public IP.
- No immediate platform health degradation observed from AWS-level checks.

Rollback needed:
- **No** (functional rollback not invoked).  
- Note: one extra stop/start cycle occurred due CLI syntax error, not due service failure.

Updated monthly savings estimate after this action:
- Incremental from resize (vs prior `t3.xlarge` baseline): ~USD **45-50/month**

### 17.3 Action 3: Hold `i-0196ded69f4366656`

Status: **Unchanged as instructed**

Current state:
- `running`, type `t2.micro`, public IP unchanged (`54.169.226.139`)

Reason held:
- Evidence gaps still open for host-level workload attribution (process/service/cron dependency proof).

### 17.4 Combined current savings estimate (run-rate)

If current state is maintained:
1. `i-0ccd...` remains stopped.
2. `i-078...` remains `t3.large`.

Estimated total monthly reduction vs prior investigated baseline:
- ~USD **51-57/month** (order-of-magnitude estimate; actual bill depends on exact running hours and effective rates).

## 18) Observation window and final decision criteria

This section defines final go/no-go criteria after executed changes.  
No additional infrastructure changes are performed by this section.

### 18.1 `i-0ccd81f0ce5bd9fa0` stop-test: observation checklist and retirement framework

Observation window recommendation:
- Minimum: **72 hours**
- Preferred: **7 full days** (captures weekday schedules + operator behavior)

Signals to monitor (check at +15m, +1h, +4h, +24h, then daily):
1. RDS health (`bess-platform-pg` status remains `available`).
2. ECS service stability in `bess-platform-cluster`:
   - no sustained `pending` growth,
   - `running`/`desired` does not regress unexpectedly.
3. Scheduled workflow outcomes:
   - EventBridge-driven jobs complete without new failure spikes.
4. CloudWatch alarm state:
   - no new critical alarms attributable to missing host.
5. Operator/admin usability:
   - no reported failures in DB access, ingestion ops, ad hoc maintenance, or OpenClaw-related workflows.
6. Security/auth/access anomalies:
   - no emergency workarounds created due host absence.

Failed stop-test definition (any one is sufficient):
1. Reproducible production/ingestion/admin workflow failure traced to host being stopped.
2. New persistent alarms/errors appear and disappear after host restart.
3. Named owner confirms active dependency requiring the host.

Successful stop-test definition (all required):
1. Entire observation window passes without attributable regressions.
2. No critical operator-reported dependency.
3. No recurring alarms/failures linked to host stop.

Restart / rollback trigger conditions:
1. Any critical workflow interruption with probable link to host stop.
2. Repeated job failures/timeouts where root cause points to missing host path.
3. Incident commander/on-call requests restoration.

Rollback action:
- `aws ec2 start-instances --region ap-southeast-1 --instance-ids i-0ccd81f0ce5bd9fa0`
- Validate recovery of impacted path(s).

Final retirement approval criteria:
1. Stop-test classified as **successful** using criteria above.
2. Explicit sign-off from platform/ops owner.
3. Snapshot/AMI rollback artifacts retained for agreed retention period.
4. Final documentation updated with retirement date and rollback artifact IDs.

### 18.2 `i-078297b9e83f03dc1` post-right-size validation checklist (`t3.large`)

Post-change checklist (day 0 to day 7):
1. Instance remains `running` as `t3.large`.
2. Admin/jump workflows operate normally:
   - SSH login works,
   - expected operator commands/tunnels succeed,
   - no unexplained session drops/timeouts.
3. Resource sufficiency:
   - CPU utilization pattern acceptable (watch sustained high usage and user-visible latency).
   - no workload backlog symptoms tied to host capacity.
4. Platform health remains stable:
   - RDS/ECS/scheduled jobs unaffected.
   - no new alarm pattern attributable to the resize.

Public IP watchpoints (important):
1. Public IP changed after restart; confirm all consumers use current endpoint.
2. Verify any allowlists, scripts, bookmarks, or runbooks are updated from old IP values.
3. Validate no hidden dependency on previous public IP (`54.254.121.148` / intermediate `13.229.131.211`).
4. If static public endpoint is required long-term, plan EIP-based stabilization in a separate approved change.

Decision criteria: keep `t3.large` vs rollback/re-upsize
- Keep `t3.large` if:
  1. No operator-impacting performance regressions during observation window.
  2. No repeated connectivity/workflow failures attributable to size reduction.
  3. No sustained resource pressure symptoms.
- Re-upsize/rollback if:
  1. Persistent degradation appears after resize and resolves only with larger size.
  2. Critical admin/gateway workload reliability is reduced.
  3. On-call determines risk is unacceptable.

Rollback path:
- Stop instance -> modify type back to `t3.xlarge` -> start instance -> revalidate admin workflows.

### 18.3 `i-0196ded69f4366656` hold note (unchanged)

State remains unchanged by design.

Evidence still needed before any future cost action:
1. Host-level process/service/cron inventory.
2. Exact mapping from recurring `AssumeRole` activity to business workflow(s).
3. Migration target and test plan for each discovered dependency.
4. Confirmed rollback path for each migrated function.
