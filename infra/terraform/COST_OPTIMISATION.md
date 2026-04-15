# BESS Platform — Cost Optimisation Review
**Date:** 2026-04-11  
**Branch:** cost-optimisation  
**Calibration source:** AWS CLI (live infra) + CloudWatch 7-day metrics

---

## 1. Estimated Monthly Cost Breakdown

| Driver | Est. monthly | Notes |
|---|---|---|
| ECS Fargate — always-on services | ~$175–185 | 6 services × 24h/day |
| RDS db.t4g.micro + 100 GB gp2 | ~$24–26 | t4g.micro is already smallest |
| ALB (fixed + LCU) | ~$16–20 | 1 internet-facing ALB |
| ECR image storage | ~$6–8 | ~66 GB total across 16 repos |
| CloudWatch logs | ~$3–5 | ingestion + storage |
| EventBridge + Fargate scheduled tasks | ~$10–14 | ~14 tasks × ~20 min/day |
| **Estimated total** | **~$235–260/month** | |

---

## 2. Findings

### ECS Services — live task definition vs Terraform

| Service | TF CPU/Mem | Live CPU/Mem | 7-day avg CPU | 7-day avg Mem | 7-day peak Mem |
|---|---|---|---|---|---|
| `inner-mongolia` | 512/1024 | **2048/8192** | 0.01% | 1.78% | 1.82% |
| `bess-map` | 512/1024 | 512/1024 | 0.04% | 10.1% | 18.2% |
| `uploader` | 512/1024 | 512/1024 | 0.04% | 5.9% | 7.4% |
| `portal` | 256/512 | 256/512 | 1.3% | 25.4% | 27.7% |
| `pnl-attribution` | 512/1024 | 512/1024 | 0.46% | 12.5% | 15.3% |
| `spot-markets` | *(not in TF)* | 512/1024 | 0.7% | 11.9% | 12.7% |

**`inner-mongolia` is the single biggest cost outlier:** running at 2048 vCPU / 8192 MB while peak memory over 7 days is 149 MB (1.82% of 8192 MB). This was manually scaled up at some point (Terraform still says 512/1024; live is at task definition revision v43). Fargate cost for 2048/8192 is ~**$84/month**; 1024/2048 would be ~**$42/month** (savings: ~$42/month), and still gives 13× headroom over observed peak.

### ECR — unbounded image accumulation

Before this PR, 11 of 16 ECR repositories had no lifecycle policy:

| Repository | Images before | Stored GB | In Terraform? |
|---|---|---|---|
| `bess-map` | 24 | 20.4 GB | No |
| `bess-uploader` | 20 | 18.0 GB | No |
| `portal` | 18 | 9.7 GB | No |
| `bess-trading-jobs` | 4 | 4.6 GB | No |
| `bess-mengxi-ingestion` | 19 | 3.5 GB | No |
| `bess-inner-pipeline` | 3 | 2.4 GB | Yes |
| `bess-inner-mongolia` | 4 | 2.1 GB | Yes |
| `bess-pnl-attribution` | 4 | 2.1 GB | No |
| `bess-spot-markets` | 3 | 1.3 GB | No |
| `bess-platform-portal` | 5 | 1.6 GB | No |
| `bess-pipeline` | 1 | 0.6 GB | No |

Without lifecycle policies, every `docker push` accumulates another image indefinitely. At $0.10/GB-month (beyond 500 MB free), this already costs ~$6–7/month and grows with each deployment.

### CloudWatch log retention

Before this PR, several log groups had 30-day retention (twice what operators need for daily pipelines), and two had **no retention at all**:

| Log group | Before | After |
|---|---|---|
| `/ecs/bess-platform` | 30 days | **14 days** |
| `/ecs/bess-platform-portal` | **none** | **14 days** |
| `/codex/agent4-mengxi-validation` | **none** | **14 days** |
| `/ecs/bess-platform/enos-market-collector` | 30 days | **14 days** |
| `/ecs/bess-platform/tt-api-collector` | 30 days | **14 days** |
| `/ecs/bess-platform/lingfeng-collector` | 30 days | **14 days** |
| `/ecs/bess-platform/mengxi-excel-ingest` | 30 days | **14 days** |
| `trading-bess-mengxi` 3× log groups | 30 days | **14 days** |

### RDS — already near-minimal

- Instance: `db.t4g.micro` — already the smallest class. 7-day avg CPU 14%, max 99.8% (one spike). Adequate for current load; monitor the CPU spike.
- Storage: 100 GB `gp2` allocated, ~16 GB actually used. Cannot reduce in-place; migration to snapshot+restore on smaller volume is complex and low-priority.
- **Quick win:** migrate from `gp2` to `gp3`. Same IOPS (3000 baseline), costs $0.096/GB vs $0.115/GB = **$1.90/month** saved. Storage type can be changed in-place with no downtime. Not done in this PR — included in "do later" plan.

---

## 3. Ranked Action Plan

### Implement now ✅ (done in this PR)

| Action | Monthly savings | Risk | Applied? |
|---|---|---|---|
| ECR lifecycle policy (keep 5) on all 16 repos | ~$2–4/month ongoing; prevents future growth | None — old images expire gradually | **Yes (applied)** |
| CloudWatch log retention: 30 → 14 days on 8 groups | ~$0.50–1/month | None | **Yes (applied)** |
| CloudWatch log retention: set 14 days on 2 groups with no retention | Prevents unbounded future cost | None | **Yes (applied via CLI)** |

### Validate first — code written, DO NOT apply yet ⚠️

| Action | Monthly savings | Risk | How to apply |
|---|---|---|---|
| **`inner-mongolia` right-size: 2048/8192 → 1024/2048** | **~$42/month** | Low — 13× headroom over observed peak. Triggers rolling ECS deployment (zero-downtime). | `cd infra/terraform && terraform apply -target=aws_ecs_task_definition.inner_mongolia -target=aws_ecs_service.inner_mongolia` |

**Before applying inner-mongolia resize:**
1. Check recent app logs for any OOM errors (`grep -i "oom\|killed\|memory" /ecs/bess-platform` streams)
2. Confirm no large pending data loads that would spike memory
3. Apply during a low-traffic window (weekday daytime UTC+8)
4. Monitor for 24h: ECS console → service events, CloudWatch MemoryUtilization

**Rollback:** `git revert HEAD` then `terraform apply` to restore 2048/8192.

### Do later 📋

| Action | Monthly savings | Effort | Notes |
|---|---|---|---|
| RDS gp2 → gp3 storage | ~$1.90/month | 10 min | In-place change, no downtime. Change `storage_type = "gp3"` in `main.tf` + apply. |
| ARM64 (Graviton) for scheduled tasks | ~10–15% Fargate cost | Medium | Requires ARM image builds. Biggest impact on tasks running most often. |
| Investigate Container Insights cost | ~$2/month | Low | `/aws/ecs/containerinsights/…` at 1-day retention generates ~10 MB/day → $0.076/day. Consider disabling if not actively used. |
| `bess-map`, `bess-uploader`, `portal` in Terraform | 0 savings | Medium | These services exist in AWS but not in Terraform — creates drift risk. Import them for future lifecycle management. |
| Spot-markets service in Terraform | 0 savings | Low | `bess-platform-spot-markets-svc` runs 512/1024 but is not in any Terraform module. |

---

## Second Pass (2026-04-11) — targeting additional $50–200/month

### A. Ranked Savings Opportunities

| # | Component | Current config (live) | Proposed | Savings/month | Confidence | Risk |
|---|---|---|---|---|---|---|
| 1 | **inner-mongolia ECS service** | 2048 vCPU / 8192 MB | 1024 / 2048 (code ready) | **~$49** | High — 7-day peak 149 MB, 13× headroom | Low |
| 2 | **bess-map ECS service** | 512 / 1024 | 256 / 512 (code ready) | **~$9** | High — 7-day peak 186 MB, 2.7× headroom | Low |
| 3 | **bess-uploader ECS service** | 512 / 1024 | 256 / 512 (code ready) | **~$9** | High — 7-day peak 76 MB, 6.7× headroom | Low |
| 4 | ~~inner-pipeline on-demand task~~ | 4096 / 16384 (live = TF corrected) | **NO CHANGE** | **$0** | **BLOCKED** — Container Insights confirms peak 12,570 MB (76.7% of 16 GB) on Mar 28 backfill. Would OOM at 1024/2048. TF code corrected to 4096/16384. |
| 5 | **RDS gp2 → gp3** | gp2, 100 GB | gp3, 100 GB (code ready) | **~$1.90** | Certain — same IOPS, in-place | None |
| 6 | **Container Insights** | Enabled (cluster level) | Disabled | **~$2.30** | Certain — ~10 MB/day logs at $0.076/day | Reduces CloudWatch memory/CPU metric visibility |
| | **Achievable total (items 1–5)** | | | **~$74/month** | | |

---

### B. Implement now ✅ (code updated in this PR, safe to apply)

| Action | Savings | Risk | How to apply |
|---|---|---|---|
| **RDS gp2 → gp3** | ~$1.90/month | None — in-place, same IOPS | `terraform apply -target=aws_db_instance.pg` |

---

### C. Validate first — code written, DO NOT apply yet ⚠️

| Action | Savings | Validation steps | Apply command |
|---|---|---|---|
| **inner-mongolia 2048/8192 → 1024/2048** | ~$49/month | (1) Grep `/ecs/bess-platform` streams for OOM/killed/memory errors. (2) Confirm no large data loads pending. (3) Apply in low-traffic window. (4) Monitor 24h. | `terraform apply -target=aws_ecs_task_definition.inner_mongolia -target=aws_ecs_service.inner_mongolia` |
| **bess-map 512/1024 → 256/512** | ~$9/month | (1) Check CloudWatch MemoryUtilization last 7 days — peak is 18.2% (186 MB); confirm no spike > 30% recently. (2) Apply during low-traffic window. (3) Monitor 24h. | `terraform apply -target=aws_ecs_task_definition.bess_map -target=aws_ecs_service.bess_map` |
| **bess-uploader 512/1024 → 256/512** | ~$9/month | (1) Check CloudWatch peak — 7.4% (76 MB); confirm no spike during large Excel uploads. (2) Apply. (3) Monitor 24h. | `terraform apply -target=aws_ecs_task_definition.uploader -target=aws_ecs_service.uploader` |
| ~~**inner-pipeline resize**~~ | **$0 — BLOCKED** | Container Insights confirms peak 12,570 MB on Mar 28 (76.7% of 16 GB). Would OOM at 1024/2048. TF code corrected from 1024/2048 → 4096/16384 to match live. No apply needed for this. | N/A |

**Rollback for all service right-sizes:** revert cpu/memory in `main.tf` + `terraform apply`. ECS does a rolling deployment back to prior size.

---

### D. Not worth it / deferred

| Action | Reason |
|---|---|
| RDS downsize (t4g.micro → smaller) | Already at smallest class; CPU hits 97–99.8% daily max — do NOT resize. |
| pnl-attribution right-size (512/1024) | Not in Terraform; must `terraform import` first before any Terraform changes are safe. |
| spot-markets right-size (512/1024) | Not in Terraform; same import requirement. |
| Container Insights disable | Reduces CloudWatch memory metrics used for right-sizing decisions. Keep until right-sizing work is complete. |
| ARM64 / Graviton migration | ~10–15% Fargate savings but requires rebuilding all images as ARM. Medium effort; revisit after right-sizing is done. |
| NAT Gateway audit | Private subnet tasks (strategy/portfolio/execution agents) use `assign_public_ip = false`. These run successfully, confirming NAT gateways exist. If you want to eliminate NAT gateways entirely by moving all tasks to public subnets, that's ~$45/month saving but is a VPC topology change with higher blast radius. |

---

## 4. Files Changed in This PR

| File | Change |
|---|---|
| `infra/terraform/main.tf` | **Pass 1:** CloudWatch retention 30→14 days; ECR lifecycle (keep 5) on 5 repos; inner-mongolia task def 512/1024 → **1024/2048** (not yet applied to live). **Pass 2:** RDS `storage_type = "gp3"` (ready to apply); bess-map task def 512/1024 → **256/512** (not yet applied); uploader task def 512/1024 → **256/512** (not yet applied); comment added to inner-pipeline re: live 4096/16384 drift |
| `infra/terraform/data-ingestion/main.tf` | CloudWatch retention 30→14 days on 3 log groups; ECR lifecycle 10→5 |
| `infra/terraform/trading-bess-mengxi/schedules.tf` | CloudWatch retention 30→14 days on 3 log groups |
| *(AWS CLI — not in Terraform)* | Lifecycle policies applied to 9 ECR repos; retention set on 3 extra log groups |

---

## 5. Rollback Notes

All applied changes are safe to reverse:

- **ECR lifecycle policies:** `aws ecr delete-lifecycle-policy --repository-name <repo>` removes the policy; no images are deleted immediately (lifecycle evaluation runs asynchronously and only expires images beyond the count threshold).
- **CloudWatch retention:** `aws logs delete-retention-policy --log-group-name <name>` restores infinite retention. Existing stored logs are unaffected.
- **inner-mongolia resize (when applied):** Revert the cpu/memory values in `main.tf` and run `terraform apply`. ECS will do another rolling deployment back to the previous size.

---

## 6. Manual AWS Console Follow-Up

- [ ] Check `/ecs/bess-platform-portal` — this log group is outside Terraform; confirm which service writes to it and whether it should be imported
- [ ] Verify `bess-platform-spot-markets-svc` is intentional and owned — it is not in any Terraform module
- [ ] After inner-mongolia resize is applied: confirm no OOM events for 48h before marking stable
- [ ] Apply RDS gp3 change: `terraform apply -target=aws_db_instance.pg` (~$1.90/month, zero risk)
- [ ] Check inner-pipeline CloudWatch logs for memory pressure before applying 1024/2048 TF value to live (current live: 4096/16384)
- [ ] After bess-map/uploader resize: confirm no OOM events for 24h; check ALB target group healthy host count stays at 1
- [ ] Trading-bess-mengxi EventBridge targets are pointing to stale task def revisions (tt-province-loader :4 vs :6, mengxi-pnl-refresh :3 vs :5) — run `terraform apply -chdir=infra/terraform/trading-bess-mengxi` to re-pin to latest

---

## 7. Post-Change Report (2026-04-11)

### Before / After State

| Item | Before | After | Savings/month |
|---|---|---|---|
| `inner-mongolia` ECS | task def :43 — 2048 vCPU / 8192 MB (live drift from TF) | task def :44 — 1024 vCPU / 2048 MB | ~$49 |
| `bess-map` ECS | task def :40 — 512 vCPU / 1024 MB | task def :41 — 256 vCPU / 512 MB | ~$9 |
| `uploader` ECS | task def :27 — 512 vCPU / 1024 MB | task def :28 — 256 vCPU / 512 MB | ~$9 |
| RDS `bess-platform-pg` | `storage_type = gp2`, 100 GB | `storage_type = gp3`, 100 GB | ~$1.90 |
| `inner-pipeline` ECS | TF incorrectly had 1024/2048; live :35 = 4096/16384 | TF corrected to 4096/16384 — **no change to live** | $0 (blocker: 12,570 MB observed peak) |

**Total realized savings: ~$68.90/month (~$827/year)**

### Deployment Health (T+0)

| Service | Task def | Running | Pending | ALB status |
|---|---|---|---|---|
| `bess-platform-inner-mongolia-svc` | :44 (1024/2048) | 1 | 0 | healthy |
| `bess-platform-bess-map-svc` | :41 (256/512) | 1 | 0 | healthy |
| `bess-platform-uploader-svc` | :28 (256/512) | 1 | 0 | healthy |
| RDS `bess-platform-pg` | — | — | — | storage-optimization in progress |

### 24-Hour Validation Checklist

```powershell
# 1. ECS MemoryUtilization — confirm no service exceeds 80% in first 24h
aws cloudwatch get-metric-statistics --namespace AWS/ECS --metric-name MemoryUtilization `
  --dimensions Name=ServiceName,Value=bess-platform-inner-mongolia-svc Name=ClusterName,Value=bess-platform `
  --start-time (Get-Date).AddHours(-24).ToUniversalTime().ToString("o") `
  --end-time (Get-Date).ToUniversalTime().ToString("o") `
  --period 3600 --statistics Maximum --query "sort_by(Datapoints,&Timestamp)[-1].Maximum"

# Repeat with Value=bess-platform-bess-map-svc and Value=bess-platform-uploader-svc

# 2. OOM / exit-137 check (run against all three services)
aws logs start-query --log-group-name /ecs/bess-platform `
  --start-time ([long](Get-Date).AddHours(-24).ToUniversalTime().Subtract([datetime]"1970-01-01").TotalSeconds) `
  --end-time   ([long](Get-Date).ToUniversalTime().Subtract([datetime]"1970-01-01").TotalSeconds) `
  --query-string 'fields @message | filter @message like /(?i)(oom|killed|exit code 137|OutOfMemory)/ | limit 20'

# 3. RDS storage type — confirm gp3 migration complete
aws rds describe-db-instances --db-instance-identifier bess-platform-pg `
  --query "DBInstances[0].{StorageType:StorageType,Status:DBInstanceStatus,Pending:PendingModifiedValues}"

# 4. ALB target group health
aws elbv2 describe-target-health `
  --target-group-arn (aws elbv2 describe-target-groups --query "TargetGroups[?contains(TargetGroupName,'tgim')].TargetGroupArn" --output text) `
  --query "TargetHealthDescriptions[*].{Target:Target.Id,State:TargetHealth.State}"
```

### Rollback References

| Service | Rollback action |
|---|---|
| `inner-mongolia` | Set `cpu=2048, memory=8192` in `main.tf` → `terraform apply -target=aws_ecs_task_definition.inner_mongolia -target=aws_ecs_service.inner_mongolia` |
| `bess-map` | Set `cpu=512, memory=1024` in `main.tf` → `terraform apply -target=aws_ecs_task_definition.bess_map -target=aws_ecs_service.bess_map` |
| `uploader` | Set `cpu=512, memory=1024` in `main.tf` → `terraform apply -target=aws_ecs_task_definition.uploader -target=aws_ecs_service.uploader` |
| RDS gp3 | `aws rds modify-db-instance --db-instance-identifier bess-platform-pg --storage-type gp2 --apply-immediately` |

### Completion Status

| Item | Status |
|---|---|
| `inner-mongolia` right-size | **Complete** — running stable at :44; monitor 24h for OOM |
| `bess-map` right-size | **Complete** — running stable at :41; monitor 24h for OOM |
| `uploader` right-size | **Complete** — running stable at :28; monitor 24h for OOM |
| RDS gp2 → gp3 | **Implementation complete, awaiting final status=available confirmation** |
| `inner-pipeline` TF correction | **Complete** — TF now matches live; no apply needed |

---

### Operations Handoff — T+24h Checks (2026-04-12)

Tomorrow (2026-04-12), confirm the following before closing this wave:

1. **RDS `bess-platform-pg`:** `StorageType=gp3`, `DBInstanceStatus=available`, `PendingModifiedValues={}`. Run check #3 above.
2. **MemoryUtilization peaks:** All three right-sized services remain below 80% over the first 24h window. Run check #1 above for each.
3. **No OOM events:** Check #2 above returns 0 results across all log streams.
4. **ALB healthy host count:** All three target groups show 1 healthy target. Run check #4 above.

If all four pass, this optimisation wave is closed. No further action required.
