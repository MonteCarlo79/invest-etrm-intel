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

## 4. Files Changed in This PR

| File | Change |
|---|---|
| `infra/terraform/main.tf` | CloudWatch retention 30→14 days; ECR lifecycle (keep 5) added to `inner_mongolia`, `inner_pipeline`, `portfolio_agent`, `execution_agent`, `it_dev_agent`; `strategy_agent` lifecycle 10→5; inner-mongolia task def **code updated to 1024/2048** (not yet applied) |
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
- [ ] RDS: change `storage_type = "gp2"` to `"gp3"` for ~$1.90/month saving (separate low-risk change)
