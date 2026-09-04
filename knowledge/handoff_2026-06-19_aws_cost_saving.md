# AWS Cost Saving — Handoff 2026-06-19

## Context

**Branch:** `cost-optimisation` (pushed to GitHub)
**Forecast before this session:** $839/month (MTD Jun 19: $502, +49% vs May $564)
**Changes applied today:** ~$141/month reduction → new forecast ~$700/month
**Primary driver of cost growth:** ECS Fargate (each 0.5 vCPU / 1 GB service = ~$21.40/month including public IPv4)

---

## What Was Done This Session

### Applied via `terraform apply` (2026-06-19)

| Change | File | Saving/month |
|--------|------|-------------|
| Scaled 5 idle services to `desired_count=0` | `terraform.tfvars` | ~$107 |
| Disabled ECS Container Insights | `main.tf` | ~$15 |
| CloudWatch log retention 14→7 days | `main.tf` | ~$5 |
| S3 lifecycle rule: expire ALB logs after 7 days | `main.tf` | ~$3 |
| ECR lifecycle policies (keep last 5 images) | `ecr-lifecycle.tf` | ~$10 |
| **Total** | | **~$141** |

### Infrastructure fixes (also in this session)
- `services-new.tf` — rewrote to remove 30+ duplicate resource definitions vs `main.tf`
- `hermes.tf` — created with import blocks (hermes was in state but had no `.tf` definition)
- `variables.tf` — added declarations for ph_market, po_market, crystal_ball_client, hermes
- `main.tf` — fixed RDS engine_version drift (18.2→18.3, AWS had auto-upgraded)

---

## Current ECS Service State (post-apply)

| Service | desired | running | Notes |
|---------|---------|---------|-------|
| portal | 1 | 1 | Core |
| bess-map | 1 | 1 | Core |
| spot-markets | 1 | 1 | Core |
| inner-mongolia | 1 | 1 | Core |
| mengxi-dashboard | 1 | 1 | Core |
| gb-market | 1 | 1 | Active v71 |
| ph-market | 1 | 1 | Active v15 |
| po-market | 1 | 1 | Active v12 |
| crystal-ball | 1 | 1 | Active v33 |
| crystal-ball-client | 1 | 1 | Active v11 |
| hermes | 1 | 1 | Messaging hub (Feishu/WeCom/OneDrive/Claude) |
| **au-market** | **0** | **0** | Scaled down: AEMO data ends Apr 2026 |
| **ercot-market** | **0** | **0** | Scaled down: US placeholder |
| **pjm-market** | **0** | **0** | Scaled down: US placeholder |
| **caiso-market** | **0** | **0** | Scaled down: US placeholder |
| **options-cockpit** | **0** | **0** | Scaled down: debug image v5-debug |
| model-catalogue | 0 | 0 | Pre-existing (was already 0) |
| uploader | 0 | 0 | Pre-existing (was already 0) |

**To restore any service:** set `desired_count_<name> = 1` in `infra/terraform/terraform.tfvars` then `terraform apply`.

---

## Remaining Cost Saving Opportunities (not yet done)

### High priority — investigate next

**1. CloudWatch Logs ingestion volume (~$5–20/month potential)**
Run this to see which log streams are heaviest:
```bash
aws logs describe-log-groups --log-group-name-prefix /ecs/bess-platform --region ap-southeast-1 \
  --query 'logGroups[*].{name:logGroupName,bytes:storedBytes}' --output table
```
Services that log most are candidates for reducing log verbosity or adding log filtering.

**2. Data transfer / VPC costs (~$10–30/month potential)**
The billing screenshot showed VPC as a secondary cost driver. Investigate:
```bash
aws ce get-cost-and-usage \
  --time-period Start=2026-06-01,End=2026-06-19 \
  --granularity MONTHLY \
  --metrics "UnblendedCost" \
  --group-by Type=DIMENSION,Key=SERVICE \
  --region us-east-1
```
Likely culprits: NAT gateway data processing, inter-AZ traffic, ECR image pulls across AZs.
Note: tasks use `assign_public_ip = true` (no NAT gateway) — ECR pulls go directly to ECR public endpoint.

**3. RDS storage optimization (~$2–5/month)**
Current: 100GB gp3 allocated. Check actual usage:
```bash
aws cloudwatch get-metric-statistics \
  --namespace AWS/RDS --metric-name FreeStorageSpace \
  --dimensions Name=DBInstanceIdentifier,Value=bess-platform-pg \
  --start-time 2026-06-19T00:00:00Z --end-time 2026-06-19T23:59:59Z \
  --period 3600 --statistics Average --region ap-southeast-1
```
If <40GB used, reduce `allocated_storage` to 50 in `main.tf` (gp3 allows shrinking to minimum).

**4. Fargate CPU right-sizing (~$30–60/month potential, requires data first)**
All services run at 512 CPU / 1024 MB. Check actual utilization in CloudWatch before reducing.
Key metrics to pull: `CPUUtilization` and `MemoryUtilization` for each service over 7 days.
If avg CPU <20% and avg memory <40%, a service can be dropped to 256 CPU / 512 MB (~50% cost cut for that service).

**5. Spot-markets service — check if actively used**
spot-markets has been running since initial deployment. If it's not actively used by anyone, scale to 0.

---

## How to Check Current AWS Costs

```bash
# MTD cost by service
aws ce get-cost-and-usage \
  --time-period Start=2026-06-01,End=$(date +%Y-%m-%d) \
  --granularity MONTHLY \
  --metrics "UnblendedCost" \
  --group-by Type=DIMENSION,Key=SERVICE \
  --region us-east-1 \
  --query 'ResultsByTime[0].Groups[*].{Service:Keys[0],Cost:Metrics.UnblendedCost.Amount}' \
  --output table | sort -k3 -rn
```

```bash
# ECS Fargate daily cost trend
aws ce get-cost-and-usage \
  --time-period Start=2026-06-01,End=$(date +%Y-%m-%d) \
  --granularity DAILY \
  --metrics "UnblendedCost" \
  --filter '{"Dimensions":{"Key":"SERVICE","Values":["Amazon Elastic Container Service"]}}' \
  --region us-east-1
```

---

## Key Terraform Files

| File | Purpose |
|------|---------|
| `infra/terraform/terraform.tfvars` | **Scale levers** — set `desired_count_*` here to scale up/down |
| `infra/terraform/main.tf` | Core resources: ALB, ECS cluster, RDS, S3, IAM, Cognito, all original services |
| `infra/terraform/services-new.tf` | ph-market, po-market, crystal-ball-client (added post-bootstrap) |
| `infra/terraform/hermes.tf` | Hermes messaging hub (restored from state 2026-06-19) |
| `infra/terraform/ecr-lifecycle.tf` | ECR image retention policies |
| `infra/terraform/variables.tf` | All variable declarations |

**Working directory for terraform:** `infra/terraform/`
**AWS region:** `ap-southeast-1` (Singapore)
**Account:** `319383842493`
**ECS cluster:** `bess-platform-cluster`
**ALB:** `bess-platform-alb`

---

## Warnings / Known Issues

- **`terraform plan` will always show task definition replacements** — ECS task defs are immutable; services have `lifecycle { ignore_changes = [task_definition] }` so replacements don't affect running containers. Safe to apply.
- **Undeclared variable warnings** in `terraform plan` — `LINGFENG_PASSWORD`, `planka_*`, `hermes_wechat_owner_id`, etc. exist in `terraform.tfvars` but their variables were removed when planka was decommissioned. Harmless warnings, can be cleaned up by removing those lines from `terraform.tfvars`.
- **Portal service** — ECS event log shows historical `CannotPullContainerError` for `:v7` tag. This is a pre-existing, non-critical issue (the running task uses a newer revision). Investigate separately.
- **AU market** — Scaled to 0 because AEMO archive data ends Apr 2026. Restore when AEMO provides updated data.

---

## Quick Reference: Scale a Service Back Up

```bash
# 1. Edit terraform.tfvars
#    change: desired_count_au_market = 0
#    to:     desired_count_au_market = 1

# 2. Apply
cd infra/terraform
terraform plan -out=scale_up.tfplan
terraform apply scale_up.tfplan
```

Takes ~2 minutes for ECS to start the task.
