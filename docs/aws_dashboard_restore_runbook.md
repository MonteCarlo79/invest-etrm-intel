# AWS Dashboard Restore Runbook

**Purpose:** Restore the 5 suspended ECS dashboard services from `desired_count=0` back to `desired_count=1`.

**Cluster:** `bess-platform-cluster`  
**Region:** `ap-southeast-1`  
**Terraform workspace:** `infra/terraform/`  
**Branch:** `cost-optimisation`

---

## Services in scope

| App | ECS service name | ALB path | Terraform resource |
|---|---|---|---|
| bess-map | `bess-platform-bess-map-svc` | `/bess-map` | `aws_ecs_service.bess_map` |
| uploader | `bess-platform-uploader-svc` | `/uploader` | `aws_ecs_service.uploader` |
| model-catalogue | `bess-platform-model-catalogue-svc` | `/model-catalogue` | `aws_ecs_service.model_catalogue` |
| spot-markets | `bess-platform-spot-markets-svc` | `/spot-markets` | `aws_ecs_service.spot_markets` |
| pnl-attribution | `bess-platform-pnl-attribution-svc` | `/pnl-attribution` | `aws_ecs_service.pnl_attribution[0]` |

**Leave unchanged:**
- `bess-platform-portal-svc` (`desired_count_portal` defaults to 1, not in tfvars)
- `bess-platform-inner-mongolia-svc` (`desired_count_inner_mongolia = 1` in tfvars)

---

## Step 1 — Edit `terraform.tfvars`

File: `infra/terraform/terraform.tfvars`

Change the 5 values from `0` to `1`:

```hcl
# Non-essential dashboards suspended to reduce Fargate cost.
# Set back to 1 to restore. No resources are deleted.
desired_count_bess_map        = 1
desired_count_uploader        = 1
desired_count_model_catalogue = 1
desired_count_spot_markets    = 1
pnl_attribution_desired_count = 1
```

| Variable | Suspended value | Restore value |
|---|---|---|
| `desired_count_bess_map` | `0` | `1` |
| `desired_count_uploader` | `0` | `1` |
| `desired_count_model_catalogue` | `0` | `1` |
| `desired_count_spot_markets` | `0` | `1` |
| `pnl_attribution_desired_count` | `0` | `1` |

---

## Step 2 — Plan (targeted)

Run from `infra/terraform/`:

```bash
cd infra/terraform

terraform plan \
  -target=aws_ecs_service.bess_map \
  -target=aws_ecs_service.uploader \
  -target=aws_ecs_service.model_catalogue \
  -target=aws_ecs_service.spot_markets \
  -target='aws_ecs_service.pnl_attribution[0]' \
  -out=tfplan_restore
```

**Expected plan output — exactly 5 changes, all `~` (update in-place):**

```
~ aws_ecs_service.bess_map            desired_count: 0 -> 1
~ aws_ecs_service.uploader            desired_count: 0 -> 1
~ aws_ecs_service.model_catalogue     desired_count: 0 -> 1
~ aws_ecs_service.spot_markets        desired_count: 0 -> 1
~ aws_ecs_service.pnl_attribution[0]  desired_count: 0 -> 1

Plan: 0 to add, 5 to change, 0 to destroy.
```

If the plan includes any RDS, IAM, CloudWatch, ALB, or other resource changes — stop and investigate before applying. See [Known drift issues](#known-drift-issues) below.

---

## Step 3 — Apply

```bash
terraform apply tfplan_restore
```

---

## Step 4 — Validate ECS desired/running counts

Wait ~2 minutes for tasks to start, then:

```bash
aws ecs describe-services \
  --cluster bess-platform-cluster \
  --region ap-southeast-1 \
  --services \
    bess-platform-bess-map-svc \
    bess-platform-uploader-svc \
    bess-platform-model-catalogue-svc \
    bess-platform-spot-markets-svc \
    bess-platform-pnl-attribution-svc \
  --query 'services[*].{name:serviceName, desired:desiredCount, running:runningCount, status:status}' \
  --output table
```

**Expected output:**

| name | desired | running | status |
|---|---|---|---|
| bess-platform-bess-map-svc | 1 | 1 | ACTIVE |
| bess-platform-uploader-svc | 1 | 1 | ACTIVE |
| bess-platform-model-catalogue-svc | 1 | 1 | ACTIVE |
| bess-platform-spot-markets-svc | 1 | 1 | ACTIVE |
| bess-platform-pnl-attribution-svc | 1 | 1 | ACTIVE |

If `running` stays at `0` after 3 minutes, check task failure reason:

```bash
aws ecs list-tasks \
  --cluster bess-platform-cluster \
  --service-name bess-platform-bess-map-svc \
  --desired-status STOPPED \
  --region ap-southeast-1 \
  --query 'taskArns[0]' \
  --output text | xargs -I{} aws ecs describe-tasks \
    --cluster bess-platform-cluster \
    --tasks {} \
    --region ap-southeast-1 \
    --query 'tasks[0].{reason:stoppedReason, containers:containers[*].{name:name,reason:reason,exit:exitCode}}'
```

---

## Step 5 — Validate ALB health

The ALB serves all apps under `https://www.pjh-etrm.ai`. Check health endpoints:

```bash
# Replace with your actual ALB DNS if not using custom domain
for path in bess-map uploader model-catalogue spot-markets pnl-attribution; do
  echo -n "$path: "
  curl -s -o /dev/null -w "%{http_code}" \
    "https://www.pjh-etrm.ai/${path}/_stcore/health"
  echo
done
```

**Expected:** `200` for all 5 paths. A `503` means the ALB target group has no healthy targets yet — wait another minute and retry.

---

## Step 6 — Confirm portal and inner-mongolia unchanged

```bash
aws ecs describe-services \
  --cluster bess-platform-cluster \
  --region ap-southeast-1 \
  --services \
    bess-platform-portal-svc \
    bess-platform-inner-mongolia-svc \
  --query 'services[*].{name:serviceName, desired:desiredCount, running:runningCount}' \
  --output table
```

Both should remain at `desired=1, running=1`.

---

## Rollback — if one app fails to come back healthy

If a specific service's task keeps stopping (image pull error, crash loop, config issue), suspend it again individually without touching the others:

```bash
# Example: bess-map fails to start
cd infra/terraform
```

Set only that app back to `0` in `terraform.tfvars`:

```hcl
desired_count_bess_map = 0   # re-suspend while investigating
```

Then apply targeted:

```bash
terraform apply -target=aws_ecs_service.bess_map -auto-approve
```

Investigate the stopped task reason (command in Step 4 above), fix the image or config, then retry.

### Common failure causes and fixes

| Symptom | Likely cause | Fix |
|---|---|---|
| `CannotPullContainerError` | ECR image tag missing or wrong | Check `image_*` variable in `terraform.tfvars`; push correct image to ECR |
| `ResourceInitializationError` | Secrets Manager / SSM param missing | Verify any `secretsFrom` references in task definition |
| App starts but ALB returns 503 | Health check path wrong or app slow to start | Increase `health_check_grace_period_seconds` or check app logs |
| App crashes immediately | Missing required env var | Check CloudWatch logs for the service |

Check CloudWatch logs for any service:

```bash
aws logs tail /ecs/bess-platform \
  --log-stream-name-prefix bess-map \
  --region ap-southeast-1 \
  --follow
```

---

## Known drift issues

The following Terraform drifts were already reconciled in `main.tf` (commit `2b838b6` + `e640d42`). A clean plan should not show these, but if they reappear they are safe to understand:

| Resource | Attribute | Reconciled as |
|---|---|---|
| `aws_db_instance.pg` | `publicly_accessible` | Set to `true` in config — no AWS change |
| `aws_db_instance.pg` | `max_allocated_storage` | Set to `1000` in config — no AWS change |
| `aws_security_group.rds` | `ingress` | `lifecycle { ignore_changes = [ingress] }` — laptop CIDR rule stays |
| `aws_ecs_cluster.this` | `configuration` | `lifecycle { ignore_changes = [configuration] }` — ECS Exec stays |

If any of these appear in the plan as a change, **do not proceed**. They indicate the state file is out of sync. Run `terraform refresh` and re-plan.

---

## Copy-paste prompt for Claude / Codex

Feed this entire file plus the prompt below:

```
I need to restore 5 suspended ECS dashboard services on AWS.
The repo is at cost-optimisation branch. The runbook is in docs/aws_dashboard_restore_runbook.md.

Please:
1. Edit infra/terraform/terraform.tfvars — set the 5 variables listed in Step 1 to 1
2. Show me the exact targeted terraform plan command from Step 2
3. After I confirm the plan is clean (exactly 5 ~ changes, nothing else), run terraform apply
4. Run the AWS CLI validation commands from Step 4 and Step 5
5. Confirm portal and inner-mongolia are still at desired=1

Do not change any other Terraform variables. Do not touch RDS, IAM, EventBridge, or CloudWatch resources.
If the plan includes anything other than the 5 ECS desired_count changes, stop and report before applying.
```
