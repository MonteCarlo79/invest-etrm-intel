# DeepTutor AWS Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy DeepTutor at `https://tutor.pjh-etrm.ai` on the existing bess-platform ECS Fargate cluster using an nginx sidecar for single-port ALB routing.

**Architecture:** A 3-container ECS Fargate task shares a localhost network. An nginx proxy container (port 8530) routes `/api/` to FastAPI (8001) and `/` to Next.js (3782). PocketBase (8090) provides multi-user auth. EFS volumes persist all data across task restarts.

**Tech Stack:** nginx:alpine, ghcr.io/hkuds/deeptutor:latest, ghcr.io/muchobien/pocketbase:latest, AWS ECS Fargate, ALB (host-header routing), EFS, Terraform, Docker, AWS CLI

---

## File Map

| Action | File | Responsibility |
|---|---|---|
| Create | `apps/deeptutor/nginx.conf` | Reverse proxy: /api/ → :8001, / → :3782 |
| Create | `apps/deeptutor/Dockerfile` | nginx:alpine image for ECR |
| Modify | `infra/terraform/variables.tf` | +5 new variables |
| Create | `infra/terraform/deeptutor.tf` | ECR, EFS, SG, ALB TG+rule, Route53, ECS task+service |
| Modify | `infra/terraform/main.tf:113` | Extend ecs_tasks SG to port 8530 |
| Modify | `infra/terraform/terraform.tfvars` | Set image URIs and PocketBase credentials |

---

## Task 1: Create nginx proxy app files

**Files:**
- Create: `apps/deeptutor/nginx.conf`
- Create: `apps/deeptutor/Dockerfile`

- [ ] **Step 1.1: Create nginx.conf**

```nginx
# apps/deeptutor/nginx.conf
events {}

http {
    upstream deeptutor_api {
        server localhost:8001;
    }

    upstream deeptutor_frontend {
        server localhost:3782;
    }

    server {
        listen 8530;

        # ALB health check → FastAPI (starts faster than Next.js)
        location /health {
            proxy_pass         http://deeptutor_api/;
            proxy_connect_timeout 5s;
            proxy_read_timeout    10s;
        }

        # All /api/ calls → FastAPI backend (routes: /api/v1/*)
        location /api/ {
            proxy_pass         http://deeptutor_api;
            proxy_http_version 1.1;
            proxy_set_header   Upgrade $http_upgrade;
            proxy_set_header   Connection "upgrade";
            proxy_set_header   Host $host;
            proxy_set_header   X-Real-IP $remote_addr;
            proxy_set_header   X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header   X-Forwarded-Proto $scheme;
            proxy_read_timeout 300s;
            proxy_send_timeout 300s;
            client_max_body_size 100m;
        }

        # Everything else → Next.js frontend
        location / {
            proxy_pass         http://deeptutor_frontend;
            proxy_http_version 1.1;
            proxy_set_header   Upgrade $http_upgrade;
            proxy_set_header   Connection "upgrade";
            proxy_set_header   Host $host;
            proxy_set_header   X-Real-IP $remote_addr;
            proxy_set_header   X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header   X-Forwarded-Proto $scheme;
            proxy_read_timeout 300s;
            client_max_body_size 100m;
        }
    }
}
```

- [ ] **Step 1.2: Create Dockerfile**

```dockerfile
# apps/deeptutor/Dockerfile
FROM nginx:alpine
COPY nginx.conf /etc/nginx/nginx.conf
EXPOSE 8530
```

- [ ] **Step 1.3: Smoke-test nginx config locally**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform/apps/deeptutor"
docker build -t deeptutor-nginx:local .
docker run --rm -d -p 8530:8530 --name dt-nginx-test deeptutor-nginx:local
```

Expected: container starts without error. Then:

```bash
curl -s -o /dev/null -w "%{http_code}" http://localhost:8530/health
```

Expected: `502` (nginx is up and routing — upstream not available in this test, so 502 is correct)

```bash
docker stop dt-nginx-test
```

- [ ] **Step 1.4: Commit**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform"
git add apps/deeptutor/nginx.conf apps/deeptutor/Dockerfile
git commit -m "feat(deeptutor): add nginx proxy app files"
```

---

## Task 2: Add terraform variables

**Files:**
- Modify: `infra/terraform/variables.tf` (append at end of file)

- [ ] **Step 2.1: Append 5 new variables to variables.tf**

Open `infra/terraform/variables.tf` and add at the end:

```hcl
# ─────────────────────────────────────────────────────────────────────────────
# DeepTutor
# ─────────────────────────────────────────────────────────────────────────────
variable "image_deeptutor_nginx" {
  description = "Docker image for DeepTutor nginx proxy (ECR)"
  type        = string
  default     = ""
}

variable "image_deeptutor" {
  description = "Docker image for DeepTutor main app"
  type        = string
  default     = "ghcr.io/hkuds/deeptutor:latest"
}

variable "desired_count_deeptutor" {
  description = "Desired task count for the DeepTutor ECS service"
  type        = number
  default     = 1
}

variable "deeptutor_pb_admin_email" {
  description = "PocketBase superadmin email for DeepTutor multi-user mode"
  type        = string
  sensitive   = true
  default     = ""
}

variable "deeptutor_pb_admin_password" {
  description = "PocketBase superadmin password for DeepTutor multi-user mode"
  type        = string
  sensitive   = true
  default     = ""
}
```

- [ ] **Step 2.2: Commit**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform"
git add infra/terraform/variables.tf
git commit -m "feat(deeptutor): add terraform variables"
```

---

## Task 3: Create deeptutor.tf

**Files:**
- Create: `infra/terraform/deeptutor.tf`

- [ ] **Step 3.1: Create the full deeptutor.tf**

```hcl
# ─────────────────────────────────────────────────────────────────────────────
# deeptutor.tf
# DeepTutor: tutor.pjh-etrm.ai
# 3-container ECS task: nginx proxy (8530) + deeptutor app + pocketbase
# ─────────────────────────────────────────────────────────────────────────────

# ── ECR ──────────────────────────────────────────────────────────────────────
resource "aws_ecr_repository" "deeptutor_nginx" {
  name                 = "deeptutor-nginx"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = false }
  tags = local.tags
}

# ── EFS file system ───────────────────────────────────────────────────────────
resource "aws_efs_file_system" "deeptutor" {
  encrypted = true
  tags      = merge(local.tags, { Name = "${var.name}-deeptutor-efs" })
}

# EFS security group: allow NFS (2049) from ECS tasks SG only
resource "aws_security_group" "deeptutor_efs" {
  name        = "${var.name}-deeptutor-efs-sg"
  description = "EFS mount targets for DeepTutor"
  vpc_id      = var.vpc_id
  tags        = local.tags

  ingress {
    description     = "NFS from ECS tasks"
    from_port       = 2049
    to_port         = 2049
    protocol        = "tcp"
    security_groups = [aws_security_group.ecs_tasks.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Mount target in each subnet used by the ECS cluster
resource "aws_efs_mount_target" "deeptutor" {
  for_each = toset(var.private_subnet_ids)

  file_system_id  = aws_efs_file_system.deeptutor.id
  subnet_id       = each.value
  security_groups = [aws_security_group.deeptutor_efs.id]
}

# Access point for DeepTutor app data (/app/data in container)
resource "aws_efs_access_point" "deeptutor_data" {
  file_system_id = aws_efs_file_system.deeptutor.id

  posix_user {
    uid = 0
    gid = 0
  }

  root_directory {
    path = "/deeptutor"
    creation_info {
      owner_uid   = 0
      owner_gid   = 0
      permissions = "755"
    }
  }

  tags = merge(local.tags, { Name = "${var.name}-deeptutor-data" })
}

# Access point for PocketBase data (/pb/pb_data in container)
resource "aws_efs_access_point" "pocketbase_data" {
  file_system_id = aws_efs_file_system.deeptutor.id

  posix_user {
    uid = 0
    gid = 0
  }

  root_directory {
    path = "/pocketbase"
    creation_info {
      owner_uid   = 0
      owner_gid   = 0
      permissions = "755"
    }
  }

  tags = merge(local.tags, { Name = "${var.name}-pocketbase-data" })
}

# ── ALB target group + listener rule ─────────────────────────────────────────
resource "aws_lb_target_group" "deeptutor" {
  name        = "bess-platform-deeptutor"
  port        = 8530
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  lifecycle { create_before_destroy = true }

  health_check {
    path                = "/health"
    protocol            = "HTTP"
    matcher             = "200-399"
    interval            = 30
    timeout             = 10
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  tags = local.tags
}

# Host-header rule: tutor.pjh-etrm.ai → deeptutor TG
resource "aws_lb_listener_rule" "deeptutor_host" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 60

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.deeptutor.arn
  }

  condition {
    host_header {
      values = ["tutor.pjh-etrm.ai"]
    }
  }
}

# ── Route53 DNS ───────────────────────────────────────────────────────────────
data "aws_route53_zone" "pjh_etrm" {
  name = "pjh-etrm.ai."
}

resource "aws_route53_record" "deeptutor" {
  zone_id = data.aws_route53_zone.pjh_etrm.zone_id
  name    = "tutor"
  type    = "A"

  alias {
    name                   = aws_lb.app.dns_name
    zone_id                = aws_lb.app.zone_id
    evaluate_target_health = true
  }
}

# ── ECS task definition ───────────────────────────────────────────────────────
resource "aws_ecs_task_definition" "deeptutor" {
  family                   = "${var.name}-deeptutor"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "2048"
  memory                   = "4096"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task_role.arn

  # EFS volumes
  volume {
    name = "deeptutor-data"
    efs_volume_configuration {
      file_system_id     = aws_efs_file_system.deeptutor.id
      transit_encryption = "ENABLED"
      authorization_config {
        access_point_id = aws_efs_access_point.deeptutor_data.id
        iam             = "DISABLED"
      }
    }
  }

  volume {
    name = "pocketbase-data"
    efs_volume_configuration {
      file_system_id     = aws_efs_file_system.deeptutor.id
      transit_encryption = "ENABLED"
      authorization_config {
        access_point_id = aws_efs_access_point.pocketbase_data.id
        iam             = "DISABLED"
      }
    }
  }

  container_definitions = jsonencode([
    # ── Container 1: PocketBase (multi-user auth+storage) ─────────────────
    {
      name      = "pocketbase"
      image     = "ghcr.io/muchobien/pocketbase:latest"
      essential = false

      mountPoints = [
        { sourceVolume = "pocketbase-data", containerPath = "/pb/pb_data", readOnly = false }
      ]

      environment = [
        { name = "PB_SUPERADMIN_EMAIL",    value = var.deeptutor_pb_admin_email },
        { name = "PB_SUPERADMIN_PASSWORD", value = var.deeptutor_pb_admin_password }
      ]

      healthCheck = {
        command     = ["CMD-SHELL", "wget -q --spider http://localhost:8090/api/health || exit 1"]
        interval    = 15
        timeout     = 5
        retries     = 5
        startPeriod = 30
      }

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = local.log_group
          awslogs-region        = var.region
          awslogs-stream-prefix = "deeptutor-pocketbase"
        }
      }
    },

    # ── Container 2: DeepTutor app (Next.js :3782 + FastAPI :8001) ────────
    {
      name      = "deeptutor"
      image     = var.image_deeptutor
      essential = true

      dependsOn = [
        { containerName = "pocketbase", condition = "HEALTHY" }
      ]

      mountPoints = [
        { sourceVolume = "deeptutor-data", containerPath = "/app/data", readOnly = false }
      ]

      environment = [
        { name = "NEXT_PUBLIC_API_BASE_EXTERNAL",          value = "https://tutor.pjh-etrm.ai" },
        { name = "DEEPTUTOR_IGNORE_PROCESS_ENV_OVERRIDES", value = "1" }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = local.log_group
          awslogs-region        = var.region
          awslogs-stream-prefix = "deeptutor-app"
        }
      }
    },

    # ── Container 3: nginx proxy — the only container exposed to the ALB ──
    {
      name      = "deeptutor-nginx"
      image     = var.image_deeptutor_nginx
      essential = true

      portMappings = [{ containerPort = 8530, protocol = "tcp" }]

      dependsOn = [
        { containerName = "deeptutor", condition = "START" }
      ]

      healthCheck = {
        command     = ["CMD-SHELL", "wget -q --spider http://localhost:8530/health || exit 1"]
        interval    = 30
        timeout     = 10
        retries     = 3
        startPeriod = 120
      }

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = local.log_group
          awslogs-region        = var.region
          awslogs-stream-prefix = "deeptutor-nginx"
        }
      }
    }
  ])

  lifecycle { ignore_changes = [container_definitions] }
  tags = local.tags
}

# ── ECS service ───────────────────────────────────────────────────────────────
resource "aws_ecs_service" "deeptutor" {
  name            = "${var.name}-deeptutor-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.deeptutor.arn
  desired_count   = var.desired_count_deeptutor
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.deeptutor.arn
    container_name   = "deeptutor-nginx"
    container_port   = 8530
  }

  depends_on = [aws_lb_listener.https]
  tags       = local.tags

  lifecycle { ignore_changes = [task_definition] }
}
```

- [ ] **Step 3.2: Commit**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform"
git add infra/terraform/deeptutor.tf
git commit -m "feat(deeptutor): add deeptutor.tf (ECR, EFS, ALB, ECS task+service)"
```

---

## Task 4: Extend ECS tasks security group to port 8530

**Files:**
- Modify: `infra/terraform/main.tf:113-114`

The existing `ecs_tasks` SG ingress rule allows ports 8500–8520. Port 8530 (deeptutor-nginx) is outside this range.

- [ ] **Step 4.1: Update to_port in ecs_tasks SG**

In `infra/terraform/main.tf`, find this block (around line 111):

```hcl
  ingress {
    description     = "Streamlit services from ALB"
    from_port       = 8500
    to_port         = 8520
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }
```

Change `to_port = 8520` to `to_port = 8530`:

```hcl
  ingress {
    description     = "Streamlit services from ALB"
    from_port       = 8500
    to_port         = 8530
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }
```

- [ ] **Step 4.2: Commit**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform"
git add infra/terraform/main.tf
git commit -m "feat(deeptutor): extend ecs_tasks SG ingress to port 8530"
```

---

## Task 5: Bootstrap ECR, build nginx image, update tfvars

This task creates the ECR repository first (using terraform -target), then builds and pushes the nginx image, then fills in tfvars so the full apply can proceed.

- [ ] **Step 5.1: terraform init and plan**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform/infra/terraform"
terraform init
terraform plan -out=deeptutor.tfplan 2>&1 | tail -30
```

Expected: plan shows resources to add (ECR repo, EFS, SG, ALB TG, listener rule, Route53 record, ECS task/service). Review for unexpected changes to existing resources — only the `ecs_tasks` SG `to_port` change should touch existing infra.

- [ ] **Step 5.2: Apply ECR repo only**

```bash
terraform apply -target=aws_ecr_repository.deeptutor_nginx -auto-approve
```

Expected:
```
aws_ecr_repository.deeptutor_nginx: Creating...
aws_ecr_repository.deeptutor_nginx: Creation complete after ...
Apply complete! Resources: 1 added, 0 changed, 0 destroyed.
```

Note the ECR URI from the output (format: `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/deeptutor-nginx`)

- [ ] **Step 5.3: Authenticate Docker to ECR**

```bash
aws ecr get-login-password --region ap-southeast-1 | \
  docker login --username AWS --password-stdin \
  319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
```

Expected: `Login Succeeded`

- [ ] **Step 5.4: Build and push nginx image**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform/apps/deeptutor"
docker build --platform linux/amd64 -t deeptutor-nginx:v1 .
docker tag deeptutor-nginx:v1 \
  319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/deeptutor-nginx:v1
docker push \
  319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/deeptutor-nginx:v1
```

Expected: push completes with digest output.

- [ ] **Step 5.5: Update terraform.tfvars**

Open `infra/terraform/terraform.tfvars` and add these lines (do NOT commit this file — it contains secrets):

```hcl
image_deeptutor_nginx   = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/deeptutor-nginx:v1"
image_deeptutor         = "ghcr.io/hkuds/deeptutor:latest"
desired_count_deeptutor = 1

# PocketBase superadmin — set a strong password
deeptutor_pb_admin_email    = "your-admin@email.com"
deeptutor_pb_admin_password = "choose-a-strong-password-here"
```

Replace `your-admin@email.com` and the password with your actual values.

---

## Task 6: Full terraform apply, DNS, and first-launch config

- [ ] **Step 6.1: Final terraform plan**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform/infra/terraform"
terraform plan -out=deeptutor_full.tfplan 2>&1 | grep -E "^  #|Plan:|will be"
```

Review: expect ~15 resources added, 1 changed (ecs_tasks SG). No existing resources should be destroyed.

- [ ] **Step 6.2: Apply**

```bash
terraform apply deeptutor_full.tfplan
```

Expected: `Apply complete! Resources: ~15 added, 1 changed, 0 destroyed.`

- [ ] **Step 6.3: Verify ECS task is running**

```bash
aws ecs list-tasks \
  --cluster bess-platform-cluster \
  --family bess-platform-deeptutor \
  --region ap-southeast-1
```

Wait ~3 minutes for DeepTutor to start (it builds vector indexes on startup). Then:

```bash
aws ecs describe-tasks \
  --cluster bess-platform-cluster \
  --tasks <task-arn-from-above> \
  --region ap-southeast-1 \
  --query 'tasks[0].lastStatus'
```

Expected: `"RUNNING"`

- [ ] **Step 6.4: Verify ALB health check is passing**

```bash
aws elbv2 describe-target-health \
  --target-group-arn $(aws elbv2 describe-target-groups \
    --names bess-platform-deeptutor \
    --region ap-southeast-1 \
    --query 'TargetGroups[0].TargetGroupArn' \
    --output text) \
  --region ap-southeast-1 \
  --query 'TargetHealthDescriptions[0].TargetHealth'
```

Expected: `{"State": "healthy", ...}`

If state is `initial` or `unhealthy`, check CloudWatch logs:
```bash
aws logs tail /ecs/bess-platform --log-stream-name-prefix deeptutor-nginx --follow
aws logs tail /ecs/bess-platform --log-stream-name-prefix deeptutor-app --follow
```

- [ ] **Step 6.5: Verify Route53 DNS record**

If Route53 is not managed in terraform (i.e., the `data.aws_route53_zone.pjh_etrm` data source fails), create the record manually:

1. AWS Console → Route53 → Hosted zones → `pjh-etrm.ai`
2. Create record:
   - Record name: `tutor`
   - Record type: `A`
   - Alias: Yes → Application Load Balancer → `ap-southeast-1` → select the ALB
3. Click Create

Then confirm DNS propagation:
```bash
nslookup tutor.pjh-etrm.ai
```

Expected: resolves to same IP as `www.pjh-etrm.ai`

- [ ] **Step 6.6: First-launch configuration**

Navigate to `https://tutor.pjh-etrm.ai` in the browser.

**Configure AI provider:**
1. Settings (gear icon) → AI Providers → Add Provider
2. Provider: OpenAI → paste your OpenAI API key → Save

**Configure PocketBase integration:**
1. Settings → Integrations → PocketBase URL: `http://localhost:8090` → Save

**Verify PocketBase admin account:**
- If `PB_SUPERADMIN_EMAIL` / `PB_SUPERADMIN_PASSWORD` env vars worked (PocketBase v0.23+), the admin account is already created
- To verify, use ECS exec:

```bash
# Get the task ARN
TASK_ARN=$(aws ecs list-tasks \
  --cluster bess-platform-cluster \
  --family bess-platform-deeptutor \
  --region ap-southeast-1 \
  --query 'taskArns[0]' --output text)

# Shell into pocketbase container
aws ecs execute-command \
  --cluster bess-platform-cluster \
  --task $TASK_ARN \
  --container pocketbase \
  --interactive \
  --command "/bin/sh" \
  --region ap-southeast-1
```

Then inside the container:
```sh
wget -q -O- http://localhost:8090/api/health
# Expected: {"code":200,"message":"API is healthy."}
```

If the admin was NOT auto-created (older PocketBase), you'll need to set it up via the API:
```sh
# Create superadmin (PocketBase < v0.23)
wget -q -O- --post-data='{"email":"your@email.com","password":"yourpassword","passwordConfirm":"yourpassword"}' \
  --header='Content-Type:application/json' \
  http://localhost:8090/api/admins
```

**Enable auth and create user accounts:**
1. Settings → Auth → Enable auth: on
2. Create user accounts for your team

- [ ] **Step 6.7: Smoke test the full app**

```bash
# Should return 200 and the DeepTutor HTML page
curl -s -o /dev/null -w "%{http_code}" https://tutor.pjh-etrm.ai
# Expected: 200

# API health check
curl -s https://tutor.pjh-etrm.ai/api/v1/health 2>/dev/null || \
  curl -s -o /dev/null -w "%{http_code}" https://tutor.pjh-etrm.ai/health
# Expected: 200
```

Navigate to `https://tutor.pjh-etrm.ai` and verify:
- Login page loads
- After login, chat interface is accessible
- Knowledge base upload works (confirms EFS is writable)

- [ ] **Step 6.8: Commit build script for future image updates**

Create `apps/deeptutor/build_and_push.sh`:

```bash
#!/bin/bash
# Usage: ./build_and_push.sh v2
set -e
TAG=${1:-v1}
ECR="319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/deeptutor-nginx"

aws ecr get-login-password --region ap-southeast-1 | \
  docker login --username AWS --password-stdin \
  319383842493.dkr.ecr.ap-southeast-1.amazonaws.com

docker build --platform linux/amd64 -t ${ECR}:${TAG} .
docker push ${ECR}:${TAG}

echo "Pushed ${ECR}:${TAG}"
echo "Update terraform.tfvars: image_deeptutor_nginx = \"${ECR}:${TAG}\""
```

```bash
chmod +x apps/deeptutor/build_and_push.sh
git add apps/deeptutor/build_and_push.sh
git commit -m "feat(deeptutor): add ECR build/push helper script"
```

---

## Updating DeepTutor in future

To pull a new DeepTutor release:

1. Update `image_deeptutor` in `terraform.tfvars` to the new tag (e.g. `ghcr.io/hkuds/deeptutor:v1.4.8`)
2. Run `terraform apply` — the `ignore_changes = [container_definitions]` means you need to force a new deployment:

```bash
aws ecs update-service \
  --cluster bess-platform-cluster \
  --service bess-platform-deeptutor-svc \
  --force-new-deployment \
  --region ap-southeast-1
```

The EFS volume ensures no data loss across deployments.
