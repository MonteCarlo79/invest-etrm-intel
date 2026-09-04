# DeepTutor AWS Deployment Design

**Date:** 2026-06-19  
**Status:** Approved  
**Source:** https://github.com/HKUDS/DeepTutor  
**Working dir for app files:** `C:\Users\dipeng.chen\OneDrive\Tutor\`  
**Infrastructure:** bess-platform AWS (ap-southeast-1), ECS Fargate cluster

---

## 1. Goal

Deploy DeepTutor (agent-native AI tutoring app) to AWS at `https://tutor.pjh-etrm.ai`, reusing the existing ECS Fargate cluster, ALB, and operational patterns established by the crystal-ball app.

---

## 2. Architecture Overview

```
Internet
   │
   ▼
ALB (existing) ─── host: tutor.pjh-etrm.ai ──► TG: deeptutor (port 8530)
                                                        │
                                                        ▼
                                              ECS Task (Fargate)
                                          ┌────────────────────────┐
                                          │  deeptutor-nginx:8530  │ ← exposed to ALB
                                          │     /api/ → :8001      │
                                          │     /     → :3782      │
                                          ├────────────────────────┤
                                          │  deeptutor (main app)  │
                                          │   Next.js  :3782       │ ← internal
                                          │   FastAPI  :8001       │ ← internal
                                          ├────────────────────────┤
                                          │  pocketbase:8090       │ ← internal
                                          └────────────────────────┘
                                                     │
                                                     ▼
                                              EFS Volume
                                          /app/data   /pb/pb_data
```

- **URL:** `tutor.pjh-etrm.ai` — Route53 A-alias record to the existing ALB
- **SSL:** Existing wildcard cert `*.pjh-etrm.ai` covers the subdomain
- **Auth:** DeepTutor's built-in auth (no Cognito). DeepTutor handles user login internally.
- **AI provider:** OpenAI (configured via Settings UI after first launch)
- **User mode:** Multi-user (PocketBase sidecar)

---

## 3. ECS Task: 3 Containers

All three containers run in the same ECS task and share a localhost network namespace.

### Container 1 — `deeptutor-nginx` (custom, built locally)

| Field | Value |
|---|---|
| Image | ECR: `deeptutor-nginx` |
| Port | 8530 (exposed to ALB) |
| Role | Nginx reverse proxy |
| Health check | `wget -q --spider http://localhost:8530/` |

Routing rules in `nginx.conf`:
- `location /api/` → `http://localhost:8001` (FastAPI backend, all routes `/api/v1/...`)
- `location /` → `http://localhost:3782` (Next.js frontend)
- WebSocket upgrade headers on both locations (for SSE/streaming)

### Container 2 — `deeptutor` (upstream image)

| Field | Value |
|---|---|
| Image | `ghcr.io/hkuds/deeptutor:latest` (or pinned tag) |
| Ports | 3782, 8001 (internal only — no `portMappings` in task def) |
| EFS mount | `/app/data` ← access point `deeptutor-data` |
| Depends on | pocketbase (HEALTHY) |
| CPU / RAM | 1024 CPU units / 2048 MB |

Key environment variables:
- `NEXT_PUBLIC_API_BASE_EXTERNAL=https://tutor.pjh-etrm.ai`
- `DEEPTUTOR_IGNORE_PROCESS_ENV_OVERRIDES=1` (already set in upstream image)

### Container 3 — `pocketbase` (upstream image)

| Field | Value |
|---|---|
| Image | `ghcr.io/muchobien/pocketbase:latest` |
| Port | 8090 (internal only) |
| EFS mount | `/pb/pb_data` ← access point `pocketbase-data` |
| Health check | `wget -q --spider http://localhost:8090/api/health` |
| CPU / RAM | 256 CPU units / 512 MB |

### Task-level sizing

- Total: 2 vCPU / 4096 MB
- Launch type: FARGATE
- Task execution role: existing (ECR pull + SSM secrets access)
- Task role: existing (CloudWatch logs)

---

## 4. Storage (EFS)

One EFS filesystem `deeptutor-efs` with two access points:

| Access Point | EFS root path | Container mount | Container |
|---|---|---|---|
| `deeptutor-data` | `/deeptutor` | `/app/data` | deeptutor |
| `pocketbase-data` | `/pocketbase` | `/pb/pb_data` | pocketbase |

**EFS security group** (`deeptutor-efs-sg`):
- Inbound: TCP 2049 (NFS) from ECS task security group

**ECS task SG**: add outbound TCP 2049 to EFS SG (if not already open).

EFS mount targets: one per subnet used by the ECS cluster (same subnets as existing Fargate services).

**What persists on EFS:**
- `data/user/settings/` — model catalog, auth config, system config (written by DeepTutor on first launch)
- `data/knowledge_bases/` — uploaded documents + vector indexes
- `data/user/workspace/` — chat history, memory, notebooks
- `data/memory/` — agent memory
- `pb_data/` — PocketBase embedded SQLite DB (user accounts, sessions)

---

## 5. Networking & ALB

**Route53**: new A-record (alias) `tutor.pjh-etrm.ai` → existing ALB DNS name.

**ALB listener rule** (existing HTTPS/443 listener):
- Condition: host-header `tutor.pjh-etrm.ai`
- Action: forward to target group `deeptutor`
- Priority: 60 (above crystal-ball at 50)
- No Cognito authenticate action

**Target group `deeptutor`**:
- Port: 8530, protocol HTTP
- Target type: ip (Fargate)
- Health check: `GET /` on port 8530, expect 200, 30s interval, 3 retries, 60s start period

**Security group on ECS tasks** (existing SG): add inbound TCP 8530 from ALB security group.

---

## 6. Secrets & Configuration

**SSM Parameter Store**:
- `/bess-platform/deeptutor/openai-api-key` — SecureString — OpenAI API key (value set manually before first deployment)

The SSM key is available as a task secret env var `OPENAI_API_KEY` for reference, but DeepTutor's config loads from EFS JSON files (not env vars). OpenAI key is configured via the Settings UI after first launch.

**First-launch checklist** (after `terraform apply`):
1. Navigate to `https://tutor.pjh-etrm.ai`
2. Settings → AI Providers → OpenAI → paste key
3. Settings → Integrations → PocketBase URL: `http://localhost:8090`
4. PocketBase admin: set `PB_SUPERADMIN_EMAIL` + `PB_SUPERADMIN_PASSWORD` env vars on the pocketbase container (v0.23+ automigrate), OR use `aws ecs execute-command` to access the container and visit `http://localhost:8090/_/`
5. Settings → Auth → enable built-in auth, create user accounts

---

## 7. File Structure

New files to create in `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\`:

```
apps/
└── deeptutor/
    ├── Dockerfile       # FROM nginx:alpine, COPY nginx.conf
    └── nginx.conf       # proxy: /api/ → :8001, / → :3782

infra/
└── terraform/
    ├── deeptutor.tf     # All new resources (see section 8)
    └── variables.tf     # +3 vars: image_deeptutor_nginx, image_deeptutor, desired_count_deeptutor
```

DeepTutor source files remain in `C:\Users\dipeng.chen\OneDrive\Tutor\DeepTutor-main\` — the upstream image is pulled directly from GHCR, not built from source.

---

## 8. Terraform Resources (`deeptutor.tf`)

```
aws_ecr_repository.deeptutor_nginx
aws_security_group.deeptutor_efs
aws_security_group_rule.deeptutor_efs_inbound_nfs
aws_security_group_rule.deeptutor_ecs_outbound_efs   # on existing ECS task SG
aws_security_group_rule.deeptutor_alb_ingress        # port 8530 on existing ECS task SG
aws_efs_file_system.deeptutor
aws_efs_mount_target.deeptutor[*]                    # one per subnet
aws_efs_access_point.deeptutor_data
aws_efs_access_point.pocketbase_data
aws_ssm_parameter.deeptutor_openai_key
aws_lb_target_group.deeptutor
aws_lb_listener_rule.deeptutor_host
aws_route53_record.deeptutor                         # tutor.pjh-etrm.ai alias
aws_ecs_task_definition.deeptutor
aws_ecs_service.deeptutor
```

---

## 9. Deployment Steps (high-level)

1. Build and push `deeptutor-nginx` image to ECR
2. Set SSM parameter value (OpenAI key)
3. `terraform apply` — creates EFS, ALB rule, ECS service
4. Wait for task to reach RUNNING state
5. First-launch configuration via Settings UI
6. Verify at `https://tutor.pjh-etrm.ai`

---

## 10. Out of Scope

- PocketBase exposed via ALB (not needed; accessed internally by DeepTutor)
- Cognito SSO (DeepTutor built-in auth used instead)
- Building DeepTutor from source (upstream GHCR image used)
- Custom domain certificate (existing `*.pjh-etrm.ai` wildcard covers it)
