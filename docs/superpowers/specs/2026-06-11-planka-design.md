# Planka — Personal Kanban Board on AWS

**Date:** 2026-06-11
**Status:** Approved for implementation

---

## Overview

Deploy [Planka](https://github.com/plankanban/planka) — an open-source Trello clone — as a standalone personal Kanban board inside the bess-platform AWS environment. Planka provides boards, lists, drag-and-drop cards, labels, and due dates out of the box with no custom code required.

---

## Architecture

```
Browser → ALB (host-based rule: planka.{domain}) → ECS Fargate (Planka) → RDS PostgreSQL (planka DB)
```

**Key decisions:**
- Planka does not support sub-path deployment, so it uses **host-based ALB routing** (`planka.{domain}`) rather than path-based (like `/todo`)
- No ECR repository needed — pulls `ghcr.io/plankanban/planka:latest` directly
- Uses existing RDS PostgreSQL instance with a dedicated `planka` database
- Planka has its own login screen — **Cognito auth is not used** for this app; the ALB listener rule forwards directly with no `authenticate-cognito` action
- Initial admin account is seeded via environment variables on first boot

---

## Components

### 1. ECS Task Definition (`planka`)
- Image: `ghcr.io/plankanban/planka:latest`
- Port: `1337`
- CPU: `256`, Memory: `512`
- Health check: `GET /` → 200
- Environment variables:
  - `DATABASE_URL` — `postgresql://{user}:{pass}@{rds_host}:5432/planka?sslmode=require`
  - `SECRET_KEY` — random 64-char secret (new Terraform variable)
  - `BASE_URL` — `https://planka.{domain}`
  - `DEFAULT_ADMIN_EMAIL` — initial admin email (new Terraform variable)
  - `DEFAULT_ADMIN_PASSWORD` — initial admin password (new Terraform variable)
  - `TRUST_PROXY` — `0` (ALB terminates TLS, forwards HTTP internally)
- CloudWatch log group: `/ecs/bess-platform` (existing), stream prefix `planka`

### 2. ALB Target Group (`planka`)
- Port: `1337`, Protocol: `HTTP`
- Health check path: `/`, matcher: `200-302`
- Deregistration delay: `30s`

### 3. ALB Listener Rule (`planka_host`)
- Priority: `45` (next free slot after priority 40)
- Condition: `host_header = ["planka.{domain}"]`
- Action: forward to Planka target group (no Cognito step)

### 4. Terraform Variables (new)
```hcl
variable "planka_secret_key"        { type = string, sensitive = true }
variable "planka_admin_email"        { type = string }
variable "planka_admin_password"     { type = string, sensitive = true }
variable "planka_base_url"           { type = string }  # e.g. https://planka.yourdomain.com
```

### 5. DNS
User must add an **A record** (alias) in Route53 pointing `planka.{domain}` → the existing ALB DNS name. The existing ACM certificate must cover this subdomain (wildcard certs like `*.domain.com` cover this automatically; single-domain certs will need a new SAN added).

### 6. RDS: `planka` database
Before first deploy, create the database on the existing RDS instance:
```sql
CREATE DATABASE planka;
```
Planka runs its own Sails.js ORM migrations on startup — no manual schema setup needed.

### 7. docker-compose (local dev)
Add a `planka` service to the existing `docker-compose.yml`:
```yaml
planka:
  image: ghcr.io/plankanban/planka:latest
  ports:
    - "1337:1337"
  environment:
    DATABASE_URL: postgresql://bess:bess@postgres:5432/planka
    SECRET_KEY: local-dev-secret-key-not-for-production
    BASE_URL: http://localhost:1337
    DEFAULT_ADMIN_EMAIL: admin@local.dev
    DEFAULT_ADMIN_PASSWORD: changeme
    TRUST_PROXY: "0"
  depends_on:
    - postgres
```

The existing postgres service needs an init script to create the `planka` database. Add to docker-compose postgres service:
```yaml
postgres:
  environment:
    POSTGRES_MULTIPLE_DATABASES: bess,planka  # see init script below
  volumes:
    - ./infra/docker/postgres-init:/docker-entrypoint-initdb.d
```
Add `infra/docker/postgres-init/create-multiple-dbs.sh` (standard multi-DB init script).

---

## Files Changed

| File | Change |
|------|--------|
| `infra/terraform/main.tf` | Add: ECS task def, ECS service, target group, ALB listener rule |
| `infra/terraform/variables.tf` | Add: 4 Planka variables |
| `infra/terraform/terraform.tfvars` | Add: values for 4 Planka variables |
| `docker-compose.yml` | Add: `planka` service; update `postgres` service |
| `infra/docker/postgres-init/create-multiple-dbs.sh` | New: multi-DB init script |

No new `apps/` directory. No custom application code.

---

## Deployment Steps

1. Add `planka` database to RDS: `CREATE DATABASE planka;`
2. Add Terraform resources and variables
3. `terraform plan` / `terraform apply`
4. Add DNS A record: `planka.{domain}` → ALB DNS name
5. Visit `https://planka.{domain}`, log in with `DEFAULT_ADMIN_EMAIL` / `DEFAULT_ADMIN_PASSWORD`
6. Change admin password immediately after first login

---

## Constraints & Risks

| Risk | Mitigation |
|------|-----------|
| ACM cert doesn't cover `planka.{domain}` | Check cert SANs before apply; add SAN or use wildcard |
| Planka login is not behind Cognito | Acceptable for personal tool; use a strong password |
| `ghcr.io` rate limits in CI/CD | Pull image once, push to ECR if needed |
| Planka stores uploaded attachments in-container | For now: no file attachments (ephemeral ECS storage); Hermes will handle file collection separately |
