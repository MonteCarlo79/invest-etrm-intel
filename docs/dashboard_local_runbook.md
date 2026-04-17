# Dashboard Local Run Runbook

Run Portal and Inner-Mongolia Streamlit dashboards locally on your laptop,
connecting directly to AWS RDS. Bypasses ALB OIDC authentication entirely.
Switching back to AWS is one Terraform apply with no variable change.

---

## Contents

1. [Port Map](#1-port-map)
2. [Network Prerequisite](#2-network-prerequisite)
3. [Environment Variables Reference](#3-environment-variables-reference)
4. [APP_URL_MAP — Local Link Routing](#4-app_url_map--local-link-routing)
5. [Running Locally — Exact Commands](#5-running-locally--exact-commands)
6. [Local vs AWS Behaviour Matrix](#6-local-vs-aws-behaviour-matrix)
7. [Terraform Scale Controls](#7-terraform-scale-controls)
8. [Validation Summary](#8-validation-summary)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Port Map

| Dashboard | Local port | AWS path | Container port |
|---|---|---|---|
| Portal | 8500 | `/portal/` | 8500 |
| Market Data Uploader | 8501 | `/uploader/` | 8501 |
| BESS Map | 8503 | `/bess-map/` | 8503 |
| Inner Mongolia Intelligence | 8504 | `/inner-mongolia/` | 8504 |
| Spot Markets | 8505 | `/spot-markets/` | 8505 |
| Mengxi Dashboard | 8505 | `/mengxi-dashboard/` | 8505 |
| Model Catalogue | 8506 | `/model-catalogue/` | 8506 |

In local mode, each app runs on its own `localhost` port with `--server.baseUrlPath ""`.
In AWS mode, each app uses its ECS-hardcoded `--server.baseUrlPath <slug>` and sits
behind the ALB at the corresponding path.

---

## 2. Network Prerequisite

The RDS instance (`bess-platform-pg`) sits in a private subnet.
`rds-sg` only allows port 5432 from `ecs_tasks-sg` by default.

**Add an inbound rule to `rds-sg`:**

```bash
# Find your public IP
curl -s ifconfig.me

# AWS CLI (replace the CIDR and SG ID)
aws ec2 authorize-security-group-ingress \
  --group-id sg-XXXXXXXXX \          # rds-sg ID
  --protocol tcp \
  --port 5432 \
  --cidr <YOUR_IP>/32 \
  --region ap-southeast-1
```

Or do this in the AWS Console: EC2 → Security Groups → `rds-sg` → Inbound rules → Add rule.

**Without this change, all DB connections from your laptop will time out.**
See `docs/knowledge_pool_aws_migration_recon.md §2` for full network topology.

---

## 3. Environment Variables Reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `AUTH_MODE` | No | `alb_oidc` | Set to `dev` to bypass ALB OIDC |
| `DEV_USER_EMAIL` | No | `dev@local` | Email shown in dev auth mode |
| `DEV_USER_ROLE` | No | `Admin` | Role injected: Admin, Trader, Quant, Analyst, Viewer |
| `DB_DSN` | Yes (Portal) | — | SQLAlchemy URL (`postgresql://...`) |
| `PGURL` | Yes (Inner-Mongolia) | — | Psycopg2 URL (set to same value as DB_DSN) |
| `APP_URL_MAP` | No | — | Override app link URLs in portal (see §4) |
| `AWS_PROFILE` | No | `default` | Named AWS credentials profile |
| `AWS_REGION` | No | `ap-southeast-1` | AWS region |
| `ECS_CLUSTER` | No | — | Enable Inner-Mongolia pipeline trigger locally |
| `PIPELINE_TASK_DEF` | No | — | Task definition for inner-mongolia pipeline |
| `PRIVATE_SUBNETS` | No | — | Comma-separated subnet IDs (for ECS trigger) |
| `TASK_SECURITY_GROUPS` | No | — | Security group IDs (for ECS trigger) |

**RDS connection string:**
```
postgresql://postgres:<PASSWORD>@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require
```
Password is in `config/.env` — do not commit that file.

---

## 4. APP_URL_MAP — Local Link Routing

The portal's application cards link to sibling dashboards using AWS relative paths
(e.g., `/inner-mongolia/`). In local mode these paths do not resolve.

Set `APP_URL_MAP` to override links to local ports:

```bash
export APP_URL_MAP="inner-mongolia=http://localhost:8504,bess-map=http://localhost:8503,uploader=http://localhost:8501,model-catalogue=http://localhost:8506"
```

**Format:** `<path-slug>=<url>` pairs separated by commas.
The slug is the path component from `APP_CATALOG` with slashes stripped:
`/inner-mongolia/` → `inner-mongolia`.

**In AWS mode:** leave `APP_URL_MAP` unset. The catalog paths (`/inner-mongolia/` etc.)
are used as-is and resolve correctly behind the ALB.

**Slug → local URL reference:**

| APP_CATALOG slug | Runs locally on |
|---|---|
| `inner-mongolia` | `http://localhost:8504` |
| `bess-map` | `http://localhost:8503` |
| `uploader` | `http://localhost:8501` |
| `model-catalogue` | `http://localhost:8506` |

Only override slugs for apps you are actually running locally. Unset slugs fall back
to the AWS path.

---

## 5. Running Locally — Exact Commands

### Prerequisites

```bash
# From repo root
pip install -r apps/portal/requirements.txt
pip install -r apps/bess-inner-mongolia/requirements.txt
```

### Windows (CMD)

```bat
REM Edit scripts\run_dashboard_local.bat to set the password in DB_DSN, then:
cd C:\...\bess-platform
scripts\run_dashboard_local.bat
```

Two terminal windows open: one for portal, one for inner-mongolia.

### Unix / Mac / WSL

```bash
cd ~/bess-platform

export DB_DSN="postgresql://postgres:<PASSWORD>@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require"
export PGURL="$DB_DSN"

bash scripts/run_dashboard_local.sh
```

### Manual — Single Dashboard

```bash
# Portal only
export AUTH_MODE=dev
export DB_DSN="postgresql://postgres:<PASSWORD>@...rds.amazonaws.com:5432/marketdata?sslmode=require"
export APP_URL_MAP="inner-mongolia=http://localhost:8504"

streamlit run apps/portal/app.py \
    --server.port 8500 \
    --server.baseUrlPath ""

# Inner-Mongolia only
export AUTH_MODE=dev
export PGURL="$DB_DSN"

streamlit run apps/bess-inner-mongolia/im/app.py \
    --server.port 8504 \
    --server.baseUrlPath ""
```

### Expected URLs

```
http://localhost:8500    ← Portal
http://localhost:8504    ← Inner Mongolia Intelligence
```

### Expected Startup Behaviour

| Check | Expected |
|---|---|
| No login prompt on portal | AUTH_MODE=dev injects synthetic user |
| Header shows "Dev Mode" instead of Logout button | AUTH_MODE guard in portal header |
| Role shown as "Admin" (or your DEV_USER_ROLE) | Dev user dict passed through get_groups() |
| Role-based visibility works (Viewer = no app cards) | DEV_USER_ROLE drives CAN_OPEN_APPS etc. |
| Cognito admin panel shows warning, not crash | COGNITO_USER_POOL_ID absent → guarded |
| Portfolio/agent/dispatch metrics show placeholder | Graceful exceptions, st.info() fallback |
| Inner-Mongolia loads station data from DB | PGURL hits RDS |
| Inner-Mongolia "Run BESS Arbitrage" shows error | ECS_CLUSTER not set → RuntimeError caught |

---

## 6. Local vs AWS Behaviour Matrix

| Feature | Local (AUTH_MODE=dev) | AWS (ALB OIDC) |
|---|---|---|
| Login | Skipped — synthetic user | Cognito OIDC via ALB |
| User role | `DEV_USER_ROLE` env var | Cognito group membership |
| Logout button | "Dev Mode" label (no-op) | Cognito logout redirect |
| Cognito admin panel | Warning (pool ID absent) | Full user management |
| DB connection | Direct RDS via DB_DSN/PGURL | Same RDS via ECS env var |
| App link routing | `APP_URL_MAP` local ports | AWS relative paths |
| S3 access | `~/.aws` credentials | ECS task IAM role |
| Inner-Mongolia data display | Full (reads from DB) | Full |
| Inner-Mongolia pipeline trigger | Error message shown | Launches Fargate task |
| Streamlit base path | `""` (root `/`) | `portal`, `inner-mongolia` etc. |

**AWS mode is completely unchanged** — `AUTH_MODE` defaults to `alb_oidc`,
`APP_URL_MAP` is unset, base paths are set by ECS task definition CMD.

---

## 7. Terraform Scale Controls

All dashboard ECS services now use variables. Set to `0` to stop tasks without
destroying the service, task definition, or ALB rules.

### Complete variable list

| Variable | Default | Service |
|---|---|---|
| `desired_count_portal` | 1 | Portal |
| `desired_count_inner_mongolia` | 1 | Inner Mongolia |
| `desired_count_bess_map` | 1 | BESS Map |
| `desired_count_uploader` | 1 | Market Data Uploader |
| `desired_count_spot_markets` | 1 | Spot Markets |
| `desired_count_mengxi_dashboard` | 1 | Mengxi Dashboard |
| `desired_count_model_catalogue` | 1 | Model Catalogue |
| `pnl_attribution_desired_count` | 1 | P&L Attribution |

**EventBridge scheduled tasks (data pipelines, agents) are NOT ECS services —
they are one-off `RunTask` calls. Scaling dashboard services to 0 does not
affect any pipeline or agent scheduling.**

### Scale non-essential dashboards to 0 (cost saving)

```bash
cd infra/terraform

terraform apply \
  -var desired_count_bess_map=0 \
  -var desired_count_uploader=0 \
  -var desired_count_spot_markets=0 \
  -var desired_count_mengxi_dashboard=0 \
  -var desired_count_model_catalogue=0 \
  -var pnl_attribution_desired_count=0
```

### Keep portal + inner-mongolia running, scale everything else to 0

```bash
terraform apply \
  -var desired_count_portal=1 \
  -var desired_count_inner_mongolia=1 \
  -var desired_count_bess_map=0 \
  -var desired_count_uploader=0 \
  -var desired_count_spot_markets=0 \
  -var desired_count_mengxi_dashboard=0 \
  -var desired_count_model_catalogue=0 \
  -var pnl_attribution_desired_count=0
```

### Restore all dashboards to 1

```bash
terraform apply
# All variables default to 1 — no -var flags needed.
```

### Dry-run check (no changes applied)

```bash
terraform plan \
  -var desired_count_portal=0 \
  -var desired_count_inner_mongolia=0
# Should show only 2x "~ aws_ecs_service" changes, no destroys.
```

---

## 8. Validation Summary

### What works locally (verified by code inspection)

| Check | Status | Evidence |
|---|---|---|
| `AUTH_MODE=dev` bypasses OIDC | Verified | `auth/rbac.py` `get_user()` short-circuits before OIDC |
| Role injection via `DEV_USER_ROLE` | Verified | User dict contains `role` key; `get_groups()` returns it directly |
| Synthetic user passes portal auth gate | Verified | `get_user()` returns non-None; `st.stop()` not called |
| No ALB header dependency in dev mode | Verified | OIDC branch never reached when `AUTH_MODE=dev` |
| Inner-Mongolia `os.environ[]` removed | Verified | All 4 vars use `os.getenv()` with None defaults |
| Inner-Mongolia pipeline guard | Verified | `RuntimeError` raised and caught by `try/except` → `st.error()` |
| Portal logout replaced in dev mode | Verified | `AUTH_MODE=dev` check in header renders `st.caption("Dev Mode")` |
| `APP_URL_MAP` overrides app links | Verified | `registry.py` `_url_overrides()` applied in `get_visible_apps()` |
| Base path `""` works locally | Verified | Standard Streamlit CLI behaviour; no application code assumes path prefix |
| Base path `portal` works in AWS | Verified | ECS task definition CMD unchanged; AWS path unchanged |
| Terraform `desired_count` variables | Verified | All 8 services now use variable references |

### What does NOT work locally (by design)

| Feature | Reason | Workaround |
|---|---|---|
| Cognito admin panel (user management) | `COGNITO_USER_POOL_ID` absent | Shows `st.warning()` — not a crash |
| Inner-Mongolia pipeline trigger ("Run BESS Arbitrage") | ECS not available locally | Set `ECS_CLUSTER` + `PIPELINE_TASK_DEF` + subnets/SGs to re-enable |
| Portfolio metrics / dispatch preview / market prices | No data in DB yet or module returns empty | `st.info()` fallback shown |
| S3 uploads | Requires `~/.aws` credentials with write access to the uploads bucket | Configure `AWS_PROFILE` |

### What remains AWS-only by design

| Feature | Notes |
|---|---|
| ALB OIDC authentication | Not needed locally; ALB is the auth gateway in production |
| ECS pipeline orchestration | Inner-Mongolia pipeline trigger, agent RunTask buttons |
| CloudWatch log streaming | Logs print to local stdout instead |
| Auto-refresh via EventBridge | Scheduled jobs still fire in AWS regardless of local run |

### Network gap — still requires manual action

**The RDS security group (`rds-sg`) must have port 5432 open for your IP.**
This is the one change that cannot be automated from the codebase.
Until it is done, all DB connections from your laptop will time out with:
```
psycopg2.OperationalError: Connection timed out
Hint: RDS security group only allows port 5432 from ecs_tasks-sg.
      Add your IP (or 172.31.30.155 for Tailscale) to rds-sg inbound rules.
```

---

## 9. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Connection timed out` on any DB op | IP not in `rds-sg` | Add inbound rule (§2) |
| `PGURL not found` / `DB_DSN not found` | Env var not set | Export `DB_DSN` and `PGURL` |
| `Please log in via SSO.` on portal | `AUTH_MODE` not set to `dev` | Set `AUTH_MODE=dev` |
| Portal app card links go to `/inner-mongolia/` (404) | `APP_URL_MAP` not set | Set `APP_URL_MAP=inner-mongolia=http://localhost:8504,...` |
| Inner-Mongolia crashes at import | Missing package | `pip install -r apps/bess-inner-mongolia/requirements.txt` |
| `ModuleNotFoundError: shared.core` | Wrong working directory | Run from repo root |
| `KeyError: 'ECS_CLUSTER'` in inner-mongolia | Old code not pulled | `git pull origin cost-optimisation` |
| Cognito admin panel shows error | No Cognito creds locally | Expected — shows `st.warning()`, not a crash |
