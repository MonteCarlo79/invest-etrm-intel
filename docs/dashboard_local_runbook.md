# Dashboard Local Run Runbook

Run the Portal and Inner-Mongolia Streamlit dashboards on your laptop,
connecting directly to AWS RDS and S3, without needing to go through the ALB
or Cognito OIDC flow.

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Python environment | Same venv/conda as the rest of the repo |
| `pip install -r apps/portal/requirements.txt` | And `apps/bess-inner-mongolia/requirements.txt` |
| `streamlit` in PATH | `pip install streamlit` |
| AWS credentials | For S3 access (`~/.aws/credentials` or `AWS_PROFILE`) |
| **RDS network access** | Your IP must be allowed on port 5432 — see below |

### RDS Network Access (required for DB connection)

The RDS instance (`bess-platform-pg`) is in a private subnet. The security group
`rds-sg` only allows port 5432 from `ecs_tasks-sg` by default.

To connect from your laptop, add an inbound rule to `rds-sg`:
- **Type**: PostgreSQL
- **Port**: 5432
- **Source**: Your public IP (check `curl ifconfig.me`)

See `docs/knowledge_pool_aws_migration_recon.md §2` for full network topology.

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `AUTH_MODE` | No | `alb_oidc` | Set to `dev` to bypass OIDC auth |
| `DEV_USER_EMAIL` | No | `dev@local` | Email shown in dev auth mode |
| `DEV_USER_ROLE` | No | `Admin` | Role injected: `Admin`, `Trader`, `Quant`, `Analyst`, `Viewer` |
| `DB_DSN` | Yes | — | SQLAlchemy URL for Portal (`postgresql://...`) |
| `PGURL` | Yes | — | Psycopg2 URL for Inner-Mongolia (can equal `DB_DSN`) |
| `AWS_PROFILE` | No | default | Named AWS profile for S3/ECS SDK calls |
| `AWS_REGION` | No | `ap-southeast-1` | AWS region |

The RDS connection string (fill in the password from `config/.env`):
```
postgresql://postgres:PASSWORD@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require
```

---

## Running Locally

### Windows

```bat
REM Edit scripts/run_dashboard_local.bat to set DB_DSN, then:
scripts\run_dashboard_local.bat
```

Both dashboards open in separate terminal windows.

### Unix / WSL / Mac

```bash
export DB_DSN="postgresql://postgres:PASSWORD@...rds.amazonaws.com:5432/marketdata?sslmode=require"
export PGURL="$DB_DSN"
bash scripts/run_dashboard_local.sh
```

### Manual (one dashboard at a time)

```bash
# Portal
AUTH_MODE=dev DB_DSN="$DB_DSN" \
  streamlit run apps/portal/app.py \
    --server.port 8500 \
    --server.baseUrlPath ""

# Inner Mongolia
AUTH_MODE=dev PGURL="$DB_DSN" \
  streamlit run apps/bess-inner-mongolia/im/app.py \
    --server.port 8504 \
    --server.baseUrlPath ""
```

---

## URLs

| Dashboard | Local URL | AWS URL |
|---|---|---|
| Portal | http://localhost:8500 | https://platform.domain.com/portal/ |
| Inner Mongolia | http://localhost:8504 | https://platform.domain.com/inner-mongolia/ |

---

## Local Mode Behaviour

| Feature | Local (AUTH_MODE=dev) | AWS (ALB OIDC) |
|---|---|---|
| Login | Skipped — synthetic user injected | Cognito OIDC via ALB |
| User role | `DEV_USER_ROLE` (default Admin) | Cognito group membership |
| DB connection | Direct RDS via `DB_DSN` / `PGURL` | Same RDS via ECS env var |
| S3 access | AWS credentials from `~/.aws` | ECS task IAM role |
| Inner-Mongolia pipeline trigger | Shows warning — ECS not configured | Launches Fargate task |

The pipeline trigger ("Run BESS Arbitrage") shows a warning in local mode:
> *Pipeline trigger unavailable in local mode (ECS_CLUSTER / PIPELINE_TASK_DEF not configured).*

Previously computed results are still readable from the DB — only triggering
new pipeline runs is disabled.

---

## Switching the AWS Service On/Off

Scale dashboards to 0 tasks (saves ECS cost, preserves all resources):

```bash
cd infra/terraform
terraform apply \
  -var desired_count_portal=0 \
  -var desired_count_inner_mongolia=0
```

Scale back to 1:

```bash
terraform apply \
  -var desired_count_portal=1 \
  -var desired_count_inner_mongolia=1
```

EventBridge scheduled tasks (data pipelines, agents) are **not affected** by
these changes — they run as one-off ECS RunTask calls, not as persistent services.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `psycopg2.OperationalError: Connection timed out` | Your IP not in `rds-sg` | Add inbound rule on port 5432 |
| `PGURL not found` / `DB_DSN not found` | Env var not set | Export `DB_DSN` and `PGURL` before running |
| `Please log in via SSO.` on portal | `AUTH_MODE` not set to `dev` | Set `AUTH_MODE=dev` |
| Inner-Mongolia crashes with `KeyError: 'ECS_CLUSTER'` | Old version of app.py | Confirm latest code from `cost-optimisation` branch |
| `ModuleNotFoundError` | Missing package | `pip install -r apps/<app>/requirements.txt` |
