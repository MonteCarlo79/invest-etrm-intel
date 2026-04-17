# Knowledge Pool — AWS / RDS Migration Reconnaissance

_Produced: 2026-04-16. Based on a full read of `infra/terraform/main.tf`, `infra/terraform/variables.tf`, `config/.env`, `apps/spot-agent/.env`, `db/init.sql`, representative DDL files, and `scripts/settlement_ingest.py` / `scripts/spot_market_ingest.py`._

---

## 1. Existing RDS Infrastructure

### Instance

| Field | Value |
|---|---|
| Identifier | `bess-platform-pg` |
| Endpoint | `bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com` |
| Port | 5432 |
| Database | `marketdata` |
| Engine | PostgreSQL 18.2 |
| Instance class | `db.t4g.micro` |
| Storage | 20 GiB gp2 |
| Multi-AZ | No |
| Publicly accessible | **No** |
| Placement | Private subnet |
| Backup retention | 7 days |
| Storage encrypted | Yes |

### Credentials

Credentials are injected directly into ECS task definitions as plaintext Terraform variables (no Secrets Manager). The `config/.env` file holds the same credentials for local script use:

```
PGURL=postgresql://postgres:!BESSmap2026@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require
PGHOST=bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com
PGPORT=5432
PGDATABASE=marketdata
PGUSER=postgres
PGPASSWORD=!BESSmap2026
```

> **Note:** `apps/spot-agent/.env` has a separate entry pointing to a **local** database (`DB_URL=postgresql://postgres:root@127.0.0.1:5433/marketdata`). Scripts that load this file as a fallback will connect to local, not RDS. See §5.

---

## 2. Network Topology and Security Groups

### VPC Layout (ap-southeast-1)

- 2 public subnets + 2 private subnets in one VPC
- RDS in private subnets, `publicly_accessible = false`
- ECS Fargate tasks in private subnets, `assign_public_ip = true` (outbound via IGW)

### Security Group Rules (critical)

| SG | Direction | Protocol | Port | Source |
|---|---|---|---|---|
| `rds-sg` | Inbound | TCP | 5432 | `ecs_tasks-sg` only |
| `ecs_tasks-sg` | Inbound | TCP | 8500-8503 | `alb-sg` |
| `ecs_tasks-sg` | Outbound | All | All | 0.0.0.0/0 |

**The `rds-sg` has no other inbound rules.** This means:
- ECS tasks CAN reach RDS (they are in `ecs_tasks-sg`)
- Developer machines CANNOT reach RDS — connections time out
- The Tailscale jump host (`172.31.30.155`) is in the VPC but is **not** in `ecs_tasks-sg`, so it also cannot reach RDS
- There is no bastion host, no VPN gateway, no developer SG in the current Terraform

### What This Means for Local Script Runs

Any script that needs `PGURL` pointing at the live RDS instance **will time out** from a developer laptop or the Tailscale host unless the network path is opened first. See §6 (Recommended Fixes).

---

## 3. How ECS Services Currently Connect

Every ECS task definition in `main.tf` injects `PGURL` as a plaintext environment variable using the Terraform template:

```
postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.pg.address}:5432/${var.db_name}?sslmode=require
```

This is the **correct and consistent pattern** across all 12+ task definitions. SSL is enforced via the `?sslmode=require` query parameter — psycopg2 honours this without any additional connection kwargs.

Services using this pattern (confirmed from `main.tf`):
- `bess-map`, `portal`, `uploader`, `inner-mongolia`, `mengxi-dashboard`
- `strategy-agent`, `portfolio-agent`, `execution-agent`, `dev-agent`
- `mengxi-ingestion` (EventBridge-scheduled pipeline)
- `trading-bess-mengxi` (TT DAAS collector, EventBridge-scheduled)

---

## 4. Knowledge Pool DB Layer (`services/knowledge_pool/db.py`)

The knowledge pool's `db.py` already resolves the database URL in the correct priority order:

```python
PGURL > DB_URL > DATABASE_URL > MARKETDATA_DB_URL
```

For local use it loads `.env` files in this order: `repo_root/.env` → `config/.env` → `apps/spot-agent/.env`.

Since `config/.env` contains the live RDS `PGURL`, **any script that `get_conn()` calls will hit RDS** as long as:
1. `repo_root/.env` does not exist (or does not set a conflicting DB variable), AND
2. The machine can actually reach the RDS endpoint (§6).

Current gap in `db.py`: it does not log which env key was resolved, and the connection error on timeout is a bare psycopg2 `OperationalError` with no guidance. A minor improvement (§7) will add a log line and a human-readable error.

---

## 5. `.env` Load Order Risk (spot_market_ingest.py)

`scripts/spot_market_ingest.py` loads `.env` files from:

1. `repo_root/.env` (preferred)
2. `apps/spot-agent/.env` (fallback)

`apps/spot-agent/.env` sets `DB_URL=postgresql://postgres:root@127.0.0.1:5433/marketdata` — a **local** database on port 5433. If `repo_root/.env` is absent (it is not committed), this script defaults to the local DB.

**Settlement scripts** (`settlement_ingest.py`, `settlement_query.py`) load `.env` in the same order via `services/knowledge_pool/db.py`. They would also fall through to the local DB if `repo_root/.env` is absent and the machine has a local Postgres.

**Safe path for RDS:** ensure `config/.env` is loaded (it is in the fallback chain for `settlement_*.py` but NOT for `spot_market_ingest.py`) or set `PGURL` explicitly in the environment before running any script.

---

## 6. DDL Application Path

### Schema Bootstrap

`db/init.sql` only creates `marketdata`, `audit`, `logs` schemas. It does **not** create `staging`, `core`, or `ops`.

All DDL files are self-bootstrapping:

```sql
-- db/ddl/staging/spot_report_knowledge.sql
CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS staging.spot_report_documents (...);
```

This means each DDL file can be applied with a plain `psql -f` and will create the schema if absent. No separate bootstrap step is needed.

### Current Apply Pattern (from settlement_ingest.py `--init-db`)

```python
subprocess.run(["psql", pgurl, "-f", str(ddl_file)], check=True)
```

`psql` is invoked directly — it must be in PATH. The `sslmode=require` in the URL is passed through to psql automatically.

### Known DDL Files Requiring Apply for Each Pool

**Spot Market Knowledge Pool:**
- `db/ddl/staging/spot_report_knowledge.sql`
- `db/ddl/core/asset_alias_map.sql`

**Settlement Knowledge Pool:**
- `db/ddl/staging/settlement_tables.sql`
- `db/ddl/staging/settlement_documents.sql` (if separate)

**Data Ingestion Ops Layer:**
- `db/ddl/ops/ingestion_control.sql`

---

## 7. IAM and Secrets

### Current State

- No Secrets Manager usage anywhere in Terraform
- No IAM role for DB access (RDS IAM auth not enabled)
- Credentials are Terraform variables → stored in `terraform.tfstate` (plaintext)
- The `config/.env` file holds the same credentials

### ECS Task IAM Roles

Two IAM roles are defined:
- `ecsTaskExecutionRole` — attached to all task definitions, has `AmazonECSTaskExecutionRolePolicy` + ECR pull + CloudWatch logs
- `ecs_task_role` — task-level role, has S3 read/write for the uploads bucket

Neither role has RDS-specific permissions (consistent with password auth, not IAM auth).

---

## 8. Summary: What Can Be Reused, What Needs Work

### Reuse As-Is

| Item | Status |
|---|---|
| RDS instance and connection string | Ready — `config/.env` has working `PGURL` |
| `services/knowledge_pool/db.py` URL resolution | Works — minor logging improvement only |
| SSL enforcement (`?sslmode=require`) | Already in all ECS task defs and config/.env |
| DDL self-bootstrapping | All files include `CREATE SCHEMA IF NOT EXISTS` |
| `settlement_ingest.py --init-db` pattern | Works on any machine that can reach RDS |

### Needs Action

| Item | Action Required | Owner |
|---|---|---|
| **RDS SG — no developer access** | Add inbound TCP/5432 rule for developer IP(s) or Tailscale CIDR to `rds-sg` | Terraform / AWS console |
| **Tailscale host not in ecs_tasks-sg** | Add 172.31.30.155/32 to `rds-sg` inbound (or attach host to SG via ENI) | AWS console |
| **spot_market_ingest.py fallback to local DB** | Set `PGURL` in shell before running, or create `repo_root/.env` pointing at RDS | Operator |
| **db.py silent error on timeout** | Add log of resolved env key + human-readable connection error hint | Code (minor, §4 in runbook) |
| **No DDL runner script** | Document `psql -f` sequence or write `scripts/knowledge_pool_apply_ddl.py` | Code / docs |

---

## 9. Recommended Minimal Migration Path

For a developer to run knowledge pool scripts against live RDS without disrupting ECS services:

1. **Open network access** — Add the developer's IP (or Tailscale VPC IP `172.31.30.155/32`) to the `rds-sg` inbound rules on port 5432. This is the only Terraform change needed.

2. **Set PGURL** — Either:
   - Create `bess-platform/repo_root_env_example.txt` → `repo_root/.env` with the RDS PGURL, OR
   - `export PGURL="postgresql://postgres:...@...ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require"` in the shell

3. **Apply DDL** — Run:
   ```bash
   psql "$PGURL" -f db/ddl/staging/spot_report_knowledge.sql
   psql "$PGURL" -f db/ddl/core/asset_alias_map.sql
   psql "$PGURL" -f db/ddl/staging/settlement_tables.sql   # if not already applied
   psql "$PGURL" -f db/ddl/ops/ingestion_control.sql       # when ready
   ```
   Each file is idempotent (`CREATE TABLE IF NOT EXISTS`).

4. **Run smoke test**:
   ```bash
   python scripts/knowledge_pool_db_smoke_test.py
   ```

5. **Run ingestion / settlement scripts** as normal — they will pick up the RDS `PGURL` from `config/.env`.

No ECS or Lambda changes are needed. Existing ECS pipelines are unaffected.

---

## 10. Open Questions / Risks

| Question | Risk | Suggested Resolution |
|---|---|---|
| Is `db.t4g.micro` sufficient for concurrent settlement ingest + ECS load? | Medium — micro has limited concurrent connections | Monitor; upgrade to `db.t3.small` if connection limits hit |
| Are credentials in `config/.env` rotated? | High if leaked — no rotation mechanism | Move to Secrets Manager in a follow-up sprint |
| `db/ddl/staging/settlement_tables.sql` — has this been applied to RDS? | Unknown | Run `settlement_ingest.py --init-db` or check `\dt staging.*` in psql |
| PostgreSQL 18.2 — is this the actual minor version? | Low — Terraform specifies `"18"` engine; RDS pins to current minor | Check `SELECT version()` after connection |
