# Knowledge Pool — AWS / RDS Operator Runbook

_Last updated: 2026-04-16_

See also: `docs/knowledge_pool_aws_migration_recon.md` for full infrastructure context.

---

## Prerequisites

### 1. Network Access to RDS

The RDS instance (`bess-platform-pg`) is in a private subnet. Its security group (`rds-sg`) allows port 5432 only from `ecs_tasks-sg`.

**To connect from a developer machine or the Tailscale jump host:**

Add an inbound rule to `rds-sg` via the AWS Console or Terraform:
- Protocol: TCP
- Port: 5432
- Source: your developer IP (`x.x.x.x/32`) or Tailscale VPC IP (`172.31.30.155/32`)

Once added, the connection will succeed immediately (no restart needed).

> ECS tasks already have access and are unaffected by this change.

### 2. PGURL

Source `config/.env` or export `PGURL` before running any script:

```bash
# Option A: source the shared env file
source config/.env

# Option B: export directly
export PGURL="postgresql://postgres:!BESSmap2026@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require"
```

> `config/.env` is in the fallback chain for settlement scripts. For spot market scripts (`spot_market_ingest.py`), always set PGURL explicitly — that script falls through to `apps/spot-agent/.env` which points at a local DB.

### 3. psql in PATH

Several scripts invoke `psql` as a subprocess. Verify it is available:

```bash
psql --version
# psql (PostgreSQL) 15.x or similar
```

On Windows with Git Bash: psql may be at `/c/Program Files/PostgreSQL/15/bin/psql`. Add to PATH or use full path in commands below.

---

## DDL — First-Time Setup

Apply DDL files in dependency order. All files are idempotent (`CREATE TABLE IF NOT EXISTS`) and self-bootstrapping (`CREATE SCHEMA IF NOT EXISTS`).

### Spot Market Knowledge Pool

```bash
psql "$PGURL" -f db/ddl/core/asset_alias_map.sql
psql "$PGURL" -f db/ddl/core/asset_alias_map_seed.sql
psql "$PGURL" -f db/ddl/staging/spot_report_knowledge.sql
```

### Settlement Knowledge Pool

```bash
psql "$PGURL" -f db/ddl/staging/settlement_report_knowledge.sql
```

### Data Ingestion Ops Layer

```bash
psql "$PGURL" -f db/ddl/ops/ingestion_control.sql
```

### Other Core / Reports Tables (as needed)

```bash
psql "$PGURL" -f db/ddl/core/asset_monthly_compensation.sql
psql "$PGURL" -f db/ddl/core/asset_monthly_compensation_seed.sql
psql "$PGURL" -f db/ddl/core/asset_scenario_availability.sql
psql "$PGURL" -f db/ddl/core/asset_scenario_availability_seed.sql
psql "$PGURL" -f db/ddl/ops/mengxi_agent4_reliability.sql
```

### Verify Tables Were Created

```bash
psql "$PGURL" -c "\dt staging.*"
psql "$PGURL" -c "\dt core.*"
psql "$PGURL" -c "\dt ops.*"
```

---

## Smoke Test

Run after DDL apply and after any major change:

```bash
python scripts/knowledge_pool_db_smoke_test.py

# With extra detail:
python scripts/knowledge_pool_db_smoke_test.py --verbose
```

Exit 0 = all checks passed. Exit 1 = failures — check output for hints.

---

## Spot Market Knowledge Pool

### Ingest

```bash
# Default (full corpus in data/spot_market_reports/):
PGURL="..." python scripts/spot_market_ingest.py

# Init tables + ingest:
PGURL="..." python scripts/spot_market_ingest.py --init-db

# Single file:
PGURL="..." python scripts/spot_market_ingest.py --file data/spot_market_reports/2025-10.pdf

# Force re-ingest all:
PGURL="..." python scripts/spot_market_ingest.py --force
```

> **Important:** Always set PGURL explicitly for this script. It falls through to `apps/spot-agent/.env` (local DB) if PGURL is not in the environment.

### Query

```bash
# Full-text search
python scripts/spot_market_query.py search "调频" --limit 10

# Structured fact lookup
python scripts/spot_market_query.py facts --asset suyou --year 2025 --month 10
```

---

## Settlement Knowledge Pool

### Ingest

```bash
# All docs in data/settlement_reports/:
python scripts/settlement_ingest.py

# Init DB tables:
python scripts/settlement_ingest.py --init-db

# Force re-ingest (e.g., after regex fix):
python scripts/settlement_ingest.py --year 2025 --force

# Single asset, single month:
python scripts/settlement_ingest.py --asset suyou --year 2025 --month 10
```

### Generate Knowledge Notes

```bash
# All note types:
python scripts/settlement_generate_notes.py

# Dry run (list without writing):
python scripts/settlement_generate_notes.py --dry-run

# One asset:
python scripts/settlement_generate_notes.py --asset suyou

# One month:
python scripts/settlement_generate_notes.py --asset suyou --year 2025 --month 10

# By type:
python scripts/settlement_generate_notes.py --type monthly
python scripts/settlement_generate_notes.py --type summary
python scripts/settlement_generate_notes.py --type component
python scripts/settlement_generate_notes.py --type reconciliation
python scripts/settlement_generate_notes.py --type index
```

Notes are written to `knowledge/settlement/`.

### Query

```bash
# Full-text search
python scripts/settlement_query.py search "市场上网电费" --limit 10
python scripts/settlement_query.py search "调频" --asset suyou

# Structured facts
python scripts/settlement_query.py facts --asset suyou --year 2025 --month 10
python scripts/settlement_query.py facts --fact-type total_amount --year 2025

# Monthly totals time-series
python scripts/settlement_query.py totals --asset wulate

# Reconciliation
python scripts/settlement_query.py recon --flagged
python scripts/settlement_query.py recon --asset wulanchabu --year 2025 --month 1

# Document registry
python scripts/settlement_query.py docs
python scripts/settlement_query.py docs --status error

# Notes index
python scripts/settlement_query.py notes
```

---

## Data Ingestion Ops Layer

### Freshness Monitor (detect gaps)

```bash
# Detect only — no dispatch:
PGURL="..." python services/data_ingestion/freshness_monitor.py

# Detect + auto-dispatch ECS reconcile tasks:
PGURL="..." ECS_DISPATCH=true ECS_CLUSTER=bess-platform \
  ENOS_MARKET_TASK_DEF=bess-enos-market-collector \
  TT_API_TASK_DEF=bess-tt-api-collector \
  python services/data_ingestion/freshness_monitor.py
```

### Check Gap Queue

```bash
psql "$PGURL" -c "SELECT dataset, collector, gap_start, gap_end, status, detected_at FROM ops.ingestion_gap_queue WHERE status = 'pending' ORDER BY detected_at;"
```

### Manual Gap Remediation

```bash
# Reconcile a date range for one collector
RUN_MODE=reconcile START_DATE=2026-03-10 END_DATE=2026-04-08 \
  PGURL="..." python services/data_ingestion/enos_market_collector.py

# Dry run first:
DRY_RUN=true RUN_MODE=reconcile START_DATE=2026-03-10 END_DATE=2026-04-08 \
  PGURL="..." python services/data_ingestion/enos_market_collector.py
```

### Check Job History

```bash
psql "$PGURL" -c "
  SELECT collector, run_mode, start_date, end_date, status, rows_written, started_at
  FROM ops.ingestion_job_runs
  ORDER BY started_at DESC
  LIMIT 20;
"
```

---

## Common Troubleshooting

### Connection times out

**Cause:** Your IP is not allowed in `rds-sg` inbound rules.  
**Fix:** Add inbound TCP/5432 rule for your IP to `rds-sg` in AWS Console.

### `psycopg2.OperationalError: SSL connection is required`

**Cause:** PGURL missing `?sslmode=require`.  
**Fix:** Ensure PGURL ends with `?sslmode=require`.

### `RuntimeError: No DB URL found`

**Cause:** PGURL not set in environment, and no `.env` file loaded.  
**Fix:** `source config/.env` or `export PGURL="..."` before running.

### Zero facts after ingest (settlement)

**Cause:** Likely Format C documents (上网电量结算单). The fix is in `settlement_fact_extraction.py`.  
**Fix:** `python scripts/settlement_ingest.py --year 2025 --force` to re-ingest after code fix.

### `psql: command not found`

**Fix:** Install PostgreSQL client tools, or on Windows add the bin directory to PATH:
```bash
export PATH="/c/Program Files/PostgreSQL/15/bin:$PATH"
```

### `ModuleNotFoundError: No module named 'psycopg2'`

**Fix:**
```bash
pip install psycopg2-binary
# or if in Anaconda:
conda install psycopg2
```

---

## ECS Pipelines (reference — do not modify)

These run automatically via EventBridge and are unaffected by local script runs:

| Task | Schedule (UTC) | Description |
|---|---|---|
| `mengxi-ingestion` | 00:05 daily | EnOS market data sync |
| `trading-bess-mengxi` | Various | TT DAAS API collector |
| `inner-mongolia` | On demand | Inner Mongolia pipeline |

To check ECS task logs:
```bash
# List recent runs:
aws logs get-log-events \
  --log-group-name /ecs/mengxi-ingestion \
  --log-stream-name "$(aws logs describe-log-streams --log-group-name /ecs/mengxi-ingestion --query 'logStreams[-1].logStreamName' --output text)" \
  --limit 50
```
