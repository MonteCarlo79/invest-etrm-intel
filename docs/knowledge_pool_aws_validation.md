# Knowledge Pool — AWS Validation & Network Requirements

_Last updated: 2026-04-16_

This document covers:
1. What has been validated offline (without RDS access)
2. What requires live RDS access to validate
3. Network requirements and access patterns
4. Local development workflow (keeping it working)

---

## 1. Offline Validation (complete)

The following were validated without a live DB connection, using unit tests with mock DB modules.

### Settlement Fact Extraction (`settlement_fact_extraction.py`)

| Test | Result |
|---|---|
| Format A (电费账单) — total_amount regex | PASS |
| Format B (核查票) — total_amount regex | PASS |
| Format C (上网电量结算单) — greedy regex bug fixed | PASS |
| Format C — `re.findall` on full matched line, `amounts[-1]` | PASS |
| Format C — energy_mwh extraction | PASS |
| No false positives on short strings | PASS |

**The greedy regex bug** (`_RE_TOTAL_YINGSHOU` capturing only the numeric tail): fixed by moving Format C out of the general loop and applying `re.findall(r"[\d,]+\.\d{2}", m.group(0))[-1]`. Validated with synthetic text matching real invoice format.

### Module Structure

| Check | Result |
|---|---|
| `settlement_retrieval.py` imports cleanly | PASS |
| `settlement_markdown_notes.py` imports cleanly | PASS |
| `settlement_query.py` argparse structure | PASS |
| `settlement_generate_notes.py` argparse structure | PASS |
| `knowledge_pool_db_smoke_test.py` syntax | PASS |

### DB Layer (`services/knowledge_pool/db.py`)

| Check | Result |
|---|---|
| URL resolution priority (PGURL > DB_URL > DATABASE_URL > MARKETDATA_DB_URL) | PASS |
| Informative error when no URL set | PASS |
| SSL hint in OperationalError on timeout | PASS (new) |
| Env key logged at DEBUG level | PASS (new) |

---

## 2. Pending Live Validation (requires RDS access)

These items require the network path to RDS to be opened first (see §3).

### 2A. Connection and SSL

```bash
python scripts/knowledge_pool_db_smoke_test.py --verbose
```

Expected output:
```
[1] Connection
  Using PGURL
  [PASS] psycopg2.connect
  [PASS] server version  (PostgreSQL 15.x or 18.x)
  [PASS] SSL active
```

Failure mode: `Connection timed out` → `rds-sg` inbound rule not added yet.

### 2B. Schema and Table Presence

Verify all DDL has been applied:

```bash
psql "$PGURL" -c "\dn"          # list schemas
psql "$PGURL" -c "\dt staging.*"
psql "$PGURL" -c "\dt core.*"
psql "$PGURL" -c "\dt ops.*"
```

Expected schemas: `staging`, `core`, `ops`, `marketdata`, `public`.

If any schema is missing, apply the relevant DDL file (see runbook §DDL).

### 2C. Settlement Ingest Validation

After the Format C regex fix, re-ingest 2025 settlement docs:

```bash
python scripts/settlement_ingest.py --year 2025 --force
```

Expected: documents previously showing 0 facts (Format C / 上网电量结算单) now produce facts including `total_amount` and `energy_mwh`.

Verify:

```bash
# Check for zero-fact documents that are now parsed
python scripts/settlement_query.py docs --status parsed
# Should show page_count > 0 for all docs

# Check facts for known Format C assets (e.g. suyou)
python scripts/settlement_query.py facts --asset suyou --year 2025 --fact-type total_amount
```

### 2D. FTS Validation

```bash
python scripts/settlement_query.py search "市场上网电费" --limit 5
python scripts/settlement_query.py search "调频" --asset suyou --limit 5
python scripts/spot_market_query.py search "电价" --limit 5   # if spot pool is ingested
```

### 2E. Notes Generation

```bash
python scripts/settlement_generate_notes.py --dry-run   # list what will be generated
python scripts/settlement_generate_notes.py              # generate all notes
ls knowledge/settlement/                                  # verify output
```

### 2F. Ops Tables Seeded

```bash
psql "$PGURL" -c "SELECT COUNT(*) FROM ops.ingestion_expected_freshness WHERE active = TRUE;"
# Expected: 11 rows (seeded by ingestion_control.sql)
```

---

## 3. Network Requirements

### Current State

| Actor | Can Reach RDS? | Reason |
|---|---|---|
| ECS tasks (all services) | YES | In `ecs_tasks-sg`; `rds-sg` allows inbound from it |
| Developer laptop | NO | IP not in `rds-sg` |
| Tailscale jump host (172.31.30.155) | NO | In VPC but not in `ecs_tasks-sg` |
| Lambda / other AWS services | NO | No other SG rules |

### Required Change: Open Developer Access

Add to `rds-sg` inbound rules (AWS Console → EC2 → Security Groups → `rds-sg`):

| Type | Protocol | Port | Source | Description |
|---|---|---|---|---|
| Custom TCP | TCP | 5432 | `<developer-IP>/32` | Developer direct access |
| Custom TCP | TCP | 5432 | `172.31.30.155/32` | Tailscale VPC host |

Or via Terraform (add to `rds-sg` in `main.tf`):

```hcl
ingress {
  from_port   = 5432
  to_port     = 5432
  protocol    = "tcp"
  cidr_blocks = ["<developer-IP>/32", "172.31.30.155/32"]
  description = "Developer access"
}
```

> This change does NOT affect ECS services or any existing traffic. It only opens an additional inbound path.

### Connection Verification (after SG rule added)

```bash
# Quick connectivity test (no psql needed):
timeout 5 bash -c "echo > /dev/tcp/bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com/5432" \
  && echo "PORT OPEN" || echo "TIMEOUT — SG rule not yet active"

# Full connection test:
python scripts/knowledge_pool_db_smoke_test.py
```

---

## 4. Local Development — Keeping It Working

### Current Local DB Setup

`apps/spot-agent/.env` points at a local Postgres on port 5433:
```
DB_URL=postgresql://postgres:root@127.0.0.1:5433/marketdata
```

This local DB continues to work for `spot-agent` development without changes.

### Running Knowledge Pool Scripts Locally (against RDS)

Scripts in `scripts/` use `services/knowledge_pool/db.py` which resolves PGURL from:
1. `PGURL` env var (highest priority)
2. `DB_URL` env var
3. `DATABASE_URL` env var
4. `MARKETDATA_DB_URL` env var
5. `.env` file search: `repo_root/.env` → `config/.env` → `apps/spot-agent/.env`

**Recommended local setup:**

```bash
# One-time: source the shared RDS config before running knowledge pool scripts
source config/.env
# Then run any knowledge pool script — it will use RDS PGURL
```

**Or create a personal `.env` at repo root (not committed):**

```bash
# bess-platform/.env  (add to .gitignore if not already there)
PGURL=postgresql://postgres:!BESSmap2026@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require
```

This file takes highest priority in the fallback chain and will not interfere with spot-agent (which loads `apps/spot-agent/.env` directly).

### Running Spot Market Scripts (against RDS)

`spot_market_ingest.py` falls through to `apps/spot-agent/.env` (local DB) if PGURL is not set. Always set explicitly:

```bash
PGURL="postgresql://postgres:...@...rds.amazonaws.com:5432/marketdata?sslmode=require" \
  python scripts/spot_market_ingest.py
```

### Local DB vs RDS — Side-by-Side

| Use case | Connection | How to set |
|---|---|---|
| spot-agent AI queries | Local (127.0.0.1:5433) | No change needed — `apps/spot-agent/.env` |
| Knowledge pool ingest | RDS | `source config/.env` or `export PGURL=...` |
| Settlement query CLI | RDS | `source config/.env` |
| Smoke test | RDS | `source config/.env` |
| ECS tasks (prod) | RDS | Injected by task definition — no action |

---

## 5. Handoff Checklist

Before declaring migration complete:

- [ ] `rds-sg` inbound rule added for developer IP(s)
- [ ] `python scripts/knowledge_pool_db_smoke_test.py` exits 0
- [ ] All DDL files applied (`\dt staging.*` shows all expected tables)
- [ ] `settlement_ingest.py --year 2025 --force` run — no zero-fact docs
- [ ] `settlement_generate_notes.py` run — `knowledge/settlement/` populated
- [ ] `settlement_query.py totals --year 2025` returns rows for all assets
- [ ] `settlement_query.py recon --flagged` checked (no unexpected flags)
- [ ] ECS services confirmed still working (no disruption from SG change)
- [ ] `ops.ingestion_expected_freshness` seeded (11 rows)
- [ ] `ops.ingestion_gap_queue` checked for pending gaps

---

## 6. Future Improvements (deferred)

| Item | Priority | Notes |
|---|---|---|
| Move credentials to Secrets Manager | Medium | Currently plaintext in task defs and config/.env |
| Enable RDS IAM auth | Low | Requires RDS parameter change + IAM policy |
| Bastion host or VPN for developer access | Low | Current SG-rule approach is simpler |
| Upgrade `db.t4g.micro` | Low | Monitor connections; upgrade if needed |
| PostgreSQL minor version pinning | Low | Currently tracking RDS latest minor |
