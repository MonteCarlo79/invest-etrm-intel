#!/usr/bin/env python3
"""
Knowledge Pool — DB smoke test.

Verifies:
  1. DB connection succeeds (correct PGURL, SSL, network path)
  2. Required schemas exist (staging, core, ops)
  3. Core tables are present for each knowledge pool
  4. Basic row-count sanity checks (tables are not empty where data is expected)
  5. FTS index smoke (one search per pool if rows exist)

Usage:
    # With config/.env loaded automatically (fallback chain):
    python scripts/knowledge_pool_db_smoke_test.py

    # Explicit PGURL (recommended for CI / first-time setup):
    PGURL="postgresql://postgres:...@...rds.amazonaws.com:5432/marketdata?sslmode=require" \
        python scripts/knowledge_pool_db_smoke_test.py

    # Verbose mode:
    python scripts/knowledge_pool_db_smoke_test.py --verbose

Exit codes:
    0 — all checks passed
    1 — one or more checks failed (details printed to stdout)

Requires PGURL (or DB_URL / DATABASE_URL) in environment or .env file.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

try:
    from dotenv import load_dotenv
    for _env_candidate in [
        _REPO / ".env",
        _REPO / "config" / ".env",
        _REPO / "apps" / "spot-agent" / ".env",
    ]:
        if _env_candidate.exists():
            load_dotenv(_env_candidate)
            break
except ImportError:
    pass

import os
import traceback

import psycopg2

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
SKIP = "\033[33mSKIP\033[0m"
WARN = "\033[33mWARN\033[0m"


def _get_url() -> str:
    for key in ["PGURL", "DB_URL", "DATABASE_URL", "MARKETDATA_DB_URL"]:
        val = os.getenv(key)
        if val:
            print(f"  Using {key}")
            return val
    raise RuntimeError(
        "No DB URL found. Set PGURL or source config/.env before running."
    )


class SmokeTest:
    def __init__(self, verbose: bool):
        self.verbose = verbose
        self.passed = 0
        self.failed = 0
        self.conn = None

    def _ok(self, label: str, detail: str = ""):
        self.passed += 1
        suffix = f"  ({detail})" if detail and self.verbose else ""
        print(f"  [{PASS}] {label}{suffix}")

    def _fail(self, label: str, detail: str = ""):
        self.failed += 1
        suffix = f"\n         {detail}" if detail else ""
        print(f"  [{FAIL}] {label}{suffix}")

    def _skip(self, label: str, reason: str = ""):
        suffix = f" — {reason}" if reason else ""
        print(f"  [{SKIP}] {label}{suffix}")

    # ── Section 1: Connection ────────────────────────────────────────────────

    def check_connection(self) -> bool:
        print("\n[1] Connection")
        try:
            url = _get_url()
        except RuntimeError as e:
            self._fail("resolve PGURL", str(e))
            return False

        try:
            self.conn = psycopg2.connect(
                url,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
                connect_timeout=10,
            )
            self._ok("psycopg2.connect")
        except psycopg2.OperationalError as e:
            msg = str(e).strip()
            if "Connection timed out" in msg or "could not connect" in msg.lower():
                self._fail(
                    "psycopg2.connect",
                    f"{msg}\n"
                    "         Hint: RDS security group only allows port 5432 from ecs_tasks-sg.\n"
                    "         Add your IP (or 172.31.30.155 for Tailscale) to rds-sg inbound rules.",
                )
            else:
                self._fail("psycopg2.connect", msg)
            return False

        # Server version
        with self.conn.cursor() as cur:
            cur.execute("SELECT version()")
            ver = cur.fetchone()[0]
            self._ok("server version", ver.split(" ")[1] if " " in ver else ver)

        # SSL status
        with self.conn.cursor() as cur:
            cur.execute("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
            row = cur.fetchone()
            if row and row[0]:
                self._ok("SSL active")
            else:
                self._fail("SSL active", "Connection is NOT encrypted — check sslmode=require in PGURL")

        return True

    # ── Section 2: Schema presence ───────────────────────────────────────────

    def check_schemas(self):
        print("\n[2] Schemas")
        required = ["staging", "core", "ops"]
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ANY(%s)",
                (required,),
            )
            found = {r[0] for r in cur.fetchall()}
        for s in required:
            if s in found:
                self._ok(f"schema {s} exists")
            else:
                self._fail(f"schema {s} exists", f"Run: psql $PGURL -f db/ddl/... to create {s} schema")

    # ── Section 3: Table presence ────────────────────────────────────────────

    def check_tables(self):
        print("\n[3] Tables")
        # (schema, table, pool)
        expected = [
            # Spot market knowledge pool
            ("staging", "spot_report_documents",  "spot"),
            ("staging", "spot_report_chunks",     "spot"),
            ("staging", "spot_report_facts",      "spot"),
            ("staging", "spot_report_notes",      "spot"),
            # Settlement knowledge pool
            ("staging", "settlement_report_documents",   "settlement"),
            ("staging", "settlement_report_chunks",      "settlement"),
            ("staging", "settlement_report_facts",       "settlement"),
            ("staging", "settlement_reconciliation",     "settlement"),
            ("staging", "settlement_report_notes",       "settlement"),
            # Core reference
            ("core",    "asset_alias_map",               "core"),
            # Ops control
            ("ops",     "ingestion_job_runs",             "ops"),
            ("ops",     "ingestion_dataset_status",       "ops"),
            ("ops",     "ingestion_expected_freshness",   "ops"),
            ("ops",     "ingestion_gap_queue",            "ops"),
        ]
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema IN ('staging', 'core', 'ops')
                  AND table_type = 'BASE TABLE'
                """
            )
            found = {(r[0], r[1]) for r in cur.fetchall()}

        for schema, table, pool in expected:
            key = (schema, table)
            label = f"{schema}.{table} [{pool}]"
            if key in found:
                self._ok(label)
            else:
                self._fail(label, f"Table missing — apply DDL for '{pool}' pool")

    # ── Section 4: Row counts ────────────────────────────────────────────────

    def check_row_counts(self):
        print("\n[4] Row Counts")
        checks = [
            ("staging.spot_report_documents", "ingest_status = 'parsed'", "parsed spot docs"),
            ("staging.settlement_report_documents",  "ingest_status = 'parsed'", "parsed settlement docs"),
            ("staging.spot_report_facts",            "TRUE",                     "spot facts total"),
            ("staging.settlement_report_facts",      "TRUE",                     "settlement facts total"),
            ("core.asset_alias_map",                 "TRUE",                     "asset alias rows"),
        ]
        for table, where, label in checks:
            schema, tname = table.split(".", 1)
            with self.conn.cursor() as cur:
                # Check table exists first
                cur.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
                    (schema, tname),
                )
                if not cur.fetchone():
                    self._skip(f"{label} ({table})", "table absent")
                    continue
                cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}")
                n = cur.fetchone()[0]
            if n > 0:
                self._ok(f"{label}", f"{n:,} rows")
            else:
                # Not a hard failure — data may not be ingested yet
                print(f"  [{WARN}] {label} — 0 rows (expected > 0 after ingest)")

    # ── Section 5: FTS smoke ─────────────────────────────────────────────────

    def check_fts(self):
        print("\n[5] Full-Text Search Smoke")

        # Spot pool
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema='staging' AND table_name='spot_report_chunks'"
            )
            if not cur.fetchone():
                self._skip("spot chunk FTS", "table absent")
            else:
                cur.execute("SELECT COUNT(*) FROM staging.spot_report_chunks")
                n = cur.fetchone()[0]
                if n == 0:
                    self._skip("spot chunk FTS", "no rows")
                else:
                    cur.execute(
                        "SELECT COUNT(*) FROM staging.spot_report_chunks "
                        "WHERE to_tsvector('simple', chunk_text) @@ plainto_tsquery('simple', '电价')"
                    )
                    hits = cur.fetchone()[0]
                    self._ok("spot FTS (电价)", f"{hits} hits in {n:,} chunks")

        # Settlement pool
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema='staging' AND table_name='settlement_report_chunks'"
            )
            if not cur.fetchone():
                self._skip("settlement chunk FTS", "table absent")
            else:
                cur.execute("SELECT COUNT(*) FROM staging.settlement_report_chunks")
                n = cur.fetchone()[0]
                if n == 0:
                    self._skip("settlement chunk FTS", "no rows")
                else:
                    cur.execute(
                        "SELECT COUNT(*) FROM staging.settlement_report_chunks "
                        "WHERE to_tsvector('simple', chunk_text) @@ plainto_tsquery('simple', '电费')"
                    )
                    hits = cur.fetchone()[0]
                    self._ok("settlement FTS (电费)", f"{hits} hits in {n:,} chunks")

    # ── Section 6: Freshness config ──────────────────────────────────────────

    def check_freshness_config(self):
        print("\n[6] Ingestion Freshness Config")
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema='ops' AND table_name='ingestion_expected_freshness'"
            )
            if not cur.fetchone():
                self._skip("freshness config rows", "ops.ingestion_expected_freshness absent")
                return
            cur.execute("SELECT COUNT(*) FROM ops.ingestion_expected_freshness WHERE active = TRUE")
            n = cur.fetchone()[0]
        if n > 0:
            self._ok("freshness config", f"{n} active datasets configured")
        else:
            print(f"  [{WARN}] freshness config — 0 active rows (seed with db/ddl/ops/ingestion_control.sql)")

    # ── Run all ──────────────────────────────────────────────────────────────

    def run(self) -> int:
        print("=" * 60)
        print("Knowledge Pool — DB Smoke Test")
        print("=" * 60)

        connected = self.check_connection()
        if not connected:
            print(f"\n{'=' * 60}")
            print(f"Cannot proceed: connection failed.")
            print(f"{'=' * 60}")
            return 1

        try:
            self.check_schemas()
            self.check_tables()
            self.check_row_counts()
            self.check_fts()
            self.check_freshness_config()
        except Exception:
            self._fail("unexpected error", traceback.format_exc())
        finally:
            if self.conn:
                try:
                    self.conn.close()
                except Exception:
                    pass

        print(f"\n{'=' * 60}")
        print(f"Results: {self.passed} passed, {self.failed} failed")
        print(f"{'=' * 60}")
        return 0 if self.failed == 0 else 1


def main():
    parser = argparse.ArgumentParser(description="Knowledge pool DB smoke test")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Show extra detail for passing checks")
    args = parser.parse_args()
    sys.exit(SmokeTest(verbose=args.verbose).run())


if __name__ == "__main__":
    main()
