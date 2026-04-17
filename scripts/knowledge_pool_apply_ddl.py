#!/usr/bin/env python3
"""
Apply all knowledge pool DDL files against the target database.

Runs DDL in dependency order:
  1. core/asset_alias_map.sql             — asset master + seed
  2. core/asset_monthly_compensation.sql  — compensation rules + seed
  3. core/asset_scenario_availability.sql — scenario availability + seed
  4. core/document_registry.sql           — shared document registry
  5. staging/spot_report_knowledge.sql    — spot market knowledge pool tables
  6. staging/settlement_report_knowledge.sql — settlement knowledge pool tables
  7. ops/mengxi_agent4_reliability.sql    — ops schema + mengxi reliability tables
  8. ops/ingestion_control.sql            — ingestion job runs / gap queue / freshness

All files use CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS — idempotent.
Safe to re-run against an existing database.

Usage:
    # With config/.env (RDS):
    python scripts/knowledge_pool_apply_ddl.py

    # Explicit PGURL:
    PGURL="postgresql://..." python scripts/knowledge_pool_apply_ddl.py

    # Dry run — show files that would be applied without running them:
    python scripts/knowledge_pool_apply_ddl.py --dry-run

    # Specific group only:
    python scripts/knowledge_pool_apply_ddl.py --group core
    python scripts/knowledge_pool_apply_ddl.py --group staging
    python scripts/knowledge_pool_apply_ddl.py --group ops

Requires psql to be on PATH (or set PSQL_PATH env var to the full path).
"""
from __future__ import annotations

import argparse
import os
import subprocess
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

# DDL files in application order.
# Each entry: (group, relative_path, description)
DDL_FILES = [
    ("core",    "db/ddl/core/asset_alias_map.sql",                 "asset alias map table"),
    ("core",    "db/ddl/core/asset_alias_map_seed.sql",            "asset alias seed data"),
    ("core",    "db/ddl/core/asset_monthly_compensation.sql",      "monthly compensation table"),
    ("core",    "db/ddl/core/asset_monthly_compensation_seed.sql", "monthly compensation seed"),
    ("core",    "db/ddl/core/asset_scenario_availability.sql",     "scenario availability table"),
    ("core",    "db/ddl/core/asset_scenario_availability_seed.sql","scenario availability seed"),
    ("core",    "db/ddl/core/document_registry.sql",               "shared document registry"),
    ("raw_data","db/ddl/raw_data/file_registry.sql",               "raw document landing zone"),
    ("raw_data","db/ddl/raw_data/file_manifest.sql",               "raw file manifest"),
    ("staging", "db/ddl/staging/spot_report_knowledge.sql",        "spot market knowledge pool"),
    ("staging", "db/ddl/staging/settlement_report_knowledge.sql",  "settlement knowledge pool"),
    ("ops",     "db/ddl/ops/mengxi_agent4_reliability.sql",        "mengxi reliability tables (creates ops schema)"),
    ("ops",     "db/ddl/ops/ingestion_control.sql",                "ingestion job control tables"),
]


def _get_pgurl() -> str:
    for key in ["PGURL", "DB_URL", "DATABASE_URL", "MARKETDATA_DB_URL"]:
        val = os.getenv(key)
        if val:
            return val
    raise SystemExit(
        "No DB URL found. Set PGURL or source config/.env before running.\n"
        "Example: source config/.env && python scripts/knowledge_pool_apply_ddl.py"
    )


def _get_psql() -> str:
    custom = os.getenv("PSQL_PATH")
    if custom:
        return custom
    # Try common locations on Windows
    for candidate in [
        "/c/Program Files/PostgreSQL/16/bin/psql",
        "/c/Program Files/PostgreSQL/15/bin/psql",
        "/c/Program Files/PostgreSQL/14/bin/psql",
        "psql",  # from PATH
    ]:
        try:
            result = subprocess.run(
                [candidate, "--version"],
                capture_output=True, timeout=5
            )
            if result.returncode == 0:
                return candidate
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue
    raise SystemExit(
        "psql not found. Install PostgreSQL client tools or set PSQL_PATH environment variable.\n"
        "Example: export PSQL_PATH='/c/Program Files/PostgreSQL/15/bin/psql'"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Apply knowledge pool DDL files against the target database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--group", choices=["core", "raw_data", "staging", "ops", "all"], default="all",
        help="DDL group to apply (default: all)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show files that would be applied without running them"
    )
    args = parser.parse_args()

    pgurl = _get_pgurl() if not args.dry_run else "DRY_RUN_URL"
    psql = _get_psql() if not args.dry_run else "psql"

    selected = [
        (group, path, desc)
        for group, path, desc in DDL_FILES
        if args.group == "all" or group == args.group
    ]

    if not selected:
        print(f"No files matched group '{args.group}'")
        return

    print(f"\nKnowledge Pool DDL Apply — {'DRY RUN' if args.dry_run else 'LIVE'}")
    print(f"Target: {'(dry run)' if args.dry_run else pgurl.split('@')[-1] if '@' in pgurl else pgurl}")
    print(f"Files : {len(selected)}\n")

    applied = 0
    skipped = 0
    failed  = 0

    for group, rel_path, desc in selected:
        ddl_path = _REPO / rel_path
        label = f"[{group}] {rel_path}"

        if not ddl_path.exists():
            print(f"  [SKIP] {label}  — file not found")
            skipped += 1
            continue

        if args.dry_run:
            print(f"  [DRY ] {label}  — {desc}")
            applied += 1
            continue

        print(f"  [....] {label}", end="", flush=True)
        result = subprocess.run(
            [psql, pgurl, "-f", str(ddl_path), "-v", "ON_ERROR_STOP=1"],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            print(f"\r  [ OK ] {label}  — {desc}")
            applied += 1
        else:
            print(f"\r  [FAIL] {label}")
            # Show first 10 lines of stderr for context
            for line in result.stderr.strip().splitlines()[:10]:
                print(f"         {line}")
            failed += 1
            # Stop on error — DDL files may have dependencies
            print(f"\nStopped after first failure. Fix the error above and re-run.")
            break

    print(f"\nApplied: {applied}  Skipped: {skipped}  Failed: {failed}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
