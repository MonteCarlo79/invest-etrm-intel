"""Add and seed marketdata.rm_assets.asset_code (idempotent).

Usage: PGURL=postgresql://... python scripts/asset_risk/migrate_asset_code.py [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import create_engine, text  # noqa: E402

from libs.asset_risk.registry import SEED_CODES  # noqa: E402

DDL = "ALTER TABLE marketdata.rm_assets ADD COLUMN IF NOT EXISTS asset_code TEXT UNIQUE"
UPDATE = ("UPDATE marketdata.rm_assets SET asset_code = :code "
          "WHERE name = :name AND asset_code IS NULL")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="print the plan, no writes")
    args = ap.parse_args()
    pgurl = os.environ.get("PGURL")
    if not pgurl:
        print("PGURL not set", file=sys.stderr)
        return 2
    if args.dry_run:
        print(f"[dry-run] {DDL}")
        for name, code in SEED_CODES.items():
            print(f"[dry-run] {UPDATE}  -- name={name!r} code={code!r}")
        return 0
    eng = create_engine(pgurl)
    with eng.begin() as conn:
        conn.execute(text(DDL))
        for name, code in SEED_CODES.items():
            n = conn.execute(text(UPDATE), {"code": code, "name": name}).rowcount
            print(f"{name} -> {code}: {n} row(s) updated")
        rows = conn.execute(text(
            "SELECT name, asset_code FROM marketdata.rm_assets ORDER BY id")).fetchall()
    for name, code in rows:
        print(f"{name}\t{code}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
