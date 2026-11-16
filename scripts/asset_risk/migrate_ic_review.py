"""Apply the IC-case + 复盘 review-store migration. Dry-run by default.

    PGURL=postgresql://... python scripts/asset_risk/migrate_ic_review.py            # dry-run
    PGURL=postgresql://... python scripts/asset_risk/migrate_ic_review.py --apply    # one transaction
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

SQL_PATH = Path(__file__).resolve().parents[2] / "db" / "ddl" / "marketdata" / "ic_review_migration.sql"

LOCK_TIMEOUT_SQL = "SET LOCAL lock_timeout = '5s'"


def build_statements() -> list[str]:
    text_ = "\n".join(l for l in SQL_PATH.read_text().splitlines() if not l.lstrip().startswith("--"))
    return [s.strip() for s in text_.split(";") if s.strip()]


def run(engine, apply: bool, out=print) -> int:
    from sqlalchemy import text
    stmts = build_statements()
    out(LOCK_TIMEOUT_SQL + ";  -- first statement inside the apply transaction")
    for s in stmts:
        out(s + ";")
    if not apply:
        out("-- dry-run: nothing executed (use --apply)")
        return 0
    with engine.begin() as conn:
        if conn.dialect.name == "postgresql":
            conn.execute(text(LOCK_TIMEOUT_SQL))
        for s in stmts:
            conn.execute(text(s))
    out("-- applied")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", default=True)
    g.add_argument("--apply", action="store_true")
    a = ap.parse_args(argv)
    url = os.environ.get("PGURL")
    if not url:
        print("PGURL not set", file=sys.stderr)
        return 2
    from sqlalchemy import create_engine
    return run(create_engine(url), apply=a.apply)


if __name__ == "__main__":
    sys.exit(main())
