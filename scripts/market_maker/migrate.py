"""Apply the market-maker additive migration. Dry-run by default.

    PGURL=postgresql://... python scripts/market_maker/migrate.py            # dry-run
    PGURL=postgresql://... python scripts/market_maker/migrate.py --apply    # one transaction
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

SQL_PATH = Path(__file__).resolve().parents[2] / "db" / "ddl" / "marketdata" / "market_maker_migration.sql"

CONSTRAINT_LOOKUP_SQL = (
    "SELECT conname FROM pg_constraint"
    " WHERE conrelid = 'marketdata.rm_forward_curves'::regclass AND contype = 'c'"
    " AND pg_get_constraintdef(oid) ILIKE '%source%'"
)

LOCK_TIMEOUT_SQL = "SET LOCAL lock_timeout = '5s'"

_DROP_RE = re.compile(r"DROP CONSTRAINT IF EXISTS \w+")


def build_statements(source_check_name: str | None = None) -> list[str]:
    """Statements from the SQL file; the source-CHECK DROP uses the real name if given."""
    text = "\n".join(l for l in SQL_PATH.read_text().splitlines() if not l.lstrip().startswith("--"))
    stmts = [s.strip() for s in text.split(";") if s.strip()]
    if source_check_name:
        stmts = [_DROP_RE.sub(f"DROP CONSTRAINT IF EXISTS {source_check_name}", s)
                 if "DROP CONSTRAINT" in s else s for s in stmts]
    return stmts


def run(engine, apply: bool, out=print) -> int:
    from sqlalchemy import text
    with engine.connect() as conn:
        names = [r[0] for r in conn.execute(text(CONSTRAINT_LOOKUP_SQL))]
        out(f"-- check: existing source CHECK constraint(s) = {names or 'none'}")
        if len(names) > 1:
            out("ABORT: more than one CHECK constraint mentions source; inspect manually.")
            return 1
        stmts = build_statements(names[0] if names else None)
    out(LOCK_TIMEOUT_SQL + ";  -- first statement inside the apply transaction")
    for s in stmts:
        out(s + ";")
    if not apply:
        out("-- dry-run: nothing executed (use --apply)")
        return 0
    with engine.begin() as conn:
        if conn.dialect.name == "postgresql":
            conn.execute(text(LOCK_TIMEOUT_SQL))  # fail fast instead of queueing behind live readers
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
