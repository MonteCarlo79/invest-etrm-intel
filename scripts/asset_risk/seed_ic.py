"""Seed the investment-committee case (rm_ic_assumptions) from IC workbooks. Dry-run by default.

    PGURL=postgresql://... python scripts/asset_risk/seed_ic.py FILE.xlsx [FILE2.xlsx]            # dry-run
    PGURL=postgresql://... python scripts/asset_risk/seed_ic.py FILE.xlsx [FILE2.xlsx] --apply    # write

Files are parsed in the order given; a later file replaces an earlier one for the same
asset_code (save_ic replaces per asset). Dry-run parses and prints only; it needs no DB.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from libs.asset_risk.ic_case import ensure_table, parse_ic_workbook, save_ic  # noqa: E402


def run(files: list[str], engine=None, apply: bool = False, out=print) -> int:
    frames = []
    for f in files:
        df, notes = parse_ic_workbook(f, Path(f).name)
        out(f"-- {Path(f).name}: {df['asset_code'].nunique()} asset(s) {sorted(df['asset_code'].unique())}")
        for n in notes:
            out(f"   note: {n}")
        if not df.empty:
            frames.append(df)
    if not frames:
        out("ABORT: nothing parsed.")
        return 1
    if not apply:
        out("-- dry-run: nothing written (use --apply)")
        return 0
    for df in frames:
        ensure_table(engine)
        save_ic(engine, df)
    out("-- applied")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("files", nargs="+")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args(argv)
    engine = None
    if args.apply:
        pgurl = os.environ.get("PGURL")
        if not pgurl:
            print("PGURL not set", file=sys.stderr)
            return 2
        from sqlalchemy import create_engine
        engine = create_engine(pgurl)
    return run(args.files, engine, args.apply)


if __name__ == "__main__":
    sys.exit(main())
