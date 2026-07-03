#!/usr/bin/env python
"""
scripts/ingest_exchange_reports.py

Backfill all exchange monthly reports from the local data folder into DB + KB.

Usage:
    # Backfill all provinces:
    py scripts/ingest_exchange_reports.py

    # One province only:
    py scripts/ingest_exchange_reports.py --province 上海

    # Dry run (print what would be ingested):
    py scripts/ingest_exchange_reports.py --dry-run

    # Custom folder:
    py scripts/ingest_exchange_reports.py --folder /path/to/reports
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

_DEFAULT_FOLDER = Path(__file__).parent.parent / "data" / "exchange-monthly-reports"


def main():
    parser = argparse.ArgumentParser(description="Ingest exchange monthly reports into DB + KB")
    parser.add_argument("--folder", default=str(_DEFAULT_FOLDER),
                        help=f"Root folder (default: {_DEFAULT_FOLDER})")
    parser.add_argument("--province", help="Only ingest one province (e.g. 上海)")
    parser.add_argument("--dry-run", action="store_true", help="Print files without ingesting")
    args = parser.parse_args()

    folder = Path(args.folder)
    if not folder.exists():
        logger.error("Folder not found: %s", folder)
        sys.exit(1)

    pg_url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not pg_url and not args.dry_run:
        logger.error("Set PGURL environment variable to connect to the database")
        sys.exit(1)

    from services.exchange_reports.ingestor import (
        ingest_folder, infer_province, infer_report_month, infer_report_type,
    )

    if args.dry_run:
        print(f"\nDRY RUN — scanning {folder}\n")
        for path in sorted(folder.rglob("*")):
            if path.suffix.lower() not in (".pdf", ".doc", ".docx"):
                continue
            if path.name.startswith("~$"):
                continue
            if args.province and infer_province(path) != args.province:
                continue
            prov = infer_province(path)
            mon = infer_report_month(path.name)
            rtype = infer_report_type(path.name)
            status = "OK" if (prov and mon) else "SKIP"
            print(f"  [{status}]  {path.relative_to(folder)}  |  province={prov}  month={mon}  type={rtype}")
        return

    # Live ingest
    if args.province:
        # Find all subfolder(s) matching the province
        sub_results = []
        for sub in folder.iterdir():
            if not sub.is_dir():
                continue
            if args.province in sub.name or sub.name == args.province + "月报":
                logger.info("Ingesting province folder: %s", sub)
                sub_results.extend(ingest_folder(sub, pg_url=pg_url))
        results = sub_results
    else:
        results = ingest_folder(folder, pg_url=pg_url)

    # Summary
    ingested  = [r for r in results if r.get("status") == "ingested"]
    duplicate = [r for r in results if r.get("status") == "duplicate"]
    skipped   = [r for r in results if r.get("status") == "skipped"]
    failed    = [r for r in results if r.get("status") in ("failed", "error")]

    print(f"\n{'='*60}")
    print(f"  Exchange Monthly Reports — Ingest Summary")
    print(f"{'='*60}")
    print(f"  Ingested  : {len(ingested)}")
    print(f"  Duplicate : {len(duplicate)}")
    print(f"  Skipped   : {len(skipped)}")
    print(f"  Failed    : {len(failed)}")
    print(f"{'='*60}")

    if ingested:
        print("\nNewly ingested:")
        for r in ingested:
            print(f"  [OK] {r.get('province')} {r.get('report_month')} -- {Path(r['file']).name}")

    if skipped:
        print("\nSkipped (cannot infer province/month):")
        for r in skipped:
            print(f"  [SKIP] {Path(r['file']).name}  -- {r.get('reason')}")

    if failed:
        print("\nFailed:")
        for r in failed:
            print(f"  [FAIL] {Path(r['file']).name}  -- {r.get('error') or r.get('parse_error')}")


if __name__ == "__main__":
    main()
