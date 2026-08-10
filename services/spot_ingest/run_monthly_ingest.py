"""Backfill national spot monthly reports into spot_monthly_* tables.

Usage:
    python -m services.spot_ingest.run_monthly_ingest --dir /path/to/folder [--dry-run]

--dry-run parses and validates but skips the DB write (prints warnings).
KB backfill is separate: scripts/ingest_knowledge_bulk.py.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_repo_root))

try:
    from dotenv import load_dotenv
    load_dotenv(_repo_root / "config" / ".env", override=False)
except ImportError:
    pass

from services.spot_ingest.monthly_report import (  # noqa: E402
    infer_report_month, ingest_monthly_report, is_spot_monthly_pdf,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def find_monthly_pdfs(folder: Path) -> list[Path]:
    """PDFs in folder matching the monthly pattern with an inferable month."""
    return [
        p for p in sorted(folder.glob("*.pdf"))
        if is_spot_monthly_pdf(p.name) and infer_report_month(p.name) is not None
    ]


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill spot monthly report PDFs")
    ap.add_argument("--dir", required=True, help="Folder containing monthly report PDFs")
    ap.add_argument("--dry-run", action="store_true", help="Parse + validate only, no DB write")
    args = ap.parse_args()

    files = find_monthly_pdfs(Path(args.dir))
    if not files:
        print("No matching monthly report PDFs found.")
        return 0

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    ok = failed = 0
    for path in files:
        try:
            if args.dry_run:
                from services.spot_ingest.monthly_report import (
                    extract_monthly_json, extract_pages_text, validate_monthly_data,
                )
                text = extract_pages_text(path)
                data = extract_monthly_json(text, infer_report_month(path.name), api_key)
                warnings = validate_monthly_data(data)
                print(f"DRY  {path.name}: {len(data['provinces'])} provinces, "
                      f"{len(warnings)} warnings")
                for w in warnings:
                    print(f"     ⚠️ {w}")
            else:
                result = ingest_monthly_report(path.name, path.read_bytes(), api_key)
                print(f"OK   {path.name}: {result['month']}, "
                      f"{result['n_provinces']} provinces, "
                      f"{len(result['warnings'])} warnings")
            ok += 1
        except Exception as exc:
            failed += 1
            print(f"FAIL {path.name}: {exc}")
    print(f"\nDone: {ok} ok, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
