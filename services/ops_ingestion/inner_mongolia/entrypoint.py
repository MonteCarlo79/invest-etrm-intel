"""
services/ops_ingestion/inner_mongolia/entrypoint.py

CLI entrypoint for Inner Mongolia BESS operations Excel ingestion.

Invocation
----------
Recommended: run as a module from the repo root so that Python package
resolution works correctly:

  py -m services.ops_ingestion.inner_mongolia.entrypoint --dir data/operations/bess/inner-mongolia/2026

Alternatively, direct script execution also works (the script adjusts sys.path
automatically):

  py services/ops_ingestion/inner_mongolia/entrypoint.py --dir data/operations/bess/inner-mongolia/2026

Usage examples
--------------
  # Dry run (parse + match only, no DB writes)
  py -m services.ops_ingestion.inner_mongolia.entrypoint \\
     --dir data/operations/bess/inner-mongolia/2026 --dry-run

  # Recursively scan all year sub-folders
  py -m services.ops_ingestion.inner_mongolia.entrypoint \\
     --dir data/operations/bess/inner-mongolia --recursive --dry-run

  # Single file
  py -m services.ops_ingestion.inner_mongolia.entrypoint \\
     --file "data/operations/bess/inner-mongolia/2026/【2月10日】内蒙储能电站运营统计.xlsx"

  # Force reprocess even if same hash
  py -m services.ops_ingestion.inner_mongolia.entrypoint \\
     --file <path> --force

  # With price verification
  py -m services.ops_ingestion.inner_mongolia.entrypoint \\
     --dir data/operations/bess/inner-mongolia/2026 --verify-prices

Flags
-----
  --dir DIR         Directory to scan for *.xlsx files
  --recursive       Scan subdirectories of --dir
  --file FILE       Single file path
  --dry-run         Parse + match only; no DB writes; prints summary
  --force           Reprocess even if file_hash already in registry
  --year INT        Override year for all files (only when path has no /YYYY/ folder
                    AND filename has no ISO date)
  --verify-prices   Fetch cleared prices from md_id_cleared_energy and compute match scores
  --pgurl URL       Database connection URL (overrides all environment variables)

Database URL resolution (first match wins)
------------------------------------------
  1. --pgurl <url>
  2. PGURL environment variable
  3. DB_DSN environment variable
  4. DATABASE_URL environment variable

  URL format: postgresql://user:pass@host:5432/dbname

PowerShell quick-start
----------------------
  # Set connection string for the session
  $env:PGURL = "postgresql://user:pass@host:5432/dbname"

  # Dry run (no DB writes)
  py -m services.ops_ingestion.inner_mongolia.entrypoint `
     --dir data/operations/bess/inner-mongolia/2026 --dry-run

  # Single-file ingest with price verification
  py -m services.ops_ingestion.inner_mongolia.entrypoint `
     --file "data/operations/bess/inner-mongolia/2026/【4月17日】内蒙储能电站运营统计.xlsx" `
     --verify-prices

  # Full directory ingest
  py -m services.ops_ingestion.inner_mongolia.entrypoint `
     --dir data/operations/bess/inner-mongolia/2026 --verify-prices

  # Pass URL directly (no env var needed)
  py -m services.ops_ingestion.inner_mongolia.entrypoint `
     --dir data/operations/bess/inner-mongolia/2026 `
     --pgurl "postgresql://user:pass@host:5432/dbname"
"""
from __future__ import annotations

import argparse
import glob
import logging
import os
import re
import sys

# ---------------------------------------------------------------------------
# sys.path fix — allows direct execution (`py entrypoint.py`) in addition to
# module execution (`py -m services.ops_ingestion.inner_mongolia.entrypoint`).
# When run as a script, __package__ is None and relative imports fail.
# We insert the repo root (3 levels up from this file) if it's not already
# on sys.path so that `from services.ops_ingestion...` imports resolve.
# ---------------------------------------------------------------------------
_repo_root = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '..'))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description='Ingest Inner Mongolia BESS daily operations Excel files into marketdata schema.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument('--file', metavar='FILE', help='Single .xlsx file to ingest')
    src.add_argument('--dir', metavar='DIR', help='Directory containing .xlsx files')

    p.add_argument('--recursive', action='store_true',
                   help='Recursively scan subdirectories of --dir')
    p.add_argument('--dry-run', action='store_true',
                   help='Parse and match only; no DB writes')
    p.add_argument('--force', action='store_true',
                   help='Reprocess even if file_hash already in registry')
    p.add_argument('--year', type=int, metavar='YEAR',
                   help='Override year for files that have no /YYYY/ folder and no ISO date in name')
    p.add_argument('--verify-prices', action='store_true',
                   help='Verify nodal prices against md_id_cleared_energy')
    p.add_argument('--pgurl', metavar='URL',
                   help='PostgreSQL connection string (overrides PGURL env var)')
    return p


def _collect_files(args: argparse.Namespace):
    """Return sorted list of .xlsx paths based on --file / --dir / --recursive."""
    if args.file:
        if not os.path.isfile(args.file):
            log.error("File not found: %s", args.file)
            sys.exit(1)
        return [args.file]

    # --dir mode
    if not os.path.isdir(args.dir):
        log.error("Directory not found: %s", args.dir)
        sys.exit(1)

    if args.recursive:
        pattern = os.path.join(args.dir, '**', '*.xlsx')
        files = glob.glob(pattern, recursive=True)
    else:
        pattern = os.path.join(args.dir, '*.xlsx')
        files = glob.glob(pattern)

    # Filter out temp files (Excel lock files start with ~$)
    files = [f for f in files if not os.path.basename(f).startswith('~$')]
    return sorted(files)


def _infer_year_hint(path: str, cli_year: int | None) -> int | None:
    """
    Determine the year_hint to pass to ingest_file().
    Priority: CLI --year > /YYYY/ path component.
    (date_parser itself handles ISO date in filename as highest priority.)
    """
    if cli_year is not None:
        return cli_year
    # Extract from path (same logic as date_parser._extract_year_from_path)
    normalised = path.replace('\\', '/')
    m = re.search(r'/(\d{4})/', normalised)
    if m:
        year = int(m.group(1))
        if 2000 <= year <= 2100:
            return year
    return None


_URL_ENV_VARS = ('PGURL', 'DB_DSN', 'DATABASE_URL')


def _resolve_db_url(cli_pgurl: str | None) -> str | None:
    """
    Resolve the database connection URL.

    Priority (first non-empty value wins):
      1. --pgurl CLI flag
      2. PGURL environment variable
      3. DB_DSN environment variable
      4. DATABASE_URL environment variable
    """
    if cli_pgurl:
        return cli_pgurl
    for var in _URL_ENV_VARS:
        val = os.environ.get(var)
        if val:
            log.debug("Using database URL from %s", var)
            return val
    return None


def _get_engine(pgurl: str | None):
    """Create a SQLAlchemy engine, resolving the URL from CLI flag or environment."""
    from sqlalchemy import create_engine
    url = _resolve_db_url(pgurl)
    if not url:
        log.error(
            "No database URL provided. Accepted sources (first match wins):\n"
            "  1. --pgurl <url>              (CLI flag)\n"
            "  2. $env:PGURL = '<url>'       (PowerShell) / export PGURL=<url> (bash)\n"
            "  3. $env:DB_DSN = '<url>'      (PowerShell) / export DB_DSN=<url>\n"
            "  4. $env:DATABASE_URL = '<url>'\n"
            "URL format: postgresql://user:pass@host:5432/dbname"
        )
        sys.exit(1)
    return create_engine(url, pool_pre_ping=True)


def main(argv=None) -> int:
    args = _build_parser().parse_args(argv)

    files = _collect_files(args)
    if not files:
        log.warning("No .xlsx files found — nothing to ingest.")
        return 0

    log.info("Found %d file(s) to process.", len(files))

    # Engine not needed for dry-run, but create it anyway for schema init
    # (unless truly dry — then skip to avoid requiring PGURL in dry-run)
    engine = None
    if not args.dry_run:
        engine = _get_engine(args.pgurl)
        from services.ops_ingestion.inner_mongolia.writer import ensure_tables
        ensure_tables(engine)

    from services.ops_ingestion.inner_mongolia.writer import ingest_file

    total_success = total_skip = total_fail = 0

    for path in files:
        year_hint = _infer_year_hint(path, args.year)
        try:
            result = ingest_file(
                path=path,
                engine=engine,
                force=args.force,
                dry_run=args.dry_run,
                year_hint=year_hint,
                verify_prices_flag=args.verify_prices,
            )
        except Exception as exc:
            log.exception("Unexpected error processing %s: %s", path, exc)
            total_fail += 1
            continue

        if result.status == 'success':
            total_success += 1
        elif result.status in ('skipped_duplicate', 'skipped_superseded'):
            total_skip += 1
        elif result.status == 'dry_run':
            total_success += 1   # count dry-run as processed for summary
        else:
            total_fail += 1
            log.error("Failed: %s — %s", os.path.basename(path), result.notes)

    log.info(
        "Done. success=%d  skipped=%d  failed=%d  total=%d",
        total_success, total_skip, total_fail, len(files),
    )
    return 0 if total_fail == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
