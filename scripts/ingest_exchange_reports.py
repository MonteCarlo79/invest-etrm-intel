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

    # Backfill metrics only (for already-ingested files with no metrics row):
    py scripts/ingest_exchange_reports.py --extract-metrics-only
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


def _run_extract_metrics_only(folder: Path, pg_url: str, args) -> None:
    """
    For every file in `folder` that is already registered in
    staging.exchange_monthly_reports but has no row in
    staging.exchange_monthly_metrics, extract and upsert metrics via Claude.
    """
    import hashlib
    import datetime
    import psycopg2

    from services.exchange_reports.ingestor import infer_province, infer_report_month, infer_report_type
    from services.exchange_reports.metrics_extractor import extract_and_store, init_metrics_table

    month_filter = None
    if args.month:
        try:
            _y, _m = args.month.split("-")
            month_filter = datetime.date(int(_y), int(_m), 1)
        except ValueError:
            logger.error("Invalid --month format: %s (expected YYYY-MM)", args.month)
            sys.exit(1)

    # Accept any supported provider: DeepSeek > Bedrock > Anthropic
    api_key = (
        os.environ.get("DEEPSEEK_API_KEY", "").strip()
        or os.environ.get("ANTHROPIC_API_KEY", "").strip()
    )
    bedrock_region = os.environ.get("BEDROCK_REGION", "").strip()
    if not api_key and not bedrock_region:
        logger.error("No LLM API key set — set DEEPSEEK_API_KEY, BEDROCK_REGION, or ANTHROPIC_API_KEY")
        sys.exit(1)

    init_metrics_table(pg_url)

    conn = psycopg2.connect(pg_url)
    try:
        # Fetch all ingested reports that have no metrics row yet
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.id, r.file_hash, r.province, r.report_month, r.report_type, r.file_name
                FROM staging.exchange_monthly_reports r
                LEFT JOIN staging.exchange_monthly_metrics m
                    ON m.exchange_report_id = r.id
                WHERE r.ingest_status = 'ingested'
                  AND m.id IS NULL
                  AND (%s IS NULL OR r.province = %s)
                  AND (%s IS NULL OR r.report_month = %s)
                ORDER BY r.province, r.report_month
                """,
                (args.province, args.province, month_filter, month_filter),
            )
            pending = cur.fetchall()
    finally:
        conn.close()

    if not pending:
        print("No ingested reports are missing metrics. Nothing to do.")
        return

    print(f"\nMetrics backfill — {len(pending)} reports need extraction\n")

    # Build a hash → path index from disk so we can find files
    hash_to_path: dict[str, Path] = {}
    for path in sorted(folder.rglob("*")):
        if path.suffix.lower() not in (".pdf", ".doc", ".docx"):
            continue
        if path.name.startswith("~$"):
            continue
        if args.province:
            prov = infer_province(path)
            if prov != args.province:
                continue
        try:
            h = hashlib.sha256(path.read_bytes()).hexdigest()
            hash_to_path[h] = path
        except Exception:
            pass

    extracted = skipped = failed = 0
    for report_id, file_hash, province, report_month, report_type, file_name in pending:
        path = hash_to_path.get(file_hash)
        if path is None:
            logger.warning("File not found on disk: %s (%s %s) — skipping", file_name, province, report_month)
            skipped += 1
            continue

        try:
            from services.exchange_reports.ingestor import extract_pages
            file_bytes = path.read_bytes()
            vision_key = os.environ.get("ANTHROPIC_API_KEY", "").strip() or None
            textract_region = os.environ.get("TEXTRACT_REGION") or os.environ.get("BEDROCK_REGION") or "ap-southeast-1"
            pages = extract_pages(
                file_bytes, path.name,
                vision_api_key=vision_key,
                textract_region=textract_region,
            )
            full_text = "\n".join(text for _, text in pages)

            row_id = extract_and_store(
                full_text=full_text,
                province=province,
                report_month=report_month,
                report_type=report_type,
                exchange_report_id=report_id,
                api_key=api_key,
                pg_url=pg_url,
            )
            if row_id:
                logger.info("  [OK] %s %s → metrics row %s", province, report_month, row_id)
                extracted += 1
            else:
                logger.warning("  [SKIP] %s %s — extraction returned None", province, report_month)
                skipped += 1
        except Exception as exc:
            logger.error("  [FAIL] %s %s — %s", province, report_month, exc)
            failed += 1

    print(f"\n{'='*50}")
    print(f"  Metrics Backfill Summary")
    print(f"{'='*50}")
    print(f"  Extracted : {extracted}")
    print(f"  Skipped   : {skipped}")
    print(f"  Failed    : {failed}")
    print(f"{'='*50}\n")


def main():
    parser = argparse.ArgumentParser(description="Ingest exchange monthly reports into DB + KB")
    parser.add_argument("--folder", default=str(_DEFAULT_FOLDER),
                        help=f"Root folder (default: {_DEFAULT_FOLDER})")
    parser.add_argument("--province", help="Only ingest one province (e.g. 上海)")
    parser.add_argument("--month", help="With --extract-metrics-only: only process reports for one month (YYYY-MM, e.g. 2026-06)")
    parser.add_argument("--dry-run", action="store_true", help="Print files without ingesting")
    parser.add_argument("--extract-metrics-only", action="store_true",
                        help="Re-extract metrics for already-ingested files that have no metrics row")
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

    if args.extract_metrics_only:
        _run_extract_metrics_only(folder, pg_url, args)
        return

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

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY not set — text will be ingested to KB but structured metrics will NOT be extracted.")

    # Live ingest
    if args.province:
        # Find all subfolder(s) matching the province
        sub_results = []
        for sub in folder.iterdir():
            if not sub.is_dir():
                continue
            if args.province in sub.name or sub.name == args.province + "月报":
                logger.info("Ingesting province folder: %s", sub)
                sub_results.extend(ingest_folder(sub, pg_url=pg_url, anthropic_api_key=api_key))
        results = sub_results
    else:
        results = ingest_folder(folder, pg_url=pg_url, anthropic_api_key=api_key)

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
            period = r.get('report_month') or r.get('year') or 'annual'
            print(f"  [OK] {r.get('province')} {period} -- {Path(r['file']).name}")

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
