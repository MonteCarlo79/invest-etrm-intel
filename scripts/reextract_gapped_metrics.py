"""
scripts/reextract_gapped_metrics.py

Targeted re-extraction for exchange_monthly_metrics rows that have NULL values
for key fields (avg_price, spot_volume, spot_avg_price).

Reads the original file from disk (using improved table extraction), re-runs
the LLM extraction, and upserts the result.

Usage:
    DEEPSEEK_API_KEY=sk-xxx PGURL=postgres://... py scripts/reextract_gapped_metrics.py
    py scripts/reextract_gapped_metrics.py --province 山东
    py scripts/reextract_gapped_metrics.py --dry-run   # show which reports would be processed
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── project root on sys.path ──────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


def main() -> None:
    parser = argparse.ArgumentParser(description="Re-extract gapped exchange report metrics")
    parser.add_argument("--province", help="Limit to specific province")
    parser.add_argument(
        "--folder",
        default=str(REPO_ROOT / "data" / "exchange-monthly-reports"),
        help="Root folder containing PDF/DOCX report files",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be processed without running extraction",
    )
    args = parser.parse_args()

    pg_url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not pg_url:
        logger.error("PGURL not set")
        sys.exit(1)

    api_key = (
        os.environ.get("DEEPSEEK_API_KEY", "").strip()
        or os.environ.get("ANTHROPIC_API_KEY", "").strip()
    )
    bedrock_region = os.environ.get("BEDROCK_REGION", "").strip()
    if not api_key and not bedrock_region:
        logger.error("No LLM key — set DEEPSEEK_API_KEY, BEDROCK_REGION, or ANTHROPIC_API_KEY")
        sys.exit(1)

    # Vision OCR uses Claude Haiku specifically (DeepSeek has no vision support)
    vision_api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip() or None
    # Textract fallback for scanned PDFs (安徽 etc.)
    textract_region = os.environ.get("TEXTRACT_REGION") or os.environ.get("BEDROCK_REGION") or "ap-southeast-1"

    import psycopg2
    from services.exchange_reports.ingestor import extract_pages, infer_province
    from services.exchange_reports.metrics_extractor import extract_and_store

    conn = psycopg2.connect(pg_url)
    try:
        with conn.cursor() as cur:
            # Find metrics rows where at least 2 of the 3 key price/volume fields are NULL
            # (excluding 安徽 which is scanned — would just waste API calls)
            province_filter = "AND m.province = %s" if args.province else ""
            cur.execute(
                f"""
                SELECT m.id, m.province, m.report_month, m.report_type,
                       m.avg_price_yuan_mwh, m.spot_volume_gwh, m.spot_avg_price_yuan_mwh,
                       r.id as report_id, r.file_hash, r.file_name
                FROM staging.exchange_monthly_metrics m
                JOIN staging.exchange_monthly_reports r ON r.id = m.exchange_report_id
                WHERE r.ingest_status = 'ingested'
                  AND m.province NOT LIKE '%%安徽%%'
                  AND (
                      CASE WHEN m.avg_price_yuan_mwh IS NULL THEN 1 ELSE 0 END +
                      CASE WHEN m.spot_volume_gwh IS NULL THEN 1 ELSE 0 END +
                      CASE WHEN m.spot_avg_price_yuan_mwh IS NULL THEN 1 ELSE 0 END
                  ) >= 2
                {province_filter}
                ORDER BY m.province, m.report_month
                """,
                (args.province,) if args.province else (),
            )
            gapped = cur.fetchall()
    finally:
        conn.close()

    if not gapped:
        print("No gapped metrics rows found. Nothing to do.")
        return

    print(f"\nTargeted re-extraction — {len(gapped)} gapped metrics rows\n")

    # Build hash → path index
    folder = Path(args.folder)
    if not folder.exists():
        logger.error("Folder not found: %s", folder)
        sys.exit(1)

    logger.info("Indexing files in %s …", folder)
    hash_to_path: dict[str, Path] = {}
    for path in sorted(folder.rglob("*")):
        if path.suffix.lower() not in (".pdf", ".doc", ".docx"):
            continue
        if path.name.startswith("~$"):
            continue
        try:
            h = hashlib.sha256(path.read_bytes()).hexdigest()
            hash_to_path[h] = path
        except Exception:
            pass
    logger.info("Indexed %d files", len(hash_to_path))

    if args.dry_run:
        for row in gapped:
            mid, prov, month, rtype, avg, svol, spx, rid, fhash, fname = row
            found = "Y" if fhash in hash_to_path else "N"
            missing = []
            if avg is None: missing.append("avg")
            if svol is None: missing.append("svol")
            if spx is None: missing.append("spx")
            print(f"  [{found}] {prov:8s} {str(month)[:7]} [{rtype}]  miss={missing}  {fname[:60]}")
        return

    extracted = skipped = failed = 0
    for row in gapped:
        mid, prov, month, rtype, avg, svol, spx, rid, fhash, fname = row

        path = hash_to_path.get(fhash)
        if path is None:
            logger.warning("[SKIP] file not on disk: %s %s — %s", prov, month, fname)
            skipped += 1
            continue

        try:
            file_bytes = path.read_bytes()
            pages = extract_pages(
                file_bytes, path.name,
                vision_api_key=vision_api_key,
                textract_region=textract_region,
            )
            full_text = "\n".join(text for _, text in pages)

            if not full_text.strip():
                logger.warning("[SKIP] no text extracted from %s %s (%s)", prov, month, path.name)
                skipped += 1
                continue

            row_id = extract_and_store(
                full_text=full_text,
                province=prov,
                report_month=month,
                report_type=rtype,
                exchange_report_id=rid,
                api_key=api_key,
                pg_url=pg_url,
            )
            if row_id:
                logger.info("[OK] %s %s [%s] → row %s", prov, month, rtype, row_id)
                extracted += 1
            else:
                logger.warning("[SKIP] extraction returned None for %s %s", prov, month)
                skipped += 1
        except Exception as exc:
            logger.error("[FAIL] %s %s — %s", prov, month, exc)
            failed += 1

    print(f"\nDone: {extracted} extracted, {skipped} skipped, {failed} failed")


if __name__ == "__main__":
    main()
