#!/usr/bin/env python3
"""
Spot market knowledge pool — PDF ingestion CLI.

Scans data/spot reports/{year}/*.pdf, registers each document by SHA-256 hash
(idempotent: already-ingested files are skipped), then:
  1. Extracts and stores full page text (staging.spot_report_pages)
  2. Builds overlapping text chunks (staging.spot_report_chunks)
  3. Extracts driver/market facts from page text (staging.spot_report_facts)
  4. Bridges price data from public.spot_daily into staging.spot_report_facts

Usage:
    python scripts/spot_market_ingest.py --year 2025
    python scripts/spot_market_ingest.py --year 2025 --limit 5   # first 5 PDFs only
    python scripts/spot_market_ingest.py --pdf path/to/single.pdf
    python scripts/spot_market_ingest.py --year 2025 --force      # re-ingest even if already parsed

Requires PGURL (or DB_URL / DATABASE_URL) in environment or .env file.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running from repo root without install
_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

try:
    from dotenv import load_dotenv
    _env = _REPO / ".env"
    if _env.exists():
        load_dotenv(_env)
except ImportError:
    pass

from services.knowledge_pool.db import get_conn, init_knowledge_tables
from services.knowledge_pool.document_registry import (
    register_document, set_document_status,
)
from services.knowledge_pool.pdf_ingestion import (
    extract_and_store_pages, build_and_store_chunks,
)
from services.knowledge_pool.fact_extraction import (
    extract_facts_for_document, pull_price_facts_from_spot_daily,
)

# Province mapping — extend as reports include more provinces
PROVINCES_MAP = {
    "山东": "Shandong",
    "山西": "Shanxi",
    "蒙西": "Mengxi",
    "内蒙古": "Mengxi",
    "甘肃": "Gansu",
    "广东": "Guangdong",
    "四川": "Sichuan",
    "云南": "Yunnan",
    "贵州": "Guizhou",
    "广西": "Guangxi",
    "湖南": "Hunan",
    "湖北": "Hubei",
    "安徽": "Anhui",
    "浙江": "Zhejiang",
    "江苏": "Jiangsu",
    "福建": "Fujian",
    "河南": "Henan",
    "陕西": "Shaanxi",
    "宁夏": "Ningxia",
    "新疆": "Xinjiang",
}


def _collect_pdfs(year: int | None, single_pdf: str | None) -> list[Path]:
    if single_pdf:
        p = Path(single_pdf)
        if not p.exists():
            print(f"[ERROR] File not found: {single_pdf}", flush=True)
            sys.exit(1)
        return [p]

    base = _REPO / "data" / "spot reports"
    if year:
        dirs = [base / str(year)]
    else:
        dirs = sorted(base.iterdir()) if base.exists() else []

    pdfs = []
    for d in dirs:
        if d.is_dir():
            pdfs.extend(sorted(d.glob("*.pdf")))
    return pdfs


def _ingest_one(pdf: Path, year: int, force: bool) -> bool:
    """Register and ingest a single PDF. Returns True if newly processed."""
    doc_id, is_new = register_document(pdf, report_year=year)

    if not is_new and not force:
        # Check if already fully parsed
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ingest_status FROM staging.spot_report_documents WHERE id = %s",
                    (doc_id,),
                )
                row = cur.fetchone()
        if row and row[0] == "parsed":
            print(f"  [SKIP] {pdf.name} — already parsed (doc_id={doc_id})", flush=True)
            return False

    print(f"  [INGEST] {pdf.name} (doc_id={doc_id})", flush=True)

    try:
        # Step 1: extract and store page text
        page_count, date_min, date_max = extract_and_store_pages(doc_id, pdf, year)
        print(f"    pages: {page_count}, dates: {date_min} → {date_max}", flush=True)

        if page_count == 0:
            set_document_status(doc_id, "empty")
            print(f"    [WARN] no pages extracted", flush=True)
            return False

        # Step 2: build overlapping chunks
        chunk_count = build_and_store_chunks(doc_id, year)
        print(f"    chunks: {chunk_count}", flush=True)

        # Step 3: extract driver/market facts from page text
        fact_count = extract_facts_for_document(doc_id, PROVINCES_MAP)
        print(f"    facts (text extraction): {fact_count}", flush=True)

        # Step 4: bridge price data already in public.spot_daily
        report_dates = []
        if date_min and date_max:
            import datetime as dt
            d = date_min
            while d <= date_max:
                report_dates.append(d)
                d += dt.timedelta(days=1)

        price_facts = pull_price_facts_from_spot_daily(doc_id, report_dates)
        print(f"    facts (price bridge): {price_facts}", flush=True)

        # Mark as parsed
        set_document_status(
            doc_id, "parsed",
            page_count=page_count,
            report_date_min=date_min,
            report_date_max=date_max,
        )
        return True

    except Exception as e:
        set_document_status(doc_id, "error", parse_error=str(e)[:500])
        print(f"    [ERROR] {e}", flush=True)
        return False


def main():
    parser = argparse.ArgumentParser(description="Ingest spot market PDFs into knowledge pool")
    parser.add_argument("--year", type=int, help="Report year (e.g. 2025); omit to scan all years")
    parser.add_argument("--pdf", help="Ingest a single PDF file instead of scanning a directory")
    parser.add_argument("--limit", type=int, help="Max PDFs to process (for smoke testing)")
    parser.add_argument("--force", action="store_true", help="Re-ingest already-parsed files")
    parser.add_argument("--init-db", action="store_true", help="Create knowledge pool tables if missing")
    args = parser.parse_args()

    if args.init_db:
        print("[DB] Initialising knowledge pool tables...", flush=True)
        init_knowledge_tables()

    year = args.year or 2025
    pdfs = _collect_pdfs(args.year, args.pdf)

    if args.limit:
        pdfs = pdfs[: args.limit]

    print(f"[INFO] {len(pdfs)} PDF(s) to process (year={year})", flush=True)

    processed = skipped = errors = 0
    for pdf in pdfs:
        try:
            result = _ingest_one(pdf, year, args.force)
            if result:
                processed += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"  [FATAL] {pdf.name}: {e}", flush=True)
            errors += 1

    print(
        f"\n[DONE] processed={processed} skipped={skipped} errors={errors}",
        flush=True,
    )


if __name__ == "__main__":
    main()
