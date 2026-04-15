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
    # Look for .env in repo root first, then apps/spot-agent (has DB_URL for this project)
    for _env_candidate in [
        _REPO / ".env",
        _REPO / "apps" / "spot-agent" / ".env",
    ]:
        if _env_candidate.exists():
            load_dotenv(_env_candidate)
            break
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
    # Core spot market provinces (formal pilot participants)
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
    # Extended / trial provinces also mentioned in reports
    "辽宁": "Liaoning",
    "吉林": "Jilin",
    "黑龙江": "Heilongjiang",
    "蒙东": "Mengdong",
    "河北": "Hebei",
    "冀北": "Hebei-North",
    "冀南": "Hebei-South",
    "青海": "Qinghai",
    "江西": "Jiangxi",
    "海南": "Hainan",
    "重庆": "Chongqing",
    "上海": "Shanghai",
    "北京": "Beijing",
    "天津": "Tianjin",
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

        # Mark core ingestion as parsed before attempting optional enrichment
        set_document_status(
            doc_id, "parsed",
            page_count=page_count,
            report_date_min=date_min,
            report_date_max=date_max,
        )

        # Step 4: bridge price data from public.spot_daily (optional enrichment)
        # Failure here does NOT roll back the parsed status — spot_daily may not exist yet.
        import datetime as dt
        report_dates = []
        if date_min and date_max:
            d = date_min
            while d <= date_max:
                report_dates.append(d)
                d += dt.timedelta(days=1)
        try:
            price_facts = pull_price_facts_from_spot_daily(doc_id, report_dates)
            print(f"    facts (price bridge): {price_facts}", flush=True)
        except Exception as bridge_err:
            print(f"    [WARN] spot_daily bridge skipped: {bridge_err}", flush=True)

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

    pdfs = _collect_pdfs(args.year, args.pdf)

    if args.limit:
        pdfs = pdfs[: args.limit]

    print(f"[INFO] {len(pdfs)} PDF(s) to process", flush=True)

    processed = skipped = errors = 0
    for pdf in pdfs:
        # Derive year from parent directory name if it looks like a year,
        # otherwise fall back to --year arg or 2025.
        try:
            dir_year = int(pdf.parent.name)
            if 2020 <= dir_year <= 2030:
                pdf_year = dir_year
            else:
                pdf_year = args.year or 2025
        except (ValueError, AttributeError):
            pdf_year = args.year or 2025

        try:
            result = _ingest_one(pdf, pdf_year, args.force)
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
