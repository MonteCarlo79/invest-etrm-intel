#!/usr/bin/env python3
"""
scripts/ingest_excel_reports.py

Walk data/exchange-monthly-reports/ for *.xlsx files, parse each with the
province-specific parser, upsert metrics to staging.exchange_excel_metrics,
and optionally ingest full sheet text to the knowledge base.

Usage:
    python scripts/ingest_excel_reports.py [--kb] [--province 山东] [--dry-run]

Options:
    --kb           Also ingest text to KB (vector store)
    --province X   Only process this province folder
    --dry-run      Parse only, do not write to DB or KB
    --show         Print parsed rows for inspection
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

# Ensure project root is on path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from services.exchange_reports.excel_ingestor import (
    excel_to_kb_text,
    parse_excel_file,
    upsert_excel_metrics,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ingest_excel_reports")

# Secondary / supplement files to skip (not primary monthly data files)
_SKIP_PATTERNS = [
    "结算数据",          # raw settlement data files (supplement)
    "结算通报",          # settlement bulletin (supplement)
    "省间",              # interprovincial flow supplements
    "调度运行",          # dispatch operations (广东 supplement)
    "运营简报",          # brief operation report (广东 supplement)
    "宽窄表",            # already handled by 广东 parser from the main file
    "结算情况及分类构成", # 贵州/天津 settlement breakdown (parsed separately)
    "结算总体情况及分类构成",  # 青海
    "跨区跨省",          # interprovincial inflows
    "结算数据库",        # 宁夏 raw settlement DB
    "云南电力交易结算情况报告", # 云南 supplement
    "冀北2024年以来",    # 冀北 settlement bulletin supplement
    "北京电网市场",      # 北京 (全国月报) - generic parse only
    "新疆结算数据",      # 新疆 settlement supplement
]

# Supplement files that have useful settlement data — parse them with generic parser
_INCLUDE_SETTLEMENT = [
    "贵州结算情况及分类构成",
    "青海电力市场结算总体情况",
    "河南结算情况及分类构成报告数据",
    "天津结算总体情况及分类构成汇总",
    "陕西结算及分类构成",
    "黑龙江结算及分类构成",
    "宁夏电力市场结算数据库",
]


def _should_skip(stem: str) -> bool:
    """Return True if this file should be skipped (it's a supplement)."""
    for pat in _INCLUDE_SETTLEMENT:
        if pat in stem:
            return False  # explicitly include
    for pat in _SKIP_PATTERNS:
        if pat in stem:
            return True
    return False


def _kb_ingest(text: str, province: str, filename: str, pg_url: str) -> None:
    """Store KB text in staging.spot_knowledge_docs."""
    try:
        import psycopg2
        conn = psycopg2.connect(pg_url)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO staging.spot_knowledge_docs
                        (namespace, source_name, content, created_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT DO NOTHING
                """, ("exchange_reports", f"excel:{province}:{filename}", text))
            conn.commit()
        finally:
            conn.close()
        logger.info("  KB ingested: %s", filename)
    except Exception as e:
        logger.warning("  KB ingest failed for %s: %s", filename, e)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--kb", action="store_true", help="Also ingest to KB")
    ap.add_argument("--province", default="", help="Filter to a single province folder")
    ap.add_argument("--dry-run", action="store_true", help="Parse only, no DB writes")
    ap.add_argument("--show", action="store_true", help="Print parsed rows")
    args = ap.parse_args()

    pg_url = os.environ.get("PGURL") or os.environ.get("DB_DSN") or os.environ.get("DATABASE_URL")
    if not pg_url and not args.dry_run:
        logger.error("No DB URL found — set PGURL env var or use --dry-run")
        sys.exit(1)

    data_dir = ROOT / "data" / "exchange-monthly-reports"
    if not data_dir.exists():
        logger.error("Data directory not found: %s", data_dir)
        sys.exit(1)

    # Collect Excel files
    xlsx_files: list[Path] = []
    for folder in sorted(data_dir.iterdir()):
        if not folder.is_dir():
            continue
        if args.province and args.province not in folder.name:
            continue
        for f in sorted(folder.glob("*.xlsx")):
            if _should_skip(f.stem):
                logger.debug("SKIP supplement: %s", f.name)
                continue
            xlsx_files.append(f)

    logger.info("Found %d Excel files to process", len(xlsx_files))

    total_records = 0
    total_upserted = 0
    province_counts: dict[str, int] = {}
    failed: list[str] = []

    for xlsx in xlsx_files:
        province, records = parse_excel_file(xlsx)
        if not province:
            logger.debug("No parser matched: %s / %s", xlsx.parent.name, xlsx.name)
            continue
        if not records:
            logger.warning("  0 records parsed from %s", xlsx.name)
            continue

        total_records += len(records)
        province_counts[province] = province_counts.get(province, 0) + len(records)

        logger.info("[%s] %s → %d rows", province, xlsx.name[:60], len(records))

        if args.show:
            for r in records[-3:]:
                print(f"  {r}")

        if not args.dry_run and pg_url:
            try:
                n = upsert_excel_metrics(records, pg_url)
                total_upserted += n
            except Exception as e:
                logger.error("  DB upsert failed for %s: %s", xlsx.name, e)
                failed.append(xlsx.name)

        if args.kb and not args.dry_run and pg_url:
            try:
                text = excel_to_kb_text(xlsx, province)
                _kb_ingest(text, province, xlsx.name, pg_url)
            except Exception as e:
                logger.warning("  KB text failed for %s: %s", xlsx.name, e)

    # ── Summary ───────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("  Provinces parsed: %d", len(province_counts))
    logger.info("  Total monthly records: %d", total_records)
    if not args.dry_run:
        logger.info("  Rows upserted to DB: %d", total_upserted)
    for prov, cnt in sorted(province_counts.items()):
        logger.info("    %-8s  %d months", prov, cnt)
    if failed:
        logger.warning("  FAILED (%d): %s", len(failed), ", ".join(failed))


if __name__ == "__main__":
    main()
