"""
Spot market ingestion pipeline — per-PDF orchestration.

Steps for each new PDF:
  1. Parse DA/RT prices from PDF  (services/spot_ingest/pdf_parser.py)
  2. For each (date, province): read DB row + read Excel row
  3. Cross-check: log discrepancies where Excel and PDF differ > 2%
  4. Upsert to DB (COALESCE — never clobber existing data)
  5. Update Excel: fill blank cells with PDF data (never overwrite manual data)
  6. Run knowledge-pool ingestion (document registry + chunks + facts)
  7. Regenerate Obsidian notes for affected dates
"""
from __future__ import annotations

import datetime as dt
import logging
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))

try:
    from dotenv import load_dotenv
    for _env in [
        _REPO / "config" / ".env",   # RDS PGURL — load first so it wins
        _REPO / ".env",
        _REPO / "apps" / "spot-agent" / ".env",
    ]:
        if _env.exists():
            load_dotenv(_env)         # load_dotenv never overwrites already-set vars
except ImportError:
    pass

from services.spot_ingest.pdf_parser import parse_pdf
from services.spot_ingest.db_upsert import upsert_rows, fetch_row
from services.spot_ingest.excel_sync import (
    find_excel_row, update_excel_row, cross_check, get_excel_path,
)

_log = logging.getLogger(__name__)

# Province CN → EN mapping (kept in sync with spot_market_ingest.py)
PROVINCES_MAP: dict[str, str] = {
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


def _derive_year(pdf_path: Path) -> int:
    """Derive year from parent directory name, or return current year."""
    try:
        y = int(pdf_path.parent.name)
        if 2020 <= y <= 2030:
            return y
    except (ValueError, AttributeError):
        pass
    return dt.date.today().year


def run(pdf_path: Path, dry_run: bool = False) -> dict:
    """
    Run the full ingestion pipeline for one PDF.

    Args:
        pdf_path: absolute path to the PDF file
        dry_run:  if True, parse and cross-check but don't write to DB/Excel

    Returns a summary dict with keys: dates, provinces, upserted, discrepancies, errors
    """
    summary = {
        "pdf": pdf_path.name,
        "dates": [],
        "provinces": 0,
        "upserted": 0,
        "discrepancies": [],
        "errors": [],
    }

    year = _derive_year(pdf_path)
    provinces_cn = list(PROVINCES_MAP.keys())
    excel_path = get_excel_path(_REPO, year)

    _log.info("[PIPELINE] %s (year=%d)", pdf_path.name, year)

    # ── Step 1: Parse PDF ────────────────────────────────────────────────────
    try:
        parsed = parse_pdf(pdf_path, year, provinces_cn)
    except Exception as e:
        msg = f"PDF parse failed: {e}"
        _log.error("[PIPELINE] %s", msg)
        summary["errors"].append(msg)
        return summary

    if not parsed:
        _log.warning("[PIPELINE] No data extracted from %s", pdf_path.name)
        return summary

    summary["dates"] = sorted(parsed.keys())
    _log.info("[PIPELINE] Dates found: %s", summary["dates"])

    # ── Steps 2-5: Cross-check + upsert + Excel sync ─────────────────────────
    db_rows = []
    all_discrepancies = []

    for report_date, provinces in parsed.items():
        for province_cn, pdf_prices in provinces.items():
            province_en = PROVINCES_MAP.get(province_cn, province_cn)

            # Read existing DB row
            try:
                db_data = fetch_row(report_date, province_en)
            except Exception as e:
                _log.warning("[PIPELINE] DB read failed for %s %s: %s", report_date, province_cn, e)
                db_data = None

            # Read Excel row
            excel_data = None
            if excel_path.exists():
                try:
                    excel_data = find_excel_row(excel_path, province_cn, report_date)
                except Exception as e:
                    _log.warning("[PIPELINE] Excel read failed for %s %s: %s", report_date, province_cn, e)

            # Cross-check
            issues = cross_check(pdf_prices, excel_data, db_data)
            if issues:
                for issue in issues:
                    msg = f"{report_date} {province_cn}: {issue}"
                    all_discrepancies.append(msg)
                    _log.warning("[CROSSCHECK] %s", msg)

            # Decide which values to write: prefer Excel data where available
            final_prices = dict(pdf_prices)
            if excel_data:
                for field in ("da_avg", "da_max", "da_min", "rt_avg", "rt_max", "rt_min"):
                    if excel_data.get(field) is not None:
                        final_prices[field] = excel_data[field]

            db_rows.append({
                "report_date": report_date,
                "province_cn": province_cn,
                "province_en": province_en,
                **final_prices,
            })

    summary["provinces"] = len(db_rows)
    summary["discrepancies"] = all_discrepancies

    if not dry_run:
        # ── Step 4: Upsert to DB ─────────────────────────────────────────────
        try:
            n = upsert_rows(db_rows)
            summary["upserted"] = n
            _log.info("[PIPELINE] Upserted %d rows to spot_daily", n)
        except Exception as e:
            msg = f"DB upsert failed: {e}"
            _log.error("[PIPELINE] %s", msg)
            summary["errors"].append(msg)

        # ── Step 5: Update Excel (fill blank cells only) ─────────────────────
        if excel_path.exists():
            for row in db_rows:
                try:
                    update_excel_row(
                        excel_path,
                        row["province_cn"],
                        row["report_date"],
                        {k: row[k] for k in ("da_avg", "da_max", "da_min",
                                              "rt_avg", "rt_max", "rt_min")},
                    )
                except Exception as e:
                    _log.warning(
                        "[PIPELINE] Excel write failed for %s %s: %s",
                        row["report_date"], row["province_cn"], e,
                    )
        else:
            _log.warning("[PIPELINE] Excel file not found at %s; skipping Excel sync", excel_path)

    # ── Steps 6-7: Knowledge-pool ingestion + Obsidian notes ─────────────────
    if not dry_run:
        _run_knowledge_pool(pdf_path)

    return summary


def _run_knowledge_pool(pdf_path: Path) -> None:
    """
    Run knowledge-pool ingestion (staging tables + fact extraction + notes)
    for the given PDF using the existing services/knowledge_pool pipeline.
    """
    try:
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
        from services.knowledge_pool.markdown_notes import (
            generate_daily_report_note, generate_index_note,
        )
    except ImportError as e:
        _log.warning("[KP] knowledge_pool import failed (%s); skipping KP ingestion", e)
        return

    year = _derive_year(pdf_path)

    try:
        doc_id, is_new = register_document(pdf_path, report_year=year)
    except Exception as e:
        _log.warning("[KP] register_document failed (%s); staging schema may not exist yet — skipping KP ingestion", e)
        return

    # Check if already parsed
    if not is_new:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT ingest_status FROM staging.spot_report_documents WHERE id = %s",
                        (doc_id,),
                    )
                    row = cur.fetchone()
        except Exception as e:
            _log.warning("[KP] status check failed (%s); skipping KP ingestion", e)
            return
        if row and row[0] == "parsed":
            _log.info("[KP] %s already parsed (doc_id=%d); skipping KP", pdf_path.name, doc_id)
            return

    try:
        page_count, date_min, date_max = extract_and_store_pages(doc_id, pdf_path, year)
        if page_count == 0:
            set_document_status(doc_id, "empty")
            return
        build_and_store_chunks(doc_id, year)
        extract_facts_for_document(doc_id, PROVINCES_MAP)
        set_document_status(doc_id, "parsed",
                            page_count=page_count,
                            report_date_min=date_min,
                            report_date_max=date_max)

        # Bridge price facts from spot_daily
        if date_min and date_max:
            dates = []
            d = date_min
            while d <= date_max:
                dates.append(d)
                d += dt.timedelta(days=1)
            try:
                pull_price_facts_from_spot_daily(doc_id, dates)
            except Exception as e:
                _log.warning("[KP] spot_daily bridge failed: %s", e)

        # Generate/update Obsidian notes
        if date_min and date_max:
            d = date_min
            while d <= date_max:
                try:
                    generate_daily_report_note(d, doc_id, str(pdf_path))
                except Exception as e:
                    _log.warning("[KP] daily note for %s failed: %s", d, e)
                d += dt.timedelta(days=1)
        generate_index_note()

        _log.info("[KP] %s ingested to knowledge pool (doc_id=%d)", pdf_path.name, doc_id)

    except Exception as e:
        set_document_status(doc_id, "error", parse_error=str(e)[:500])
        _log.error("[KP] knowledge pool ingestion failed: %s", e)


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Run spot market ingestion pipeline for one PDF")
    parser.add_argument("pdf", help="Path to PDF file")
    parser.add_argument("--dry-run", action="store_true", help="Parse and cross-check only; no writes")
    args = parser.parse_args()

    result = run(Path(args.pdf), dry_run=args.dry_run)
    print(f"\n[RESULT] {result['pdf']}")
    print(f"  Dates: {result['dates']}")
    print(f"  Provinces processed: {result['provinces']}")
    print(f"  DB rows upserted: {result['upserted']}")
    if result["discrepancies"]:
        print(f"  Discrepancies ({len(result['discrepancies'])}):")
        for d in result["discrepancies"]:
            print(f"    ! {d}")
    if result["errors"]:
        print(f"  Errors:")
        for e in result["errors"]:
            print(f"    x {e}")
