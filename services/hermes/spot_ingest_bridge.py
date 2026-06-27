"""Bridge: run spot_ingest pipeline on PDF bytes received via Feishu."""
from __future__ import annotations

import datetime as dt
import logging
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

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

# Filename patterns that trigger spot market ingest (case-insensitive)
SPOT_PDF_PATTERNS = [
    "电力现货市场价格与运行日报",
    "spot report",
    "spot_report",
]


def is_spot_pdf(filename: str) -> bool:
    name_lower = filename.lower()
    return any(p.lower() in name_lower for p in SPOT_PDF_PATTERNS)


def ingest_pdf_bytes(filename: str, pdf_bytes: bytes) -> dict:
    """Parse and ingest a spot market PDF received as bytes.

    Returns a summary dict: {dates, provinces, upserted, errors}
    """
    summary: dict = {"dates": [], "provinces": 0, "upserted": 0, "errors": []}

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(pdf_bytes)
        tmp_path = Path(tmp.name)

    try:
        from services.spot_ingest.pdf_parser import parse_pdf_flat
        from services.spot_ingest.db_upsert import upsert_rows
        from services.spot_ingest.interprov_parser import parse_interprov
        from services.spot_ingest.interprov_upsert import upsert_interprov_rows, upsert_summary
        from services.spot_ingest.ai_summary import generate_summary

        year = dt.date.today().year
        provinces_cn = list(PROVINCES_MAP.keys())

        # ── Parse DA/RT prices ────────────────────────────────────────────────
        try:
            flat_rows = parse_pdf_flat(tmp_path, year=year, provinces_cn=provinces_cn)
        except Exception as exc:
            logger.error("spot_ingest: PDF parse failed for %s: %s", filename, exc)
            summary["errors"].append(f"PDF parse failed: {exc}")
            return summary

        for row in flat_rows:
            row.setdefault("province_en", PROVINCES_MAP.get(row.get("province_cn", ""), row.get("province_cn", "")))

        if flat_rows:
            try:
                summary["upserted"] = upsert_rows(flat_rows)
            except Exception as exc:
                logger.error("spot_ingest: upsert_rows failed: %s", exc)
                summary["errors"].append(f"DB upsert failed: {exc}")

        dates = sorted({r["report_date"] for r in flat_rows}) if flat_rows else []
        summary["dates"] = [str(d) for d in dates]
        summary["provinces"] = len({r.get("province_cn") for r in flat_rows})
        logger.info("spot_ingest: %s — %d rows over %d dates", filename, len(flat_rows), len(dates))

        # ── Parse 省间交易 ─────────────────────────────────────────────────────
        interprov_rows: list[dict] = []
        try:
            interprov_rows = parse_interprov(tmp_path, year=year)
            if interprov_rows:
                upsert_interprov_rows(interprov_rows)
                logger.info("spot_ingest: upserted %d interprov rows", len(interprov_rows))
        except Exception as exc:
            logger.warning("spot_ingest: interprov step skipped: %s", exc)

        # ── AI summary per date ───────────────────────────────────────────────
        for report_date in dates:
            try:
                day_prices = [
                    {
                        "province_en": r.get("province_en", r.get("province_cn", "")),
                        "da_avg": r.get("da_avg"),
                        "rt_avg": r.get("rt_avg"),
                    }
                    for r in flat_rows
                    if r.get("report_date") == report_date
                ]
                day_interprov = [r for r in interprov_rows if r.get("report_date") == report_date]
                result = generate_summary(
                    report_date=report_date,
                    price_rows=day_prices,
                    interprov_rows=day_interprov,
                    source_pdf=filename,
                )
                if result:
                    upsert_summary(result)
                    logger.info("spot_ingest: AI summary saved for %s", report_date)
            except Exception as exc:
                logger.warning("spot_ingest: summary failed for %s: %s", report_date, exc)

        # ── Knowledge pool ingestion ──────────────────────────────────────────
        _run_knowledge_pool(tmp_path, filename, year)

    finally:
        tmp_path.unlink(missing_ok=True)

    return summary


def _run_knowledge_pool(tmp_path: Path, filename: str, year: int) -> None:
    try:
        from services.knowledge_pool.document_registry import register_document, set_document_status
        from services.knowledge_pool.pdf_ingestion import extract_and_store_pages, build_and_store_chunks
        from services.knowledge_pool.fact_extraction import extract_facts_for_document, pull_price_facts_from_spot_daily
        from services.knowledge_pool.db import get_conn
    except ImportError as exc:
        logger.warning("spot_ingest: knowledge_pool not available (%s); skipping", exc)
        return

    try:
        doc_id, is_new = register_document(tmp_path, report_year=year)
    except Exception as exc:
        logger.warning("spot_ingest: register_document failed (%s); skipping KP", exc)
        return

    if not is_new:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT ingest_status FROM staging.spot_report_documents WHERE id = %s",
                        (doc_id,),
                    )
                    row = cur.fetchone()
            if row and row[0] == "parsed":
                logger.info("spot_ingest: %s already in KP (doc_id=%d); skipping", filename, doc_id)
                return
        except Exception as exc:
            logger.warning("spot_ingest: KP status check failed (%s); skipping", exc)
            return

    try:
        page_count, date_min, date_max = extract_and_store_pages(doc_id, tmp_path, year)
        if page_count == 0:
            set_document_status(doc_id, "empty")
            return
        build_and_store_chunks(doc_id, year)
        extract_facts_for_document(doc_id, PROVINCES_MAP)
        set_document_status(doc_id, "parsed", page_count=page_count,
                            report_date_min=date_min, report_date_max=date_max)

        if date_min and date_max:
            dates_kp = []
            d = date_min
            while d <= date_max:
                dates_kp.append(d)
                d += dt.timedelta(days=1)
            try:
                pull_price_facts_from_spot_daily(doc_id, dates_kp)
            except Exception as exc:
                logger.warning("spot_ingest: KP spot_daily bridge failed: %s", exc)

        logger.info("spot_ingest: %s ingested into knowledge pool (doc_id=%d)", filename, doc_id)
    except Exception as exc:
        try:
            set_document_status(doc_id, "error", parse_error=str(exc)[:500])
        except Exception:
            pass
        logger.error("spot_ingest: knowledge pool ingestion failed: %s", exc)
