"""
services/exchange_reports/ingestor.py

ETL pipeline for provincial power exchange monthly/quarterly reports.

Supports: PDF, DOCX
Provinces: 上海, 冀南, 安徽, 山东, 广东, 江苏, 浙江, 福建, 蒙西, 广西

Pipeline:
  1. Infer province + report_month from folder path / filename
  2. Extract text (pdfplumber / python-docx)
  3. Upsert registry row in staging.exchange_monthly_reports
  4. Ingest to staging.spot_knowledge_docs (shared KB) with category='monthly_report'
  5. Update registry with kb_doc_id + status='ingested'

Usage (programmatic):
    from services.exchange_reports.ingestor import ingest_report, ingest_folder, init_table
    init_table(conn)
    ingest_report(file_bytes, filename, province="上海", report_month=date(2026,1,1))

Usage (CLI):
    python scripts/ingest_exchange_reports.py
"""
from __future__ import annotations

import hashlib
import io
import logging
import re
from datetime import date
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── Province catalogue ─────────────────────────────────────────────────────────

# Maps folder-name keyword → canonical province label
_FOLDER_TO_PROVINCE: dict[str, str] = {
    "上海月报": "上海",
    "冀南月报": "冀南",
    "安徽月报": "安徽",
    "山东月报": "山东",
    "广东月报": "广东",
    "江苏月报": "江苏",
    "浙江月报": "浙江",
    "福建月报": "福建",
    "蒙西月报": "蒙西",
    "广西月报": "广西",
}

# Also detect from filenames directly
_NAME_TO_PROVINCE: dict[str, str] = {
    "上海": "上海",
    "上海电网": "上海",
    "河北南网": "冀南",
    "冀南": "冀南",
    "安徽": "安徽",
    "山东": "山东",
    "广东": "广东",
    "江苏": "江苏",
    "浙江": "浙江",
    "福建": "福建",
    "内蒙古": "蒙西",
    "蒙西": "蒙西",
    "广西": "广西",
}

# Quarter → first month of quarter
_QUARTER_TO_MONTH = {"一季度": 1, "二季度": 4, "三季度": 7, "四季度": 10}


# ── Province / month inference ────────────────────────────────────────────────

def infer_province(path: Path) -> Optional[str]:
    """
    Infer province from folder name or filename.
    Checks all parent parts first (longest match wins), then filename.
    """
    parts = list(path.parts) + [path.stem]
    # Check each path component from deepest first
    for part in reversed(parts):
        for key, prov in _FOLDER_TO_PROVINCE.items():
            if key in part:
                return prov
        for key, prov in _NAME_TO_PROVINCE.items():
            if key in part:
                return prov
    return None


def infer_report_month(filename: str) -> Optional[date]:
    """
    Infer report_month (first day of month) from filename.

    Handles:
      - 2026年1月  / 2026年01月     → 2026-01-01
      - 第1期 / 第01期              → 2026-01-01 (needs year context from filename)
      - 一季度 / 二季度 ...          → first month of the quarter
    """
    # e.g. "2026年5月" or "2026年05月"
    m = re.search(r"(\d{4})年(\d{1,2})月", filename)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), 1)
        except ValueError:
            pass

    # Quarterly: "2026年一季度"
    qm = re.search(r"(\d{4})年([一二三四]季度)", filename)
    if qm:
        yr = int(qm.group(1))
        mon = _QUARTER_TO_MONTH.get(qm.group(2))
        if mon:
            return date(yr, mon, 1)

    # 第N期 with year somewhere in filename: "2026年第3期" or "2026年第3期"
    pm = re.search(r"(\d{4})年第(\d{1,2})期", filename)
    if pm:
        try:
            return date(int(pm.group(1)), int(pm.group(2)), 1)
        except ValueError:
            pass

    return None


def infer_report_type(filename: str) -> str:
    """Return 'quarterly' if filename contains a quarter keyword, else 'monthly'."""
    if re.search(r"[一二三四]季度|季度|quarterly|Q[1-4]", filename):
        return "quarterly"
    return "monthly"


def infer_report_year(filename: str) -> Optional[int]:
    """Extract the year from a filename (e.g. '2025年年报' → 2025)."""
    m = re.search(r"(\d{4})年", filename)
    if m:
        yr = int(m.group(1))
        if 2015 <= yr <= 2035:
            return yr
    return None


# ── Text extraction ────────────────────────────────────────────────────────────

def _render_pdf_page_to_png(file_bytes: bytes, page_index: int, max_bytes: int = 4_500_000) -> bytes:
    """Render a single PDF page to PNG bytes at 2× zoom using PyMuPDF.

    If the resulting PNG exceeds *max_bytes* (default 4.5 MB, just under
    Textract's 5 MB limit), the scale is halved until it fits.
    """
    import fitz  # pymupdf
    doc = fitz.open(stream=file_bytes, filetype="pdf")
    page = doc[page_index]
    scale = 2.0
    while scale >= 0.5:
        mat = fitz.Matrix(scale, scale)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        png = pix.tobytes("png")
        if len(png) <= max_bytes or scale <= 0.5:
            return png
        scale /= 2


def _ocr_page_with_vision(image_bytes: bytes, api_key: str) -> str:
    """
    Send a rendered page image to Claude Haiku for OCR.
    Returns the extracted text, or empty string on failure.
    """
    import base64
    from shared.anthropic_client import make_client as _make_anthropic_client
    client = _make_anthropic_client(api_key)
    resp = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=3000,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": base64.b64encode(image_bytes).decode(),
                    },
                },
                {
                    "type": "text",
                    "text": (
                        "This is a page from a Chinese provincial power exchange monthly "
                        "market report. Extract ALL visible text, numbers, percentages, "
                        "and units — especially from tables and charts. "
                        "For tables: output each row as 'col1 | col2 | col3'. "
                        "Preserve all numerical values exactly as shown. "
                        "Output the raw extracted text only, no commentary."
                    ),
                },
            ],
        }],
    )
    return resp.content[0].text.strip()


def _ocr_page_with_textract(image_bytes: bytes, region: str = "ap-southeast-1") -> str:
    """
    OCR a rendered page image via AWS Textract detect_document_text.
    Returns extracted text lines joined by newline, or empty string on failure.
    Works without S3 — passes PNG bytes directly (sync API, < 10 MB).
    """
    import boto3
    client = boto3.client("textract", region_name=region)
    resp = client.detect_document_text(Document={"Bytes": image_bytes})
    lines = [b["Text"] for b in resp.get("Blocks", []) if b["BlockType"] == "LINE"]
    return "\n".join(lines)


def _extract_text_pdf(
    file_bytes: bytes,
    vision_api_key: Optional[str] = None,
    textract_region: Optional[str] = None,
) -> list[tuple[int, str]]:
    """
    Return [(page_no, text), ...] from PDF, including table cell content.

    For pages with < 50 chars of extractable text, OCR fallback chain:
      1. Claude Haiku Vision  (if vision_api_key set)
      2. AWS Textract          (if textract_region set, e.g. 'ap-southeast-1')
    Handles scanned PDFs (安徽) and vector-graphic table pages (上海).
    """
    import pdfplumber
    pages = []
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            # Also extract table content — many reports store key data (prices,
            # volumes, capacity) in tables that extract_text() skips entirely.
            table_lines = []
            for table in (page.extract_tables() or []):
                for row in (table or []):
                    if row:
                        row_str = " | ".join(str(c).strip() for c in row if c)
                        if row_str.strip():
                            table_lines.append(row_str)
            if table_lines:
                text = text + "\n" + "\n".join(table_lines)

            # OCR fallback chain for pages with no extractable text
            if len(text.strip()) < 50:
                img_bytes = None  # render once, reuse for both fallbacks

                # 1. Claude Haiku Vision
                if vision_api_key:
                    try:
                        img_bytes = _render_pdf_page_to_png(file_bytes, i - 1)
                        ocr_text = _ocr_page_with_vision(img_bytes, vision_api_key)
                        if ocr_text:
                            logger.info("Vision OCR page %d: %d chars", i, len(ocr_text))
                            text = ocr_text
                    except Exception as exc:
                        logger.warning("Vision OCR failed for page %d: %s", i, exc)

                # 2. AWS Textract (if vision didn't produce text)
                if len(text.strip()) < 50 and textract_region:
                    try:
                        if img_bytes is None:
                            img_bytes = _render_pdf_page_to_png(file_bytes, i - 1)
                        ocr_text = _ocr_page_with_textract(img_bytes, textract_region)
                        if ocr_text:
                            logger.info("Textract OCR page %d: %d chars", i, len(ocr_text))
                            text = ocr_text
                    except Exception as exc:
                        logger.warning("Textract OCR failed for page %d: %s", i, exc)

            pages.append((i, text))
    return pages


def _extract_text_docx(file_bytes: bytes) -> list[tuple[int, str]]:
    """Return paragraphs from DOCX grouped into pages of 50 paragraphs."""
    from docx import Document
    doc = Document(io.BytesIO(file_bytes))
    paras = [p.text.strip() for p in doc.paragraphs if p.text.strip()]
    pages = []
    for i in range(0, max(len(paras), 1), 50):
        block = "\n".join(paras[i:i + 50])
        if block:
            pages.append((i // 50 + 1, block))
    return pages or [(1, "")]


def extract_pages(
    file_bytes: bytes,
    filename: str,
    vision_api_key: Optional[str] = None,
    textract_region: Optional[str] = None,
) -> list[tuple[int, str]]:
    """
    Dispatch to correct extractor based on extension.

    vision_api_key:   Anthropic API key for Claude Vision OCR (1st fallback).
    textract_region:  AWS region for Textract OCR (2nd fallback, e.g. 'ap-southeast-1').
    Both fallbacks apply only to PDF pages with < 50 chars of extractable text.
    """
    ext = Path(filename).suffix.lower()
    if ext in (".doc", ".docx"):
        return _extract_text_docx(file_bytes)
    return _extract_text_pdf(
        file_bytes,
        vision_api_key=vision_api_key,
        textract_region=textract_region,
    )


# ── DB helpers ────────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS staging.exchange_monthly_reports (
    id              SERIAL PRIMARY KEY,
    province        TEXT NOT NULL,
    report_month    DATE NOT NULL,
    report_type     TEXT NOT NULL DEFAULT 'monthly',
    file_name       TEXT NOT NULL,
    file_hash       TEXT UNIQUE NOT NULL,
    kb_doc_id       INT,
    ingest_status   TEXT NOT NULL DEFAULT 'pending',
    parse_error     TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_emr_province_month
    ON staging.exchange_monthly_reports(province, report_month);
"""


def init_table(pg_url: Optional[str] = None) -> None:
    """Create the registry table if it doesn't exist. Can be called repeatedly."""
    _with_conn(pg_url, _init_table_inner)


def _init_table_inner(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(_DDL)
    conn.commit()


def _get_conn(pg_url: Optional[str] = None):
    """Return a psycopg2 connection. Uses PGURL env var if pg_url is None."""
    import os
    import psycopg2
    url = pg_url or os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not url:
        raise RuntimeError("pg_url required (or set PGURL env var)")
    return psycopg2.connect(url)


def _with_conn(pg_url, fn, *args, **kwargs):
    conn = _get_conn(pg_url)
    try:
        result = fn(conn, *args, **kwargs)
        return result
    finally:
        conn.close()


# ── Core ingestion ────────────────────────────────────────────────────────────

def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _chunk_text(text: str, size: int = 800, overlap: int = 150) -> list[str]:
    text = text.replace("\x00", "").strip()
    if not text:
        return []
    chunks, start = [], 0
    while start < len(text):
        end = min(start + size, len(text))
        chunks.append(text[start:end])
        if end == len(text):
            break
        start += size - overlap
    return chunks


def ingest_report(
    file_bytes: bytes,
    filename: str,
    province: Optional[str] = None,
    report_month: Optional[date] = None,
    report_type: Optional[str] = None,
    pg_url: Optional[str] = None,
    anthropic_api_key: Optional[str] = None,
    embed: bool = True,
) -> dict:
    """
    Ingest one exchange monthly report.

    Returns:
        {
            "report_id": int,
            "kb_doc_id": int | None,
            "is_new": bool,
            "province": str,
            "report_month": date,
            "status": "ingested" | "duplicate" | "failed",
        }
    """
    # Infer metadata from filename if not supplied
    if province is None:
        province = infer_province(Path(filename))
    if report_month is None:
        report_month = infer_report_month(filename)
    if report_type is None:
        report_type = infer_report_type(filename)

    # If province not in known list but file looks like an exchange report,
    # try Claude-based detection from the first page of text
    if province is None and anthropic_api_key and report_month is not None:
        province = _detect_province_via_llm(file_bytes, filename, anthropic_api_key)

    if province is None:
        raise ValueError(f"Cannot infer province from filename: {filename!r}")
    if report_month is None:
        raise ValueError(f"Cannot infer report_month from filename: {filename!r}")

    file_hash = _sha256(file_bytes)

    conn = _get_conn(pg_url)
    try:
        _init_table_inner(conn)

        with conn.cursor() as cur:
            # Dedup check
            cur.execute(
                "SELECT id, kb_doc_id, ingest_status FROM staging.exchange_monthly_reports "
                "WHERE file_hash = %s",
                (file_hash,),
            )
            existing = cur.fetchone()

        if existing:
            return {
                "report_id": existing[0],
                "kb_doc_id": existing[1],
                "is_new": False,
                "province": province,
                "report_month": report_month,
                "status": "duplicate",
            }

        # Register pending row
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO staging.exchange_monthly_reports
                    (province, report_month, report_type, file_name, file_hash, ingest_status)
                VALUES (%s, %s, %s, %s, %s, 'pending')
                ON CONFLICT (file_hash) DO UPDATE SET ingest_status='pending'
                RETURNING id
                """,
                (province, report_month, report_type, filename, file_hash),
            )
            report_id = cur.fetchone()[0]
        conn.commit()

        # Extract text (with OCR fallback chain: Vision → Textract)
        import os as _os
        _vision_key = (
            (anthropic_api_key or "").strip()
            or _os.environ.get("ANTHROPIC_API_KEY", "").strip()
        ) or None
        _textract_region = _os.environ.get("TEXTRACT_REGION") or _os.environ.get("BEDROCK_REGION") or None
        try:
            pages = extract_pages(
                file_bytes, filename,
                vision_api_key=_vision_key,
                textract_region=_textract_region,
            )
        except Exception as exc:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE staging.exchange_monthly_reports "
                    "SET ingest_status='failed', parse_error=%s WHERE id=%s",
                    (str(exc), report_id),
                )
            conn.commit()
            logger.error("Text extraction failed for %s: %s", filename, exc)
            return {
                "report_id": report_id, "kb_doc_id": None, "is_new": True,
                "province": province, "report_month": report_month, "status": "failed",
            }

        # Ingest to shared KB via knowledge_pool
        kb_doc_id = None
        try:
            from services.knowledge_pool.knowledge_docs import (
                init_knowledge_tables,
                sha256_bytes,
                _extract_pages,
                _chunk_text as _kp_chunk,
                _infer_title,
                auto_categorize,
            )
            from services.knowledge_pool.db import get_conn as _kp_conn

            init_knowledge_tables()

            kb_hash = sha256_bytes(file_bytes)
            with _kp_conn() as kconn:
                with kconn.cursor() as cur:
                    cur.execute(
                        "SELECT id FROM staging.spot_knowledge_docs WHERE file_hash = %s",
                        (kb_hash,),
                    )
                    row = cur.fetchone()

            if row:
                kb_doc_id = row[0]
            else:
                # Add 'monthly_report' category if needed (migration)
                _ensure_monthly_report_category()

                first_text = pages[0][1] if pages else ""
                title = _infer_title(filename, first_text)

                with _kp_conn() as kconn:
                    with kconn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO staging.spot_knowledge_docs
                                (file_name, file_hash, category, app, title,
                                 region_province, source_name,
                                 file_size_bytes, page_count, ingest_status)
                            VALUES (%s, %s, 'monthly_report', 'shared', %s,
                                    %s, %s,
                                    %s, %s, 'parsed')
                            RETURNING id
                            """,
                            (
                                filename, kb_hash, title,
                                province,
                                f"{province}电力交易中心",
                                len(file_bytes), len(pages),
                            ),
                        )
                        kb_doc_id = cur.fetchone()[0]

                        chunk_index = 0
                        inserts = []
                        for page_no, text in pages:
                            for chunk in _kp_chunk(text):
                                inserts.append((kb_doc_id, page_no, chunk_index, chunk))
                                chunk_index += 1

                        if inserts:
                            cur.executemany(
                                """
                                INSERT INTO staging.spot_knowledge_chunks
                                    (doc_id, page_no, chunk_index, chunk_text)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (doc_id, chunk_index) DO NOTHING
                                """,
                                inserts,
                            )
                    kconn.commit()

                # Trigger background embedding (best-effort; skip during bulk backfill)
                if embed:
                    try:
                        import threading
                        from services.knowledge_pool.knowledge_docs import _embed_chunks_for_doc
                        threading.Thread(
                            target=_embed_chunks_for_doc, args=(kb_doc_id,), daemon=True,
                        ).start()
                    except Exception:
                        pass

        except Exception as exc:
            logger.error("KB ingestion failed for %s: %s", filename, exc)
            # Don't fail the whole pipeline — still mark registry as ingested

        # Update registry row
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE staging.exchange_monthly_reports "
                "SET kb_doc_id=%s, ingest_status='ingested' WHERE id=%s",
                (kb_doc_id, report_id),
            )
        conn.commit()

        # Extract structured metrics via Claude (best-effort; non-blocking for backfill)
        if anthropic_api_key:
            try:
                from services.exchange_reports.metrics_extractor import extract_and_store
                full_text = "\n".join(text for _, text in pages)
                extract_and_store(
                    full_text=full_text,
                    province=province,
                    report_month=report_month,
                    report_type=report_type,
                    exchange_report_id=report_id,
                    api_key=anthropic_api_key,
                    pg_url=pg_url,
                )
                logger.info("Metrics extracted for %s %s", province, report_month)
            except Exception as exc:
                logger.error("Metrics extraction failed for %s: %s", filename, exc)

        logger.info(
            "Ingested %s: province=%s month=%s kb_doc_id=%s",
            filename, province, report_month, kb_doc_id,
        )
        return {
            "report_id": report_id, "kb_doc_id": kb_doc_id, "is_new": True,
            "province": province, "report_month": report_month, "status": "ingested",
        }
    finally:
        conn.close()


def _ensure_monthly_report_category() -> None:
    """No-op: spot_knowledge_docs.category is a free-form TEXT column."""
    pass


def _detect_province_via_llm(
    file_bytes: bytes,
    filename: str,
    api_key: str,
) -> Optional[str]:
    """
    Use Claude Haiku to detect the province from the first page of the report.
    Returns the Chinese province name (e.g. '河南') or None.
    """
    try:
        pages = extract_pages(file_bytes, filename)
        sample = (pages[0][1] if pages else "")[:1500]
        if not sample.strip():
            return None

        from services.exchange_reports.metrics_extractor import _get_client
        client, model_id, provider = _get_client(api_key=api_key)
        resp = client.messages.create(
            model=model_id,
            max_tokens=30,
            system=(
                "You are identifying which Chinese province a power exchange monthly report "
                "belongs to. Reply with ONLY the Chinese province/region name (e.g. '河南', '四川', "
                "'湖北', '云南', '新疆') and nothing else. If you cannot determine it, reply 'unknown'."
            ),
            messages=[{"role": "user", "content": f"文件名: {filename}\n\n{sample}"}],
        )
        detected = resp.content[0].text.strip()
        if detected and detected != "unknown" and len(detected) <= 10:
            logger.info("LLM detected province '%s' from %s", detected, filename)
            return detected
    except Exception as exc:
        logger.error("Province LLM detection failed: %s", exc)
    return None


# ── Annual / non-monthly KB ingest ───────────────────────────────────────────

def ingest_annual_report(
    file_bytes: bytes,
    filename: str,
    province: str,
    year: Optional[int] = None,
    pg_url: Optional[str] = None,
    embed: bool = True,
) -> dict:
    """
    Ingest a non-monthly exchange publication (annual report, semi-annual summary,
    supply forecast, operations bulletin, etc.) directly into the shared KB.

    Unlike ingest_report(), this does NOT write to staging.exchange_monthly_reports
    (which requires a report_month). It uses the KB's own SHA256 dedup.

    Returns:
        {"status": "ingested"|"duplicate"|"failed", "province": str, "year": int|None, ...}
    """
    try:
        pages = extract_pages(file_bytes, filename)
    except Exception as exc:
        logger.error("Text extraction failed for %s: %s", filename, exc)
        return {"file": filename, "status": "failed", "error": str(exc),
                "province": province, "year": year}

    try:
        from services.knowledge_pool.knowledge_docs import (
            init_knowledge_tables, sha256_bytes,
            _chunk_text as _kp_chunk, _infer_title,
        )
        from services.knowledge_pool.db import get_conn as _kp_conn

        init_knowledge_tables()
        kb_hash = sha256_bytes(file_bytes)

        with _kp_conn() as kconn:
            with kconn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM staging.spot_knowledge_docs WHERE file_hash = %s",
                    (kb_hash,),
                )
                row = cur.fetchone()

        if row:
            logger.info("Annual report already in KB: %s (kb_doc_id=%s)", filename, row[0])
            return {"file": filename, "status": "duplicate",
                    "province": province, "year": year, "kb_doc_id": row[0]}

        _ensure_monthly_report_category()
        first_text = pages[0][1] if pages else ""
        title = _infer_title(filename, first_text)

        with _kp_conn() as kconn:
            with kconn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO staging.spot_knowledge_docs
                        (file_name, file_hash, category, app, title,
                         region_province, source_name,
                         file_size_bytes, page_count, ingest_status)
                    VALUES (%s, %s, 'monthly_report', 'shared', %s,
                            %s, %s, %s, %s, 'parsed')
                    RETURNING id
                    """,
                    (
                        filename, kb_hash, title,
                        province, f"{province}电力交易中心",
                        len(file_bytes), len(pages),
                    ),
                )
                kb_doc_id = cur.fetchone()[0]

                chunk_index = 0
                inserts = []
                for page_no, text in pages:
                    for chunk in _kp_chunk(text):
                        inserts.append((kb_doc_id, page_no, chunk_index, chunk))
                        chunk_index += 1

                if inserts:
                    cur.executemany(
                        """
                        INSERT INTO staging.spot_knowledge_chunks
                            (doc_id, page_no, chunk_index, chunk_text)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (doc_id, chunk_index) DO NOTHING
                        """,
                        inserts,
                    )
            kconn.commit()

        if embed:
            try:
                import threading
                from services.knowledge_pool.knowledge_docs import _embed_chunks_for_doc
                threading.Thread(
                    target=_embed_chunks_for_doc, args=(kb_doc_id,), daemon=True,
                ).start()
            except Exception:
                pass

    except Exception as exc:
        logger.error("KB ingest failed for annual report %s: %s", filename, exc)
        return {"file": filename, "status": "failed", "error": str(exc),
                "province": province, "year": year}

    logger.info("Ingested annual report %s: province=%s year=%s kb_doc_id=%s",
                filename, province, year, kb_doc_id)
    return {"file": filename, "status": "ingested",
            "province": province, "year": year, "kb_doc_id": kb_doc_id}


# ── Folder batch ingest ───────────────────────────────────────────────────────

def ingest_folder(
    folder_path: str | Path,
    pg_url: Optional[str] = None,
    anthropic_api_key: Optional[str] = None,
) -> list[dict]:
    """
    Recursively ingest all PDF/DOCX files in a folder.

    Returns list of result dicts (one per file).
    """
    folder = Path(folder_path)
    results = []
    for path in sorted(folder.rglob("*")):
        if path.suffix.lower() not in (".pdf", ".doc", ".docx"):
            continue
        if path.name.startswith("~$"):  # Office temp files
            continue
        try:
            file_bytes = path.read_bytes()
            province = infer_province(path)
            report_month = infer_report_month(path.name)
            if province is None or report_month is None:
                if province is not None and report_month is None:
                    # Annual/semi-annual/forecast — no specific month; ingest to KB only
                    year = infer_report_year(path.name)
                    res = ingest_annual_report(
                        file_bytes=file_bytes,
                        filename=path.name,
                        province=province,
                        year=year,
                        pg_url=pg_url,
                        embed=False,
                    )
                    res["file"] = str(path)
                    results.append(res)
                else:
                    logger.warning(
                        "Skipping %s — cannot infer province=%r month=%r",
                        path.name, province, report_month,
                    )
                    results.append({"file": str(path), "status": "skipped",
                                     "reason": f"province={province} month={report_month}"})
                continue

            res = ingest_report(
                file_bytes=file_bytes,
                filename=path.name,
                province=province,
                report_month=report_month,
                report_type=infer_report_type(path.name),
                pg_url=pg_url,
                anthropic_api_key=anthropic_api_key,
                embed=False,  # skip OpenBLAS embedding during bulk backfill
            )
            res["file"] = str(path)
            results.append(res)
        except Exception as exc:
            logger.error("Failed to ingest %s: %s", path, exc)
            results.append({"file": str(path), "status": "error", "error": str(exc)})

    return results


# ── Query helpers (used by app + Hermes) ─────────────────────────────────────

def list_reports(
    province: Optional[str] = None,
    year: Optional[int] = None,
    pg_url: Optional[str] = None,
) -> list[dict]:
    """
    Return list of ingested reports sorted by province, report_month DESC.
    """
    conn = _get_conn(pg_url)
    try:
        conditions = ["ingest_status = 'ingested'"]
        params: list = []
        if province:
            conditions.append("province = %s")
            params.append(province)
        if year:
            conditions.append("EXTRACT(YEAR FROM report_month) = %s")
            params.append(year)
        where = " AND ".join(conditions)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, province, report_month, report_type, file_name,
                       kb_doc_id, created_at
                FROM staging.exchange_monthly_reports
                WHERE {where}
                ORDER BY province, report_month DESC
                """,
                params,
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()


_EXCHANGE_REPORT_KEYWORDS = re.compile(r"月报|市场信息|结算概况|交易信息|信息披露|电力交易|电力市场.*报告")
_EXCHANGE_UNKNOWN_PROVINCE = "__unknown__"  # sentinel: looks like exchange report but province unclear


def is_exchange_report(filename: str) -> Optional[str]:
    """
    Return province if filename looks like an exchange monthly report, else None.

    Returns '__unknown__' if the file has exchange-report keywords and a
    detectable month but province cannot be inferred from filename alone
    (e.g. a new province not yet in the mapping). In that case Hermes will
    use Claude to detect the province from the file content.

    Used by Hermes file handler for auto-ingest.
    """
    if not _EXCHANGE_REPORT_KEYWORDS.search(filename):
        return None
    if infer_report_month(filename) is None:
        return None
    province = infer_province(Path(filename))
    if province is None:
        return _EXCHANGE_UNKNOWN_PROVINCE  # known exchange report, unknown province
    return province
