"""
services/knowledge_pool/settlement_ingestion.py

Page extraction + chunking for settlement PDFs.
Writes to staging.settlement_report_pages and staging.settlement_report_chunks.
Analogous to pdf_ingestion.py but uses settlement-specific tables and date inference.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional
import datetime as dt

import pdfplumber

from .db import get_conn


def _infer_settlement_period(text: str) -> tuple[int | None, int | None]:
    """Extract YYYY and M from page text like '2025年10月'."""
    m = re.search(r"(\d{4})\s*年\s*0?(\d{1,2})\s*月", text)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None


def _chunk_text(text: str, chunk_size: int = 500, overlap: int = 100) -> list[str]:
    if not text or not text.strip():
        return []
    text = text.strip()
    chunks, start = [], 0
    while start < len(text):
        end = min(start + chunk_size, len(text))
        chunks.append(text[start:end])
        if end == len(text):
            break
        start += chunk_size - overlap
    return chunks


def _classify_chunk(text: str) -> str:
    t = text.strip()
    if re.search(r"[\d,]+\s*元", t) and re.search(r"(?:电费|服务费|补偿|调整|合计|总计)", t):
        return "amount_line"
    if re.search(r"(?:电量|电费|单价|金额|合计)\s*[\(（]", t):
        return "table"
    if re.search(r"(?:结算单|电费清单|补偿费用统计|容量补偿)", t):
        return "header"
    return "body"


def extract_and_store_settlement_pages(
    doc_id: int,
    pdf_path: Path,
    settlement_year: int,
    settlement_month: int,
) -> tuple[int, Optional[dt.date], Optional[dt.date]]:
    """
    Extract page text, write to staging.settlement_report_pages.
    Returns (page_count, date_min, date_max).
    date_min/max derived from settlement year/month (first and last day).
    """
    pdf_path = Path(pdf_path)
    pages_data = []

    with pdfplumber.open(str(pdf_path)) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            pages_data.append({
                "page_no": i,
                "extracted_text": text,
                "char_count": len(text),
            })

    if not pages_data:
        return 0, None, None

    try:
        date_min = dt.date(settlement_year, settlement_month, 1)
        if settlement_month == 12:
            date_max = dt.date(settlement_year + 1, 1, 1) - dt.timedelta(days=1)
        else:
            date_max = dt.date(settlement_year, settlement_month + 1, 1) - dt.timedelta(days=1)
    except ValueError:
        date_min = date_max = None

    with get_conn() as conn:
        with conn.cursor() as cur:
            for p in pages_data:
                cur.execute(
                    """
                    INSERT INTO staging.settlement_report_pages
                        (document_id, page_no, extracted_text, char_count)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (document_id, page_no) DO UPDATE SET
                        extracted_text = EXCLUDED.extracted_text,
                        char_count     = EXCLUDED.char_count
                    """,
                    (doc_id, p["page_no"], p["extracted_text"], p["char_count"]),
                )
        conn.commit()

    return len(pages_data), date_min, date_max


def build_and_store_settlement_chunks(doc_id: int) -> int:
    """Read settlement pages for doc_id, build chunks, store in settlement_report_chunks."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT page_no, extracted_text FROM staging.settlement_report_pages "
                "WHERE document_id = %s ORDER BY page_no",
                (doc_id,),
            )
            rows = cur.fetchall()

    chunk_index = 0
    inserts = []
    for page_no, text in rows:
        if not text or not text.strip():
            continue
        for chunk_text in _chunk_text(text):
            inserts.append((doc_id, page_no, chunk_index, chunk_text, _classify_chunk(chunk_text)))
            chunk_index += 1

    if not inserts:
        return 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO staging.settlement_report_chunks
                    (document_id, page_no, chunk_index, chunk_text, chunk_type)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (document_id, chunk_index) DO UPDATE SET
                    chunk_text  = EXCLUDED.chunk_text,
                    chunk_type  = EXCLUDED.chunk_type
                """,
                inserts,
            )
        conn.commit()

    return len(inserts)
