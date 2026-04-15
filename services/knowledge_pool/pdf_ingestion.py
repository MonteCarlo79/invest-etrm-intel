"""
PDF text extraction and page/chunk storage.

Wraps pdfplumber (already used by tools_pdf.py).
Stores raw page text and 500-char overlapping chunks in DB.
Does NOT re-implement price parsing — that stays in tools_pdf.py.
"""
from __future__ import annotations

import datetime as dt
import re
from pathlib import Path
from typing import List, Optional

import pdfplumber

from .db import get_conn


# ── Page date inference (same logic as tools_pdf._infer_page_date) ──────────

def _infer_page_date(text: str, year: int) -> Optional[dt.date]:
    m = re.search(r"(\d{1,2})\s*月\s*(\d{1,2})\s*日", text)
    if not m:
        return None
    try:
        return dt.date(year, int(m.group(1)), int(m.group(2)))
    except ValueError:
        return None


# ── Chunking ─────────────────────────────────────────────────────────────────

def _chunk_text(text: str, chunk_size: int = 500, overlap: int = 100) -> List[str]:
    """Split text into overlapping chunks of ~chunk_size characters."""
    if not text or not text.strip():
        return []
    text = text.strip()
    chunks = []
    start = 0
    while start < len(text):
        end = min(start + chunk_size, len(text))
        chunks.append(text[start:end])
        if end == len(text):
            break
        start += chunk_size - overlap
    return chunks


def _classify_chunk(text: str) -> str:
    """Rough chunk type classification based on content signals."""
    t = text.strip()
    if re.search(r"原因为|价格.*?(偏高|偏低|上涨|下降|走低|走高)", t):
        return "reason"
    if re.search(r"均价|最高价|最低价|出清价", t):
        return "table"
    if re.search(r"现货.{0,10}市场.{0,10}(运行|价格|情况)", t):
        return "header"
    return "body"


# ── Main ingestion functions ─────────────────────────────────────────────────

def extract_and_store_pages(
    doc_id: int,
    pdf_path: str | Path,
    year: int,
) -> tuple[int, Optional[dt.date], Optional[dt.date]]:
    """
    Extract text from all pages and store in staging.spot_report_pages.

    Returns:
        (page_count, date_min, date_max)
    """
    pdf_path = Path(pdf_path)
    pages_data = []

    with pdfplumber.open(str(pdf_path)) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            page_date = _infer_page_date(text, year) if text else None
            pages_data.append(
                {
                    "page_no": i,
                    "page_date": page_date,
                    "extracted_text": text,
                    "char_count": len(text),
                }
            )

    if not pages_data:
        return 0, None, None

    # Compute date range
    dates = [p["page_date"] for p in pages_data if p["page_date"]]
    date_min = min(dates) if dates else None
    date_max = max(dates) if dates else None

    with get_conn() as conn:
        with conn.cursor() as cur:
            for p in pages_data:
                cur.execute(
                    """
                    INSERT INTO staging.spot_report_pages
                        (document_id, page_no, page_date, extracted_text, char_count)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (document_id, page_no) DO UPDATE SET
                        page_date      = EXCLUDED.page_date,
                        extracted_text = EXCLUDED.extracted_text,
                        char_count     = EXCLUDED.char_count
                    """,
                    (
                        doc_id,
                        p["page_no"],
                        p["page_date"],
                        p["extracted_text"],
                        p["char_count"],
                    ),
                )
        conn.commit()

    return len(pages_data), date_min, date_max


def build_and_store_chunks(doc_id: int, year: int) -> int:
    """
    Read stored pages for doc_id, build chunks, store in staging.spot_report_chunks.
    Returns total chunks written.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT page_no, page_date, extracted_text "
                "FROM staging.spot_report_pages "
                "WHERE document_id = %s ORDER BY page_no",
                (doc_id,),
            )
            rows = cur.fetchall()

    chunk_index = 0
    inserts = []
    for page_no, page_date, text in rows:
        if not text or not text.strip():
            continue
        for chunk_text in _chunk_text(text):
            inserts.append(
                (
                    doc_id,
                    page_no,
                    chunk_index,
                    chunk_text,
                    _classify_chunk(chunk_text),
                    page_date,
                )
            )
            chunk_index += 1

    if not inserts:
        return 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO staging.spot_report_chunks
                    (document_id, page_no, chunk_index, chunk_text, chunk_type, report_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (document_id, chunk_index) DO UPDATE SET
                    chunk_text  = EXCLUDED.chunk_text,
                    chunk_type  = EXCLUDED.chunk_type,
                    report_date = EXCLUDED.report_date
                """,
                inserts,
            )
        conn.commit()

    return len(inserts)
