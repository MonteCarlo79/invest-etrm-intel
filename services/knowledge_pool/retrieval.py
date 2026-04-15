"""
Retrieval helpers for the spot market knowledge pool.

Queries:
  search_chunks    — full-text search over staging.spot_report_chunks
  get_facts        — structured fact lookup with province/date/type filters
  get_note_index   — list all registered notes from staging.spot_report_notes
"""
from __future__ import annotations

import datetime as dt
from typing import List, Optional

from .db import get_conn


def search_chunks(
    query: str,
    province_cn: Optional[str] = None,
    date_from: Optional[dt.date] = None,
    date_to: Optional[dt.date] = None,
    chunk_type: Optional[str] = None,
    limit: int = 20,
) -> List[dict]:
    """
    Full-text search over staging.spot_report_chunks using pg tsvector GIN index.
    Falls back to ILIKE if query contains CJK characters (tsvector 'simple' config
    tokenises CJK by character, so short queries work either way).

    Returns list of dicts with keys:
        document_id, page_no, chunk_index, chunk_type, report_date, chunk_text, rank
    """
    conditions = ["TRUE"]
    params: list = []

    # Build FTS condition
    # Use ILIKE for short CJK queries; plainto_tsquery for longer text
    if len(query) <= 4:
        conditions.append("chunk_text ILIKE %s")
        params.append(f"%{query}%")
        rank_expr = "1.0::float"
    else:
        conditions.append(
            "to_tsvector('simple', chunk_text) @@ plainto_tsquery('simple', %s)"
        )
        params.append(query)
        rank_expr = "ts_rank(to_tsvector('simple', chunk_text), plainto_tsquery('simple', %s))"
        params.append(query)

    if province_cn:
        conditions.append("chunk_text ILIKE %s")
        params.append(f"%{province_cn}%")

    if date_from:
        conditions.append("report_date >= %s")
        params.append(date_from)

    if date_to:
        conditions.append("report_date <= %s")
        params.append(date_to)

    if chunk_type:
        conditions.append("chunk_type = %s")
        params.append(chunk_type)

    where = " AND ".join(conditions)

    sql = f"""
        SELECT document_id, page_no, chunk_index, chunk_type, report_date,
               chunk_text,
               {rank_expr} AS rank
        FROM staging.spot_report_chunks
        WHERE {where}
        ORDER BY rank DESC, report_date DESC
        LIMIT %s
    """
    params.append(limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return rows


def get_facts(
    fact_type: Optional[str] = None,
    province_cn: Optional[str] = None,
    date_from: Optional[dt.date] = None,
    date_to: Optional[dt.date] = None,
    limit: int = 100,
) -> List[dict]:
    """
    Return structured facts from staging.spot_report_facts.

    fact_type options: price_da | price_rt | driver | interprovincial | section_marker
    """
    conditions = ["TRUE"]
    params: list = []

    if fact_type:
        conditions.append("fact_type = %s")
        params.append(fact_type)

    if province_cn:
        conditions.append("province_cn = %s")
        params.append(province_cn)

    if date_from:
        conditions.append("report_date >= %s")
        params.append(date_from)

    if date_to:
        conditions.append("report_date <= %s")
        params.append(date_to)

    where = " AND ".join(conditions)

    sql = f"""
        SELECT document_id, report_date, province_cn, province_en,
               fact_type, metric_name, metric_value, metric_unit,
               fact_text, confidence
        FROM staging.spot_report_facts
        WHERE {where}
        ORDER BY report_date DESC, province_cn, fact_type
        LIMIT %s
    """
    params.append(limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return rows


def get_note_index(note_type: Optional[str] = None) -> List[dict]:
    """Return all registered notes, optionally filtered by type."""
    sql = """
        SELECT note_type, note_key, note_path, note_title,
               report_date_min, report_date_max, updated_at
        FROM staging.spot_report_notes
        {where}
        ORDER BY note_type, note_key
    """
    if note_type:
        sql = sql.format(where="WHERE note_type = %s")
        params = [note_type]
    else:
        sql = sql.format(where="")
        params = []

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return rows


def get_document_list(status: Optional[str] = None) -> List[dict]:
    """Return all registered source documents."""
    sql = """
        SELECT id, source_path, file_name, report_year, ingest_status,
               page_count, report_date_min, report_date_max, created_at
        FROM staging.spot_report_documents
        {where}
        ORDER BY report_date_min DESC NULLS LAST, created_at DESC
    """
    if status:
        sql = sql.format(where="WHERE ingest_status = %s")
        params = [status]
    else:
        sql = sql.format(where="")
        params = []

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return rows
