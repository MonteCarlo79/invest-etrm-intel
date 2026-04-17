"""
services/knowledge_pool/settlement_retrieval.py

Retrieval helpers for the settlement knowledge pool.

Queries:
  search_settlement_chunks  — FTS over staging.settlement_report_chunks
  get_settlement_facts       — structured fact lookup with asset/period/type filters
  get_settlement_totals      — monthly total amounts per asset × invoice_type
  get_reconciliation_deltas  — reconciliation rows with flag filter
  get_settlement_documents   — document registry with status/type filters
  get_settlement_note_index  — registered notes from staging.settlement_report_notes
"""
from __future__ import annotations

import datetime as dt
from typing import List, Optional

from .db import get_conn


def search_settlement_chunks(
    query: str,
    asset_slug: Optional[str] = None,
    invoice_type: Optional[str] = None,
    settlement_year: Optional[int] = None,
    settlement_month: Optional[int] = None,
    chunk_type: Optional[str] = None,
    limit: int = 20,
) -> List[dict]:
    """
    Full-text search over staging.settlement_report_chunks using the GIN index.
    Falls back to ILIKE for short (≤4 char) queries.

    Returns list of dicts:
        document_id, page_no, chunk_index, chunk_type, chunk_text, rank
    """
    conditions = ["TRUE"]
    params: list = []

    # FTS or ILIKE depending on query length
    if len(query) <= 4:
        conditions.append("c.chunk_text ILIKE %s")
        params.append(f"%{query}%")
        rank_expr = "1.0::float"
    else:
        conditions.append(
            "to_tsvector('simple', c.chunk_text) @@ plainto_tsquery('simple', %s)"
        )
        params.append(query)
        rank_expr = "ts_rank(to_tsvector('simple', c.chunk_text), plainto_tsquery('simple', %s))"
        params.append(query)

    if asset_slug:
        conditions.append("d.asset_slug = %s")
        params.append(asset_slug)

    if invoice_type:
        conditions.append("d.invoice_type = %s")
        params.append(invoice_type)

    if settlement_year:
        conditions.append("d.settlement_year = %s")
        params.append(settlement_year)

    if settlement_month:
        conditions.append("d.settlement_month = %s")
        params.append(settlement_month)

    if chunk_type:
        conditions.append("c.chunk_type = %s")
        params.append(chunk_type)

    where = " AND ".join(conditions)

    sql = f"""
        SELECT c.document_id, c.page_no, c.chunk_index, c.chunk_type,
               c.chunk_text,
               d.asset_slug, d.invoice_type,
               d.settlement_year, d.settlement_month,
               {rank_expr} AS rank
        FROM staging.settlement_report_chunks c
        JOIN staging.settlement_report_documents d ON d.id = c.document_id
        WHERE {where}
        ORDER BY rank DESC, d.settlement_year DESC, d.settlement_month DESC
        LIMIT %s
    """
    params.append(limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def get_settlement_facts(
    asset_slug: Optional[str] = None,
    invoice_type: Optional[str] = None,
    fact_type: Optional[str] = None,
    component_name: Optional[str] = None,
    component_group: Optional[str] = None,
    settlement_year: Optional[int] = None,
    settlement_month: Optional[int] = None,
    period_half: Optional[str] = None,
    limit: int = 200,
) -> List[dict]:
    """
    Structured fact lookup from staging.settlement_report_facts.

    fact_type options: energy_mwh | energy_kwh | charge_component | total_amount
                       capacity_compensation
    component_group: energy | ancillary | system | capacity | power_quality |
                     policy | subsidy | adjustment | compensation | total
    """
    conditions = ["TRUE"]
    params: list = []

    if asset_slug:
        conditions.append("asset_slug = %s")
        params.append(asset_slug)
    if invoice_type:
        conditions.append("invoice_type = %s")
        params.append(invoice_type)
    if fact_type:
        conditions.append("fact_type = %s")
        params.append(fact_type)
    if component_name:
        conditions.append("component_name = %s")
        params.append(component_name)
    if component_group:
        conditions.append("component_group = %s")
        params.append(component_group)
    if settlement_year:
        conditions.append("settlement_year = %s")
        params.append(settlement_year)
    if settlement_month:
        conditions.append("settlement_month = %s")
        params.append(settlement_month)
    if period_half:
        conditions.append("period_half = %s")
        params.append(period_half)

    where = " AND ".join(conditions)

    sql = f"""
        SELECT document_id, asset_slug, settlement_year, settlement_month,
               period_half, invoice_type, fact_type, component_name,
               component_group, metric_value, metric_unit,
               fact_text, page_no, confidence, source_method
        FROM staging.settlement_report_facts
        WHERE {where}
        ORDER BY asset_slug, settlement_year, settlement_month,
                 invoice_type, component_group, component_name
        LIMIT %s
    """
    params.append(limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def get_settlement_totals(
    asset_slug: Optional[str] = None,
    settlement_year: Optional[int] = None,
    invoice_type: Optional[str] = None,
) -> List[dict]:
    """
    Return monthly total_amount facts (总电费) per asset × period × invoice_type.
    Useful for building time-series views of settlement totals.
    """
    conditions = ["fact_type = 'total_amount'", "component_name = '总电费'"]
    params: list = []

    if asset_slug:
        conditions.append("asset_slug = %s")
        params.append(asset_slug)
    if settlement_year:
        conditions.append("settlement_year = %s")
        params.append(settlement_year)
    if invoice_type:
        conditions.append("invoice_type = %s")
        params.append(invoice_type)

    where = " AND ".join(conditions)

    sql = f"""
        SELECT asset_slug, settlement_year, settlement_month,
               invoice_type, period_half, metric_value, confidence,
               document_id
        FROM staging.settlement_report_facts
        WHERE {where}
        ORDER BY asset_slug, settlement_year, settlement_month, invoice_type
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def get_reconciliation_deltas(
    asset_slug: Optional[str] = None,
    settlement_year: Optional[int] = None,
    settlement_month: Optional[int] = None,
    invoice_type: Optional[str] = None,
    flagged_only: bool = False,
    limit: int = 200,
) -> List[dict]:
    """
    Return reconciliation rows from staging.settlement_reconciliation.

    Each row represents one (component, doc_version_a, doc_version_b) comparison.
    Set flagged_only=True to return only rows exceeding the delta thresholds.
    """
    conditions = ["TRUE"]
    params: list = []

    if asset_slug:
        conditions.append("asset_slug = %s")
        params.append(asset_slug)
    if settlement_year:
        conditions.append("settlement_year = %s")
        params.append(settlement_year)
    if settlement_month:
        conditions.append("settlement_month = %s")
        params.append(settlement_month)
    if invoice_type:
        conditions.append("invoice_type = %s")
        params.append(invoice_type)
    if flagged_only:
        conditions.append("flagged = TRUE")

    where = " AND ".join(conditions)

    sql = f"""
        SELECT r.asset_slug, r.settlement_year, r.settlement_month,
               r.invoice_type, r.fact_type, r.component_name,
               r.value_a, r.value_b, r.delta, r.delta_pct,
               r.flagged, r.flag_reason,
               r.flag_threshold_pct, r.flag_threshold_abs,
               r.version_a_doc_id, r.version_b_doc_id,
               da.file_name AS file_a, db2.file_name AS file_b
        FROM staging.settlement_reconciliation r
        JOIN staging.settlement_report_documents da ON da.id = r.version_a_doc_id
        JOIN staging.settlement_report_documents db2 ON db2.id = r.version_b_doc_id
        WHERE {where}
        ORDER BY r.flagged DESC, ABS(r.delta) DESC NULLS LAST,
                 r.asset_slug, r.settlement_year, r.settlement_month
        LIMIT %s
    """
    params.append(limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def get_settlement_documents(
    status: Optional[str] = None,
    asset_slug: Optional[str] = None,
    invoice_type: Optional[str] = None,
    settlement_year: Optional[int] = None,
) -> List[dict]:
    """Return settlement document registry, optionally filtered."""
    conditions = ["TRUE"]
    params: list = []

    if status:
        conditions.append("ingest_status = %s")
        params.append(status)
    if asset_slug:
        conditions.append("asset_slug = %s")
        params.append(asset_slug)
    if invoice_type:
        conditions.append("invoice_type = %s")
        params.append(invoice_type)
    if settlement_year:
        conditions.append("settlement_year = %s")
        params.append(settlement_year)

    where = " AND ".join(conditions)

    sql = f"""
        SELECT id, file_name, asset_slug, invoice_dir_code,
               settlement_year, settlement_month, period_half, invoice_type,
               ingest_status, page_count, file_size_bytes, parse_error,
               created_at
        FROM staging.settlement_report_documents
        WHERE {where}
        ORDER BY settlement_year DESC, settlement_month DESC, asset_slug, invoice_type
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def get_settlement_note_index(note_type: Optional[str] = None) -> List[dict]:
    """Return registered settlement notes, optionally filtered by type."""
    conditions = ["TRUE"]
    params: list = []

    if note_type:
        conditions.append("note_type = %s")
        params.append(note_type)

    where = " AND ".join(conditions)

    sql = f"""
        SELECT note_type, note_key, note_path, note_title,
               settlement_year, settlement_month, asset_slug,
               generated_at, updated_at
        FROM staging.settlement_report_notes
        WHERE {where}
        ORDER BY note_type, asset_slug NULLS LAST, settlement_year DESC, settlement_month DESC
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
