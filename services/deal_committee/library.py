# services/deal_committee/library.py
"""Persist deal briefs and generated DAF PDFs to marketdata.* (idempotent DDL)."""
from __future__ import annotations

from pathlib import Path

from sqlalchemy import text

from services.deal_committee.brief import DealBrief

_DDL_PATH = Path(__file__).resolve().parents[2] / "db" / "ddl" / "marketdata" / "deal_committee.sql"


def ensure_tables(engine) -> None:
    with engine.begin() as conn:
        for stmt in _DDL_PATH.read_text(encoding="utf-8").split(";"):
            if stmt.strip():
                conn.execute(text(stmt))


def save_brief(engine, brief: DealBrief) -> int:
    ensure_tables(engine)
    sql = text("""
        INSERT INTO marketdata.deal_briefs (deal_name, brief, confirmed, source_files)
        VALUES (:name, CAST(:brief AS jsonb), :confirmed, :files)
        RETURNING id
    """)
    with engine.begin() as conn:
        row = conn.execute(sql, {
            "name": brief.deal_name or "(未命名)",
            "brief": brief.model_dump_json(),
            "confirmed": brief.confirmed,
            "files": brief.source_files or None,
        }).fetchone()
    return int(row[0])


def save_daf(engine, brief_id: int, brief: DealBrief, pdf_bytes: bytes,
             filename: str, recommendation: str) -> int:
    ensure_tables(engine)
    sql = text("""
        INSERT INTO marketdata.deal_daf_library
            (brief_id, deal_name, filename, pdf_data, file_size_kb, recommendation)
        VALUES (:bid, :name, :filename, :pdf, :size_kb, :recommendation)
        RETURNING id
    """)
    with engine.begin() as conn:
        row = conn.execute(sql, {
            "bid": brief_id, "name": brief.deal_name or "(未命名)",
            "filename": filename, "pdf": pdf_bytes,
            "size_kb": max(1, len(pdf_bytes) // 1024),
            "recommendation": recommendation or None,
        }).fetchone()
    return int(row[0])


def list_dafs(engine, limit: int = 20) -> list[dict]:
    ensure_tables(engine)
    sql = text("""
        SELECT id, deal_name, filename, file_size_kb, recommendation, created_at
        FROM marketdata.deal_daf_library
        ORDER BY id DESC LIMIT :n
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"n": limit}).fetchall()
    return [{"id": r[0], "deal_name": r[1], "filename": r[2], "file_size_kb": r[3],
             "recommendation": r[4], "created_at": str(r[5])} for r in rows]


def load_daf(engine, daf_id: int) -> tuple[bytes, str]:
    sql = text("SELECT pdf_data, filename FROM marketdata.deal_daf_library WHERE id = :i")
    with engine.connect() as conn:
        row = conn.execute(sql, {"i": daf_id}).fetchone()
    if row is None:
        raise KeyError(f"DAF id={daf_id} 不存在")
    return bytes(row[0]), row[1]


# ── Full analysis results (history view in Tab 6) ─────────────────────────────

def save_result(engine, brief_id: int | None, result) -> int:
    """Persist a complete CommitteeResult (sections + KPIs + synthesis). Returns result id."""
    import json as _json

    from services.deal_committee.result_store import result_to_record
    ensure_tables(engine)
    rec = result_to_record(result)
    sql = text("""
        INSERT INTO marketdata.deal_daf_results
            (brief_id, deal_name, province, asset_type,
             brief, sections, economics, synthesis, recommendation)
        VALUES (:bid, :name, :province, :asset_type,
                CAST(:brief AS jsonb), CAST(:sections AS jsonb), CAST(:economics AS jsonb),
                :synthesis, :recommendation)
        RETURNING id
    """)
    with engine.begin() as conn:
        row = conn.execute(sql, {
            "bid": brief_id,
            "name": result.brief.deal_name or "(未命名)",
            "province": result.brief.province or None,
            "asset_type": result.brief.asset_type,
            "brief": _json.dumps(rec["brief"]),
            "sections": _json.dumps(rec["sections"]),
            "economics": _json.dumps(rec["economics"]) if rec["economics"] else None,
            "synthesis": result.synthesis or None,
            "recommendation": result.recommendation or None,
        }).fetchone()
    return int(row[0])


def link_result_pdf(engine, result_id: int, daf_id: int) -> None:
    """Back-fill daf_id on a saved result once its PDF lands in deal_daf_library."""
    sql = text("UPDATE marketdata.deal_daf_results SET daf_id = :d WHERE id = :i")
    with engine.begin() as conn:
        conn.execute(sql, {"d": daf_id, "i": result_id})


def list_results(engine, limit: int = 20) -> list[dict]:
    ensure_tables(engine)
    sql = text("""
        SELECT r.id, r.deal_name, r.province, r.asset_type, r.recommendation,
               r.created_at, r.daf_id, l.filename
        FROM marketdata.deal_daf_results r
        LEFT JOIN marketdata.deal_daf_library l ON l.id = r.daf_id
        ORDER BY r.id DESC LIMIT :n
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"n": limit}).fetchall()
    return [{"id": r[0], "deal_name": r[1], "province": r[2], "asset_type": r[3],
             "recommendation": r[4], "created_at": str(r[5]),
             "daf_id": r[6], "filename": r[7]} for r in rows]


def load_result(engine, result_id: int) -> dict:
    sql = text("""
        SELECT brief, sections, economics, synthesis, recommendation, deal_name, daf_id
        FROM marketdata.deal_daf_results WHERE id = :i
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, {"i": result_id}).fetchone()
    if row is None:
        raise KeyError(f"结果 id={result_id} 不存在")
    return {"brief": row[0], "sections": row[1], "economics": row[2],
            "synthesis": row[3] or "", "recommendation": row[4] or "",
            "deal_name": row[5], "daf_id": row[6]}
