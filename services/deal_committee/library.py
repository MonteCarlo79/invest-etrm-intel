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
