"""
Document registry — register/lookup PDF source documents by SHA-256 hash.
Idempotent: calling register_document() twice on the same file is a no-op.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import os
from pathlib import Path
from typing import Optional

from .db import get_conn


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def register_document(
    source_path: str | Path,
    report_year: int,
    parser_version: str = "v1",
) -> tuple[int, bool]:
    """
    Register a PDF in staging.spot_report_documents.

    Returns:
        (document_id, is_new)  — is_new=False if already registered (same hash).
    """
    source_path = Path(source_path)
    file_hash = sha256_file(source_path)
    file_size = source_path.stat().st_size
    file_name = source_path.name

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Check if already registered
            cur.execute(
                "SELECT id, ingest_status FROM staging.spot_report_documents WHERE file_hash = %s",
                (file_hash,),
            )
            row = cur.fetchone()
            if row:
                return row[0], False

            # Insert new record
            cur.execute(
                """
                INSERT INTO staging.spot_report_documents
                    (source_path, file_name, report_year, file_hash,
                     file_size_bytes, parser_version, ingest_status)
                VALUES (%s, %s, %s, %s, %s, %s, 'pending')
                RETURNING id
                """,
                (
                    str(source_path),
                    file_name,
                    report_year,
                    file_hash,
                    file_size,
                    parser_version,
                ),
            )
            doc_id = cur.fetchone()[0]
        conn.commit()
    return doc_id, True


def set_document_status(
    doc_id: int,
    status: str,
    page_count: Optional[int] = None,
    report_date_min: Optional[dt.date] = None,
    report_date_max: Optional[dt.date] = None,
    parse_error: Optional[str] = None,
):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE staging.spot_report_documents
                SET ingest_status  = %s,
                    page_count     = COALESCE(%s, page_count),
                    report_date_min= COALESCE(%s, report_date_min),
                    report_date_max= COALESCE(%s, report_date_max),
                    parse_error    = %s,
                    updated_at     = now()
                WHERE id = %s
                """,
                (status, page_count, report_date_min, report_date_max,
                 parse_error, doc_id),
            )
        conn.commit()


def get_document_by_hash(file_hash: str) -> Optional[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM staging.spot_report_documents WHERE file_hash = %s",
                (file_hash,),
            )
            row = cur.fetchone()
    return dict(row) if row else None


import psycopg2.extras  # noqa: E402 (needed for RealDictCursor above)
