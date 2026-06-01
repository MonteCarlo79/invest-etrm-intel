"""Report library — persist and retrieve generated PDF reports.

All generated daily reports are saved to intl_market.report_library so users
can retrieve historical reports from the Library tab in each market app.
"""
from __future__ import annotations

import os
from datetime import date

import pandas as pd
import psycopg2


def _get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def _ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS intl_market.report_library (
                id           SERIAL PRIMARY KEY,
                market_code  TEXT        NOT NULL,
                report_type  TEXT        NOT NULL DEFAULT 'daily',
                report_date  DATE        NOT NULL,
                filename     TEXT        NOT NULL,
                pdf_data     BYTEA       NOT NULL,
                file_size_kb INTEGER,
                created_at   TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (market_code, report_type, report_date)
            )
        """)
        cur.execute("""
            ALTER TABLE intl_market.report_library
            ADD COLUMN IF NOT EXISTS file_size_kb INTEGER
        """)
    conn.commit()


def save_report(
    market_code: str,
    report_date: date,
    pdf_bytes: bytes,
    filename: str,
    report_type: str = "daily",
) -> None:
    """Upsert a report PDF into the library. Silently no-ops on DB errors."""
    conn = _get_conn()
    try:
        _ensure_table(conn)
        size_kb = max(1, len(pdf_bytes) // 1024)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO intl_market.report_library
                    (market_code, report_type, report_date, filename, pdf_data, file_size_kb)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (market_code, report_type, report_date)
                DO UPDATE SET
                    pdf_data     = EXCLUDED.pdf_data,
                    filename     = EXCLUDED.filename,
                    file_size_kb = EXCLUDED.file_size_kb,
                    created_at   = NOW()
                """,
                (market_code, report_type, report_date, filename,
                 psycopg2.Binary(pdf_bytes), size_kb),
            )
        conn.commit()
    finally:
        conn.close()


def list_reports(market_code: str, limit: int = 365) -> pd.DataFrame:
    """Return report metadata (no PDF blobs) for a market, newest first."""
    try:
        conn = _get_conn()
        try:
            _ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, report_type, report_date, filename,
                           COALESCE(file_size_kb,
                                    octet_length(pdf_data) / 1024) AS file_size_kb,
                           created_at
                    FROM intl_market.report_library
                    WHERE market_code = %s
                    ORDER BY report_date DESC, report_type
                    LIMIT %s
                    """,
                    (market_code, limit),
                )
                rows = cur.fetchall()
                cols = ["id", "report_type", "report_date", "filename",
                        "file_size_kb", "created_at"]
                return pd.DataFrame(rows, columns=cols)
        finally:
            conn.close()
    except Exception:
        return pd.DataFrame()


def get_report_pdf(report_id: int) -> bytes | None:
    """Return PDF bytes for a given report ID."""
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pdf_data FROM intl_market.report_library WHERE id = %s",
                    (report_id,),
                )
                row = cur.fetchone()
                return bytes(row[0]) if row else None
        finally:
            conn.close()
    except Exception:
        return None
