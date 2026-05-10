"""
Lightweight ops logging to marketdata.data_ops_log.

Provides three helpers:
    ensure_table()             — idempotent DDL
    start_op(op, market, dr)  → int op_id
    finish_op(op_id, ok, msg) — mark success/failed

Table is in the marketdata schema (same as other pipeline tables).
"""
from __future__ import annotations

import os
import logging

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS marketdata.data_ops_log (
    id           BIGSERIAL        PRIMARY KEY,
    op_name      TEXT             NOT NULL,
    market       TEXT,
    date_range   TEXT,
    status       TEXT             NOT NULL DEFAULT 'running',
    message      TEXT,
    started_at   TIMESTAMPTZ      DEFAULT now(),
    finished_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_dol_started
    ON marketdata.data_ops_log (started_at DESC);
"""


def _dsn() -> str:
    dsn = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not dsn:
        raise RuntimeError("PGURL / DB_DSN env var not set — cannot write ops log")
    return dsn


def ensure_table() -> None:
    """Create marketdata.data_ops_log if it does not exist (idempotent)."""
    import psycopg2
    with psycopg2.connect(_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()


def start_op(op_name: str, market: str = "all", date_range: str = "") -> int:
    """
    Insert a 'running' row and return its id.

    Parameters
    ----------
    op_name    : short label, e.g. "lingfeng_ingest", "capture"
    market     : market name or "all"
    date_range : human-readable range, e.g. "2026-05-01→2026-05-09"
    """
    import psycopg2
    with psycopg2.connect(_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO marketdata.data_ops_log (op_name, market, date_range, status)
                VALUES (%s, %s, %s, 'running')
                RETURNING id
                """,
                (op_name, market, date_range),
            )
            op_id = cur.fetchone()[0]
        conn.commit()
    return op_id


def finish_op(op_id: int, success: bool, message: str = "") -> None:
    """
    Mark an op as 'success' or 'failed', set finished_at = now().

    Parameters
    ----------
    op_id   : id returned by start_op
    success : True → status = 'success', False → 'failed'
    message : optional summary / error text (truncated to 2000 chars)
    """
    import psycopg2
    status = "success" if success else "failed"
    if len(message) > 2000:
        message = message[:1997] + "..."
    with psycopg2.connect(_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE marketdata.data_ops_log
                SET status = %s, message = %s, finished_at = now()
                WHERE id = %s
                """,
                (status, message, op_id),
            )
        conn.commit()
