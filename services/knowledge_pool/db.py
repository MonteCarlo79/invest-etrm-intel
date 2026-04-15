"""
DB helpers for knowledge pool.
Uses PGURL (primary platform convention) with DB_URL / DATABASE_URL as fallbacks
to match the spot-agent tools_db.py pattern.
"""
from __future__ import annotations

import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras


def _get_url() -> str:
    url = (
        os.getenv("PGURL")
        or os.getenv("DB_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("MARKETDATA_DB_URL")
    )
    if not url:
        raise RuntimeError(
            "No DB URL found. Set PGURL (or DB_URL / DATABASE_URL)."
        )
    return url


@contextmanager
def get_conn():
    conn = psycopg2.connect(
        _get_url(),
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


def init_knowledge_tables():
    """Create all knowledge pool staging tables if they do not exist."""
    import pathlib
    ddl_path = (
        pathlib.Path(__file__).resolve().parents[2]
        / "db" / "ddl" / "staging" / "spot_report_knowledge.sql"
    )
    if not ddl_path.exists():
        raise FileNotFoundError(f"DDL file not found: {ddl_path}")
    sql = ddl_path.read_text(encoding="utf-8")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    print("[DB] knowledge pool tables initialised")
