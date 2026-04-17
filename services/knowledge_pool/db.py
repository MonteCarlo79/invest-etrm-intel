"""
DB helpers for knowledge pool.
Uses PGURL (primary platform convention) with DB_URL / DATABASE_URL as fallbacks
to match the spot-agent tools_db.py pattern.
"""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

_log = logging.getLogger(__name__)

_URL_ENV_KEYS = ["PGURL", "DB_URL", "DATABASE_URL", "MARKETDATA_DB_URL"]


def _get_url() -> tuple[str, str]:
    """Return (url, env_key_used). Raises RuntimeError if none set."""
    for key in _URL_ENV_KEYS:
        val = os.getenv(key)
        if val:
            return val, key
    raise RuntimeError(
        "No DB URL found. Set PGURL (or DB_URL / DATABASE_URL / MARKETDATA_DB_URL).\n"
        "For RDS access set: export PGURL='postgresql://user:pass@host:5432/db?sslmode=require'\n"
        "Hint: config/.env contains the live RDS PGURL — source it or copy to repo root .env"
    )


@contextmanager
def get_conn():
    url, key = _get_url()
    _log.debug("Connecting via %s", key)
    try:
        conn = psycopg2.connect(
            url,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
    except psycopg2.OperationalError as exc:
        msg = str(exc)
        if "Connection timed out" in msg or "could not connect" in msg.lower():
            raise psycopg2.OperationalError(
                f"{msg}\n"
                "Hint: RDS security group only allows inbound 5432 from ecs_tasks-sg.\n"
                "To connect from a developer machine, add your IP to the rds-sg inbound rules (port 5432).\n"
                "Tailscale VPC host 172.31.30.155 also needs to be added if connecting via jump host."
            ) from exc
        raise
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
