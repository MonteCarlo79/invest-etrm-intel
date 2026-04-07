# tools_db.py
from __future__ import annotations

import hashlib
import logging
import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import register_default_json, register_default_jsonb

from pathlib import Path

# Load .env from project root (spot-agent/.env)
try:
    from dotenv import load_dotenv, find_dotenv

    root_env = Path(__file__).resolve().parent.parent / ".env"
    if root_env.exists():
        load_dotenv(root_env)
    else:
        load_dotenv(find_dotenv())
except Exception:
    pass

# Ensure JSON types don't break anything
register_default_json(loads=None)
register_default_jsonb(loads=None)

log = logging.getLogger(__name__)


def _get_db_url():
    url = (
        os.getenv("MARKETDATA_DB_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("DB_URL")
    )
    if not url:
        raise RuntimeError("DB_URL (MARKETDATA_DB_URL or DATABASE_URL) not set")
    return url


@contextmanager
def _conn():
    url = _get_db_url()
    conn = psycopg2.connect(url)
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    """
    Create all spot market tables and indexes if they do not exist.

    Tables:
      spot_daily      — daily DA / RT price summaries per province
      spot_parse_log  — per-file ingestion audit trail (status tracking)
      spot_hourly     — hourly price series (populated by chart digitizer)

    Idempotent: safe to run on every startup or re-run.
    """
    ddl = """
    -- ── spot_daily ───────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS spot_daily (
        id           SERIAL PRIMARY KEY,
        report_date  DATE        NOT NULL,
        province_cn  TEXT        NOT NULL,
        province_en  TEXT        NOT NULL,
        da_avg       NUMERIC,
        da_max       NUMERIC,
        da_min       NUMERIC,
        rt_avg       NUMERIC,
        rt_max       NUMERIC,
        rt_min       NUMERIC,
        highlights   TEXT,
        source_file  TEXT
    );

    CREATE UNIQUE INDEX IF NOT EXISTS idx_spot_daily_unique
        ON spot_daily (report_date, province_en);

    -- Back-fill source_file column on pre-existing tables
    ALTER TABLE spot_daily ADD COLUMN IF NOT EXISTS source_file TEXT;

    -- ── spot_parse_log ────────────────────────────────────────────
    -- Records every ingestion attempt: supports idempotent skips and backfill.
    CREATE TABLE IF NOT EXISTS spot_parse_log (
        id           SERIAL PRIMARY KEY,
        pdf_path     TEXT        NOT NULL,
        file_sha256  TEXT        NOT NULL,
        started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        finished_at  TIMESTAMPTZ,
        status       TEXT        NOT NULL DEFAULT 'started',
        n_dates      INTEGER     DEFAULT 0,
        n_da         INTEGER     DEFAULT 0,
        n_rt         INTEGER     DEFAULT 0,
        n_hi         INTEGER     DEFAULT 0,
        error_msg    TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_spot_parse_log_path
        ON spot_parse_log (pdf_path);

    -- ── spot_hourly ───────────────────────────────────────────────
    -- Hourly price time-series, populated by chart digitizer (future).
    CREATE TABLE IF NOT EXISTS spot_hourly (
        id           SERIAL PRIMARY KEY,
        ts           TIMESTAMPTZ NOT NULL,
        province_en  TEXT        NOT NULL,
        province_cn  TEXT        NOT NULL,
        price        NUMERIC,
        source_file  TEXT
    );

    CREATE UNIQUE INDEX IF NOT EXISTS idx_spot_hourly_unique
        ON spot_hourly (ts, province_en);
    """

    with _conn() as c:
        with c.cursor() as cur:
            cur.execute(ddl)
        c.commit()
    log.debug("init_db: schema ensured")


# ── Parse log helpers ─────────────────────────────────────────────────────────

def sha256_file(path: str) -> str:
    """Compute SHA-256 of a file in 64 KB chunks."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def already_processed(pdf_path: str, file_sha256: str) -> bool:
    """
    Return True if a successful 'done' parse log entry exists for this
    exact file (path + content hash).  Use --force to bypass.
    """
    sql = """
        SELECT 1 FROM spot_parse_log
        WHERE pdf_path = %(p)s AND file_sha256 = %(h)s AND status = 'done'
        LIMIT 1
    """
    with _conn() as c:
        with c.cursor() as cur:
            cur.execute(sql, {"p": pdf_path, "h": file_sha256})
            return cur.fetchone() is not None


def log_parse_start(pdf_path: str, file_sha256: str) -> int:
    """
    Insert a new 'started' log row and return its id.
    Multiple 'started' rows for the same file are allowed (each run gets its own row).
    """
    sql = """
        INSERT INTO spot_parse_log (pdf_path, file_sha256, status)
        VALUES (%(p)s, %(h)s, 'started')
        RETURNING id
    """
    with _conn() as c:
        with c.cursor() as cur:
            cur.execute(sql, {"p": pdf_path, "h": file_sha256})
            row = cur.fetchone()
        c.commit()
    return row[0]


def log_parse_done(
    log_id: int, n_dates: int, n_da: int, n_rt: int, n_hi: int
) -> None:
    """Mark an in-progress log row as 'done' with row counts."""
    sql = """
        UPDATE spot_parse_log
        SET status      = 'done',
            finished_at = NOW(),
            n_dates     = %(nd)s,
            n_da        = %(nda)s,
            n_rt        = %(nrt)s,
            n_hi        = %(nhi)s
        WHERE id = %(id)s
    """
    with _conn() as c:
        with c.cursor() as cur:
            cur.execute(sql, {"id": log_id, "nd": n_dates, "nda": n_da,
                              "nrt": n_rt, "nhi": n_hi})
        c.commit()


def log_parse_error(log_id: int, error_msg: str) -> None:
    """Mark an in-progress log row as 'error' with the exception message."""
    sql = """
        UPDATE spot_parse_log
        SET status      = 'error',
            finished_at = NOW(),
            error_msg   = %(msg)s
        WHERE id = %(id)s
    """
    with _conn() as c:
        with c.cursor() as cur:
            cur.execute(sql, {"id": log_id, "msg": str(error_msg)[:2000]})
        c.commit()


# ── Upsert helpers ────────────────────────────────────────────────────────────

def upsert_da_rows(rows: list[dict]) -> int:
    """
    Insert / update day-ahead rows.

    Each row dict must contain:
        report_date, province_cn, province_en, da_avg, da_max, da_min
    Optional: source_file
    """
    if not rows:
        return 0

    sql = """
    INSERT INTO spot_daily (
        report_date, province_cn, province_en,
        da_avg, da_max, da_min, source_file
    )
    VALUES (
        %(report_date)s, %(province_cn)s, %(province_en)s,
        %(da_avg)s, %(da_max)s, %(da_min)s, %(source_file)s
    )
    ON CONFLICT (report_date, province_en)
    DO UPDATE SET
        province_cn = EXCLUDED.province_cn,
        da_avg      = EXCLUDED.da_avg,
        da_max      = EXCLUDED.da_max,
        da_min      = EXCLUDED.da_min,
        source_file = COALESCE(EXCLUDED.source_file, spot_daily.source_file);
    """

    with _conn() as c:
        with c.cursor() as cur:
            for r in rows:
                r.setdefault("source_file", None)
                cur.execute(sql, r)
        c.commit()
    return len(rows)


def upsert_rt_rows(rows: list[dict]) -> int:
    """
    Insert / update real-time rows.

    Each row dict must contain:
        report_date, province_cn, province_en, rt_avg, rt_max, rt_min
    Optional: source_file
    """
    if not rows:
        return 0

    sql = """
    INSERT INTO spot_daily (
        report_date, province_cn, province_en,
        rt_avg, rt_max, rt_min, source_file
    )
    VALUES (
        %(report_date)s, %(province_cn)s, %(province_en)s,
        %(rt_avg)s, %(rt_max)s, %(rt_min)s, %(source_file)s
    )
    ON CONFLICT (report_date, province_en)
    DO UPDATE SET
        province_cn = EXCLUDED.province_cn,
        rt_avg      = EXCLUDED.rt_avg,
        rt_max      = EXCLUDED.rt_max,
        rt_min      = EXCLUDED.rt_min,
        source_file = COALESCE(EXCLUDED.source_file, spot_daily.source_file);
    """

    with _conn() as c:
        with c.cursor() as cur:
            for r in rows:
                r.setdefault("source_file", None)
                cur.execute(sql, r)
        c.commit()
    return len(rows)


def upsert_highlights_rows(rows: list[dict]) -> int:
    """
    Insert / update highlights (mainly province narrative summaries).

    Each row dict must contain:
        report_date, province_cn, province_en, highlights
    Optional: source_file
    """
    if not rows:
        return 0

    sql = """
    INSERT INTO spot_daily (
        report_date, province_cn, province_en,
        highlights, source_file
    )
    VALUES (
        %(report_date)s, %(province_cn)s, %(province_en)s,
        %(highlights)s, %(source_file)s
    )
    ON CONFLICT (report_date, province_en)
    DO UPDATE SET
        highlights  = EXCLUDED.highlights,
        source_file = COALESCE(EXCLUDED.source_file, spot_daily.source_file);
    """

    with _conn() as c:
        with c.cursor() as cur:
            for r in rows:
                r.setdefault("source_file", None)
                cur.execute(sql, r)
        c.commit()
    return len(rows)
