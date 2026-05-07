# tools_db.py
from __future__ import annotations

import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import register_default_json, register_default_jsonb

from pathlib import Path

# Load .env from project root (spot-agent/.env)
try:
    from dotenv import load_dotenv, find_dotenv

    # 1) explicit expected location: parent of /agent
    root_env = Path(__file__).resolve().parent.parent / ".env"
    if root_env.exists():
        load_dotenv(root_env)
    else:
        # 2) fallback: search upwards
        load_dotenv(find_dotenv())
except Exception:
    pass



# Ensure JSON types don’t break anything
register_default_json(loads=None)
register_default_jsonb(loads=None)


def _get_db_url():
    url = (
        os.getenv("MARKETDATA_DB_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("DB_URL")  # <- add this
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
    Create spot_daily table if it does not exist.

    Schema:
      id           serial
      report_date  date                       -- 'logical' delivery date
      province_cn  text
      province_en  text
      da_avg       numeric
      da_max       numeric
      da_min       numeric
      rt_avg       numeric
      rt_max       numeric
      rt_min       numeric
      highlights   text
    """
    sql = """
    CREATE TABLE IF NOT EXISTS spot_daily (
        id           SERIAL PRIMARY KEY,
        report_date  DATE NOT NULL,
        province_cn  TEXT NOT NULL,
        province_en  TEXT NOT NULL,
        da_avg       NUMERIC,
        da_max       NUMERIC,
        da_min       NUMERIC,
        rt_avg       NUMERIC,
        rt_max       NUMERIC,
        rt_min       NUMERIC,
        highlights   TEXT
    );

    CREATE UNIQUE INDEX IF NOT EXISTS idx_spot_daily_unique
        ON spot_daily (report_date, province_en);
    """

    with _conn() as c:
        with c.cursor() as cur:
            cur.execute(sql)
        c.commit()


def upsert_da_rows(rows: list[dict]) -> int:
    """
    Insert / update day-ahead rows.

    Each row dict must contain:
        report_date, province_cn, province_en, da_avg, da_max, da_min
    """
    if not rows:
        return 0

    sql = """
    INSERT INTO spot_daily (
        report_date, province_cn, province_en,
        da_avg, da_max, da_min
    )
    VALUES (
        %(report_date)s, %(province_cn)s, %(province_en)s,
        %(da_avg)s, %(da_max)s, %(da_min)s
    )
    ON CONFLICT (report_date, province_en)
    DO UPDATE SET
        province_cn = EXCLUDED.province_cn,
        da_avg      = EXCLUDED.da_avg,
        da_max      = EXCLUDED.da_max,
        da_min      = EXCLUDED.da_min;
    """

    with _conn() as c:
        with c.cursor() as cur:
            for r in rows:
                cur.execute(sql, r)
        c.commit()
    return len(rows)


def upsert_rt_rows(rows: list[dict]) -> int:
    """
    Insert / update real-time rows.

    Each row dict must contain:
        report_date, province_cn, province_en, rt_avg, rt_max, rt_min
    """
    if not rows:
        return 0

    sql = """
    INSERT INTO spot_daily (
        report_date, province_cn, province_en,
        rt_avg, rt_max, rt_min
    )
    VALUES (
        %(report_date)s, %(province_cn)s, %(province_en)s,
        %(rt_avg)s, %(rt_max)s, %(rt_min)s
    )
    ON CONFLICT (report_date, province_en)
    DO UPDATE SET
        province_cn = EXCLUDED.province_cn,
        rt_avg      = EXCLUDED.rt_avg,
        rt_max      = EXCLUDED.rt_max,
        rt_min      = EXCLUDED.rt_min;
    """

    with _conn() as c:
        with c.cursor() as cur:
            for r in rows:
                cur.execute(sql, r)
        c.commit()
    return len(rows)


def upsert_highlights_rows(rows: list[dict]) -> int:
    """
    Insert / update highlights (mainly 'National' summary).

    Each row dict must contain:
        report_date, province_cn, province_en, highlights
    """
    if not rows:
        return 0

    sql = """
    INSERT INTO spot_daily (
        report_date, province_cn, province_en, highlights
    )
    VALUES (
        %(report_date)s, %(province_cn)s, %(province_en)s, %(highlights)s
    )
    ON CONFLICT (report_date, province_en)
    DO UPDATE SET
        highlights = EXCLUDED.highlights;
    """

    with _conn() as c:
        with c.cursor() as cur:
            for r in rows:
                cur.execute(sql, r)
        c.commit()
    return len(rows)
