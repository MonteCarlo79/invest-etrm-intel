"""
Atomic DA+RT upsert for public.spot_daily.

Uses COALESCE so that a partial row (e.g. only DA data available) never
overwrites already-stored RT data, and vice versa. This replaces the two
separate upsert calls in apps/spot-agent/agent/tools_db.py.
"""
from __future__ import annotations

import datetime as dt
import logging
import time
from typing import List

from services.knowledge_pool.db import get_conn

_log = logging.getLogger(__name__)

# Province EN names — imported from the spot_ingest module config, or
# provided by the caller via the province_en field in each row.
_UPSERT_SQL = """
INSERT INTO spot_daily (
    report_date, province_cn, province_en,
    da_avg, da_max, da_min,
    rt_avg, rt_max, rt_min
)
VALUES (
    %(report_date)s, %(province_cn)s, %(province_en)s,
    %(da_avg)s, %(da_max)s, %(da_min)s,
    %(rt_avg)s, %(rt_max)s, %(rt_min)s
)
ON CONFLICT (report_date, province_en) DO UPDATE SET
    province_cn = EXCLUDED.province_cn,
    da_avg = COALESCE(EXCLUDED.da_avg, spot_daily.da_avg),
    da_max = COALESCE(EXCLUDED.da_max, spot_daily.da_max),
    da_min = COALESCE(EXCLUDED.da_min, spot_daily.da_min),
    rt_avg = COALESCE(EXCLUDED.rt_avg, spot_daily.rt_avg),
    rt_max = COALESCE(EXCLUDED.rt_max, spot_daily.rt_max),
    rt_min = COALESCE(EXCLUDED.rt_min, spot_daily.rt_min);
"""


def upsert_rows(rows: List[dict], max_retries: int = 3) -> int:
    """
    Upsert a list of price rows into public.spot_daily.

    Each row must contain:
        report_date  (dt.date or str)
        province_cn  (str)
        province_en  (str)
        da_avg, da_max, da_min  (float | None)
        rt_avg, rt_max, rt_min  (float | None)

    Retries up to max_retries times on deadlock with exponential backoff.
    Returns the number of rows processed.
    """
    if not rows:
        return 0

    params_list = [
        {
            "report_date": row["report_date"],
            "province_cn": row["province_cn"],
            "province_en": row.get("province_en", row["province_cn"]),
            "da_avg": row.get("da_avg"),
            "da_max": row.get("da_max"),
            "da_min": row.get("da_min"),
            "rt_avg": row.get("rt_avg"),
            "rt_max": row.get("rt_max"),
            "rt_min": row.get("rt_min"),
        }
        for row in rows
    ]

    for attempt in range(1, max_retries + 1):
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    for params in params_list:
                        cur.execute(_UPSERT_SQL, params)
                conn.commit()
            return len(rows)
        except Exception as exc:
            # Check for deadlock (psycopg2 raises this as an OperationalError or
            # as errors.DeadlockDetected, both with pgcode '40P01')
            pgcode = getattr(exc, "pgcode", None)
            if pgcode == "40P01" and attempt < max_retries:
                sleep_s = 0.5 * (2 ** (attempt - 1))  # 0.5s, 1s, 2s
                _log.warning(
                    "[DB] Deadlock detected on attempt %d/%d; retrying in %.1fs",
                    attempt, max_retries, sleep_s,
                )
                time.sleep(sleep_s)
            else:
                raise


def fetch_row(report_date: dt.date, province_en: str) -> dict | None:
    """
    Read a single (date, province_en) row from spot_daily.
    Returns a dict with all price columns, or None if not found.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT report_date, province_cn, province_en,
                       da_avg, da_max, da_min, rt_avg, rt_max, rt_min
                FROM spot_daily
                WHERE report_date = %s AND province_en = %s
                """,
                (report_date, province_en),
            )
            row = cur.fetchone()
    if row is None:
        return None
    cols = ["report_date", "province_cn", "province_en",
            "da_avg", "da_max", "da_min", "rt_avg", "rt_max", "rt_min"]
    return dict(zip(cols, row))
