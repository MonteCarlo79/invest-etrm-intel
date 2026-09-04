"""
Query helpers for Data Operations Status displays in portal and bess-map.

Usage:
    from shared.data_ops.status import get_recent_ops, get_pipeline_jobs
    ops_df  = get_recent_ops(engine, hours=48)
    jobs_df = get_pipeline_jobs(engine)
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import text as sql_text


def get_recent_ops(engine, hours: int = 48) -> pd.DataFrame:
    """
    Return recent rows from marketdata.data_ops_log, newest first.

    Columns: op_name, market, date_range, status, message,
             started_at, finished_at, duration_s
    """
    sql = sql_text("""
        SELECT
            op_name,
            market,
            date_range,
            status,
            COALESCE(message, '') AS message,
            started_at,
            finished_at,
            EXTRACT(EPOCH FROM (COALESCE(finished_at, now()) - started_at))::int AS duration_s
        FROM marketdata.data_ops_log
        WHERE started_at >= now() - make_interval(hours => :hours)
        ORDER BY started_at DESC
        LIMIT 200
    """)
    try:
        return pd.read_sql(sql, engine, params={"hours": hours})
    except Exception:
        return pd.DataFrame()


def get_pipeline_jobs(engine) -> pd.DataFrame:
    """
    Return latest status of all pipeline_job_status rows.

    Columns: job_name, status, progress_percent, message, started_at, updated_at
    """
    sql = sql_text("""
        SELECT job_name, status, progress_percent, message, started_at, updated_at
        FROM pipeline_job_status
        ORDER BY updated_at DESC NULLS LAST
    """)
    try:
        return pd.read_sql(sql, engine)
    except Exception:
        return pd.DataFrame()
