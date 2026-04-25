import os
import pandas as pd
import psycopg2
from contextlib import contextmanager


def get_dsn() -> str:
    dsn = os.getenv("DB_DSN") or os.getenv("PGURL")
    if not dsn:
        raise ValueError("DB_DSN is not set")
    return dsn


@contextmanager
def get_conn():
    conn = psycopg2.connect(get_dsn())
    try:
        yield conn
    finally:
        conn.close()


def run_query(sql: str, params=None) -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=params)


def execute_sql(sql: str, params=None) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()