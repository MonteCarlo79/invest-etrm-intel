import os
from typing import List, Dict

from datetime import date
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import psycopg2

load_dotenv()


def _build_db_url() -> str:
    url = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    if url and url.startswith("postgresql+psycopg2://"):
        url = url.replace("postgresql+psycopg2://", "postgresql://", 1)
    if url:
        return url

    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "5433")
    name = os.getenv("DB_NAME", "marketdata")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "root")
    return f"postgresql://{user}:{password}@{host}:{port}/{name}"


DB_URL = _build_db_url()


def _conn():
    return psycopg2.connect(DB_URL)


app = FastAPI(title="Spot Market API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    try:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.get("/v1/spot/hourly")
def get_hourly(province: str, start: str, end: str):
    """
    NOT IMPLEMENTED.

    The daily PDF reports store hourly price data as embedded chart images, not tables.
    Extracting per-hour prices would require vision-model chart digitization, which is
    not supported in this release.

    Use /v1/spot/daily for daily DA/RT average/max/min per province.
    """
    raise HTTPException(
        status_code=501,
        detail=(
            "Hourly data is not supported. "
            "The source PDFs contain hourly prices as chart images (not tables), "
            "requiring vision-based extraction that is not implemented. "
            "Use /v1/spot/daily for daily DA/RT summaries."
        ),
    )


@app.get("/v1/spot/daily")
def get_daily(
    province: str = Query(..., description="Province code, e.g. Shanxi"),
    start: str = Query(..., description="Start date YYYY-MM-DD"),
    end: str = Query(..., description="End date YYYY-MM-DD"),
):
    """
    Return daily DA / RT price summaries from spot_daily.
    This is the primary table populated by spot_ingest.py.
    """
    sql = """
        SELECT report_date, province_en, province_cn,
               da_avg, da_max, da_min,
               rt_avg, rt_max, rt_min,
               highlights, source_file
        FROM spot_daily
        WHERE province_en = %s
          AND report_date >= %s
          AND report_date <= %s
        ORDER BY report_date
    """
    try:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (province, start, end))
            rows = cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return [
        {
            "report_date":  str(r[0]),
            "province":     r[1],
            "province_cn":  r[2],
            "da_avg":       float(r[3]) if r[3] is not None else None,
            "da_max":       float(r[4]) if r[4] is not None else None,
            "da_min":       float(r[5]) if r[5] is not None else None,
            "rt_avg":       float(r[6]) if r[6] is not None else None,
            "rt_max":       float(r[7]) if r[7] is not None else None,
            "rt_min":       float(r[8]) if r[8] is not None else None,
            "highlights":   r[9],
            "source_file":  r[10],
        }
        for r in rows
    ]


@app.get("/v1/spot/parse-log")
def get_parse_log(limit: int = 100):
    """
    Return recent parse log entries for monitoring ingestion status.
    """
    sql = """
        SELECT id, pdf_path, file_sha256, started_at, finished_at,
               status, n_dates, n_da, n_rt, n_hi, error_msg
        FROM spot_parse_log
        ORDER BY started_at DESC
        LIMIT %s
    """
    try:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return [
        {
            "id":           r[0],
            "pdf_path":     r[1],
            "file_sha256":  r[2],
            "started_at":   r[3].isoformat() if r[3] else None,
            "finished_at":  r[4].isoformat() if r[4] else None,
            "status":       r[5],
            "n_dates":      r[6],
            "n_da":         r[7],
            "n_rt":         r[8],
            "n_hi":         r[9],
            "error_msg":    r[10],
        }
        for r in rows
    ]
