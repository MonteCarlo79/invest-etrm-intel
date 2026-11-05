"""Nomination file-ingestion status — one row per WeCom nomination workbook,
written by the daily Mac pipeline after each ship, read by the mengxi-dashboard
Data Management "Nomination File Coverage" panel.

Answers: which nomination files have been downloaded AND ingested, with what
day coverage? The panel joins this table (the local-file side: what exists on
disk and how many days it carries) against marketdata.bess_nomination_96
grouped by source_file (the DB side: what actually landed).

Latest-wins upsert keyed on (filename, asset_code) — history has no value
here; the panel wants the freshest assessment per file.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

from sqlalchemy import text

_DDL = """
CREATE TABLE IF NOT EXISTS marketdata.wecom_nomination_file_status (
    filename     TEXT NOT NULL,
    asset_code   TEXT NOT NULL,
    fmt          TEXT,
    days_in_file INTEGER,
    first_day    DATE,
    last_day     DATE,
    rows_parsed  INTEGER,
    shipped_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    tag          TEXT,
    PRIMARY KEY (filename, asset_code)
)
"""

_UPSERT_SQL = """
INSERT INTO marketdata.wecom_nomination_file_status
    (filename, asset_code, fmt, days_in_file, first_day, last_day,
     rows_parsed, shipped_at, tag)
VALUES (:filename, :asset_code, :fmt, :days_in_file, :first_day, :last_day,
        :rows_parsed, :shipped_at, :tag)
ON CONFLICT (filename, asset_code)
DO UPDATE SET fmt = EXCLUDED.fmt,
              days_in_file = EXCLUDED.days_in_file,
              first_day = EXCLUDED.first_day,
              last_day = EXCLUDED.last_day,
              rows_parsed = EXCLUDED.rows_parsed,
              shipped_at = EXCLUDED.shipped_at,
              tag = EXCLUDED.tag
"""

# Panel query: the DB-ingested side per source file.
_DB_SIDE_SQL = """
SELECT asset_code, source_file,
       COUNT(DISTINCT data_date) AS days_db,
       MIN(data_date) AS db_first, MAX(data_date) AS db_last
FROM marketdata.bess_nomination_96
GROUP BY 1, 2
"""

_STATUS_SIDE_SQL = """
SELECT filename, asset_code, fmt, days_in_file, first_day, last_day,
       rows_parsed, shipped_at, tag
FROM marketdata.wecom_nomination_file_status
"""


def ensure_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_DDL))


def file_stat(asset_code: str, path: Path, rows: list[dict]) -> dict:
    """One status row for a parsed workbook (rows = its parse output)."""
    days = sorted({str(r["data_date"])[:10] for r in rows})
    return {
        "filename": path.name,
        "asset_code": asset_code,
        "fmt": path.suffix.lower().lstrip("."),
        "days_in_file": len(days),
        "first_day": days[0] if days else None,
        "last_day": days[-1] if days else None,
        "rows_parsed": len(rows),
    }


def parse_error_stat(asset_code: str, path: Path, err: Exception) -> dict:
    """Status row for a workbook whose parse failed (kept visible, not
    silently absent, with rows_parsed=-1 as the failure marker)."""
    return {
        "filename": path.name,
        "asset_code": asset_code,
        "fmt": path.suffix.lower().lstrip("."),
        "days_in_file": -1,
        "first_day": None,
        "last_day": None,
        "rows_parsed": -1,
    }
