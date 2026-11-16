"""Store for monthly 复盘 review metrics (marketdata.rm_review_metrics).

Pure domain code (no streamlit imports). Values are ¥ / MWh / ¥-per-MWh as parsed.
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import bindparam, text

DDL = """CREATE TABLE IF NOT EXISTS marketdata.rm_review_metrics (
  asset_code TEXT NOT NULL, month DATE NOT NULL, metric TEXT NOT NULL, value DOUBLE PRECISION, unit TEXT,
  source_file TEXT, file_hash TEXT, extracted_by TEXT,
  uploaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (asset_code, month, metric))"""

REVIEW_COLUMNS = ["asset_code", "month", "metric", "value", "unit", "source_file", "file_hash",
                  "extracted_by", "uploaded_at"]


def ensure_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(DDL))


def _iso(m) -> str:
    return pd.Timestamp(m).date().isoformat()


def save_review(engine, df: pd.DataFrame, source_file: str, file_hash: str,
                extracted_by: str = "table") -> int:
    """Store a parsed deck. Returns rows written; 0 if this file hash is already stored.

    Per (asset_code, month) present in df, existing metrics are deleted first (a corrected
    deck replaces the whole asset-month), then the deck's rows inserted — one transaction.
    """
    if df is None or df.empty:
        return 0
    rows = [{"asset_code": r.asset_code, "month": _iso(r.month), "metric": r.metric,
             "value": None if pd.isna(r.value) else float(r.value), "unit": r.unit,
             "source_file": source_file, "file_hash": file_hash, "extracted_by": extracted_by}
            for r in df.itertuples(index=False)]
    with engine.begin() as conn:
        seen = conn.execute(text("SELECT 1 FROM marketdata.rm_review_metrics WHERE file_hash = :h LIMIT 1"),
                            {"h": file_hash}).first()
        if seen:
            return 0
        for code, month in sorted({(r["asset_code"], r["month"]) for r in rows}):
            conn.execute(text("DELETE FROM marketdata.rm_review_metrics "
                              "WHERE asset_code = :c AND month = :m"), {"c": code, "m": month})
        conn.execute(text(
            "INSERT INTO marketdata.rm_review_metrics (asset_code, month, metric, value, unit, source_file, "
            "file_hash, extracted_by) VALUES (:asset_code, :month, :metric, :value, :unit, :source_file, "
            ":file_hash, :extracted_by)"), rows)
    return len(rows)


def load_review(engine, asset_codes=None, months=None) -> pd.DataFrame:
    """Long rows; month is a datetime.date column. months: iterable of dates / ISO strings."""
    sql = "SELECT " + ", ".join(REVIEW_COLUMNS) + " FROM marketdata.rm_review_metrics WHERE 1=1"
    params: dict = {}
    binds = []
    if asset_codes is not None:
        sql += " AND asset_code IN :codes"
        params["codes"] = list(asset_codes) or [""]
        binds.append(bindparam("codes", expanding=True))
    if months is not None:
        sql += " AND month IN :months"
        params["months"] = [_iso(m) for m in months] or [""]
        binds.append(bindparam("months", expanding=True))
    sql += " ORDER BY asset_code, month, metric"
    with engine.connect() as conn:
        df = pd.read_sql(text(sql).bindparams(*binds), conn, params=params)
    df["month"] = pd.to_datetime(df["month"]).dt.date
    return df
