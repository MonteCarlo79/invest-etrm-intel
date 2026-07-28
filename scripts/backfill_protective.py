"""
Batch ingest backfill Excel files with zero-protection.

For each .xlsx in --indir, parses fundamentals columns, aggregates 15-min → hourly,
then upserts with a COALESCE guard: incoming zeros/NULLs never overwrite existing
non-zero values in the DB.

Usage:
    python scripts/backfill_protective.py --indir data/backfill
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Reuse column-detection logic from run_fundamentals_ingest
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services.bess_map.run_fundamentals_ingest import (
    _detect_fundamentals_cols,
    _parse_date,
    _hhmm_to_hm,
    _ensure_table,
    COLUMN_GROUPS,
)

ALL_DB_COLS = [
    "load_mw", "load_d1_mw", "load_d2_mw", "load_d3_mw",
    "net_export_mw", "net_export_d1_mw", "net_export_d2_mw", "net_export_d3_mw",
    "renewable_total_mw", "renewable_d1_mw", "renewable_d2_mw", "renewable_d3_mw",
    "bidding_space_mw", "bidding_space_d1_mw", "bidding_space_d2_mw", "bidding_space_d3_mw",
    "wind_mw", "wind_d1_mw", "wind_d2_mw", "wind_d3_mw",
    "solar_mw", "solar_d1_mw", "solar_d2_mw", "solar_d3_mw",
]


def _zero_fraction(series: pd.Series) -> float:
    """Fraction of rows where value is 0 or NaN."""
    if len(series) == 0:
        return 1.0
    return ((series.isna()) | (series == 0)).sum() / len(series)


def ingest_file_protective(
    xlsx_path: Path,
    dsn: str,
    schema: str = "marketdata",
    zero_threshold: float = 0.5,
) -> dict:
    """
    Parse and upsert one file with zero-protection.

    For each column: if >zero_threshold fraction of values are 0/NULL in the Excel,
    use COALESCE (keep existing non-zero), else overwrite normally.

    Returns dict with stats.
    """
    stem = re.sub(r"[^\u4e00-\u9fa5]", "", xlsx_path.stem)
    province = stem
    if not province:
        return {"file": xlsx_path.name, "status": "skip", "reason": "cannot detect province"}

    col_map = _detect_fundamentals_cols(xlsx_path)
    if not col_map:
        return {"file": xlsx_path.name, "province": province,
                "status": "skip", "reason": "no fundamentals columns detected"}

    df = pd.read_excel(xlsx_path, sheet_name=0)
    date_col = df.columns[0]
    time_col = df.columns[1]

    df = df.dropna(subset=[date_col])
    df["_date"] = df[date_col].apply(_parse_date)
    df = df.dropna(subset=["_date"])

    def _to_hour(v):
        try:
            h, _ = _hhmm_to_hm(v)
            return h
        except Exception:
            return None

    df["_hour"] = df[time_col].apply(_to_hour)
    df = df.dropna(subset=["_hour"])
    df["_hour"] = df["_hour"].astype(int)
    df["_dt"] = pd.to_datetime(df["_date"]) + pd.to_timedelta(df["_hour"].astype(int), unit="h")

    rename_map = {v: k for k, v in col_map.items()}
    unique_excel_cols = list(dict.fromkeys(col_map.values()))
    fund_df = df[["_dt"] + unique_excel_cols].copy()
    fund_df = fund_df.rename(columns=rename_map)
    fund_df = fund_df.loc[:, ~fund_df.columns.duplicated(keep="first")]
    for c in fund_df.columns:
        if c != "_dt":
            fund_df[c] = pd.to_numeric(fund_df[c], errors="coerce")

    hourly = fund_df.groupby("_dt", as_index=False).mean(numeric_only=True)
    hourly["province"] = province

    present_cols = [c for c in ALL_DB_COLS if c in hourly.columns]

    # Assess zero fraction per column
    zero_cols = []
    normal_cols = []
    for c in present_cols:
        zf = _zero_fraction(hourly[c])
        if zf > zero_threshold:
            zero_cols.append(c)
        else:
            normal_cols.append(c)

    if zero_cols:
        print(f"  [WARN] {province}: {len(zero_cols)} columns are >{zero_threshold*100:.0f}% zero "
              f"→ will COALESCE (keep existing non-zero): {zero_cols}")
    if normal_cols:
        print(f"  [INFO] {province}: {len(normal_cols)} columns have real data → overwrite normally")

    # Build per-column update clause
    # Normal cols: SET col = EXCLUDED.col
    # Zero-flood cols: SET col = CASE WHEN EXCLUDED.col IS NOT NULL AND EXCLUDED.col != 0
    #                               THEN EXCLUDED.col
    #                               ELSE COALESCE(t.col, EXCLUDED.col) END
    tbl = f"{schema}.spot_fundamentals_hourly"
    update_parts = []
    for c in present_cols:
        if c in zero_cols:
            update_parts.append(
                f"{c} = CASE WHEN EXCLUDED.{c} IS NOT NULL AND EXCLUDED.{c} != 0 "
                f"THEN EXCLUDED.{c} "
                f"ELSE COALESCE({tbl}.{c}, EXCLUDED.{c}) END"
            )
        else:
            update_parts.append(f"{c} = EXCLUDED.{c}")

    insert_cols = ["province", "datetime"] + present_cols
    sql = (
        f"INSERT INTO {tbl} ({', '.join(insert_cols)}) VALUES %s "
        f"ON CONFLICT (province, datetime) DO UPDATE SET {', '.join(update_parts)}"
    )

    rows = []
    for _, row in hourly.iterrows():
        dt_val = row["_dt"].to_pydatetime() if hasattr(row["_dt"], "to_pydatetime") else row["_dt"]
        vals = [province, dt_val] + [
            None if pd.isna(row[c]) else float(row[c])
            for c in present_cols
        ]
        rows.append(tuple(vals))

    _ensure_table(dsn, schema)
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
        conn.commit()

    date_range = f"{hourly['_dt'].min().date()} → {hourly['_dt'].max().date()}"
    print(f"  [OK] {province}: {len(rows)} rows upserted  ({date_range})")
    return {
        "file": xlsx_path.name, "province": province,
        "status": "ok", "rows": len(rows),
        "date_range": date_range,
        "zero_protected_cols": zero_cols,
        "overwritten_cols": normal_cols,
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--indir", required=True, help="Folder containing .xlsx backfill files")
    p.add_argument("--schema", default="marketdata")
    p.add_argument("--zero-threshold", type=float, default=0.5,
                   help="Fraction of zeros above which a column is considered zero-flood (default 0.5)")
    args = p.parse_args()

    dsn = os.environ.get("PGURL", "")
    if not dsn:
        sys.exit("[ERROR] PGURL not set in environment")

    indir = Path(args.indir)
    files = sorted(indir.glob("*.xlsx"))
    if not files:
        sys.exit(f"[ERROR] No .xlsx files found in {indir}")

    print(f"Found {len(files)} file(s) in {indir}")
    print(f"Zero-protection threshold: >{args.zero_threshold*100:.0f}% zeros → COALESCE\n")

    results = []
    for f in files:
        print(f"\n[FILE] {f.name}")
        try:
            r = ingest_file_protective(f, dsn, schema=args.schema,
                                        zero_threshold=args.zero_threshold)
            results.append(r)
        except Exception as e:
            print(f"  [FAIL] {f.name}: {e}")
            results.append({"file": f.name, "status": "error", "reason": str(e)})

    ok = sum(1 for r in results if r.get("status") == "ok")
    skip = sum(1 for r in results if r.get("status") == "skip")
    err = sum(1 for r in results if r.get("status") == "error")
    total_rows = sum(r.get("rows", 0) for r in results)
    print(f"\n{'='*60}")
    print(f"DONE: {ok} ok, {skip} skip, {err} error | {total_rows} total rows upserted")


if __name__ == "__main__":
    main()
