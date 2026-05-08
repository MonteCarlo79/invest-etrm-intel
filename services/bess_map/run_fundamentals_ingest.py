from __future__ import annotations

"""
Ingest province-level market fundamentals from 15-min Excel files into
marketdata.spot_fundamentals_hourly.

Source files: data/market-fundamentals/各省现货价格及边界数据/<province>.xlsx
Column structure: date col, time col (HHMM int), then fundamentals columns.
Time is 15-min interval; we aggregate to hourly (mean) before inserting.

Usage:
    python run_fundamentals_ingest.py \
        --indir "data/market-fundamentals/各省现货价格及边界数据" \
        --env none --schema marketdata --continue-on-error
"""

import argparse
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2
import psycopg2.extras


# ---------------------------------------------------------------------------
# Column keyword detection
# ---------------------------------------------------------------------------

# group_key → (Chinese keyword substring, DB column prefix)
# DB column is: <db_prefix>_mw / <db_prefix>_d1_mw / etc.
COLUMN_GROUPS = {
    "load":         ("省内负荷",   "load"),
    "net_export":   ("净外送",     "net_export"),
    "renewable":    ("新能源总出力", "renewable_total"),  # 新能源总出力量 and 新能源总出力
    "bidding":      ("竞价空间",   "bidding_space"),      # 竞价空间(MW) and 竞价空间(B区合计)
    "wind":         ("风电",       "wind"),
    "solar":        ("光伏",       "solar"),
}

# suffix_key → suffix keyword and the DB column suffix it maps to
SUFFIX_SPECS = [
    ("实时",   "_mw"),
    ("D-1",   "_d1_mw"),
    ("D-2",   "_d2_mw"),
    ("D-3",   "_d3_mw"),
]

# Derived full mapping: (group_key, suffix_key) → db_col_name
_FULL_MAP: dict[tuple[str, str], str] = {}
for _g, (_, _db_prefix) in COLUMN_GROUPS.items():
    for _sfx, _db_sfx in SUFFIX_SPECS:
        _FULL_MAP[(_g, _sfx)] = f"{_db_prefix}{_db_sfx}"
# e.g. ("load", "实时") → "load_mw"
#       ("bidding", "实时") → "bidding_space_mw"
#       ("renewable", "D-1") → "renewable_total_d1_mw"


def _detect_fundamentals_cols(xlsx_path: Path) -> dict[str, str]:
    """
    Return {db_col_name: excel_col_name} for found fundamentals columns.
    Skips price columns (contain 价格 or 出清价).
    Missing combinations are simply absent from the returned dict → will be NULL.
    """
    df0 = pd.read_excel(xlsx_path, sheet_name=0, nrows=0)
    cols = [str(c).strip() for c in df0.columns]

    mapping: dict[str, str] = {}
    for (group_key, sfx_key), db_col in _FULL_MAP.items():
        group_kw = COLUMN_GROUPS[group_key][0]   # first element is the Chinese keyword
        for c in cols:
            # Skip price columns
            if "价格" in c or "出清价" in c or ("价" in c and "出力" not in c and "负荷" not in c and "空间" not in c):
                continue
            if group_kw in c and sfx_key in c:
                mapping[db_col] = c
                break  # take first match per (group, suffix)
    return mapping


# ---------------------------------------------------------------------------
# Province name extraction (same logic as run_all_provinces.py)
# ---------------------------------------------------------------------------

def _clean_province_from_stem(stem: str) -> str:
    s = stem.strip()
    s = re.sub(r"^\d+[.\s]*\d*[.\s]*", "", s)  # strip leading numbers like "5.1 "
    s = re.sub(r"[^\u4e00-\u9fa5]", "", s)      # keep only Chinese chars
    return s


# ---------------------------------------------------------------------------
# Time parsing: HHMM integer → (hour, minute)
# ---------------------------------------------------------------------------

def _hhmm_to_hm(val) -> tuple[int, int]:
    """Convert Excel time value (int HHMM or float or time) to (hour, minute)."""
    if isinstance(val, (int, float)):
        hhmm = int(val)
        return hhmm // 100, hhmm % 100
    # sometimes openpyxl returns datetime.time
    if hasattr(val, "hour"):
        return val.hour, val.minute
    # string fallback e.g. "01:30"
    s = str(val).strip()
    if ":" in s:
        parts = s.split(":")
        return int(parts[0]), int(parts[1])
    hhmm = int(float(s))
    return hhmm // 100, hhmm % 100


def _parse_date(val) -> Optional[date]:
    """Parse date column value to a date object."""
    if pd.isna(val):
        return None
    if isinstance(val, (datetime, pd.Timestamp)):
        return val.date()
    if isinstance(val, date):
        return val
    s = str(val).strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y年%m月%d日"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


# ---------------------------------------------------------------------------
# Main per-file ingestion
# ---------------------------------------------------------------------------

def _ingest_one_file(
    xlsx_path: Path,
    province: str,
    col_map: dict[str, str],
    dsn: str,
    schema: str,
) -> int:
    """Read one file, aggregate 15-min → hourly, upsert into DB. Returns row count."""
    if not col_map:
        print(f"  [SKIP] {province}: no fundamentals columns detected")
        return 0

    # Read only needed columns: date (col0), time (col1), detected fundamentals cols
    usecols = [0, 1] + [col_map[k] for k in sorted(col_map)]
    df = pd.read_excel(xlsx_path, sheet_name=0)

    date_col = df.columns[0]
    time_col = df.columns[1]

    # Drop rows where date is missing
    df = df.dropna(subset=[date_col])

    # Parse date
    df["_date"] = df[date_col].apply(_parse_date)
    df = df.dropna(subset=["_date"])

    # Parse time → floor to hour
    def _to_hour(v):
        try:
            h, _ = _hhmm_to_hm(v)
            return h
        except Exception:
            return None

    df["_hour"] = df[time_col].apply(_to_hour)
    df = df.dropna(subset=["_hour"])
    df["_hour"] = df["_hour"].astype(int)

    # Build datetime at hour start (Asia/Shanghai)
    df["_dt"] = df.apply(
        lambda r: datetime.combine(r["_date"], datetime.min.time())
                  + timedelta(hours=int(r["_hour"])),
        axis=1,
    )

    # Aggregate 15-min → hourly mean for each fundamentals column
    agg_cols = {db_col: excel_col for db_col, excel_col in col_map.items()}
    rename_map = {v: k for k, v in agg_cols.items()}

    fund_df = df[["_dt"] + list(agg_cols.values())].copy()
    fund_df = fund_df.rename(columns=rename_map)
    for c in fund_df.columns:
        if c != "_dt":
            fund_df[c] = pd.to_numeric(fund_df[c], errors="coerce")

    hourly = fund_df.groupby("_dt", as_index=False).mean(numeric_only=True)
    hourly["province"] = province

    # Ensure table exists
    _ensure_table(dsn, schema)

    # Upsert
    all_db_cols = [
        "load_mw", "load_d1_mw", "load_d2_mw", "load_d3_mw",
        "net_export_mw", "net_export_d1_mw", "net_export_d2_mw", "net_export_d3_mw",
        "renewable_total_mw", "renewable_d1_mw", "renewable_d2_mw", "renewable_d3_mw",
        "bidding_space_mw", "bidding_space_d1_mw", "bidding_space_d2_mw", "bidding_space_d3_mw",
        "wind_mw", "wind_d1_mw", "wind_d2_mw", "wind_d3_mw",
        "solar_mw", "solar_d1_mw", "solar_d2_mw", "solar_d3_mw",
    ]
    present_cols = [c for c in all_db_cols if c in hourly.columns]
    insert_cols = ["province", "datetime"] + present_cols
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in present_cols)

    sql = f"""
        INSERT INTO {schema}.spot_fundamentals_hourly ({", ".join(insert_cols)})
        VALUES %s
        ON CONFLICT (province, datetime) DO UPDATE SET {update_clause}
    """

    rows = []
    for _, row in hourly.iterrows():
        dt_tz = row["_dt"].to_pydatetime() if hasattr(row["_dt"], "to_pydatetime") else row["_dt"]
        vals = [province, dt_tz] + [
            None if pd.isna(row[c]) else float(row[c])
            for c in present_cols
        ]
        rows.append(tuple(vals))

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
        conn.commit()

    print(f"  [OK] {province}: {len(rows)} hourly rows upserted ({len(present_cols)} columns)")
    return len(rows)


# ---------------------------------------------------------------------------
# DB schema migration (idempotent)
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.spot_fundamentals_hourly (
    province             TEXT             NOT NULL,
    datetime             TIMESTAMPTZ      NOT NULL,
    load_mw              DOUBLE PRECISION,
    load_d1_mw           DOUBLE PRECISION,
    load_d2_mw           DOUBLE PRECISION,
    load_d3_mw           DOUBLE PRECISION,
    net_export_mw        DOUBLE PRECISION,
    net_export_d1_mw     DOUBLE PRECISION,
    net_export_d2_mw     DOUBLE PRECISION,
    net_export_d3_mw     DOUBLE PRECISION,
    renewable_total_mw   DOUBLE PRECISION,
    renewable_d1_mw      DOUBLE PRECISION,
    renewable_d2_mw      DOUBLE PRECISION,
    renewable_d3_mw      DOUBLE PRECISION,
    bidding_space_mw     DOUBLE PRECISION,
    bidding_space_d1_mw  DOUBLE PRECISION,
    bidding_space_d2_mw  DOUBLE PRECISION,
    bidding_space_d3_mw  DOUBLE PRECISION,
    wind_mw              DOUBLE PRECISION,
    wind_d1_mw           DOUBLE PRECISION,
    wind_d2_mw           DOUBLE PRECISION,
    wind_d3_mw           DOUBLE PRECISION,
    solar_mw             DOUBLE PRECISION,
    solar_d1_mw          DOUBLE PRECISION,
    solar_d2_mw          DOUBLE PRECISION,
    solar_d3_mw          DOUBLE PRECISION,
    PRIMARY KEY (province, datetime)
);
CREATE INDEX IF NOT EXISTS idx_sfh_datetime
    ON {schema}.spot_fundamentals_hourly (datetime);
"""


def _ensure_table(dsn: str, schema: str) -> None:
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL.format(schema=schema))
        conn.commit()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _get_dsn(env_path: Optional[str]) -> str:
    if env_path and env_path.lower() != "none":
        from dotenv import load_dotenv
        load_dotenv(env_path)
    dsn = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not dsn:
        raise RuntimeError(
            "No DB DSN found. Set PGURL env var or pass --env /path/to/.env"
        )
    return dsn


def main() -> None:
    p = argparse.ArgumentParser(
        description="Ingest market fundamentals from province Excel files into DB."
    )
    p.add_argument("--indir", required=True, help="Folder containing province .xlsx files")
    p.add_argument("--env", default=None, help='Path to .env file, or "none" to use PGURL env var')
    p.add_argument("--schema", default="marketdata", help="DB schema (default: marketdata)")
    p.add_argument("--continue-on-error", action="store_true",
                   help="Continue processing when one province file fails")
    p.add_argument("--only-files", default=None,
                   help="Comma-separated filenames to process, e.g. '山东.xlsx,广东.xlsx'")
    p.add_argument("--dry-run", action="store_true",
                   help="Detect columns and report, but do not write to DB")
    args = p.parse_args()

    dsn = _get_dsn(args.env) if not args.dry_run else None

    indir = Path(args.indir)
    if not indir.exists():
        raise FileNotFoundError(f"--indir not found: {indir}")

    if args.only_files:
        names = {n.strip() for n in args.only_files.split(",")}
        xlsx_files = [indir / n for n in names if (indir / n).exists()]
    else:
        xlsx_files = sorted(indir.glob("*.xlsx"))

    if not xlsx_files:
        print(f"[WARN] No .xlsx files found under {indir}")
        return

    # Ensure table exists (once, non-dry-run only)
    if not args.dry_run:
        if not dsn:
            raise RuntimeError("DB DSN required for non-dry-run. Pass --env or set PGURL.")
        _ensure_table(dsn, args.schema)
        print(f"[DB] Table {args.schema}.spot_fundamentals_hourly ready.")

    ok, failed = 0, 0
    for xlsx in xlsx_files:
        province = _clean_province_from_stem(xlsx.stem)
        if not province:
            print(f"[SKIP] Could not extract province from filename: {xlsx.name}")
            continue

        print(f"[FILE] {xlsx.name}  →  province={province}")

        try:
            col_map = _detect_fundamentals_cols(xlsx)
            if args.dry_run:
                print(f"  [DRY] detected {len(col_map)} columns: {list(col_map.keys())}")
                ok += 1
                continue

            if not col_map:
                print(f"  [SKIP] no fundamentals columns found — skipping")
                ok += 1
                continue

            _ingest_one_file(xlsx, province, col_map, dsn, args.schema)
            ok += 1

        except Exception as e:
            failed += 1
            print(f"  [FAIL] {province}: {type(e).__name__}: {e}")
            if not args.continue_on_error:
                print("[ABORT] Use --continue-on-error to skip failed files.")
                break

    print(f"\n[DONE] ok={ok}, failed={failed}")
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
