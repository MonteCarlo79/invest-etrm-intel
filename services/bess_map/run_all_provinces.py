from __future__ import annotations

import argparse
import re
import sys
import runpy
from pathlib import Path
from typing import Optional, Tuple

try:
    import pandas as pd
except Exception:
    pd = None

import os
import psycopg
from datetime import datetime


## debug########
print("RUN_ALL_PROVINCES VERSION 2026-02-22")
#################


def get_db_dsn(env_path: str | None):
    if not env_path:
        return None
    from dotenv import load_dotenv
    load_dotenv(env_path)
    return os.getenv("PGURL")

def get_processed_files(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS audit.processed_files (
                file_name TEXT PRIMARY KEY,
                processed_at TIMESTAMPTZ DEFAULT now()
            )
        """)
        cur.execute("SELECT file_name FROM audit.processed_files")
        return {r[0] for r in cur.fetchall()}

def mark_file_processed(conn, fname):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO audit.processed_files (file_name)
            VALUES (%s)
            ON CONFLICT (file_name) DO NOTHING;
        """, (fname,))
    conn.commit()

def _clean_province_from_stem(stem: str) -> str:
    s = stem.strip()
    s = re.sub(r"^\d+\s*[.\-_ ]*", "", s)
    s = re.sub(r"[^\u4e00-\u9fa5]", "", s)  # keep only Chinese chars
    return s

def _guess_cols_from_header(xlsx_path: Path, province: str) -> Tuple[str, str]:
    """ Guess (rt_col, da_col) from the header row."""
    if pd is None:
        raise RuntimeError("pandas is required for --auto-cols. Please `pip install pandas openpyxl` in your env.")

    df0 = pd.read_excel(xlsx_path, sheet_name=0, nrows=0)
    cols = [str(c).strip() for c in df0.columns]

    def pick(keyword: str) -> str:
        cands = [c for c in cols if (keyword in c and "价" in c)]
        if not cands:
            cands = [c for c in cols if keyword in c]
        if not cands:
            raise KeyError(f"Cannot find any column containing '{keyword}' in {xlsx_path.name}. Columns={cols[:10]}...")

        # prefer containing province
        with_prov = [c for c in cands if province in c]
        if with_prov:
            cands = with_prov

        # prefer "修正" versions if present
        fix = [c for c in cands if "修正" in c]
        if fix:
            cands = fix

        # stable pick: longest name (often most specific)
        cands = sorted(cands, key=lambda x: (-len(x), x))
        return cands[0]

    rt_col = pick("实时")
    da_col = pick("日前")
    return rt_col, da_col


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--indir", required=True, help="Folder with province XLSX files")
    p.add_argument("--price-type", choices=["rt", "da"], default="rt")
    p.add_argument("--duration-h", type=float, default=4.0)
    p.add_argument("--power-mw", type=float, default=1.0)
    p.add_argument("--roundtrip-eff", type=float, default=0.85)

    # Column selection strategies (choose one)
    p.add_argument("--rt-col", default=None, help="Exact RT column name (all files same)")
    p.add_argument("--da-col", default=None, help="Exact DA column name (all files same)")
    p.add_argument("--rt-col-template", default=None, help='Template, e.g. "{province}全省现货价格...实时价格"')
    p.add_argument("--da-col-template", default=None, help='Template, e.g. "{province}全省现货价格...日前价格"')
    p.add_argument("--auto-cols", action="store_true", help="Auto-guess RT/DA columns from header per file")

    p.add_argument("--outdir", default="outputs", help="Root output folder")
    p.add_argument("--upload-db", action="store_true")
    p.add_argument("--env", default=None, help="Path to .env for DB (only if --upload-db)")
    p.add_argument("--schema", default=None, help="DB schema override (only if --upload-db)")
    p.add_argument("--pattern", default="*.xlsx", help="Glob pattern under indir")
    p.add_argument("--continue-on-error", action="store_true", help="Continue processing when one file fails")
    p.add_argument("--upload-only", action="store_true", help="Only ingest prices and upload to DB; no dispatch/optimisation")
    p.add_argument("--only-files", default=None, help="Comma-separated list of files to process (e.g. '山东.xlsx,广东.xlsx')")

    return p

def _resolve_cols(args, xlsx_path: Path, province: str) -> Tuple[str, str]:
    if args.auto_cols:
        return _guess_cols_from_header(xlsx_path, province)
    if args.rt_col_template and args.da_col_template:
        return args.rt_col_template.format(province=province), args.da_col_template.format(province=province)
    if args.rt_col and args.da_col:
        return args.rt_col, args.da_col
    raise ValueError("Need --auto-cols OR both templates OR both --rt-col/--da-col.")

def get_province_progress(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS audit;
            CREATE TABLE IF NOT EXISTS audit.province_progress (
                province TEXT NOT NULL,
                duration_h DOUBLE PRECISION NOT NULL,
                last_ts TIMESTAMPTZ,
                updated_at TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (province, duration_h)
            );

        """)
        cur.execute("""
            SELECT province, duration_h, last_ts
            FROM audit.province_progress
        """)
        return {(r[0], r[1]): r[2] for r in cur.fetchall()}

def update_province_progress(conn, province, duration_h, last_ts):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO audit.province_progress (province, duration_h, last_ts)
            VALUES (%s, %s, %s)
            ON CONFLICT (province, duration_h)
            DO UPDATE SET
              last_ts = EXCLUDED.last_ts,
              updated_at = now();
        """, (province, duration_h, last_ts))
    conn.commit()

def main() -> None:
    args = build_parser().parse_args()

    db_dsn = get_db_dsn(args.env) if args.upload_db else None
    
    print("DEBUG DB_DSN =", db_dsn)
    
    progress = {}

    if db_dsn:
        with psycopg.connect(db_dsn) as conn:
            progress = get_province_progress(conn)

    processed = set()

    indir = Path(args.indir)
    if not indir.exists():
        raise FileNotFoundError(f"--indir not found: {indir}")

    out_root = Path(args.outdir)
    out_root.mkdir(parents=True, exist_ok=True)

    this_dir = Path(__file__).resolve().parent
    one_script = this_dir / "run_one_province.py"
    if not one_script.exists():
        raise FileNotFoundError(f"Cannot find {one_script}. Please keep run_all_provinces.py in the same folder as run_one_province.py")

    # Ensure only the uploaded files are processed
    xlsx_files = []
    if args.only_files:
        names = {n.strip() for n in args.only_files.split(",")}
        xlsx_files = [indir / n for n in names if (indir / n).exists()]
    else:
        xlsx_files = sorted(indir.glob(args.pattern))
    
    if not xlsx_files:
        raise FileNotFoundError(f"No files matched {args.pattern} under {indir}")

    summary = []
    ok = 0
    failed = 0

    for xlsx in xlsx_files:
        province = _clean_province_from_stem(xlsx.stem)
        print(f"Processing file: {xlsx.name} for province: {province}")  # Debugging line

        try:
            # Compute latest timestamp in Excel (robust)
            if pd is None:
                raise RuntimeError("pandas required")

            df_tmp = pd.read_excel(xlsx)

            datetime_cols = [c for c in df_tmp.columns if "时间" in c or "datetime" in c.lower()]
            if datetime_cols:
                excel_max_ts = pd.to_datetime(df_tmp[datetime_cols[0]], errors="coerce").max()
            else:
                date_col = next((c for c in df_tmp.columns if "日期" in c), None)
                hour_col = next((c for c in df_tmp.columns if "时" in c), None)

                if date_col and hour_col:
                    ts_series = pd.to_datetime(
                        df_tmp[date_col].astype(str).str.strip() + " " +
                        df_tmp[hour_col].astype(str).str.strip(),
                        errors="coerce"
                    )
                    excel_max_ts = ts_series.max()
                else:
                    raise ValueError("No recognizable date/hour columns")

            excel_max_ts = excel_max_ts.tz_localize("Asia/Shanghai", nonexistent="shift_forward")

        except Exception as e:
            print(f"[WARN] Timestamp read failed for {xlsx.name}: {e}")
            excel_max_ts = None

        DUR_KEY = 0.0
        print(f"[DEBUG] {province}: Excel max ts = {excel_max_ts}, DB last_ts = {progress.get((province, DUR_KEY))}")


        if db_dsn and excel_max_ts is not None:
            last_ts = progress.get((province, DUR_KEY))

            if last_ts is not None and excel_max_ts <= last_ts:
                print(f"[SKIP] {province} {args.duration_h}h already up to date")
                continue

        try:
            rt_col, da_col = _resolve_cols(args, xlsx, province)

            argv = [
                str(one_script),
                "--xlsx", str(xlsx),
                "--price-type", args.price_type,
                "--rt-col", rt_col,
                "--da-col", da_col,
                "--outdir", str(out_root),
            ]

            if args.upload_db:
                argv.append("--upload-db")
                if args.env:
                    argv += ["--env", str(args.env)]
                if args.schema:
                    argv += ["--schema", str(args.schema)]
                    
            if args.upload_only:
                argv.append("--upload-only")

            print(f"[RUN] {province}: {xlsx.name}")
            _old_argv = sys.argv[:]
            sys.argv = argv
            try:
                runpy.run_path(str(one_script), run_name="__main__")
            finally:
                sys.argv = _old_argv

            ok += 1
            summary.append({"province": province, "file": xlsx.name, "status": "ok", "rt_col": rt_col, "da_col": da_col})
            if db_dsn and excel_max_ts is not None:
                with psycopg.connect(db_dsn) as conn:
                    update_province_progress(conn, province, DUR_KEY, excel_max_ts)

        except SystemExit as e:
            failed += 1
            msg = f"SystemExit={getattr(e, 'code', None)}"
            print(f"[FAIL] {province}: {msg}")
            summary.append({"province": province, "file": xlsx.name, "status": "fail", "error": msg})
            if not args.continue_on_error:
                break
        except Exception as e:
            failed += 1
            print(f"[FAIL] {province}: {type(e).__name__}: {e}")
            summary.append({"province": province, "file": xlsx.name, "status": "fail", "error": f"{type(e).__name__}: {e}"})
            if not args.continue_on_error:
                break

    try:
        if pd is not None:
            pd.DataFrame(summary).to_csv(out_root / "batch_summary.csv", index=False, encoding="utf-8-sig")
    except Exception:
        pass

    print(f"[DONE] ok={ok}, failed={failed}, out={out_root}")

if __name__ == "__main__":
    main()
