"""
Download Shanxi nodal prices from Fengxing API.

Download to CSV (fast, no DB writes):
    py scripts/download_shanxi_nodal.py --start 2026-01-01 --end 2026-05-12 --csv-only

Download directly to RDS:
    py scripts/download_shanxi_nodal.py --start 2026-01-01 --end 2026-05-12

Upload a previously saved CSV to RDS:
    py scripts/download_shanxi_nodal.py --from-csv shanxi_nodal_2026-01-01_2026-05-12.csv

Reads FENGXING_API_KEY and PGURL from config/.env (or environment).
"""
import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from dotenv import load_dotenv
load_dotenv(_REPO / "config" / ".env", override=False)


def _get_engine():
    from sqlalchemy import create_engine
    pgurl = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not pgurl:
        sys.exit("PGURL not set in config/.env")
    return create_engine(pgurl, pool_pre_ping=True)


def cmd_download(args):
    """Fetch from API day-by-day, save to CSV and/or RDS."""
    import csv

    api_key = os.environ.get("FENGXING_API_KEY")
    if not api_key:
        sys.exit("FENGXING_API_KEY not set in config/.env")

    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)

    csv_path = Path(args.out) if args.out else Path(f"shanxi_nodal_{start}_{end}.csv")

    from services.fengxing.nodal_price import (
        _fetch_day, init_table, upsert,
        _COLUMNS,
    )

    engine = None if args.csv_only else _get_engine()
    if engine:
        init_table(engine)

    fieldnames = _COLUMNS + ["avg_node_price"]
    total_rows = 0
    failed = []

    mode = "CSV only" if args.csv_only else f"CSV + RDS"
    print(f"Downloading {start} → {end}  [{mode}]")
    print(f"Output CSV : {csv_path}\n")

    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        d = start
        while d <= end:
            try:
                rows = _fetch_day(d, api_key)
                writer.writerows(rows)
                fh.flush()
                if engine:
                    upsert(rows, engine)
                total_rows += len(rows)
                print(f"  ✅  {d}  {len(rows):,} rows")
            except Exception as exc:
                failed.append(str(d))
                print(f"  ❌  {d}  {exc}")
            d += timedelta(days=1)

    print(f"\nDone — {total_rows:,} rows written to {csv_path}"
          + (f"  |  {len(failed)} failed: {failed}" if failed else ""))
    if failed:
        sys.exit(1)


def cmd_upload(args):
    """Load a previously saved CSV into RDS."""
    import pandas as pd
    from services.fengxing.nodal_price import init_table, upsert

    csv_path = Path(args.from_csv)
    if not csv_path.exists():
        sys.exit(f"File not found: {csv_path}")

    engine = _get_engine()
    init_table(engine)

    print(f"Loading {csv_path} → RDS …")
    df = pd.read_csv(csv_path)
    rows = df.to_dict(orient="records")
    n = upsert(rows, engine)
    print(f"Done — {n:,} rows upserted.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start",    help="YYYY-MM-DD  (required unless --from-csv)")
    parser.add_argument("--end",      help="YYYY-MM-DD  (required unless --from-csv)")
    parser.add_argument("--csv-only", action="store_true",
                        help="Save to CSV only, skip RDS write")
    parser.add_argument("--out",      help="CSV output path (default: shanxi_nodal_START_END.csv)")
    parser.add_argument("--from-csv", dest="from_csv",
                        help="Skip download — load this CSV file into RDS")
    args = parser.parse_args()

    if args.from_csv:
        cmd_upload(args)
    elif args.start and args.end:
        cmd_download(args)
    else:
        parser.error("Provide --start and --end to download, or --from-csv to upload.")


if __name__ == "__main__":
    main()
