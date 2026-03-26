from __future__ import annotations

import pandas as pd
import numpy as np
import sys
import argparse
from pathlib import Path

THIS_DIR = str(Path(__file__).resolve().parent)
if THIS_DIR not in sys.path:
    sys.path.insert(0, THIS_DIR)

from utils import (
    infer_province_from_filename,
    load_prices_from_xlsx,
    to_hourly,
)

try:
    from db import (
        load_db_config, get_engine, ensure_tables, upsert_raw_timeseries,
        ensure_hourly_price_table, upsert_hourly_prices
    )
except Exception:
    load_db_config = get_engine = ensure_tables = upsert_raw_timeseries = ensure_hourly_price_table = upsert_hourly_prices = None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", required=True)
    ap.add_argument("--price-type", choices=["rt", "da"], default="rt")
    ap.add_argument("--duration-h", type=float, default=2.0)
    ap.add_argument("--power-mw", type=float, default=1.0)
    ap.add_argument("--roundtrip-eff", type=float, default=0.85)
    ap.add_argument("--rt-col", required=True)
    ap.add_argument("--da-col", required=True)
    ap.add_argument("--outdir", default="outputs")
    ap.add_argument("--upload-db", action="store_true")
    ap.add_argument("--env", default=".env")
    ap.add_argument("--schema", default=None)
    ap.add_argument("--upload-only", action="store_true")

    args = ap.parse_args()

    # Initialize file paths and province details
    xlsx_path = Path(args.xlsx).expanduser().resolve()
    province = infer_province_from_filename(xlsx_path)

    outdir = Path(args.outdir).expanduser().resolve() / province
    outdir.mkdir(parents=True, exist_ok=True)

    # Load prices from the provided Excel file
    prices = load_prices_from_xlsx(xlsx_path, rt_col=args.rt_col, da_col=args.da_col)
    
    # Convert the price data to hourly format
    hourly_rt = to_hourly(prices["rt"]).rename("rt_price")
    hourly_da = to_hourly(prices["da"]).rename("da_price")
    
    hourly_prices = pd.concat([hourly_rt, hourly_da], axis=1)
    hourly_prices.index.name = "datetime"
    
    # Save the hourly data in both wide and long formats
    hourly_fp = outdir / f"prices_hourly_rt_da.csv"
    hourly_prices.to_csv(hourly_fp)
    
    hourly_long = hourly_prices.reset_index()
    hourly_long["date"] = hourly_long["datetime"].dt.date
    hourly_long["hour"] = hourly_long["datetime"].dt.hour
    hourly_long_fp = outdir / f"prices_hourly_rt_da_long.csv"
    hourly_long.to_csv(hourly_long_fp, index=False)

    # If only uploading data (no optimisation), return early
    if args.upload_only:
        print(f"[INFO] {province} -> Upload-only mode, skipping optimisation and dispatch")
        if args.upload_db:
            # Upload hourly prices to the database
            if load_db_config is None:
                raise RuntimeError("db.py not available or DB dependencies missing.")
            cfg = load_db_config(args.env)
            schema = args.schema or cfg.schema
            eng = get_engine(cfg)
            ensure_tables(eng, schema=schema)
            
            ensure_hourly_price_table(eng, schema=schema)
            upsert_hourly_prices(
                eng, schema=schema, province=province,
                hourly_prices_df=hourly_prices,  # this is the dataframe with rt_price/da_price
                source_file=xlsx_path.name
            )

        print(f"[OK] {province} -> {outdir}")
        return

    # No optimisation or dispatch logic will be run if we are here,
    # so everything related to those calculations is removed.
    # Your previous code for storage optimisation and dispatch calculations is omitted here.

    print(f"[INFO] {province} -> All data processed and saved to {outdir}")

    # If uploading to DB is required
    if args.upload_db:
        if load_db_config is None:
            raise RuntimeError("db.py not available or DB dependencies missing.")
        cfg = load_db_config(args.env)
        schema = args.schema or cfg.schema
        eng = get_engine(cfg)
        ensure_tables(eng, schema=schema)
        
        # Ingest hourly price data into the database
        ensure_hourly_price_table(eng, schema=schema)
        upsert_hourly_prices(
            eng, schema=schema, province=province,
            hourly_prices_df=hourly_prices,  # this is the dataframe with rt_price/da_price
            source_file=xlsx_path.name
        )

    print(f"[OK] {province} -> {outdir}")

if __name__ == "__main__":
    main()
