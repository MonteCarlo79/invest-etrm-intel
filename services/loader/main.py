# -*- coding: utf-8 -*-
"""
Created on Tue Feb  3 06:24:31 2026

@author: dipeng.chen
"""

import os
import glob
import pandas as pd
import psycopg

DB_DSN = os.environ["DB_DSN"]
UPLOAD_DIR = os.environ["UPLOAD_DIR"]

def main():
    files = glob.glob(os.path.join(UPLOAD_DIR, "*.xlsx"))
    if not files:
        print("No files to load")
        return

    for f in files:
        print(f"Loading {f}")
        df = pd.read_excel(f)

        # Example expected columns — we will adjust to your real file later
        df["ts"] = pd.to_datetime(df["ts"], utc=True)

        rows = [
            (r.site_id, r.ts.to_pydatetime(), r.price)
            for r in df.itertuples(index=False)
        ]

        with psycopg.connect(DB_DSN) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO marketdata.bess_15min(site_id, ts, price)
                    VALUES (%s,%s,%s)
                    """,
                    rows,
                )
            conn.commit()

        print(f"Loaded {len(rows)} rows from {f}")

if __name__ == "__main__":
    main()
