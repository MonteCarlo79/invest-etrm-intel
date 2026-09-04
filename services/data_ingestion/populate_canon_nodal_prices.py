"""
services/data_ingestion/populate_canon_nodal_prices.py

ETL: populate canon.nodal_rt_price_15min_id_cleared (backing table for the
4 Inner Mongolia BESS assets) from marketdata.md_id_cleared_energy, then
recreate the canon.nodal_rt_price_15min view to include all assets.

Background
----------
canon.nodal_rt_price_15min is a UNION view over per-asset hist_mengxi_*_clear_15min
tables.  The 4 IM BESS assets (suyou/hangjinqi/siziwangqi/gushanliang) are
sourced from md_id_cleared_energy.cleared_price, written to the backing table:

  canon.nodal_rt_price_15min_id_cleared   ← cleared_price (upserted)

The view is then recreated to include:
  - wuhai, wulate, wulanchabu     from hist_mengxi_*_clear_15min (unchanged)
  - suyou, hangjinqi, siziwangqi, gushanliang  from the new backing table

Note: canon.scenario_dispatch_15min is already a live view over
md_id_cleared_energy — no ETL needed for dispatch.

Usage:
  # Backfill
  py services/data_ingestion/populate_canon_nodal_prices.py --start-date 2026-03-01 --end-date 2026-04-23

  # Daily (yesterday)
  py services/data_ingestion/populate_canon_nodal_prices.py
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# dispatch_unit_name → asset_code
DISPATCH_UNIT_MAP = {
    "景蓝乌尔图储能电站": "suyou",
    "悦杭独贵储能电站":   "hangjinqi",
    "景通四益堂储能电站": "siziwangqi",
    "裕昭沙子坝储能电站": "gushanliang",
}

# Physical backing table for IM-asset nodal prices
_PRICE_TABLE = "canon.nodal_rt_price_15min_id_cleared"

# View DDL — keeps wuhai/wulate/wulanchabu from their hist tables;
# replaces suyou and adds the 3 new IM assets from the backing table.
_VIEW_DDL = """\
CREATE OR REPLACE VIEW canon.nodal_rt_price_15min AS
  SELECT "time"::timestamptz AS "time",
         'wuhai'::text       AS asset_code,
         price::numeric      AS price
    FROM hist_mengxi_wuhai_clear_15min
   WHERE "time" IS NOT NULL
UNION ALL
  SELECT "time"::timestamptz,
         'wulate'::text,
         price::numeric
    FROM hist_mengxi_wulate_clear_15min
   WHERE "time" IS NOT NULL
UNION ALL
  SELECT "time"::timestamptz,
         'wulanchabu'::text,
         price::numeric
    FROM hist_mengxi_wulanchabu_clear_15min
   WHERE "time" IS NOT NULL
UNION ALL
  SELECT "time",
         asset_code,
         price::numeric
    FROM canon.nodal_rt_price_15min_id_cleared
"""


def get_engine():
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not url:
        raise SystemExit("PGURL environment variable is required")
    return create_engine(url, pool_pre_ping=True)


def load_id_cleared(engine, start_date: date, end_date: date) -> pd.DataFrame:
    """Load cleared price for the 4 IM assets from md_id_cleared_energy."""
    unit_names = list(DISPATCH_UNIT_MAP.keys())
    sql = text("""
        SELECT
            datetime                    AS time,
            dispatch_unit_name,
            cleared_price::float        AS price
        FROM marketdata.md_id_cleared_energy
        WHERE dispatch_unit_name = ANY(:units)
          AND data_date BETWEEN :start_date AND :end_date
          AND cleared_price IS NOT NULL
        ORDER BY datetime
    """)
    df = pd.read_sql(sql, engine, params={
        "units": unit_names,
        "start_date": str(start_date),
        "end_date": str(end_date),
    })
    df["asset_code"] = df["dispatch_unit_name"].map(DISPATCH_UNIT_MAP)
    log.info("Loaded %d rows from md_id_cleared_energy (%s → %s)", len(df), start_date, end_date)
    return df


def ensure_backing_table(engine) -> None:
    """Create schema and backing table if they don't exist."""
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS canon"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {_PRICE_TABLE} (
                time        timestamptz NOT NULL,
                asset_code  text        NOT NULL,
                price       float       NOT NULL,
                PRIMARY KEY (time, asset_code)
            )
        """))


def recreate_view(engine) -> None:
    """Replace canon.nodal_rt_price_15min to include the IM backing table."""
    with engine.begin() as conn:
        conn.execute(text(_VIEW_DDL))
    log.info("canon.nodal_rt_price_15min view recreated")


def upsert(engine, df: pd.DataFrame, table: str, pk_cols: list[str]) -> int:
    if df.empty:
        return 0
    tmp = f"_tmp_{table.replace('.', '_')}_{os.getpid()}"
    schema, bare = table.split(".", 1)
    with engine.begin() as conn:
        df.to_sql(tmp, conn, schema=schema, if_exists="replace", index=False)
        cols = list(df.columns)
        insert_cols = ", ".join(cols)
        conflict_cols = ", ".join(pk_cols)
        update_set = ", ".join(
            f"{c} = EXCLUDED.{c}" for c in cols if c not in pk_cols
        )
        conn.execute(text(f"""
            INSERT INTO {table} ({insert_cols})
            SELECT {insert_cols} FROM {schema}."{tmp}"
            ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}
        """))
        conn.execute(text(f'DROP TABLE IF EXISTS {schema}."{tmp}"'))
    return len(df)


def run(start_date: date, end_date: date) -> None:
    engine = get_engine()
    ensure_backing_table(engine)

    df = load_id_cleared(engine, start_date, end_date)
    if df.empty:
        log.warning("No data found — nothing written.")
        return

    # Upsert prices into the backing table
    price_df = df[["time", "asset_code", "price"]].copy()
    n = upsert(engine, price_df, _PRICE_TABLE, ["time", "asset_code"])
    log.info("%s: %d rows upserted", _PRICE_TABLE, n)

    # Per-asset summary
    for asset in df["asset_code"].unique():
        sub = df[df["asset_code"] == asset]
        log.info("  %s: %d slots | price range ¥%.1f–%.1f",
                 asset, len(sub),
                 sub["price"].min(), sub["price"].max())

    # Recreate the view to include the backing table
    recreate_view(engine)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start-date", default=None, help="ISO date (default: yesterday)")
    p.add_argument("--end-date",   default=None, help="ISO date (default: yesterday)")
    args = p.parse_args()

    yesterday = date.today() - timedelta(days=1)
    start = date.fromisoformat(args.start_date) if args.start_date else yesterday
    end   = date.fromisoformat(args.end_date)   if args.end_date   else yesterday

    log.info("Populating canon nodal prices for %s → %s", start, end)
    run(start, end)


if __name__ == "__main__":
    main()
