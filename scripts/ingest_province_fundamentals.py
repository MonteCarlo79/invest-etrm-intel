"""
Ingest province-level market fundamentals from the Excel file into
marketdata.province_fundamentals.

Source: data/market-fundamentals/2023-2025 全国各省电力市场基础信息汇总*.xlsx
        (parsed by services/market_fundamentals/loader.py)

Usage:
    python scripts/ingest_province_fundamentals.py [--env path/to/.env]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

try:
    from dotenv import load_dotenv
    for _f in [_REPO / "config" / ".env", _REPO / ".env"]:
        if _f.exists():
            load_dotenv(_f)
except ImportError:
    pass

import psycopg2
import psycopg2.extras

from services.market_fundamentals.loader import load_province_data

_DDL = """
CREATE TABLE IF NOT EXISTS marketdata.province_fundamentals (
    province_cn          TEXT             NOT NULL,
    province_en          TEXT             NOT NULL,
    year                 INT              NOT NULL,
    wind_cap_10kw        DOUBLE PRECISION,
    solar_cap_10kw       DOUBLE PRECISION,
    thermal_cap_10kw     DOUBLE PRECISION,
    hydro_cap_10kw       DOUBLE PRECISION,
    nuclear_cap_10kw     DOUBLE PRECISION,
    storage_cap_10kw     DOUBLE PRECISION,
    wind_gen_100gwh      DOUBLE PRECISION,
    solar_gen_100gwh     DOUBLE PRECISION,
    thermal_gen_100gwh   DOUBLE PRECISION,
    hydro_gen_100gwh     DOUBLE PRECISION,
    nuclear_gen_100gwh   DOUBLE PRECISION,
    storage_gen_100gwh   DOUBLE PRECISION,
    peak_summer_mw       DOUBLE PRECISION,
    peak_winter_mw       DOUBLE PRECISION,
    peak_other_mw        DOUBLE PRECISION,
    updated_at           TIMESTAMPTZ      DEFAULT now(),
    PRIMARY KEY (province_cn, year)
);
CREATE INDEX IF NOT EXISTS idx_pf_year ON marketdata.province_fundamentals (year);
"""

_UPSERT = """
INSERT INTO marketdata.province_fundamentals (
    province_cn, province_en, year,
    wind_cap_10kw, solar_cap_10kw, thermal_cap_10kw, hydro_cap_10kw,
    nuclear_cap_10kw, storage_cap_10kw,
    wind_gen_100gwh, solar_gen_100gwh, thermal_gen_100gwh, hydro_gen_100gwh,
    nuclear_gen_100gwh, storage_gen_100gwh,
    peak_summer_mw, peak_winter_mw, peak_other_mw, updated_at
) VALUES %s
ON CONFLICT (province_cn, year) DO UPDATE SET
    province_en       = EXCLUDED.province_en,
    wind_cap_10kw     = EXCLUDED.wind_cap_10kw,
    solar_cap_10kw    = EXCLUDED.solar_cap_10kw,
    thermal_cap_10kw  = EXCLUDED.thermal_cap_10kw,
    hydro_cap_10kw    = EXCLUDED.hydro_cap_10kw,
    nuclear_cap_10kw  = EXCLUDED.nuclear_cap_10kw,
    storage_cap_10kw  = EXCLUDED.storage_cap_10kw,
    wind_gen_100gwh   = EXCLUDED.wind_gen_100gwh,
    solar_gen_100gwh  = EXCLUDED.solar_gen_100gwh,
    thermal_gen_100gwh= EXCLUDED.thermal_gen_100gwh,
    hydro_gen_100gwh  = EXCLUDED.hydro_gen_100gwh,
    nuclear_gen_100gwh= EXCLUDED.nuclear_gen_100gwh,
    storage_gen_100gwh= EXCLUDED.storage_gen_100gwh,
    peak_summer_mw    = EXCLUDED.peak_summer_mw,
    peak_winter_mw    = EXCLUDED.peak_winter_mw,
    peak_other_mw     = EXCLUDED.peak_other_mw,
    updated_at        = now()
"""


def _cap(d: dict, key: str):
    return d.get(key, {}).get("value")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default=None)
    ap.add_argument("--schema", default="marketdata")
    args = ap.parse_args()

    if args.env and args.env.lower() != "none":
        from dotenv import load_dotenv as _ld
        _ld(args.env)

    dsn = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not dsn:
        sys.exit("ERROR: Set PGURL env var or pass --env /path/.env")

    print("Loading province data from Excel...")
    data = load_province_data()
    if not data:
        sys.exit("ERROR: No province data loaded — check that the Excel file exists in data/market-fundamentals/")

    print(f"  Loaded {len(data)} provinces")

    rows = []
    for pcn, info in data.items():
        pen = info["province_en"]
        for year in (2024, 2025):
            cap = info["capacity"].get(year, {})
            gen = info["generation"].get(year, {})
            pl  = info["peak_load"].get(year, {})
            rows.append((
                pcn, pen, year,
                _cap(cap, "风电"), _cap(cap, "光伏"), _cap(cap, "火电"),
                _cap(cap, "水电"), _cap(cap, "核电"), _cap(cap, "储能"),
                _cap(gen, "风电"), _cap(gen, "光伏"), _cap(gen, "火电"),
                _cap(gen, "水电"), _cap(gen, "核电"), _cap(gen, "储能"),
                pl.get("summer"), pl.get("winter"), pl.get("other"),
                "now()",
            ))

    # Replace the string "now()" placeholder with None — psycopg2 will use DEFAULT
    rows_clean = [r[:-1] for r in rows]  # drop the updated_at; let DEFAULT handle it

    upsert_sql = _UPSERT.replace(", updated_at", "").replace(", EXCLUDED.updated_at\n", "\n").replace(
        ",\n    updated_at        = now()", ""
    )
    # Rebuild cleanly:
    upsert_sql = """
INSERT INTO {schema}.province_fundamentals (
    province_cn, province_en, year,
    wind_cap_10kw, solar_cap_10kw, thermal_cap_10kw, hydro_cap_10kw,
    nuclear_cap_10kw, storage_cap_10kw,
    wind_gen_100gwh, solar_gen_100gwh, thermal_gen_100gwh, hydro_gen_100gwh,
    nuclear_gen_100gwh, storage_gen_100gwh,
    peak_summer_mw, peak_winter_mw, peak_other_mw
) VALUES %s
ON CONFLICT (province_cn, year) DO UPDATE SET
    province_en        = EXCLUDED.province_en,
    wind_cap_10kw      = EXCLUDED.wind_cap_10kw,
    solar_cap_10kw     = EXCLUDED.solar_cap_10kw,
    thermal_cap_10kw   = EXCLUDED.thermal_cap_10kw,
    hydro_cap_10kw     = EXCLUDED.hydro_cap_10kw,
    nuclear_cap_10kw   = EXCLUDED.nuclear_cap_10kw,
    storage_cap_10kw   = EXCLUDED.storage_cap_10kw,
    wind_gen_100gwh    = EXCLUDED.wind_gen_100gwh,
    solar_gen_100gwh   = EXCLUDED.solar_gen_100gwh,
    thermal_gen_100gwh = EXCLUDED.thermal_gen_100gwh,
    hydro_gen_100gwh   = EXCLUDED.hydro_gen_100gwh,
    nuclear_gen_100gwh = EXCLUDED.nuclear_gen_100gwh,
    storage_gen_100gwh = EXCLUDED.storage_gen_100gwh,
    peak_summer_mw     = EXCLUDED.peak_summer_mw,
    peak_winter_mw     = EXCLUDED.peak_winter_mw,
    peak_other_mw      = EXCLUDED.peak_other_mw,
    updated_at         = now()
""".format(schema=args.schema)

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL.replace("marketdata", args.schema))
        conn.commit()
        print(f"[DB] Table {args.schema}.province_fundamentals ready.")

        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, upsert_sql, rows_clean, page_size=500)
        conn.commit()

    print(f"[DONE] {len(rows_clean)} rows upserted ({len(data)} provinces × 2 years).")


if __name__ == "__main__":
    main()
