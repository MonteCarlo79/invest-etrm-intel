# -*- coding: utf-8 -*-
"""
init_results_table.py

Creates the schema + tables required by the Inner Mongolia BESS pipeline.

Key design choice:
- Store the full per-plant result row as JSONB to avoid losing columns / formats as the analysis evolves.
- Keep a few typed columns for fast filtering/querying.

Tables:
- marketdata.inner_mongolia_bess_results
- marketdata.inner_mongolia_nodal_clusters
"""

from sqlalchemy import create_engine, text
import os

def create_schema_and_table():
    pgurl = os.getenv("PGURL")
    if not pgurl:
        raise ValueError("PGURL not provided")

    engine = create_engine(pgurl)

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS marketdata;"))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS marketdata.inner_mongolia_bess_results (
                plant_name TEXT NOT NULL,
                owner TEXT,
                mw NUMERIC,
                irr NUMERIC,
                payback_years NUMERIC,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                result_json JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (plant_name, start_date, end_date)
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS marketdata.inner_mongolia_nodal_clusters (
                plant_name TEXT NOT NULL,
                signature BIGINT,
                cluster_id INTEGER,
                cluster_size INTEGER,
                asset_type TEXT,
                inferred_mw NUMERIC,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (plant_name, start_date, end_date)
            );
        """))

if __name__ == "__main__":
    create_schema_and_table()
