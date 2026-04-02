# -*- coding: utf-8 -*-
"""
services/bess_inner_mongolia/queries.py
────────────────────────────────────────
DB read helpers used by the Strategy Diagnostics page and other
analytics consumers.

All functions accept a SQLAlchemy Engine and return DataFrames.
Result schema mirrors what inner_pipeline.py writes into:
  marketdata.inner_mongolia_bess_results  (JSONB column: result_json)
  marketdata.inner_mongolia_nodal_clusters
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Core result loader (expands result_json → flat DataFrame) ─────────────────

def load_results_flat(
    engine: Engine,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Load all BESS results for the given date window and expand the
    JSONB 'result_json' column into a flat DataFrame.

    Returns an empty DataFrame if no rows are found.

    Evidence level of returned data: **observed**
    (direct pipeline output computed from market-cleared prices/volumes).
    """
    q = text("""
        SELECT result_json
        FROM marketdata.inner_mongolia_bess_results
        WHERE start_date = :start AND end_date = :end
        ORDER BY plant_name
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"start": start_date, "end": end_date}).fetchall()

    if not rows:
        return pd.DataFrame()

    records = [r[0] for r in rows]
    df = pd.DataFrame.from_records(records)

    # Coerce numeric columns; leave text identifiers as-is
    _text_cols = {"plant_name", "owner"}
    for col in df.columns:
        if col not in _text_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def load_clusters(
    engine: Engine,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Load nodal cluster membership for the given date window.

    Returns columns:
      plant_name, signature, cluster_id, cluster_size, asset_type, inferred_mw

    Evidence level: **proxy-based**
    (cluster membership inferred from price-signature similarity, not from
    official grid topology disclosure).
    """
    q = text("""
        SELECT plant_name, signature, cluster_id, cluster_size,
               asset_type, inferred_mw
        FROM marketdata.inner_mongolia_nodal_clusters
        WHERE start_date = :start AND end_date = :end
    """)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"start": start_date, "end": end_date})
    return df


def load_available_date_ranges(engine: Engine) -> list[tuple[str, str]]:
    """
    Return distinct (start_date, end_date) pairs available in the results table,
    ordered most-recent first.  Drives the page period selector.
    """
    q = text("""
        SELECT DISTINCT start_date, end_date
        FROM marketdata.inner_mongolia_bess_results
        ORDER BY end_date DESC, start_date DESC
        LIMIT 24
    """)
    with engine.connect() as conn:
        rows = conn.execute(q).fetchall()
    return [(str(r[0]), str(r[1])) for r in rows]
