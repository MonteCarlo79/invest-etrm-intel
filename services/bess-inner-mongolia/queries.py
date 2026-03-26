# -*- coding: utf-8 -*-
"""
Created on Sun Mar 22 22:35:20 2026

@author: dipeng.chen
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def load_station_master(engine: Engine) -> pd.DataFrame:
    query = text("""
        SELECT *
        FROM marketdata.station_master
        ORDER BY station_name
    """)
    return pd.read_sql(query, engine)


def load_inner_mongolia_results(
    engine: Engine,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    base_sql = """
        SELECT *
        FROM marketdata.inner_mongolia_bess_results
        WHERE 1=1
    """
    params: dict[str, object] = {}

    if start_date:
        base_sql += " AND trade_date >= :start_date"
        params["start_date"] = start_date

    if end_date:
        base_sql += " AND trade_date <= :end_date"
        params["end_date"] = end_date

    base_sql += " ORDER BY trade_date DESC, station_name"

    return pd.read_sql(text(base_sql), engine, params=params)


def load_nodal_clusters(engine: Engine) -> pd.DataFrame:
    query = text("""
        SELECT *
        FROM marketdata.inner_mongolia_nodal_clusters
        ORDER BY cluster_id, node_name
    """)
    return pd.read_sql(query, engine)


def load_focused_assets_data(
    engine: Engine,
    province: str | None = None,
) -> pd.DataFrame:
    base_sql = """
        SELECT *
        FROM marketdata.focused_assets_data
        WHERE 1=1
    """
    params: dict[str, object] = {}

    if province:
        base_sql += " AND province = :province"
        params["province"] = province

    base_sql += " ORDER BY as_of_date DESC, asset_name"

    return pd.read_sql(text(base_sql), engine, params=params)