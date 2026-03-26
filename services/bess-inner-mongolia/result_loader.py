# -*- coding: utf-8 -*-
"""
Created on Sun Mar 22 22:36:07 2026

@author: dipeng.chen
"""

from __future__ import annotations

from sqlalchemy.engine import Engine
import pandas as pd

from services.bess_inner_mongolia.queries import (
    load_station_master,
    load_inner_mongolia_results,
    load_nodal_clusters,
    load_focused_assets_data,
)


def get_station_master(engine: Engine) -> pd.DataFrame:
    return load_station_master(engine)


def get_results(
    engine: Engine,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    return load_inner_mongolia_results(engine, start_date=start_date, end_date=end_date)


def get_clusters(engine: Engine) -> pd.DataFrame:
    return load_nodal_clusters(engine)


def get_focused_assets(engine: Engine, province: str | None = None) -> pd.DataFrame:
    return load_focused_assets_data(engine, province=province)