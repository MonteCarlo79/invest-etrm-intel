# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 17:47:39 2026

@author: dipeng.chen
"""

import pandas as pd
from sqlalchemy import create_engine
import os


def build_training_dataset():

    pgurl = os.getenv("PGURL")

    engine = create_engine(pgurl)

    df = pd.read_sql(
        """
        SELECT
            timestamp,
            price,
            wind_forecast,
            load_forecast
        FROM marketdata.training_features
        ORDER BY timestamp
        """,
        engine,
    )

    return df