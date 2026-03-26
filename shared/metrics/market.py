# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 22:27:17 2026

@author: dipeng.chen
"""

import os
import pandas as pd
from sqlalchemy import create_engine


def get_price_series():

    pgurl = os.getenv("DB_DSN")

    engine = create_engine(pgurl)

    df = pd.read_sql(
        """
        SELECT timestamp, price
        FROM marketdata.prices
        ORDER BY timestamp DESC
        LIMIT 48
        """,
        engine,
    )

    return df.sort_values("timestamp")