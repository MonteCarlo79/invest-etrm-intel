# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 22:24:27 2026

@author: dipeng.chen
"""

import os
import pandas as pd
from sqlalchemy import create_engine


def get_dispatch_preview():

    pgurl = os.getenv("DB_DSN")

    engine = create_engine(pgurl)

    df = pd.read_sql(
        """
        SELECT
            timestamp,
            asset_id,
            charge_mw,
            discharge_mw,
            expected_profit
        FROM marketdata.execution_plan
        ORDER BY timestamp
        LIMIT 24
        """,
        engine,
    )

    return df