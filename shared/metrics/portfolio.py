# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 22:22:56 2026

@author: dipeng.chen
"""

import os
import pandas as pd
from sqlalchemy import create_engine


def get_portfolio_metrics():

    pgurl = os.getenv("DB_DSN")

    engine = create_engine(pgurl)

    df = pd.read_sql(
        """
        SELECT
            SUM(pnl) AS total_pnl,
            SUM(CASE WHEN date = CURRENT_DATE THEN pnl ELSE 0 END) AS today_pnl,
            COUNT(DISTINCT asset_id) AS assets
        FROM marketdata.portfolio_results
        """,
        engine,
    )

    return df.iloc[0].to_dict()