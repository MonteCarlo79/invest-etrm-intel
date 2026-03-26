# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 22:23:51 2026

@author: dipeng.chen
"""

import os
import pandas as pd
from sqlalchemy import create_engine


def get_agent_status():

    pgurl = os.getenv("DB_DSN")

    engine = create_engine(pgurl)

    df = pd.read_sql(
        """
        SELECT
            agent,
            MAX(run_time) AS last_run,
            MAX(status) AS status
        FROM marketdata.agent_runs
        GROUP BY agent
        ORDER BY agent
        """,
        engine,
    )

    return df