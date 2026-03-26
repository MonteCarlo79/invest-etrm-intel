# -*- coding: utf-8 -*-
"""
Created on Mon Feb  2 23:27:26 2026

@author: dipeng.chen
"""

import streamlit as st
import psycopg
import os

DB_DSN = os.getenv("PGURL")

if not DB_DSN:
    raise RuntimeError("PGURL not set")


st.title("BESS KPI Dashboard")

with psycopg.connect(DB_DSN) as conn:
    cur = conn.cursor()
    cur.execute("SELECT * FROM result.bess_daily_kpi LIMIT 20")
    rows = cur.fetchall()

st.write(rows)
