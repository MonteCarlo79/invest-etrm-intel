# services/nodal_forecast/db.py
"""DB contract for the nodal forecast layer. L1 (bess-map, parallel session)
writes nodal_fc_grid_daily; L2/L3 read it. LingFeng RT fallback when empty."""
from __future__ import annotations

from datetime import date

import pandas as pd

DDL = [
    """CREATE TABLE IF NOT EXISTS marketdata.nodal_fc_grid_daily (
        id BIGSERIAL PRIMARY KEY,
        province TEXT NOT NULL, fc_date DATE NOT NULL, target_date DATE NOT NULL,
        price_hat NUMERIC, model_version TEXT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (province, target_date, model_version))""",
    """CREATE TABLE IF NOT EXISTS marketdata.nodal_node_registry (
        name TEXT PRIMARY KEY, voltage_kv INT, substation TEXT,
        transformers INT, rated_mva NUMERIC, n1_firm_mva NUMERIC,
        zone TEXT, fengxing_node_name TEXT, connected_plants TEXT,
        source TEXT, updated_at TIMESTAMPTZ DEFAULT NOW())""",
    """CREATE TABLE IF NOT EXISTS marketdata.nodal_asset_registry (
        plant_name TEXT PRIMARY KEY, node TEXT, substation TEXT,
        capacity_mw NUMERIC, duration_h NUMERIC, rte_pct NUMERIC,
        zone TEXT, settle_node TEXT, active BOOLEAN DEFAULT TRUE,
        updated_at TIMESTAMPTZ DEFAULT NOW())""",
    """CREATE TABLE IF NOT EXISTS marketdata.nodal_strategy_daily (
        id BIGSERIAL PRIMARY KEY,
        plant_name TEXT NOT NULL, target_date DATE NOT NULL,
        curve_json TEXT NOT NULL, assumptions_json TEXT,
        iterations INT DEFAULT 1, convergence_delta_mwh NUMERIC,
        model_version TEXT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (plant_name, target_date, model_version))""",
]

_FC_UPSERT = """INSERT INTO marketdata.nodal_fc_grid_daily
    (province, fc_date, target_date, price_hat, model_version)
    VALUES (%(province)s, %(fc_date)s, %(target_date)s, %(price_hat)s, %(model_version)s)
    ON CONFLICT (province, target_date, model_version) DO UPDATE SET
        price_hat = EXCLUDED.price_hat, created_at = NOW()"""

_COLS = ["province", "target_date", "price_hat", "model_version"]


def ensure_tables(conn) -> None:
    cur = conn.cursor()
    for stmt in DDL:
        cur.execute(stmt)
    conn.commit()


def upsert_grid_forecast(conn, rows: list[dict]) -> int:
    today = date.today()
    rows = [{"fc_date": today, **r} for r in rows]
    cur = conn.cursor()
    cur.executemany(_FC_UPSERT, rows)
    conn.commit()
    return len(rows)


def _lingfeng_rt_daily(conn, province: str) -> list[tuple[date, float]]:
    """Naive fallback: latest 30 days of RT prices, averaged per day — used
    only when the L1 forecast table is empty for the province."""
    cur = conn.cursor()
    cur.execute(
        """SELECT datetime::date AS d, AVG(rt_price) AS p
           FROM marketdata.spot_prices_hourly
           WHERE province = %s AND datetime >= CURRENT_DATE - 30
           GROUP BY 1 ORDER BY 1""",
        (province,),
    )
    return [(r[0], float(r[1])) for r in cur.fetchall() if r[1] is not None]


def get_grid_forecast(conn, target_date: date, province: str = "蒙西") -> pd.DataFrame:
    cur = conn.cursor()
    cur.execute(
        """SELECT province, target_date, price_hat, model_version
           FROM marketdata.nodal_fc_grid_daily
           WHERE province = %s AND target_date = %s""",
        (province, target_date),
    )
    rows = cur.fetchall()
    if rows:
        return pd.DataFrame(rows, columns=_COLS)
    # fallback: repeat the most recent RT average as the forecast, flagged
    rt = _lingfeng_rt_daily(conn, province)
    if not rt:
        return pd.DataFrame(columns=_COLS)
    _, p = rt[-1]
    return pd.DataFrame([{"province": province, "target_date": target_date,
                          "price_hat": p, "model_version": "lingfeng_rt_fallback"}])
