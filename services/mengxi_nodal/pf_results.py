"""Precomputed nodal PF results — query + aggregation helpers for the Nodal Maps tab.

The batch job (scripts/run_nodal_pf_node_daily.py) writes one row per
(province, node, day) into reports.nodal_pf_node_daily at a fixed BESS config
(100 MW / 2h / 85% RTE). The app only reads + aggregates here — no LP in-session.
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
from sqlalchemy import text

_TABLE = "reports.nodal_pf_node_daily"

CONFIG = {"power_mw": 100.0, "duration_h": 2.0, "rte_pct": 85.0}


def get_pf_results(engine, province: str, start: date, end: date) -> pd.DataFrame:
    """Daily PF revenue rows for a province+range at the stored config."""
    q = text(f"""
        SELECT data_date, node_name, revenue_cny
        FROM {_TABLE}
        WHERE province = :prov
          AND power_mw = :pw AND duration_h = :dur AND rte_pct = :rte
          AND data_date BETWEEN :s AND :e
    """)
    return pd.read_sql(q, engine, params={
        "prov": province, "pw": CONFIG["power_mw"], "dur": CONFIG["duration_h"],
        "rte": CONFIG["rte_pct"], "s": start, "e": end,
    })


def get_latest_date(engine, province: str) -> date | None:
    q = text(f"""
        SELECT MAX(data_date) FROM {_TABLE}
        WHERE province = :prov AND power_mw = :pw AND duration_h = :dur AND rte_pct = :rte
    """)
    with engine.connect() as conn:
        return conn.execute(q, {"prov": province, "pw": CONFIG["power_mw"],
                                "dur": CONFIG["duration_h"], "rte": CONFIG["rte_pct"]}).scalar()


def aggregate_pf(df: pd.DataFrame, power_mw: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Aggregate daily rows into per-node totals + per-node-month breakdown (per MW).

    Returns:
      totals_df:  [rank, node_name, rev_per_mw, total_profit_cny] sorted by rev_per_mw desc
                    (ties broken by node_name asc for determinism)
      monthly_df: index=node_name, columns=YYYY-MM, values=revenue/power_mw (zero-filled)
    """
    if df.empty:
        return (pd.DataFrame(columns=["rank", "node_name", "rev_per_mw", "total_profit_cny"]),
                pd.DataFrame())
    df = df.copy()
    df["data_date"] = pd.to_datetime(df["data_date"])
    df["month"] = df["data_date"].dt.to_period("M").astype(str)

    totals = (df.groupby("node_name")["revenue_cny"].sum().reset_index()
              .rename(columns={"revenue_cny": "total_profit_cny"}))
    totals["rev_per_mw"] = totals["total_profit_cny"] / power_mw
    totals = totals.sort_values(["rev_per_mw", "node_name"], ascending=[False, True]).reset_index(drop=True)
    totals["rank"] = range(1, len(totals) + 1)
    totals = totals[["rank", "node_name", "rev_per_mw", "total_profit_cny"]]

    monthly = (df.groupby(["node_name", "month"])["revenue_cny"].sum().unstack(fill_value=0.0) / power_mw)
    monthly = monthly.reindex(columns=sorted(monthly.columns), fill_value=0.0)
    return totals, monthly
