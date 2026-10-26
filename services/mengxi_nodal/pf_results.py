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
        SELECT data_date, node_name, revenue_cny, discharge_mwh
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
    """Aggregate daily rows into per-node totals + per-node-month breakdown.

    Returns:
      totals_df:  [rank, node_name, rev_per_mw, rev_per_mwh, total_profit_cny,
                   total_discharge_mwh] — ranked by rev_per_mwh desc (¥ per MWh
                   discharged; the market-common spread-capture metric)
      monthly_df: index=node_name, columns=YYYY-MM, values=¥/MWh discharged
                  (zero-filled where a month has no discharge)
    """
    if df.empty:
        return (pd.DataFrame(columns=["rank", "node_name", "rev_per_mw", "rev_per_mwh",
                                      "total_profit_cny", "total_discharge_mwh"]),
                pd.DataFrame())
    df = df.copy()
    df["data_date"] = pd.to_datetime(df["data_date"])
    df["month"] = df["data_date"].dt.to_period("M").astype(str)

    totals = (df.groupby("node_name")
                .agg(total_profit_cny=("revenue_cny", "sum"),
                     total_discharge_mwh=("discharge_mwh", "sum"))
                .reset_index())
    totals["rev_per_mw"] = totals["total_profit_cny"] / power_mw
    totals["rev_per_mwh"] = (totals["total_profit_cny"]
                             / totals["total_discharge_mwh"].replace(0, pd.NA)).fillna(0.0)
    totals = totals.sort_values(["rev_per_mwh", "node_name"], ascending=[False, True]).reset_index(drop=True)
    totals["rank"] = range(1, len(totals) + 1)
    totals = totals[["rank", "node_name", "rev_per_mw", "rev_per_mwh",
                     "total_profit_cny", "total_discharge_mwh"]]

    msum = df.groupby(["node_name", "month"]).agg(
        rev=("revenue_cny", "sum"), mwh=("discharge_mwh", "sum"))
    monthly = (msum["rev"] / msum["mwh"].replace(0, pd.NA)).unstack(fill_value=None)
    monthly = monthly.fillna(0.0)
    monthly = monthly.reindex(columns=sorted(monthly.columns), fill_value=0.0)
    return totals, monthly
