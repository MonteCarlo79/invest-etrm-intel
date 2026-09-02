"""DB query helpers for the Mengxi nodal tab.

Thin wrappers: query → pandas. Business logic lives in analysis.py.
Timezone note: md_shanxi_nodal_price_96.metric_time is timestamptz (CST);
md_id_cleared_energy.datetime is naive CST wall clock.
"""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import text

_VIEW = "marketdata.md_mengxi_nodal_price_96"
_CLEARED = "marketdata.md_id_cleared_energy"


def _to_vec(df: pd.DataFrame, slot_col: str, price_col: str) -> np.ndarray:
    v = np.full(97, np.nan)
    for slot, price in zip(df[slot_col], df[price_col]):
        if 1 <= int(slot) <= 96:
            v[int(slot)] = price
    return v[1:]


def get_asset_price_vectors(engine, plant_name: str, start: date, end: date) -> dict[date, np.ndarray]:
    """Asset cleared_price (ID market) as {day: 96-slot vector}."""
    q = text(f"""
        SELECT data_date, datetime, cleared_price
        FROM {_CLEARED}
        WHERE plant_name = :p AND data_date BETWEEN :s AND :e
        ORDER BY datetime
    """)
    df = pd.read_sql(q, engine, params={"p": plant_name, "s": start, "e": end})
    out: dict[date, np.ndarray] = {}
    if df.empty:
        return out
    df["slot"] = pd.to_datetime(df["datetime"]).dt.hour * 4 + pd.to_datetime(df["datetime"]).dt.minute // 15 + 1
    for d, g in df.groupby("data_date"):
        out[d] = _to_vec(g, "slot", "cleared_price")
    return out


def get_asset_interval_series(engine, plant_name: str, start: date, end: date) -> pd.DataFrame:
    """Per-interval cleared price + energy for charge/discharge shading."""
    q = text(f"""
        SELECT datetime, cleared_price, cleared_energy_mwh
        FROM {_CLEARED}
        WHERE plant_name = :p AND data_date BETWEEN :s AND :e
        ORDER BY datetime
    """)
    return pd.read_sql(q, engine, params={"p": plant_name, "s": start, "e": end})


def get_node_price_vectors(engine, node_names: list[str], start: date, end: date) -> dict[str, dict[date, np.ndarray]]:
    """Fengxing node prices as {node_name: {day: 96-slot vector}} for the given nodes."""
    if not node_names:
        return {}
    q = text(f"""
        SELECT node_name, metric_time::date AS d, time_order_96, avg_node_price
        FROM {_VIEW}
        WHERE node_name = ANY(:names)
          AND metric_time >= :s AND metric_time < :e2
    """)
    df = pd.read_sql(q, engine, params={"names": node_names, "s": start, "e2": end + timedelta(days=1)})
    out: dict[str, dict[date, np.ndarray]] = {}
    for (node, d), g in df.groupby(["node_name", "d"]):
        out.setdefault(node, {})[d] = _to_vec(g, "time_order_96", "avg_node_price")
    return out


def get_day_node_matrix(engine, day: date) -> dict[str, np.ndarray]:
    """All 蒙西 node price vectors for one day: {node_name: 96-slot vector}."""
    q = text(f"""
        SELECT node_name, time_order_96, avg_node_price
        FROM {_VIEW}
        WHERE metric_time >= :s AND metric_time < :e2
    """)
    df = pd.read_sql(q, engine, params={"s": day, "e2": day + timedelta(days=1)})
    return {node: _to_vec(g, "time_order_96", "avg_node_price")
            for node, g in df.groupby("node_name")}
