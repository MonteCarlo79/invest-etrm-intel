# -*- coding: utf-8 -*-
"""
Marginal portfolio contribution for new-asset screening (Ardian Opta lesson:
quantify a candidate's incremental effect on fleet cashflow volatility and
zone diversification).

Fleet cashflow proxy per day:  Σ_i cap_i · dur_i · price_zone(i),t
Candidate adds:                Σ_c cap_c · dur_c · price_zone(c),t

Metrics per candidate:
  corr(candidate_zone, fleet)        — pairwise daily-price correlation
  vol_per_mw before → after          — portfolio σ per unit throughput weight
  diversification benefit            — 1 − vol_per_mw_after / vol_per_mw_before
  zone concentration (fleet weights) — for the concentration map

Price series use DAILY MEANS of 15-min nodal RT prices over a trailing window
(default 120 days) — aligned across nodes, robust to single-day outliers.
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

DEFAULT_WINDOW_DAYS = 120


# ── Pure compute (testable without DB) ───────────────────────────────────────

def fleet_proxy(prices: pd.DataFrame, fleet: pd.DataFrame) -> pd.Series:
    """Fleet daily cashflow proxy series.

    prices: DataFrame indexed by date with one column per zone_price_node (daily means).
    fleet:  DataFrame with columns zone_price_node, capacity_mw, duration_h.
    Returns Series indexed by date.
    """
    if prices.empty or fleet.empty:
        return pd.Series(dtype=float)
    out = None
    for _, r in fleet.iterrows():
        node = r["zone_price_node"]
        if node not in prices.columns:
            continue
        w = float(r["capacity_mw"]) * float(r["duration_h"] or 1.0)
        s = prices[node] * w
        out = s if out is None else out.add(s, fill_value=0.0)
    return out if out is not None else pd.Series(dtype=float)


def contribution_metrics(fleet_series: pd.Series,
                         candidate_series: pd.Series,
                         fleet_weight: float,
                         candidate_weight: float) -> dict:
    """Correlation + vol-per-MW before/after for one candidate.

    Weights are throughput weights (capacity_mw × duration_h). Vol-per-MW is
    the daily-series std divided by total weight — the capacity-normalized
    volatility, which falls when the candidate diversifies.
    """
    empty = {"corr": None, "vol_per_mw_before": None, "vol_per_mw_after": None,
             "diversification": None, "vol_before": None, "vol_after": None}
    joined = pd.concat([fleet_series.rename("f"), candidate_series.rename("c")],
                       axis=1, join="inner").dropna()
    if len(joined) < 10 or fleet_weight <= 0 or candidate_weight <= 0:
        return empty

    f, c = joined["f"], joined["c"]
    corr = float(f.corr(c)) if f.std() > 0 and c.std() > 0 else None
    combined = f + c
    vol_b = float(f.std())
    vol_a = float(combined.std())
    vpm_b = vol_b / fleet_weight
    vpm_a = vol_a / (fleet_weight + candidate_weight)
    return {
        "corr": corr,
        "vol_before": vol_b,
        "vol_after": vol_a,
        "vol_per_mw_before": vpm_b,
        "vol_per_mw_after": vpm_a,
        "diversification": (1.0 - vpm_a / vpm_b) if vpm_b > 0 else None,
    }


# ── DB layer ─────────────────────────────────────────────────────────────────

def fetch_zone_daily_prices(engine: Engine, nodes: list[str],
                            window_days: int = DEFAULT_WINDOW_DAYS,
                            end_date=None) -> pd.DataFrame:
    """Daily mean nodal RT price per node for the trailing window.

    Returns DataFrame indexed by date with one column per requested node.
    """
    if not nodes:
        return pd.DataFrame()
    sql = """
        SELECT datetime::date AS d, node_name, AVG(node_price) AS p
        FROM marketdata.md_rt_nodal_price
        WHERE node_name = ANY(:nodes)
          AND datetime::date >= COALESCE(:start, '1900-01-01')
          AND datetime::date <= COALESCE(:end, CURRENT_DATE)
        GROUP BY 1, 2
        ORDER BY 1
    """
    end = end_date
    start = None
    if end is not None:
        start = pd.Timestamp(end) - pd.Timedelta(days=window_days - 1)
    df = pd.read_sql(sql_text(sql), engine,
                     params={"nodes": nodes, "start": start, "end": end})
    if df.empty:
        return pd.DataFrame()
    df["d"] = pd.to_datetime(df["d"])
    return df.pivot(index="d", columns="node_name", values="p").sort_index()


def compute_contribution(engine: Engine, candidate: dict, fleet: pd.DataFrame,
                         window_days: int = DEFAULT_WINDOW_DAYS,
                         proxy_node: Optional[str] = None) -> dict:
    """Full marginal-contribution assessment for one candidate asset (registry row).

    proxy_node: fallback price node when the candidate has no zone_price_node
    (e.g. alashan → 德岭山 proxy, flagged in the result).
    """
    node = candidate.get("zone_price_node") or proxy_node
    used_proxy = candidate.get("zone_price_node") is None and proxy_node is not None
    result = {"asset_code": candidate.get("asset_code"),
              "candidate_node": node, "used_proxy_node": used_proxy,
              "metrics": None, "fleet_weights": {}, "warnings": []}

    nodes = sorted({n for n in fleet["zone_price_node"].dropna().unique()} | ({node} if node else set()))
    prices = fetch_zone_daily_prices(engine, nodes, window_days=window_days)
    if prices.empty:
        result["warnings"].append("no price data in window")
        return result

    # Fleet: drop assets whose price node is missing from the price frame
    have = [n for n in fleet["zone_price_node"].dropna().unique() if n in prices.columns]
    missing = sorted(set(fleet["zone_price_node"].dropna().unique()) - set(have))
    if missing:
        result["warnings"].append(f"price nodes missing in window: {missing}")
    f_fleet = fleet[fleet["zone_price_node"].isin(have)]

    f_series = fleet_proxy(prices, f_fleet)
    fleet_weight = float((f_fleet["capacity_mw"] * f_fleet["duration_h"].fillna(1.0)).sum())
    result["fleet_weights"] = {
        n: float((f_fleet[f_fleet["zone_price_node"] == n]["capacity_mw"]
                  * f_fleet[f_fleet["zone_price_node"] == n]["duration_h"].fillna(1.0)).sum())
        for n in have
    }

    if node is None or node not in prices.columns:
        result["warnings"].append(
            "candidate has no price node coverage — cannot compute contribution")
        return result

    cand_weight = float(candidate["capacity_mw"]) * float(candidate.get("duration_h") or 1.0)
    result["metrics"] = contribution_metrics(
        f_series, prices[node], fleet_weight, cand_weight)
    result["fleet_series"] = f_series
    result["candidate_series"] = prices[node]
    return result
