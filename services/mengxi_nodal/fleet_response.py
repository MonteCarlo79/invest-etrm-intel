"""Aggregate fleet-response estimator — 蒙西 book (plan task 8).

Reduced-form provincial supply stack: scatter of (nodal price, fleet
response MW) over a lookback window, binned into price-quantile bands with
the mean response per band. Task 9 persists the returned dict into
ForecastSet.fleet_response.

Response variable: provincial net load — spot_fundamentals_hourly
.bidding_space_mw (竞价空间: load minus must-run renewables; what the
dispatchable fleet is called to serve).

Frames (hard invariants):
- md_mengxi_nodal_price_96_filled.metric_time is CST-NAIVE (v5): naive CST
  literals via CAST(:s AS timestamp) — no '+08' suffix, no AT TIME ZONE.
- spot_fundamentals_hourly.datetime is TIMESTAMPTZ (DDL in
  services/bess_map/run_fundamentals_ingest.py): explicit '+08' CST window
  literals are correct there; values are converted to naive CST in pandas
  for the join with the filled table.

Floors are v1 code constants (plan: "not a coal registry"):
- coal offers must cover variable cost → price floor for thermal response;
- BESS discharges only when the expected spread clears zero.
"""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import text

_LOOKBACK_DAYS = 60
_N_BANDS = 8

# v1 optimization floors (code constants, not a coal registry).
_COAL_VAR_COST_CNY_MWH = 220.0   # typical IM coal variable cost, CNY/MWh
_BESS_MIN_SPREAD_CNY_MWH = 0.0   # discharge only when spread > 0

_PRICE_SQL = """
    SELECT metric_time, AVG(avg_node_price) AS px
    FROM marketdata.md_mengxi_nodal_price_96_filled
    WHERE metric_time >= CAST(:s AS timestamp)
      AND metric_time <  CAST(:e AS timestamp)
    GROUP BY metric_time
    ORDER BY metric_time
"""

_NETLOAD_SQL = """
    SELECT datetime, bidding_space_mw AS net_load_mw
    FROM marketdata.spot_fundamentals_hourly
    WHERE province = :province
      AND datetime >= :s
      AND datetime <  :e
    ORDER BY datetime
"""


def _naive_cst_window(lookback_days: int, end: date) -> tuple[str, str]:
    """Naive CST bounds [end-lookback, end) for the filled table — no tz
    suffix (v5 frame)."""
    return (f"{end - timedelta(days=lookback_days)} 00:00:00",
            f"{end} 00:00:00")


def _cst_tz_window(lookback_days: int, end: date) -> tuple[str, str]:
    """Explicit CST ('+08') bounds [end-lookback, end) for timestamptz
    columns — passing bare dates lets PG coerce to midnight in the session
    tz (UTC on the RDS), shifting the CST window 8h."""
    return (f"{end - timedelta(days=lookback_days)} 00:00:00+08",
            f"{end} 00:00:00+08")


def _fit_bands(prices, response, n_bands: int = _N_BANDS) -> list[tuple[float, float, float]]:
    """Quantile price bands with mean response per band.

    -> [(p_lo, p_hi, mean_response_mw)], ascending in price. Band edges are
    price quantiles, so the outer edges cover [min, max] of the observed
    prices. Non-finite pairs are dropped; empty input -> []. Degenerate
    prices (all equal) collapse to a single (p, p, mean) band. Means are
    plain per-band averages — monotonicity is a property of the data, not
    enforced.
    """
    if n_bands < 1:
        raise ValueError(f"n_bands must be >= 1, got {n_bands}")
    p = np.asarray(prices, dtype=float).ravel()
    r = np.asarray(response, dtype=float).ravel()
    if p.shape != r.shape:
        raise ValueError(f"prices/response shape mismatch: {p.shape} vs {r.shape}")
    ok = np.isfinite(p) & np.isfinite(r)
    p, r = p[ok], r[ok]
    if p.size == 0:
        return []
    edges = np.unique(np.quantile(p, np.linspace(0.0, 1.0, n_bands + 1)))
    if edges.size < 2:
        return [(float(p.min()), float(p.max()), float(r.mean()))]
    idx = np.clip(np.searchsorted(edges, p, side="right") - 1, 0, edges.size - 2)
    bands: list[tuple[float, float, float]] = []
    for i in range(edges.size - 1):
        m = idx == i
        if m.any():
            bands.append((float(edges[i]), float(edges[i + 1]), float(r[m].mean())))
    return bands


def estimate_stack(engine, province: str = "蒙西",
                   lookback_days: int = _LOOKBACK_DAYS,
                   n_bands: int = _N_BANDS,
                   as_of: date | None = None) -> dict:
    """Estimate the aggregate fleet-response stack for a province.

    Pulls provincial-average nodal prices from the filled table (naive CST
    window) and net load (bidding_space_mw) from spot_fundamentals_hourly
    ('+08' timestamptz window), joins on the CST hour (hourly fundamentals
    apply to all four 15-min slots), and fits price-quantile bands.

    Returns {"bands": [(p_lo, p_hi, mean_response_mw)], "floors": {...},
    "province": ..., "as_of": ..., "lookback_days": ..., "n_obs": int}.
    bands is [] (loud empty) when either frame has no rows in the window —
    callers (Task 9) must treat an empty stack as "no fleet model".
    """
    end = as_of or date.today()
    s_naive, e_naive = _naive_cst_window(lookback_days, end)
    s_tz, e_tz = _cst_tz_window(lookback_days, end)
    floors = {"coal_var_cost_cny_mwh": _COAL_VAR_COST_CNY_MWH,
              "bess_spread_cny_mwh": _BESS_MIN_SPREAD_CNY_MWH}
    out = {"province": province, "as_of": end.isoformat(),
           "lookback_days": lookback_days, "bands": [], "floors": floors,
           "n_obs": 0}

    px = pd.read_sql(text(_PRICE_SQL), engine,
                     params={"s": s_naive, "e": e_naive})
    nl = pd.read_sql(text(_NETLOAD_SQL), engine,
                     params={"province": province, "s": s_tz, "e": e_tz})
    if px.empty or nl.empty:
        return out

    px["metric_time"] = pd.to_datetime(px["metric_time"])
    px["hour"] = px["metric_time"].dt.floor("h")
    nl["datetime"] = pd.to_datetime(nl["datetime"], utc=True)
    nl["hour"] = nl["datetime"].dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)

    df = px.merge(nl[["hour", "net_load_mw"]], on="hour", how="inner")
    df = df[np.isfinite(df["net_load_mw"])]
    if df.empty:
        return out
    out["bands"] = _fit_bands(df["px"].to_numpy(), df["net_load_mw"].to_numpy(),
                              n_bands=n_bands)
    out["n_obs"] = int(len(df))
    return out
