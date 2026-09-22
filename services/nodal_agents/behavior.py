# services/nodal_agents/behavior.py
"""Behavior forecasting for the L3 asset agents.

Two forecasts feed the agent layer:
- forecast_zone_bess: day-type-matched mean dispatch of a zone's OTHER BESS
  (the part of the fleet we do not optimize), from md_id_cleared_energy-shaped
  history. MW convention: positive = discharging to grid, negative = charging
  (cleared_energy_mwh x 4 upstream).
- coal_stack_response: aggregate coal fleet output given a residual-load
  curve and a fuel-fleet stack (marketdata.province_fuel_fleet shape).
  Spec SS5.3: "response ~ historical stack utilization by residual-load level".
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

MIN_DAYTYPE_DAYS = 2  # fewer matching days than this -> fall back to all days


def _daytype_mean(hist: np.ndarray, mask) -> np.ndarray:
    """Column mean of the rows NOT masked out (numpy masked-array convention:
    mask True = exclude that day). All rows masked -> mean of the full history.
    NaN-safe per interval column."""
    h = np.asarray(hist, dtype=float)
    m = np.asarray(mask, dtype=bool)
    if h.ndim != 2:
        raise ValueError(f"hist must be (n_days, n_intervals), got {h.shape}")
    if m.shape[0] != h.shape[0]:
        raise ValueError(f"mask length {m.shape[0]} != n_days {h.shape[0]}")
    keep = h[~m]
    if keep.shape[0] == 0:
        keep = h
    return np.nanmean(keep, axis=0)


def forecast_zone_bess(zone: str, target_date: date, hist: pd.DataFrame) -> np.ndarray:
    """Day-type-matched mean dispatch curve (96,) MW of the zone's other BESS.

    hist: long-format DataFrame, one row per day x interval:
        d (date), interval (int 0..95), dispatch_mw (float)
    Optional column `zone` — when present, only rows for this zone are used
    (hist is assumed pre-filtered otherwise). Days of the opposite day-type
    (weekday vs weekend) are masked out; with fewer than MIN_DAYTYPE_DAYS
    matching days the full history is used. Empty hist -> zeros.
    """
    if hist is None or hist.empty:
        return np.zeros(96, dtype=float)
    df = hist
    if "zone" in df.columns:
        df = df[df["zone"] == zone]
        if df.empty:
            return np.zeros(96, dtype=float)
    missing = [c for c in ("d", "interval", "dispatch_mw") if c not in df.columns]
    if missing:
        raise ValueError(f"hist missing required columns: {missing}")

    mat = (df.pivot_table(index="d", columns="interval", values="dispatch_mw",
                          aggfunc="mean")
             .reindex(columns=range(96)))
    days = pd.to_datetime(mat.index)
    target_weekend = pd.Timestamp(target_date).dayofweek >= 5
    row_weekend = np.asarray(days.dayofweek >= 5, dtype=bool)
    mask = row_weekend != target_weekend
    if int((~mask).sum()) < MIN_DAYTYPE_DAYS:
        mask = np.zeros(len(mat), dtype=bool)
    out = _daytype_mean(mat.to_numpy(dtype=float), mask)
    return np.nan_to_num(out, nan=0.0)


def coal_stack_response(residual: np.ndarray, fuel: dict) -> np.ndarray:
    """Aggregate coal output (96,) MW from the fuel-fleet stack.

    residual: (96,) residual load MW (load - renewable) at grid level.
    fuel: {"segments": [{"capacity_mw", "must_run_mw", "cost_yuan_per_mwh"}, ...]}
        Segments are aggregated (already merit-ordered upstream); utilization
        u_t = normalized residual level in [0, 1] ramps the fleet linearly
        between total must-run and total capacity:
            out_t = sum(must_run) + u_t * sum(capacity - must_run)
        Degenerate residual (constant / NaN) -> u = 0.5 mid-load.
    """
    segments = (fuel or {}).get("segments") or []
    if not segments:
        return np.zeros(96, dtype=float)
    must_run = 0.0
    flex = 0.0
    for s in segments:
        cap = float(s.get("capacity_mw") or 0.0)
        mr = float(s.get("must_run_mw") or 0.0)
        cap, mr = max(cap, 0.0), min(max(mr, 0.0), max(cap, 0.0))
        must_run += mr
        flex += cap - mr
    r = np.asarray(residual, dtype=float)
    if not np.isfinite(r).all():
        finite = r[np.isfinite(r)]
        fill = float(finite.mean()) if finite.size else 0.0
        r = np.where(np.isfinite(r), r, fill)
    span = float(r.max() - r.min())
    u = (r - r.min()) / span if span > 1e-9 else np.full_like(r, 0.5)
    return must_run + u * flex
