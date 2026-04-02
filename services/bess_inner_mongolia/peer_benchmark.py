# -*- coding: utf-8 -*-
"""
services/bess_inner_mongolia/peer_benchmark.py
───────────────────────────────────────────────
Peer-benchmark analytics for Inner Mongolia BESS.
No Streamlit dependency — testable in isolation.

Evidence levels used in outputs
────────────────────────────────
observed           – metric direct from pipeline-computed market output
proxy-based        – additive decomposition model; approximation only
heuristic inference – rule-based classification; competitors' actual
                      strategies are not known and must not be claimed
"""
from __future__ import annotations

import numpy as np
import pandas as pd

ENVISION_TAG: str = "远景"

# Metric config: col_name -> (display_label, format_str)
_METRIC_META: dict[str, tuple[str, str]] = {
    "total_profit_per_installed_volume_per_day": ("Profit/MW·day (¥)",    "{:,.0f}"),
    "arbitrage_per_installed_volume_per_day":    ("Arb/MW·day (¥)",       "{:,.0f}"),
    "total_profit_per_discharge_mwh":            ("Total profit/MWh (¥)", "{:,.0f}"),
    "arbitrage_profit_per_discharge_mwh":        ("Arb profit/MWh (¥)",   "{:,.0f}"),
    "estimated_cycles_per_day":                  ("Cycles/day",            "{:.3f}"),
    "efficiency":                                ("Round-trip eff.",        "{:.1%}"),
    "irr":                                       ("IRR",                    "{:.1%}"),
    "payback_years":                             ("Payback (yr)",           "{:.1f}"),
    "expected_total_profit_万元":                ("Total profit (万¥)",    "{:,.2f}"),
}

_PRIMARY_RANK_COL = "total_profit_per_installed_volume_per_day"


# ── Leaderboard ───────────────────────────────────────────────────────────────

def compute_leaderboard(
    df: pd.DataFrame,
    envision_tag: str = ENVISION_TAG,
) -> pd.DataFrame:
    """
    Rank all BESS assets by total_profit_per_installed_volume_per_day.

    Added columns:
      overall_rank   – 1 = best
      is_envision    – bool
      evidence_level – "observed"

    Evidence: **observed** (direct pipeline output).
    """
    if df.empty:
        return df.copy()

    out = df.copy()
    if _PRIMARY_RANK_COL in out.columns:
        out["overall_rank"] = (
            out[_PRIMARY_RANK_COL]
            .rank(ascending=False, method="min")
            .astype("Int64")
        )
    else:
        out["overall_rank"] = pd.NA

    owner_col = out.get("owner", pd.Series(dtype=str))
    out["is_envision"] = owner_col.str.contains(envision_tag, na=False)
    out["evidence_level"] = "observed"

    sort_col = "overall_rank" if "overall_rank" in out.columns else out.columns[0]
    return out.sort_values(sort_col, na_position="last").reset_index(drop=True)


# ── Envision vs Top Performers ────────────────────────────────────────────────

def compute_envision_vs_top(
    df: pd.DataFrame,
    top_n: int = 5,
    envision_tag: str = ENVISION_TAG,
) -> dict[str, pd.DataFrame]:
    """
    Split into Envision assets and the top-N non-Envision assets.

    Returns dict:
      "envision"   – Envision subset
      "top_peers"  – top-N non-Envision by _PRIMARY_RANK_COL
      "comparison" – long-form metric table with gap_mean_vs_mean

    Evidence: **observed** for all numeric values.
    """
    _empty = {"envision": pd.DataFrame(), "top_peers": pd.DataFrame(),
              "comparison": pd.DataFrame()}
    if df.empty:
        return _empty

    owner_series = df.get("owner", pd.Series(dtype=str))
    is_env  = owner_series.str.contains(envision_tag, na=False)
    env_df  = df[is_env].copy()
    peer_df = df[~is_env].copy()

    if _PRIMARY_RANK_COL in peer_df.columns:
        peer_df = (
            peer_df
            .sort_values(_PRIMARY_RANK_COL, ascending=False)
            .head(top_n)
            .reset_index(drop=True)
        )

    metrics = [m for m in _METRIC_META if m in df.columns]
    records = []
    for metric in metrics:
        label, _ = _METRIC_META[metric]
        env_vals  = env_df[metric].dropna()
        peer_vals = peer_df[metric].dropna()

        env_mean  = float(env_vals.mean())  if not env_vals.empty  else float("nan")
        env_best  = float(env_vals.max())   if not env_vals.empty  else float("nan")
        peer_mean = float(peer_vals.mean()) if not peer_vals.empty else float("nan")
        peer_best = float(peer_vals.max())  if not peer_vals.empty else float("nan")
        gap       = (env_mean - peer_mean)

        records.append({
            "metric":              label,
            "col":                 metric,
            "envision_mean":       env_mean,
            "envision_best":       env_best,
            "top_peer_mean":       peer_mean,
            "top_peer_best":       peer_best,
            "gap_mean_vs_mean":    gap,
            "evidence_level":      "observed",
        })

    return {
        "envision":   env_df,
        "top_peers":  peer_df,
        "comparison": pd.DataFrame(records),
    }


# ── Gap Decomposition ─────────────────────────────────────────────────────────

def compute_gap_decomposition(
    df: pd.DataFrame,
    envision_tag: str = ENVISION_TAG,
) -> pd.DataFrame:
    """
    For each Envision asset decompose the gap in total_profit_per_installed_volume_per_day
    vs the single best non-Envision plant into three additive proxy components:

      utilization_gap    – (best_cycles - env_cycles) * env_profit_per_mwh * DURATION_PROXY
      price_capture_gap  – (best_profit_per_mwh - env_profit_per_mwh) * env_cycles * DURATION_PROXY
      efficiency_gap     – (best_eff - env_eff) * env_cycles * env_profit_per_mwh * DURATION_PROXY
      unexplained        – total_gap - sum(above components)

    All components: Evidence = **proxy-based**
    (additive decomposition is a modelling approximation; each term isolates
    one driver while holding others at the Envision asset's observed level.
    Duration proxy = 2h is conservative and documented in the UI).
    """
    if df.empty:
        return pd.DataFrame()

    owner_series = df.get("owner", pd.Series(dtype=str))
    is_env  = owner_series.str.contains(envision_tag, na=False)
    env_df  = df[is_env].copy()
    peer_df = df[~is_env].copy()

    if env_df.empty or peer_df.empty:
        return pd.DataFrame()

    _profit_col = _PRIMARY_RANK_COL
    _pmwh_col   = "total_profit_per_discharge_mwh"
    _cycles_col = "estimated_cycles_per_day"
    _eff_col    = "efficiency"

    # Best non-Envision benchmark row
    if _profit_col not in peer_df.columns:
        return pd.DataFrame()

    best_idx = peer_df[_profit_col].idxmax()
    best_row = peer_df.loc[best_idx]

    # Conservative duration proxy (2h) — avoids needing the full sidebar config
    DURATION_PROXY = 2.0

    records = []
    for _, row in env_df.iterrows():
        total_gap = _diff(best_row, row, _profit_col)

        env_cycles  = _get(row,      _cycles_col)
        best_cycles = _get(best_row, _cycles_col)
        env_pmwh    = _get(row,      _pmwh_col)
        best_pmwh   = _get(best_row, _pmwh_col)
        env_eff     = _get(row,      _eff_col)
        best_eff    = _get(best_row, _eff_col)

        util_gap = (
            (best_cycles - env_cycles) * env_pmwh * DURATION_PROXY
            if _all_ok(best_cycles, env_cycles, env_pmwh) else float("nan")
        )
        price_gap = (
            (best_pmwh - env_pmwh) * env_cycles * DURATION_PROXY
            if _all_ok(best_pmwh, env_pmwh, env_cycles) else float("nan")
        )
        eff_gap = (
            (best_eff - env_eff) * env_cycles * env_pmwh * DURATION_PROXY
            if _all_ok(best_eff, env_eff, env_cycles, env_pmwh) else float("nan")
        )

        explained   = _nansum(util_gap, price_gap, eff_gap)
        unexplained = (total_gap - explained) if _all_ok(total_gap) else float("nan")

        records.append({
            "plant_name":           _get_str(row,      "plant_name"),
            "owner":                _get_str(row,      "owner"),
            "envision_profit_day":  _get(row,          _profit_col),
            "best_peer_profit_day": _get(best_row,     _profit_col),
            "best_peer_name":       _get_str(best_row, "plant_name"),
            "total_gap":            total_gap,
            "utilization_gap":      util_gap,
            "price_capture_gap":    price_gap,
            "efficiency_gap":       eff_gap,
            "unexplained":          unexplained,
            "evidence_level":       "proxy-based",
            "decomp_note": (
                "Additive decomposition; each component holds other factors at the "
                f"Envision asset's observed level. Duration proxy = {DURATION_PROXY}h "
                "(model assumption, not actual config)."
            ),
        })

    return pd.DataFrame(records)


# ── Private helpers ───────────────────────────────────────────────────────────

def _get(row: pd.Series, col: str) -> float:
    v = row.get(col, float("nan"))
    try:
        f = float(v)
        return f if not np.isnan(f) else float("nan")
    except (TypeError, ValueError):
        return float("nan")


def _get_str(row: pd.Series, col: str) -> str:
    return str(row.get(col, ""))


def _diff(a: pd.Series, b: pd.Series, col: str) ->float:
    return _get(a, col) - _get(b, col)


def _all_ok(*vals: float) -> bool:
    return all(not np.isnan(v) for v in vals)


def _nansum(*vals: float) -> float:
    good = [v for v in vals if not np.isnan(v)]
    return float(sum(good)) if good else float("nan")
