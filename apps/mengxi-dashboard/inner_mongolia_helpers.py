# -*- coding: utf-8 -*-
"""
Helper functions shared between the inner-Mongolia pipeline and this app.
Copied verbatim from apps/bess-inner-mongolia/shared/core.py — the functions
here have no internal dependencies and can be imported standalone.
"""
import numpy as np
import pandas as pd


def infer_asset_type(name) -> str:
    """Classify a plant's dispatch_unit_name into wind/bess/solar/thermal."""
    if pd.isna(name):
        return "thermal"
    s = str(name)
    if "光储" in s:
        return "solar"
    if "风储" in s:
        return "wind"
    if "风电场" in s:
        return "wind"
    if "储能" in s:
        return "bess"
    if "光伏" in s:
        return "solar"
    if "风场" in s:
        return "wind"
    return "thermal"


def irr_from_cashflows(cashflows) -> float:
    """Compute IRR from a list of cashflows. Returns nan on failure."""
    try:
        import numpy_financial as npf
        r = npf.irr(cashflows)
        if r is None or np.isnan(r) or np.isinf(r):
            return np.nan
        return float(r)
    except Exception:
        def npv(rate):
            return sum(cf / ((1 + rate) ** t) for t, cf in enumerate(cashflows))
        lo, hi = -0.9, 2.0
        f_lo, f_hi = npv(lo), npv(hi)
        if np.isnan(f_lo) or np.isnan(f_hi) or f_lo * f_hi > 0:
            return np.nan
        for _ in range(80):
            mid = (lo + hi) / 2
            f_mid = npv(mid)
            if abs(f_mid) < 1e-8:
                return mid
            if f_lo * f_mid <= 0:
                hi, f_hi = mid, f_mid
            else:
                lo, f_lo = mid, f_mid
        return mid


def build_peer_detail_table(selected_bess: str, clusters: pd.DataFrame) -> pd.DataFrame:
    """Return peer table (excluding self) for the selected BESS station."""
    cluster_id = clusters.loc[
        clusters["plant_name"] == selected_bess,
        "cluster_id",
    ]
    if cluster_id.empty:
        return pd.DataFrame()
    cluster_id = cluster_id.iloc[0]
    peers = clusters[clusters["cluster_id"] == cluster_id].copy()
    peers = peers[peers["plant_name"] != selected_bess]
    return (
        peers[["plant_name", "asset_type", "inferred_mw"]]
        .rename(columns={
            "plant_name": "peer_plant",
            "asset_type": "asset_type",
            "inferred_mw": "inferred_capacity_MW",
        })
        .sort_values("inferred_capacity_MW", ascending=False)
    )
