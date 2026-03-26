# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
import os

def build_master(df_id, conversion_factor):
    # Define your master build logic here
    asset_meta = (
        df_id.groupby("plant_name")["dispatch_unit_name"]
        .first()
        .reset_index()
    )
    asset_meta["asset_type"] = asset_meta["dispatch_unit_name"].apply(infer_asset_type)

    clusters = nodal_clustering(df_id).merge(
        asset_meta[["plant_name", "asset_type"]],
        on="plant_name",
        how="left"
    )

    # Attach inferred MW from full DB
    capacity_df = infer_capacity_from_history(conversion_factor)

    clusters = clusters.merge(
        capacity_df,
        on="plant_name",
        how="left"
    )

    clusters["inferred_mw"] = clusters["inferred_mw"].round(0)

    return clusters

def build_peer_tables(station_df, clusters):
    # Map each BESS plant -> cluster_id
    
    bess_map = station_df[["plant_name"]].merge(
        clusters[["plant_name", "cluster_id"]],
        on="plant_name",
        how="left"
    ).rename(columns={"plant_name": "bess_plant"})

    # All peers in same cluster (long form)
    peers_long = bess_map.merge(
        clusters[["plant_name", "cluster_id", "asset_type"]],
        on="cluster_id",
        how="left"
    ).rename(columns={"plant_name": "peer_plant"})

    # Exclude self from peer stats
    peers_excl_self = peers_long[peers_long["peer_plant"] != peers_long["bess_plant"]].copy()

    # Counts by type (always available)
    peer_type_counts = (
        peers_excl_self
        .groupby(["bess_plant", "asset_type"])["peer_plant"]
        .nunique()
        .unstack(fill_value=0)
        .add_prefix("peer_count_")
        .reset_index()
    )

    # "Known MW" by type (only where we have MW for that peer from uploaded file)
    # print("station_df columns:", station_df.columns)
    station_df_local = station_df.copy()
    station_df_local.columns = station_df_local.columns.str.lower()
    

    # print("station_df columns:", station_df.columns)
    # MW by type (using inferred full-history capacity)
    peer_type_mw = (
        peers_excl_self
        .merge(
            clusters[["plant_name", "inferred_mw"]],
            left_on="peer_plant",
            right_on="plant_name",
            how="left"
        )
        .groupby(["bess_plant", "asset_type"])["inferred_mw"]
        .sum()
        .unstack(fill_value=0)
        .add_prefix("peer_MW_")
        .reset_index()
    )

    # Combine
    peer_summary = peer_type_counts.merge(peer_type_mw, on="bess_plant", how="left")

    # Also return mapping table for Tab1
    nodal_mapping = peers_long[["bess_plant", "cluster_id", "peer_plant", "asset_type"]].copy()

    return bess_map, nodal_mapping, peer_summary

# ==========================================================
# IRR helper
# ==========================================================
def irr_from_cashflows(cashflows):
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

def build_peer_detail_table(selected_bess, clusters):
    """
    Returns peer table (excluding self)
    """
    # Find cluster
    cluster_id = clusters.loc[
        clusters["plant_name"] == selected_bess,
        "cluster_id"
    ]

    if cluster_id.empty:
        return pd.DataFrame()

    cluster_id = cluster_id.iloc[0]

    peers = clusters[clusters["cluster_id"] == cluster_id].copy()

    # Exclude itself
    peers = peers[peers["plant_name"] != selected_bess]

    return peers[["plant_name", "asset_type", "inferred_mw"]].rename(
        columns={
            "plant_name": "peer_plant",
            "asset_type": "asset_type",
            "inferred_mw": "inferred_capacity_MW"
        }
    ).sort_values("inferred_capacity_MW", ascending=False)


def infer_asset_type(name):
    # Logic to infer asset type
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

def infer_capacity_from_history(conversion_factor):
    # Inference logic for historical capacities
    engine = create_engine(os.getenv("PGURL"))
    sql = """
        SELECT plant_name, MAX(ABS(cleared_energy_mwh)) AS inferred_mw
        FROM marketdata.md_id_cleared_energy
        GROUP BY plant_name
    """
    with engine.connect() as conn:
        df_cap = pd.read_sql(sql, conn)

    df_cap["inferred_mw"] = df_cap["inferred_mw"].fillna(0) * conversion_factor
    return df_cap

def nodal_clustering(df_id):
    # Cluster logic
    pivot = df_id.pivot_table(
        index="datetime",
        columns="plant_name",
        values="cleared_price",
        aggfunc="mean"
    ).sort_index()

    signatures = {}
    for plant in pivot.columns:
        s = pivot[plant].round(2)
        h = pd.util.hash_pandas_object(s, index=True).sum()
        signatures[plant] = h

    cluster_df = pd.DataFrame({
        "plant_name": list(signatures.keys()),
        "signature": list(signatures.values())
    })
    cluster_df["cluster_id"] = cluster_df.groupby("signature").ngroup() + 1
    cluster_df["cluster_size"] = cluster_df.groupby("cluster_id")["plant_name"].transform("count")
    return cluster_df