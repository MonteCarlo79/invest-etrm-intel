# -*- coding: utf-8 -*-

import hashlib
import logging
import os
import resource
from datetime import datetime

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


logger = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


def _log_mem(tag: str) -> None:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    logger.info("[MEM] %s: RSS=%.1f MB", tag, rss)


def _log_df(name: str, df: pd.DataFrame) -> None:
    mb = df.memory_usage(deep=True).sum() / 1024 / 1024
    logger.info("[DF] %s: shape=%s, mem=%.1f MB", name, df.shape, mb)

def build_master(df_id, conversion_factor):
    _log_df("build_master.df_id", df_id)
    _log_mem("build_master.start")

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
    _log_df("build_master.clusters", clusters)
    _log_mem("build_master.done")
    return clusters

def build_peer_tables(station_df, clusters, include_mapping: bool = True):
    """
    Build peer summary with a memory-safe path that avoids constructing the
    full BESS×peer cross-product unless explicitly requested.
    """
    station_df = station_df.copy()
    station_df["plant_name"] = station_df["plant_name"].astype(str).str.strip()
    # station_df = station_df.drop_duplicates(subset=["plant_name"])
    station_df.loc[station_df["plant_name"].isin(["", "nan", "None"]), "plant_name"] = pd.NA
    station_df = station_df[station_df["plant_name"].notna()]

    clusters = clusters.copy()
    clusters["plant_name"] = clusters["plant_name"].astype(str).str.strip()
    clusters = clusters.drop_duplicates(subset=["plant_name"])

    bess_map = station_df[["plant_name"]].merge(
        clusters[["plant_name", "cluster_id"]],
        on="plant_name",
        how="left",
        validate="one_to_one",
    ).rename(columns={"plant_name": "bess_plant"})

    cluster_totals = (
        clusters.groupby(["cluster_id", "asset_type"], as_index=False)
        .agg(peer_count=("plant_name", "nunique"), peer_mw=("inferred_mw", "sum"))
    )
    _log_df("build_peer_tables.cluster_totals", cluster_totals)

    bess_clusters = bess_map.merge(
        clusters[["plant_name", "cluster_id", "asset_type", "inferred_mw"]],
        left_on="bess_plant",
        right_on="plant_name",
        how="left",
        validate="one_to_one",
    ).rename(columns={"asset_type": "self_asset_type", "inferred_mw": "self_inferred_mw"})

    rows = []
    for row in bess_clusters.itertuples(index=False):
        rec = {"bess_plant": row.bess_plant}
        if pd.isna(row.cluster_id):
            rows.append(rec)
            continue
        sub = cluster_totals[cluster_totals["cluster_id"] == row.cluster_id]
        for _, srow in sub.iterrows():
            asset_type = srow["asset_type"]
            count_val = int(srow["peer_count"])
            mw_val = float(srow["peer_mw"])
            if asset_type == row.self_asset_type:
                count_val = max(0, count_val - 1)
                mw_val = max(0.0, mw_val - float(row.self_inferred_mw or 0.0))
            rec[f"peer_count_{asset_type}"] = count_val
            rec[f"peer_MW_{asset_type}"] = mw_val
        rows.append(rec)

    peer_summary = pd.DataFrame(rows)
    _log_df("build_peer_tables.peer_summary", peer_summary)
    _log_mem("build_peer_tables.done")

    if not include_mapping:
        nodal_mapping = pd.DataFrame(columns=["bess_plant", "cluster_id", "peer_plant", "asset_type"])
        return bess_map, nodal_mapping, peer_summary

    # Only build full mapping when needed by UI/detail view.
    peers_long = bess_map.merge(
        clusters[["plant_name", "cluster_id", "asset_type"]],
        on="cluster_id",
        how="left",
        validate="many_to_many",
    ).rename(columns={"plant_name": "peer_plant"})
    nodal_mapping = peers_long[["bess_plant", "cluster_id", "peer_plant", "asset_type"]].copy()
    _log_df("build_peer_tables.nodal_mapping", nodal_mapping)
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
    # Avoid wide pivot tables that can spike memory.
    tmp = df_id[["plant_name", "datetime", "cleared_price"]].copy()
    tmp["plant_name"] = tmp["plant_name"].astype(str).str.strip()
    tmp["datetime"] = pd.to_datetime(tmp["datetime"], errors="coerce")
    tmp["cleared_price"] = pd.to_numeric(tmp["cleared_price"], errors="coerce")
    tmp = tmp[tmp["datetime"].notna() & tmp["plant_name"].notna()]
    tmp["px2"] = tmp["cleared_price"].round(2)
    tmp = tmp.sort_values(["plant_name", "datetime"])

    signatures = {}
    for plant, g in tmp.groupby("plant_name", sort=False):
        h = hashlib.sha1()
        for ts, px in g[["datetime", "px2"]].itertuples(index=False):
            if pd.isna(px):
                continue
            h.update(f"{ts.isoformat()}|{px:.2f};".encode("utf-8"))
        signatures[plant] = h.hexdigest()

    cluster_df = pd.DataFrame({
        "plant_name": list(signatures.keys()),
        "signature": list(signatures.values())
    })
    cluster_df["cluster_id"] = cluster_df.groupby("signature").ngroup() + 1
    cluster_df["cluster_size"] = cluster_df.groupby("cluster_id")["plant_name"].transform("count")
    return cluster_df
