# -*- coding: utf-8 -*-

import gc
import hashlib
import json
import logging
import math
import os
from collections import defaultdict

import numpy as np
import numpy_financial as npf
import pandas as pd
import psutil
from sqlalchemy import create_engine, text

from shared.core import build_peer_tables, infer_asset_type, infer_capacity_from_history
from shared.init_results_table import create_schema_and_table


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)


# ==========================================================
# LOGGING HELPERS
# ==========================================================

def log_mem(tag: str) -> None:
    rss = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
    logger.info("[MEM] %s: RSS=%.1f MB", tag, rss)


def log_df(name: str, df: pd.DataFrame) -> None:
    mb = df.memory_usage(deep=True).sum() / 1024 / 1024
    logger.info("[DF] %s: shape=%s, mem=%.1f MB", name, df.shape, mb)


def log_keys(df: pd.DataFrame, keys: list[str], name: str) -> None:
    if not keys:
        logger.info("[KEYS] %s: keys=[] (skip)", name)
        return
    distinct = df[keys].drop_duplicates().shape[0]
    logger.info("[KEYS] %s: rows=%s, distinct_keys=%s, keys=%s", name, len(df), distinct, keys)


def merge_with_logging(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    name: str,
    on: list[str],
    how: str = "left",
    validate: str | None = None,
) -> pd.DataFrame:
    logger.info("[MERGE] %s begin", name)
    logger.info("[MERGE] %s left_rows=%s", name, len(left))
    logger.info("[MERGE] %s right_rows=%s", name, len(right))
    log_keys(left, on, f"{name}-left")
    log_keys(right, on, f"{name}-right")

    merged = left.merge(right, on=on, how=how, validate=validate)

    logger.info("[MERGE] %s merged_rows=%s", name, len(merged))
    expansion = len(merged) / max(len(left), 1)
    if expansion > 1.2:
        logger.warning("[MERGE] %s row expansion ratio=%.2f", name, expansion)
    log_df(f"merge:{name}", merged)
    log_mem(f"merge:{name}")
    return merged


# ==========================================================
# INIT
# ==========================================================

create_schema_and_table()

START = os.getenv("START_DATE")
END = os.getenv("END_DATE")
PGURL = os.getenv("PGURL")

if not START or not END:
    raise ValueError("START_DATE or END_DATE not provided")
if not PGURL:
    raise ValueError("PGURL not provided")

engine = create_engine(PGURL)

conversion_factor = int(os.getenv("CONVERSION_FACTOR", 4))
duration = int(os.getenv("DURATION_H", 4))
subsidy_per_mwh = float(os.getenv("SUBSIDY_PER_MWH", 350))
capex = float(os.getenv("CAPEX_YUAN_PER_KWH", 600))
degradation = float(os.getenv("DEGRADATION_RATE", 0.04))
om_cost = float(os.getenv("OM_COST_PER_MW_PER_YEAR", 24000))
life_years = int(os.getenv("LIFE_YEARS", 10))
read_chunk_rows = int(os.getenv("PIPELINE_READ_CHUNK_ROWS", "250000"))

logger.info(
    "Pipeline config: start=%s end=%s conversion_factor=%s duration=%s chunk_rows=%s",
    START,
    END,
    conversion_factor,
    duration,
    read_chunk_rows,
)
log_mem("startup")


# ==========================================================
# LOAD STATION MASTER
# ==========================================================

station_df = pd.read_sql(
    'SELECT plant_name, mw AS "MW", owner FROM marketdata.station_master',
    engine,
)
station_df["plant_name"] = station_df["plant_name"].astype(str).str.strip()
station_df = station_df[station_df["plant_name"].notna()]
station_df = station_df.drop_duplicates(subset=["plant_name"])
log_df("station_df", station_df)
log_mem("after_station_load")

station_set = set(station_df["plant_name"].tolist())


# ==========================================================
# STREAM INPUT + AGGREGATE TO REDUCE PEAK MEMORY
# ==========================================================

query = text(
    """
    SELECT plant_name, dispatch_unit_name, datetime,
           cleared_price, cleared_energy_mwh
    FROM marketdata.md_id_cleared_energy
    WHERE datetime >= :start AND datetime < :end
    ORDER BY datetime, plant_name
    """
)

hasher_by_plant: dict[str, hashlib._hashlib.HASH] = {}
asset_type_by_plant: dict[str, str] = {}

agg_by_plant: dict[str, dict[str, float]] = defaultdict(
    lambda: {
        "discharge_mwh": 0.0,
        "charge_mwh": 0.0,
        "energy_revenue": 0.0,
        "charge_cost": 0.0,
        "subsidy": 0.0,
    }
)

chunk_idx = 0
for chunk in pd.read_sql(query, engine, params={"start": START, "end": END}, chunksize=read_chunk_rows):
    chunk_idx += 1
    log_df(f"df_id_chunk_{chunk_idx}_raw", chunk)

    chunk["datetime"] = pd.to_datetime(chunk["datetime"], errors="coerce")
    chunk = chunk[chunk["datetime"].notna()]

    chunk["plant_name"] = chunk["plant_name"].astype(str).str.strip()
    chunk.loc[chunk["plant_name"].isin(["", "nan", "None"]), "plant_name"] = pd.NA
    chunk = chunk[chunk["plant_name"].notna()]

    chunk["dispatch_unit_name"] = chunk["dispatch_unit_name"].astype(str)

    # shrink numeric dtypes safely
    chunk["cleared_price"] = pd.to_numeric(chunk["cleared_price"], errors="coerce").astype("float32")
    chunk["cleared_energy_mwh"] = pd.to_numeric(chunk["cleared_energy_mwh"], errors="coerce").astype("float32")

    # capture asset type mapping (first seen dispatch unit)
    meta = chunk[["plant_name", "dispatch_unit_name"]].drop_duplicates(subset=["plant_name"], keep="first")
    for plant, dispatch in meta.itertuples(index=False):
        if plant not in asset_type_by_plant:
            asset_type_by_plant[plant] = infer_asset_type(dispatch)

    # streaming nodal signature to avoid giant pivot table
    chunk_hashable = chunk[["plant_name", "datetime", "cleared_price"]].copy()
    chunk_hashable["price_r2"] = chunk_hashable["cleared_price"].round(2)

    for plant, grp in chunk_hashable.groupby("plant_name", sort=False):
        h = hasher_by_plant.setdefault(plant, hashlib.sha1())
        for ts, px in grp[["datetime", "price_r2"]].itertuples(index=False):
            h.update(f"{ts.isoformat()}|{px:.2f};".encode("utf-8"))

    # aggregate only station plants for arbitrage metrics
    chunk_bess = chunk[chunk["plant_name"].isin(station_set)][
        ["plant_name", "cleared_energy_mwh", "cleared_price"]
    ]

    if not chunk_bess.empty:
        chunk_bess["discharge_mwh_raw"] = chunk_bess["cleared_energy_mwh"].clip(lower=0)
        chunk_bess["charge_mwh_raw"] = chunk_bess["cleared_energy_mwh"].clip(upper=0).abs()
        chunk_bess["energy_revenue_raw"] = chunk_bess["discharge_mwh_raw"] * chunk_bess["cleared_price"]
        chunk_bess["charge_cost_raw"] = chunk_bess["charge_mwh_raw"] * chunk_bess["cleared_price"]
        chunk_bess["subsidy_raw"] = chunk_bess["discharge_mwh_raw"] * subsidy_per_mwh

        grouped = chunk_bess.groupby("plant_name", as_index=False).agg(
            discharge_mwh=("discharge_mwh_raw", "sum"),
            charge_mwh=("charge_mwh_raw", "sum"),
            energy_revenue=("energy_revenue_raw", "sum"),
            charge_cost=("charge_cost_raw", "sum"),
            subsidy=("subsidy_raw", "sum"),
        )

        for row in grouped.itertuples(index=False):
            rec = agg_by_plant[row.plant_name]
            rec["discharge_mwh"] += float(row.discharge_mwh)
            rec["charge_mwh"] += float(row.charge_mwh)
            rec["energy_revenue"] += float(row.energy_revenue)
            rec["charge_cost"] += float(row.charge_cost)
            rec["subsidy"] += float(row.subsidy)

    logger.info("Processed chunk %s", chunk_idx)
    log_mem(f"after_chunk_{chunk_idx}")

    del chunk
    gc.collect()


# ==========================================================
# BUILD CLUSTERS + PEER SUMMARY (MEMORY-SAFE)
# ==========================================================

cluster_df = pd.DataFrame(
    {
        "plant_name": list(hasher_by_plant.keys()),
        "signature": [h.hexdigest() for h in hasher_by_plant.values()],
    }
)
cluster_df["cluster_id"] = cluster_df.groupby("signature", sort=False).ngroup() + 1
cluster_df["cluster_size"] = cluster_df.groupby("cluster_id")["plant_name"].transform("count")
cluster_df["asset_type"] = cluster_df["plant_name"].map(asset_type_by_plant).fillna("thermal")

capacity_df = infer_capacity_from_history(conversion_factor)
capacity_df = capacity_df[["plant_name", "inferred_mw"]].drop_duplicates(subset=["plant_name"])

clusters = merge_with_logging(
    cluster_df,
    capacity_df,
    name="clusters+capacity",
    on=["plant_name"],
    how="left",
    validate="one_to_one",
)
clusters["inferred_mw"] = pd.to_numeric(clusters["inferred_mw"], errors="coerce").fillna(0).round(0)
log_df("clusters", clusters)

_, _, peer_summary = build_peer_tables(station_df, clusters, include_mapping=False)
log_df("peer_summary", peer_summary)
log_mem("after_peer_summary")


# ==========================================================
# ARBITRAGE LOGIC (from aggregated station metrics)
# ==========================================================

agg_df = pd.DataFrame(
    [
        {
            "plant_name": plant,
            **vals,
        }
        for plant, vals in agg_by_plant.items()
    ]
)
if agg_df.empty:
    agg_df = pd.DataFrame(
        columns=[
            "plant_name",
            "discharge_mwh",
            "charge_mwh",
            "energy_revenue",
            "charge_cost",
            "subsidy",
        ]
    )

result = merge_with_logging(
    agg_df,
    station_df[["plant_name", "MW", "owner"]],
    name="agg+station",
    on=["plant_name"],
    how="left",
    validate="one_to_one",
)

for col in ["discharge_mwh", "charge_mwh", "energy_revenue", "charge_cost", "subsidy"]:
    result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0)

result[["discharge_mwh", "charge_mwh", "energy_revenue", "charge_cost", "subsidy"]] /= conversion_factor

result["arbitrage_profit"] = result["energy_revenue"] - result["charge_cost"]
result["expected_total_profit"] = result["arbitrage_profit"] + result["subsidy"]

total_days = max((pd.to_datetime(END) - pd.to_datetime(START)).days, 1)
denom = pd.to_numeric(result["MW"], errors="coerce") * duration * total_days

result["unit_charging_cost"] = np.where(
    result["charge_mwh"] > 0,
    result["charge_cost"] / result["charge_mwh"],
    np.nan,
)

result["unit_discharging_revenue"] = np.where(
    result["discharge_mwh"] > 0,
    result["energy_revenue"] / result["discharge_mwh"],
    np.nan,
)

result["efficiency"] = np.where(
    result["charge_mwh"] > 0,
    result["discharge_mwh"] / result["charge_mwh"],
    np.nan,
)

result["estimated_cycles_per_day"] = np.where(
    denom > 0,
    result["discharge_mwh"] / denom,
    np.nan,
)

result["arbitrage_profit_per_discharge_mwh"] = np.where(
    result["discharge_mwh"] > 0,
    result["arbitrage_profit"] / result["discharge_mwh"],
    np.nan,
)

result["total_profit_per_discharge_mwh"] = np.where(
    result["discharge_mwh"] > 0,
    result["expected_total_profit"] / result["discharge_mwh"],
    np.nan,
)

result["arbitrage_per_installed_volume_per_day"] = np.where(
    denom > 0,
    result["arbitrage_profit"] / denom,
    np.nan,
)

result["total_profit_per_installed_volume_per_day"] = np.where(
    denom > 0,
    result["expected_total_profit"] / denom,
    np.nan,
)

cluster_map = clusters[["plant_name", "cluster_id"]].drop_duplicates(subset=["plant_name"])
result = merge_with_logging(
    result,
    cluster_map,
    name="result+clusters",
    on=["plant_name"],
    how="left",
    validate="one_to_one",
)

peer_map = peer_summary.rename(columns={"bess_plant": "plant_name"}).drop_duplicates(subset=["plant_name"])
result = merge_with_logging(
    result,
    peer_map,
    name="result+peer_summary",
    on=["plant_name"],
    how="left",
    validate="one_to_one",
)

for col in [
    "peer_count_bess",
    "peer_count_solar",
    "peer_count_wind",
    "peer_count_thermal",
    "peer_MW_bess",
    "peer_MW_solar",
    "peer_MW_wind",
    "peer_MW_thermal",
]:
    if col not in result.columns:
        result[col] = 0


# ==========================================================
# IRR & PAYBACK
# ==========================================================

investment_per_mw = capex * 1000 * duration
result["irr"] = np.nan
result["payback_years"] = np.nan

for idx, row in result.iterrows():
    annual_profit = row["total_profit_per_installed_volume_per_day"] * duration * 365

    if pd.isna(annual_profit) or annual_profit <= 0:
        continue

    cashflows = [-investment_per_mw]
    cumulative = -investment_per_mw
    payback = np.nan

    for y in range(1, life_years + 1):
        degraded = annual_profit * ((1 - degradation) ** (y - 1))
        net = degraded - om_cost
        cashflows.append(net)
        cumulative += net

        if cumulative >= 0 and pd.isna(payback):
            payback = y

    irr_value = npf.irr(cashflows)
    if irr_value is not None and np.isfinite(irr_value):
        result.at[idx, "irr"] = irr_value

    result.at[idx, "payback_years"] = payback

result.replace([np.inf, -np.inf], np.nan, inplace=True)

result["rank_total_profit_per_installed_volume"] = (
    result["total_profit_per_installed_volume_per_day"]
    .rank(ascending=False, method="min")
    .fillna(0)
    .astype(int)
)

result["rank_arbitrage_profit"] = (
    result["arbitrage_profit_per_discharge_mwh"]
    .rank(ascending=False, method="min")
    .fillna(0)
    .astype(int)
)

result["rank_cycles"] = (
    result["estimated_cycles_per_day"]
    .rank(ascending=False, method="min")
    .fillna(0)
    .astype(int)
)

result["rank_efficiency"] = (
    result["efficiency"]
    .rank(ascending=False, method="min")
    .fillna(0)
    .astype(int)
)

clusters["start_date"] = START
clusters["end_date"] = END

for col in clusters.columns:
    if str(clusters[col].dtype) == "uint64":
        clusters[col] = clusters[col].astype("int64")

with engine.begin() as conn:
    conn.execute(
        text(
            """
            DELETE FROM marketdata.inner_mongolia_nodal_clusters
            WHERE start_date = :start AND end_date = :end
        """
        ),
        {"start": START, "end": END},
    )

    clusters.to_sql(
        "inner_mongolia_nodal_clusters",
        conn,
        schema="marketdata",
        if_exists="append",
        index=False,
    )


# ==========================================================
# FINAL FORMAT
# ==========================================================

result[[
    "arbitrage_profit",
    "charge_cost",
    "energy_revenue",
    "subsidy",
    "expected_total_profit",
]] /= 10000

result.rename(
    columns={
        "arbitrage_profit": "arbitrage_profit_万元",
        "charge_cost": "charge_cost_万元",
        "energy_revenue": "energy_revenue_万元",
        "subsidy": "subsidy_万元",
        "expected_total_profit": "expected_total_profit_万元",
    },
    inplace=True,
)


# ==========================================================
# SAVE TO DB
# ==========================================================

with engine.begin() as conn:
    conn.execute(
        text(
            """
            DELETE FROM marketdata.inner_mongolia_bess_results
            WHERE start_date = :start AND end_date = :end
        """
        ),
        {"start": START, "end": END},
    )

result = result.replace({np.nan: None, np.inf: None, -np.inf: None})


def clean_json_value(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


records = []
for _, row in result.iterrows():
    row_dict = row.to_dict()
    row_dict = {k: clean_json_value(v) for k, v in row_dict.items()}

    owner = row_dict.get("owner")
    if owner is None or str(owner).strip().lower() == "nan":
        owner = None

    records.append(
        {
            "plant_name": row_dict["plant_name"],
            "owner": owner,
            "mw": row_dict.get("MW"),
            "irr": row_dict.get("irr"),
            "payback_years": row_dict.get("payback_years"),
            "start_date": START,
            "end_date": END,
            "result_json": json.dumps(row_dict, ensure_ascii=False),
        }
    )

records_df = pd.DataFrame(records)
log_df("records_df", records_df)
log_mem("before_results_write")

with engine.begin() as conn:
    records_df.to_sql(
        "inner_mongolia_bess_results",
        conn,
        schema="marketdata",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

log_mem("done")
print("✅ Pipeline finished successfully.")
