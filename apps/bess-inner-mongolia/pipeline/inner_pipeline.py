# -*- coding: utf-8 -*-

import os
import pandas as pd
from sqlalchemy import create_engine
from shared.core import build_master, build_peer_tables
from shared.init_results_table import create_schema_and_table
import numpy as np
import numpy_financial as npf
from sqlalchemy import text
import json
import math

# ==========================================================
# INIT
# ==========================================================

create_schema_and_table()

START = os.getenv("START_DATE")
END = os.getenv("END_DATE")
PGURL = os.getenv("PGURL")

if not START or not END:
    raise ValueError("START_DATE or END_DATE not provided")

engine = create_engine(PGURL)

conversion_factor = int(os.getenv("CONVERSION_FACTOR", 4))
duration = int(os.getenv("DURATION_H", 4))
subsidy_per_mwh = float(os.getenv("SUBSIDY_PER_MWH", 350))
capex = float(os.getenv("CAPEX_YUAN_PER_KWH", 600))
degradation = float(os.getenv("DEGRADATION_RATE", 0.04))
om_cost = float(os.getenv("OM_COST_PER_MW_PER_YEAR", 24000))
life_years = int(os.getenv("LIFE_YEARS", 10))


# ==========================================================
# LOAD INPUT DATA
# ==========================================================


station_df = pd.read_sql(
    'SELECT plant_name, mw AS "MW", owner FROM marketdata.station_master',
    engine
)

# station_df.columns = station_df.columns.str.lower()
# station_df = station_df.rename(columns={"mw": "MW"})

df_id = pd.read_sql(
    """
    SELECT plant_name, dispatch_unit_name, datetime,
           cleared_price, cleared_energy_mwh
    FROM marketdata.md_id_cleared_energy
    WHERE datetime >= %(start)s AND datetime < %(end)s
    """,
    engine,
    params={"start": START, "end": END}
)

df_id["datetime"] = pd.to_datetime(df_id["datetime"])

# normalize join keys (strong version)
for df in [df_id, station_df]:
    if "plant_name" in df.columns:
        df["plant_name"] = (
            df["plant_name"]
            .astype(str)
            .str.strip()
        )

        df.loc[df["plant_name"].isin(["", "nan", "None"]), "plant_name"] = None

df_id = df_id[df_id["plant_name"].notna()]
station_df = station_df[station_df["plant_name"].notna()]
# ==========================================================
# CLUSTERING
# ==========================================================



clusters = build_master(df_id, conversion_factor)

bess_map, nodal_mapping, peer_summary = build_peer_tables(
    station_df,
    clusters
)

# ==========================================================
# ARBITRAGE LOGIC
# ==========================================================

df_bess = df_id[df_id["plant_name"].isin(station_df["plant_name"])].copy()
df_bess = df_bess.merge(station_df, on="plant_name", how="left")

df_bess["discharge_mwh_raw"] = df_bess["cleared_energy_mwh"].clip(lower=0)
df_bess["charge_mwh_raw"] = df_bess["cleared_energy_mwh"].clip(upper=0).abs()

df_bess["energy_revenue_raw"] = df_bess["discharge_mwh_raw"] * df_bess["cleared_price"]
df_bess["charge_cost_raw"] = df_bess["charge_mwh_raw"] * df_bess["cleared_price"]
df_bess["subsidy_raw"] = df_bess["discharge_mwh_raw"] * subsidy_per_mwh

result = df_bess.groupby(["plant_name", "MW", "owner"]).agg(
    discharge_mwh=("discharge_mwh_raw", "sum"),
    charge_mwh=("charge_mwh_raw", "sum"),
    energy_revenue=("energy_revenue_raw", "sum"),
    charge_cost=("charge_cost_raw", "sum"),
    subsidy=("subsidy_raw", "sum")
).reset_index()

result[["discharge_mwh","charge_mwh","energy_revenue","charge_cost","subsidy"]] /= conversion_factor

result["arbitrage_profit"] = result["energy_revenue"] - result["charge_cost"]
result["expected_total_profit"] = result["arbitrage_profit"] + result["subsidy"]

total_days = max((pd.to_datetime(END) - pd.to_datetime(START)).days, 1)
denom = result["MW"] * duration * total_days

result["unit_charging_cost"] = np.where(
    result["charge_mwh"] > 0,
    result["charge_cost"] / result["charge_mwh"],
    np.nan
)

result["unit_discharging_revenue"] = np.where(
    result["discharge_mwh"] > 0,
    result["energy_revenue"] / result["discharge_mwh"],
    np.nan
)

result["efficiency"] = np.where(
    result["charge_mwh"] > 0,
    result["discharge_mwh"] / result["charge_mwh"],
    np.nan
)

result["estimated_cycles_per_day"] = np.where(
    denom > 0,
    result["discharge_mwh"] / denom,
    np.nan
)

result["arbitrage_profit_per_discharge_mwh"] = np.where(
    result["discharge_mwh"] > 0,
    result["arbitrage_profit"] / result["discharge_mwh"],
    np.nan
)

result["total_profit_per_discharge_mwh"] = np.where(
    result["discharge_mwh"] > 0,
    result["expected_total_profit"] / result["discharge_mwh"],
    np.nan
)

result["arbitrage_per_installed_volume_per_day"] = np.where(
    denom > 0,
    result["arbitrage_profit"] / denom,
    np.nan
)

result["total_profit_per_installed_volume_per_day"] = np.where(
    denom > 0,
    result["expected_total_profit"] / denom,
    np.nan
)

clusters["plant_name"] = clusters["plant_name"].astype(str).str.strip()

result = result.merge(
    clusters[["plant_name", "cluster_id"]].drop_duplicates(),
    
    on="plant_name",
    how="left"
)

result = result.merge(
    peer_summary.rename(columns={"bess_plant": "plant_name"}),
    on="plant_name",
    how="left"
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

# Fix unsupported PostgreSQL types
for col in clusters.columns:
    if str(clusters[col].dtype) == "uint64":
        clusters[col] = clusters[col].astype("int64")

with engine.begin() as conn:
    conn.execute(
        text("""
            DELETE FROM marketdata.inner_mongolia_nodal_clusters
            WHERE start_date = :start AND end_date = :end
        """),
        {"start": START, "end": END}
    )

    clusters.to_sql(
        "inner_mongolia_nodal_clusters",
        conn,
        schema="marketdata",
        if_exists="append",
        index=False
    )
# ==========================================================
# FINAL FORMAT
# ==========================================================

result[[
    "arbitrage_profit",
    "charge_cost",
    "energy_revenue",
    "subsidy",
    "expected_total_profit"
]] /= 10000

result.rename(columns={
    "arbitrage_profit": "arbitrage_profit_万元",
    "charge_cost": "charge_cost_万元",
    "energy_revenue": "energy_revenue_万元",
    "subsidy": "subsidy_万元",
    "expected_total_profit": "expected_total_profit_万元"
}, inplace=True)

# ==========================================================
# SAVE TO DB
# ==========================================================

with engine.begin() as conn:
    conn.execute(
        text("""
            DELETE FROM marketdata.inner_mongolia_bess_results
            WHERE start_date = :start AND end_date = :end
        """),
        {"start": START, "end": END}
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

    records.append({
        "plant_name": row_dict["plant_name"],
        "owner": owner,
        "mw": row_dict.get("MW"),
        "irr": row_dict.get("irr"),
        "payback_years": row_dict.get("payback_years"),
        "start_date": START,
        "end_date": END,
        "result_json": json.dumps(row_dict, ensure_ascii=False)
    })
records_df = pd.DataFrame(records)

with engine.begin() as conn:

    records_df.to_sql(
        "inner_mongolia_bess_results",
        conn,
        schema="marketdata",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )

print("✅ Pipeline finished successfully.")