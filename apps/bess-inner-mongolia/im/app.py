# -*- coding: utf-8 -*-
"""
Streamlit UI for Inner Mongolia BESS Market Analytics.

Design goals:
- Keep the original functions / display formats from InnerMongolia_MarketAssets_v7.py
- Offload heavy arbitrage computation to an async ECS Fargate "pipeline" task (inner_pipeline.py)
- After the task finishes, read results from Postgres and display with the original UI formatting
"""
import io
import os
import time
import sys
import logging
from datetime import date
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))
if "/apps" not in sys.path:
    sys.path.insert(0, "/apps")

import boto3
import numpy as np
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from streamlit_autorefresh import st_autorefresh

from shared.core import irr_from_cashflows, infer_asset_type, build_peer_detail_table
from auth.rbac import require_role

role = require_role(["Admin", "Trader", "Quant", "Analyst"])


# ==========================================================
# LOAD ENV
# ==========================================================
load_dotenv()

# ==========================================================
# DB
# ==========================================================
def get_engine():
    pg_url = os.getenv("PGURL")
    if not pg_url:
        st.error("❌ PGURL not found (set env var PGURL).")
        st.stop()
    return create_engine(pg_url)

# ==========================================================
# ECS ASYNC PIPELINE INTEGRATION
# ==========================================================
ecs = boto3.client("ecs", region_name=os.getenv("AWS_REGION", "ap-southeast-1"))

_ECS_CLUSTER = os.getenv("ECS_CLUSTER")
_PIPELINE_TASK_DEF = os.getenv("PIPELINE_TASK_DEF")
_PRIVATE_SUBNETS = os.getenv("PRIVATE_SUBNETS", "")
_TASK_SECURITY_GROUPS = os.getenv("TASK_SECURITY_GROUPS", "")

def start_pipeline_task(start_date: str, end_date: str, config: dict) -> str:
    """
    Start the ECS task that runs inner_pipeline.py.
    Pass runtime config via container environment variables.
    """
    # Local mode: ECS_CLUSTER / PIPELINE_TASK_DEF are not set.
    # This app runs in view-only mode locally — previously computed results
    # (stored in the DB) are fully readable.  Triggering a new pipeline run
    # requires AWS credentials + network access to ECS; set ECS_CLUSTER and
    # PIPELINE_TASK_DEF env vars to re-enable in a hybrid local setup.
    if not _ECS_CLUSTER or not _PIPELINE_TASK_DEF:
        raise RuntimeError(
            "Pipeline trigger unavailable in local mode "
            "(ECS_CLUSTER / PIPELINE_TASK_DEF not configured)."
        )

    env_list = [
        {"name": "START_DATE", "value": start_date},
        {"name": "END_DATE", "value": end_date},
        # config passthrough
        {"name": "CONVERSION_FACTOR", "value": str(config["conversion_factor"])},
        {"name": "DURATION_H", "value": str(config["duration"])},
        {"name": "SUBSIDY_PER_MWH", "value": str(config["subsidy_per_mwh"])},
        {"name": "CAPEX_YUAN_PER_KWH", "value": str(config["capex_yuan_per_kwh"])},
        {"name": "DEGRADATION_RATE", "value": str(config["degradation_rate"])},
        {"name": "OM_COST_PER_MW_PER_YEAR", "value": str(config["om_cost_per_mw_per_year"])},
        {"name": "LIFE_YEARS", "value": str(config["life_years"])},
    ]

    resp = ecs.run_task(
        cluster=_ECS_CLUSTER,
        taskDefinition=_PIPELINE_TASK_DEF,
        launchType="FARGATE",
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": _PRIVATE_SUBNETS.split(","),
                "securityGroups": _TASK_SECURITY_GROUPS.split(","),
                "assignPublicIp": "ENABLED"
            }
        },
        overrides={
            "containerOverrides": [
                {
                    "name": os.getenv("PIPELINE_CONTAINER_NAME", "pipeline"),
                    "environment": env_list
                }
            ]
        }
    )

    print(resp)
    
    tasks = resp.get("tasks", [])
    if not tasks:
        raise RuntimeError(f"ECS run_task returned no tasks: {resp}")
    return tasks[0]["taskArn"]

def get_task_status(task_arn: str) -> dict:
    """
    Return status dict: {"lastStatus": ..., "stopCode":..., "stoppedReason":..., "exitCode":...}
    """
    r = ecs.describe_tasks(cluster=_ECS_CLUSTER, tasks=[task_arn])
    tasks = r.get("tasks", [])
    if not tasks:
        return {"lastStatus": "UNKNOWN"}

    t = tasks[0]
    out = {
        "lastStatus": t.get("lastStatus"),
        "desiredStatus": t.get("desiredStatus"),
        "stopCode": t.get("stopCode"),
        "stoppedReason": t.get("stoppedReason"),
    }
    # container exit code (best-effort)
    containers = t.get("containers", []) or []
    if containers:
        out["containerName"] = containers[0].get("name")
        out["exitCode"] = containers[0].get("exitCode")
        out["reason"] = containers[0].get("reason")
    return out

# ==========================================================
# STATION MASTER DB STORAGE (same as v7)
# ==========================================================
def ensure_station_table():
    engine = get_engine()
    create_sql = text("""
        CREATE SCHEMA IF NOT EXISTS marketdata;
        CREATE TABLE IF NOT EXISTS marketdata.station_master (
            plant_name TEXT PRIMARY KEY,
            MW NUMERIC,
            owner TEXT,
            updated_at TIMESTAMP DEFAULT NOW()
        );
    """)
    with engine.begin() as conn:
        conn.execute(create_sql)

def save_station_to_db(df: pd.DataFrame):
    engine = get_engine()
    df_to_save = df.copy()
    
    df_to_save = df_to_save.rename(columns={
    "MW": "mw"
    })
    df_to_save["updated_at"] = pd.Timestamp.now()

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE marketdata.station_master"))
        df_to_save.to_sql(
            "station_master",
            conn,
            schema="marketdata",
            if_exists="append",
            index=False
        )

def load_station_from_db() -> pd.DataFrame:
    engine = get_engine()
    sql = "SELECT plant_name, mw AS \"MW\", owner FROM marketdata.station_master"
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)

ensure_station_table()

# ==========================================================
# EXCEL LOADER (same as v7)
# ==========================================================
@st.cache_data
def load_station_excel(file) -> pd.DataFrame:
    df = pd.read_excel(file)
    df.columns = df.columns.str.strip()

    required_cols = ["调度机组名称", "额定功率", "业主"]
    for col in required_cols:
        if col not in df.columns:
            st.error(f"Excel must contain column: {col}")
            st.stop()

    df = df.rename(columns={
        "调度机组名称": "plant_name",
        "额定功率": "MW",
        "业主": "owner"
    })

    df["plant_name"] = df["plant_name"].astype(str).str.strip()
    df["owner"] = df["owner"].astype(str).str.strip()

    df = df.drop_duplicates(subset=["plant_name"])
    return df[["plant_name", "MW", "owner"]]

# ==========================================================
# ORIGINAL FUNCTIONS KEPT (for compatibility / future use)
# NOTE: in the new architecture, the heavy computation is done in inner_pipeline.py,
# so these are typically not called. They are kept to meet "do not lose anything".
# ==========================================================



def to_excel(sheets: dict) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name[:31], index=False)
    return output.getvalue()

# ==========================================================
# LOAD RESULTS FROM DB (written by pipeline)
# ==========================================================
@st.cache_data(ttl=60)
def load_results_from_db(start: str, end: str) -> pd.DataFrame:
    engine = get_engine()
    q = text("""
        SELECT result_json
        FROM marketdata.inner_mongolia_bess_results
        WHERE start_date = :start AND end_date = :end
        ORDER BY plant_name
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"start": start, "end": end}).fetchall()

    if not rows:
        return pd.DataFrame()

    # expand json payload
    records = [r[0] for r in rows]
    df = pd.DataFrame.from_records(records)

    # Ensure numeric columns are numeric (best-effort)
    for c in df.columns:
        if c in {"plant_name", "owner"}:
            continue
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

@st.cache_data(ttl=60)
def load_clusters_from_db(start: str, end: str) -> pd.DataFrame:
    engine = get_engine()
    q = text("""
        SELECT plant_name, signature, cluster_id, cluster_size, asset_type, inferred_mw
        FROM marketdata.inner_mongolia_nodal_clusters
        WHERE start_date = :start AND end_date = :end
    """)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"start": start, "end": end})
    return df

# ==========================================================
# STREAMLIT CONFIG (same style as v7)
# ==========================================================
st.set_page_config(layout="wide")
st.title("Inner Mongolia BESS Market Analytics")
st.subheader("BESS Arbitrage & Nodal Intelligence")

# ==========================================================
# SMART DEFAULT DATE RANGE (same as v7)
# ==========================================================
today = date.today()
first_day_current_month = date(today.year, today.month, 1)
first_day_previous_month = date(today.year - 1, 12, 1) if today.month == 1 else date(today.year, today.month - 1, 1)

# ==========================================================
# SIDEBAR CONTROLS (same as v7)
# ==========================================================
start_date = st.sidebar.date_input("Start Date", value=first_day_previous_month)
end_date = st.sidebar.date_input("End Date (exclusive)", value=first_day_current_month)

if start_date >= end_date:
    st.sidebar.error("End date must be after start date.")
    st.stop()

start = str(start_date)
end = str(end_date)

start_dt = pd.to_datetime(start_date).date()
end_dt = pd.to_datetime(end_date).date()


uploaded_file = st.sidebar.file_uploader("Upload 电站.xlsx", type=["xlsx"])
# uploaded_file = st.file_uploader("Upload 电站.xlsx", type=["xlsx"])

is_15min = st.sidebar.checkbox("Data is 15-min settlement (divide by 4)", value=True)
conversion_factor = 4 if is_15min else 1

duration = st.sidebar.number_input("BESS duration (hours)", min_value=1, max_value=12, value=4, step=1)
subsidy_per_mwh = st.sidebar.number_input("Subsidy (yuan/MWh discharged)", min_value=0, max_value=2000, value=350, step=10)

capex_yuan_per_kwh = st.sidebar.number_input(
    "Investment cost (yuan/kWh)",
    min_value=100,
    max_value=2000,
    value=600,
    step=50
)

degradation_rate = st.sidebar.number_input(
    "Annual degradation rate (%)",
    min_value=0.0,
    max_value=20.0,
    value=4.0,
    step=0.5
) / 100

om_cost_per_mw_per_year = st.sidebar.number_input(
    "Annual O&M (yuan per MW per year)",
    min_value=0,
    max_value=500000,
    value=24000,
    step=10000
)

life_years = st.sidebar.number_input("Project life (years)", min_value=1, max_value=30, value=10, step=1)

show_only_yuanjing = st.sidebar.checkbox("Show only 远景 assets", value=False)

# ----------------------------------------------------------
# STATION DATA LOADING (AUTO LOAD FROM DB) — same UX as v7
# ----------------------------------------------------------
if uploaded_file is not None:
    station_df = load_station_excel(uploaded_file)
    save_station_to_db(station_df)
    st.success("Station master updated in DB.")
else:
    station_df = load_station_from_db()

if station_df.empty:
    st.warning("No station data found in DB. Please upload 电站.xlsx.")
    st.stop()
else:
    st.info("Loaded station master from database.")

# ==========================================================
# MAIN EXECUTION (ASYNC ECS)
# ==========================================================
run_clicked = st.button("Run BESS Arbitrage", type="primary")

if run_clicked:
    
    overrides={
    "containerOverrides": [
        {
            "name": "pipeline",
            "environment": [
                {"name": "DURATION_H", "value": str(duration)},
                {"name": "SUBSIDY_PER_MWH", "value": str(subsidy_per_mwh)},
                {"name": "CONVERSION_FACTOR", "value": str(conversion_factor)},
                {"name": "CAPEX_YUAN_PER_KWH", "value": str(capex_yuan_per_kwh)},
                {"name": "DEGRADATION_RATE", "value": str(degradation_rate)},
                {"name": "OM_COST_PER_MW_PER_YEAR", "value": str(om_cost_per_mw_per_year)},
                {"name": "LIFE_YEARS", "value": str(life_years)}
                ]
            }
        ]
    }
    
    cfg = dict(
        conversion_factor=conversion_factor,
        duration=duration,
        subsidy_per_mwh=subsidy_per_mwh,
        capex_yuan_per_kwh=capex_yuan_per_kwh,
        degradation_rate=degradation_rate,
        om_cost_per_mw_per_year=om_cost_per_mw_per_year,
        life_years=life_years,
    )

    try:
        task_arn = start_pipeline_task(start, end, cfg)
    except Exception as e:
        st.error(f"Failed to start ECS task: {e}")
        st.stop()

    st.session_state["pipeline_task_arn"] = task_arn
    st.session_state["pipeline_started_at"] = time.time()
    st.success("Pipeline started on ECS.")
    st.rerun()

# UI: show status without blocking websocket
if "pipeline_task_arn" in st.session_state:
    task_arn = st.session_state["pipeline_task_arn"]
    status = get_task_status(task_arn)

    st.info(f"Pipeline status: {status.get('lastStatus')}")
    
    
    # AUTO REFRESH while pipeline is running
    if status.get("lastStatus") != "STOPPED":
        st_autorefresh(interval=5000, key="pipeline_poll")

    if status.get("lastStatus") == "STOPPED":
        exit_code = status.get("exitCode")
        if exit_code not in (None, 0):
            st.error(f"Pipeline STOPPED with exitCode={exit_code}. Reason: {status.get('reason') or status.get('stoppedReason')}")
        else:
            st.success("Pipeline finished.")
        

    if status.get("lastStatus") == "STOPPED" and status.get("exitCode") in (None, 0):
        # Load results + clusters written by pipeline
        result = load_results_from_db(start, end)
        
        st.caption(f"Data period: {start_dt} → {end_dt}")
        clusters = load_clusters_from_db(start, end)

        if result.empty:
            st.warning("No results found for this date range in DB yet.")
        else:
            st.session_state["run"] = {"result": result, "clusters": clusters}

# ==========================================================
# DISPLAY (same formatting as v7)
# ==========================================================
if "run" in st.session_state:
    result = st.session_state["run"]["result"]
    clusters = st.session_state["run"].get("clusters", pd.DataFrame())

    if show_only_yuanjing:
        result = result[result["owner"].str.contains("远景", na=False)]

    # ---------------- Portfolio IRR ----------------
        
    
    # Ensure derived metric exists
    if "total_profit_per_installed_volume_per_day" not in result.columns:
        if "expected_total_profit_万元" in result.columns:
            mw_col = "MW" if "MW" in result.columns else "mw"
            result["total_profit_per_installed_volume_per_day"] = (
                result["expected_total_profit_万元"] * 10000
                / pd.to_numeric(result[mw_col], errors="coerce").fillna(0)
                / 365
            )

    # Daily average revenue
    num_days = (end_dt - start_dt).days
    if "expected_total_profit_万元" in result.columns and num_days > 0:
        result["daily_avg_revenue_万元"] = result["expected_total_profit_万元"] / num_days
            
    yuanjing_df = result[result["owner"].str.contains("远景", na=False)]

    portfolio_irr = np.nan

    if not yuanjing_df.empty:
        mw_col = "MW" if "MW" in yuanjing_df.columns else "mw"
        total_mw = pd.to_numeric(yuanjing_df[mw_col], errors="coerce").fillna(0).sum()
        # print("yuanjing_df columns:", yuanjing_df.columns.tolist(), flush=True)
        # sys.stdout.flush()
        
        # logging.warning("yuanjing_df columns: %s", yuanjing_df.columns.tolist())
        weighted_profit = (
            yuanjing_df["total_profit_per_installed_volume_per_day"]
            * duration * 365
            * pd.to_numeric(yuanjing_df.get("MW", yuanjing_df.get("mw")), errors="coerce").fillna(0)
        ).sum()

        weighted_invest = capex_yuan_per_kwh * 1000 * duration * total_mw

        if weighted_invest > 0 and weighted_profit > 0:
            cashflows = [-weighted_invest]
            for y in range(1, life_years + 1):
                degraded = weighted_profit * ((1 - degradation_rate) ** (y - 1))
                net = degraded - om_cost_per_mw_per_year * total_mw
                cashflows.append(net)
            portfolio_irr = irr_from_cashflows(cashflows)

    col1, col2 = st.columns(2)
    period = f"{start_dt} → {end_dt}"
    
    col1.metric("远景 Portfolio IRR", f"{portfolio_irr*100:.2f}%" if not np.isnan(portfolio_irr) else "N/A", delta=period)
    
    col2.metric("远景 Total MW", f"{yuanjing_df.get('MW', yuanjing_df.get('mw')).sum():,.0f}" if not yuanjing_df.empty else "0")

    # ---------------- Table formatting ----------------
    format_dict = {
        "discharge_mwh": "{:,.0f}",
        "charge_mwh": "{:,.0f}",
        "MW": "{:,.0f}",
        "arbitrage_profit_per_discharge_mwh": "{:,.0f}",
        "total_profit_per_discharge_mwh": "{:,.0f}",
        "arbitrage_per_installed_volume_per_day": "{:,.0f}",
        "total_profit_per_installed_volume_per_day": "{:,.0f}",
        "peer_MW_bess": "{:,.0f}",
        "peer_MW_solar": "{:,.0f}",
        "peer_MW_wind": "{:,.0f}",
        "peer_MW_thermal": "{:,.0f}",
        "peer_count_bess": "{:,.0f}",
        "peer_count_solar": "{:,.0f}",
        "peer_count_wind": "{:,.0f}",
        "peer_count_thermal": "{:,.0f}",
        "payback_years": "{:,.0f}",
    }
    format_dict["irr"] = "{:.2%}"
    format_dict["efficiency"] = "{:.2%}"

    for col in [
        "arbitrage_profit_万元",
        "charge_cost_万元",
        "energy_revenue_万元",
        "subsidy_万元",
        "expected_total_profit_万元",
        "daily_avg_revenue_万元",
    ]:
        if col in result.columns:
            format_dict[col] = "{:,.2f}"

    def highlight(row):
        if "远景" in str(row.get("owner", "")):
            return ["background-color:#28a745;color:white;font-weight:bold"] * len(row)
        return [""] * len(row)

    cols = [
        "plant_name",
        "owner",
        "rank_total_profit_per_installed_volume",
        "rank_arbitrage_profit",
        "rank_cycles",
        "rank_efficiency",
        "MW",
        "irr",
        "payback_years",
        "discharge_mwh",
        "charge_mwh",
        "efficiency",
        "estimated_cycles_per_day",
        "discharging_revenue",
        "charging_cost",
        "arbitrage_profit_per_discharge_mwh",
        "total_profit_per_discharge_mwh",
        "arbitrage_per_installed_volume_per_day",
        "total_profit_per_installed_volume_per_day",
        "arbitrage_profit_万元",
        "charge_cost_万元",
        "energy_revenue_万元",
        "subsidy_万元",
        "expected_total_profit_万元",
        "daily_avg_revenue_万元",
        "peer_count_bess",
        "peer_count_solar",
        "peer_count_wind",
        "peer_count_thermal",
        "peer_MW_bess",
        "peer_MW_solar",
        "peer_MW_wind",
        "peer_MW_thermal",
    ]

    display_df = result[[c for c in cols if c in result.columns]].copy()
    styled = display_df.style.format(format_dict).apply(highlight, axis=1)
    st.dataframe(styled, use_container_width=True)

    # ---------------- Peer Explorer ----------------
    if not clusters.empty:
        st.markdown("---")
        st.subheader("Nodal Peer Explorer")

        selected_bess = st.selectbox("Select BESS station:", sorted(display_df["plant_name"].unique()))
        peer_detail = build_peer_detail_table(selected_bess, clusters)
        st.dataframe(peer_detail, use_container_width=True)

    # ---------------- Excel Export ----------------
    excel = to_excel({"bess_arbitrage": display_df})
    st.download_button("Download Excel", excel, f"bess_arbitrage_{start}_to_{end}.xlsx")
