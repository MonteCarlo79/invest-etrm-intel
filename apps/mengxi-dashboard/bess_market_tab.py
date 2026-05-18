# -*- coding: utf-8 -*-
"""
BESS Market Ranking tab — Inner Mongolia all-BESS performance.

Ported from apps/bess-inner-mongolia/im/app.py.
All heavy computation is delegated to the inner_pipeline ECS task (shared with
the inner-mongolia app). Results are read from:
  marketdata.inner_mongolia_bess_results
  marketdata.inner_mongolia_nodal_clusters
"""
from __future__ import annotations

import io
import os
import time
from datetime import date

import numpy as np
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

from inner_mongolia_helpers import irr_from_cashflows, build_peer_detail_table


# ---------------------------------------------------------------------------
# ECS integration
# ---------------------------------------------------------------------------
def _get_ecs_client():
    import boto3
    return boto3.client("ecs", region_name=os.getenv("AWS_REGION", "ap-southeast-1"))


def _start_pipeline_task(start_date: str, end_date: str, config: dict) -> str:
    """Start the inner_pipeline ECS task. Raises RuntimeError if env vars missing."""
    cluster = os.getenv("ECS_CLUSTER")
    task_def = os.getenv("PIPELINE_TASK_DEF")
    subnets = os.getenv("PRIVATE_SUBNETS", "")
    sg_ids = os.getenv("TASK_SECURITY_GROUPS", "")

    if not cluster or not task_def:
        raise RuntimeError(
            "Pipeline trigger unavailable: ECS_CLUSTER / PIPELINE_TASK_DEF not set."
        )

    env_list = [
        {"name": "START_DATE",            "value": start_date},
        {"name": "END_DATE",              "value": end_date},
        {"name": "CONVERSION_FACTOR",     "value": str(config["conversion_factor"])},
        {"name": "DURATION_H",            "value": str(config["duration"])},
        {"name": "SUBSIDY_PER_MWH",       "value": str(config["subsidy_per_mwh"])},
        {"name": "CAPEX_YUAN_PER_KWH",    "value": str(config["capex_yuan_per_kwh"])},
        {"name": "DEGRADATION_RATE",      "value": str(config["degradation_rate"])},
        {"name": "OM_COST_PER_MW_PER_YEAR","value": str(config["om_cost_per_mw_per_year"])},
        {"name": "LIFE_YEARS",            "value": str(config["life_years"])},
    ]

    ecs = _get_ecs_client()
    resp = ecs.run_task(
        cluster=cluster,
        taskDefinition=task_def,
        launchType="FARGATE",
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": subnets.split(","),
                "securityGroups": sg_ids.split(","),
                "assignPublicIp": "ENABLED",
            }
        },
        overrides={
            "containerOverrides": [{
                "name": os.getenv("PIPELINE_CONTAINER_NAME", "pipeline"),
                "environment": env_list,
            }]
        },
    )
    tasks = resp.get("tasks", [])
    if not tasks:
        raise RuntimeError(f"ECS run_task returned no tasks: {resp}")
    return tasks[0]["taskArn"]


def _get_task_status(task_arn: str) -> dict:
    cluster = os.getenv("ECS_CLUSTER")
    ecs = _get_ecs_client()
    r = ecs.describe_tasks(cluster=cluster, tasks=[task_arn])
    tasks = r.get("tasks", [])
    if not tasks:
        return {"lastStatus": "UNKNOWN"}
    t = tasks[0]
    out = {
        "lastStatus":    t.get("lastStatus"),
        "desiredStatus": t.get("desiredStatus"),
        "stopCode":      t.get("stopCode"),
        "stoppedReason": t.get("stoppedReason"),
    }
    containers = t.get("containers", []) or []
    if containers:
        out["exitCode"] = containers[0].get("exitCode")
        out["reason"]   = containers[0].get("reason")
    return out


# ---------------------------------------------------------------------------
# Station master helpers
# ---------------------------------------------------------------------------
def _ensure_station_table(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE SCHEMA IF NOT EXISTS marketdata;
            CREATE TABLE IF NOT EXISTS marketdata.station_master (
                plant_name TEXT PRIMARY KEY,
                MW NUMERIC,
                owner TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            );
        """))


def _save_station_to_db(df: pd.DataFrame, engine):
    df2 = df.copy().rename(columns={"MW": "mw"})
    df2["updated_at"] = pd.Timestamp.now()
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE marketdata.station_master"))
        df2.to_sql("station_master", conn, schema="marketdata", if_exists="append", index=False)


def _load_station_from_db(engine) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql('SELECT plant_name, mw AS "MW", owner FROM marketdata.station_master', conn)


# ---------------------------------------------------------------------------
# Results readers — read PGURL from env to avoid SQLAlchemy URL masking issue
# ---------------------------------------------------------------------------
def _make_engine():
    import os
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    return create_engine(url, pool_pre_ping=True)


@st.cache_data(ttl=60)
def _load_results(start: str, end: str) -> pd.DataFrame:
    engine = _make_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT result_json FROM marketdata.inner_mongolia_bess_results "
                 "WHERE start_date = :s AND end_date = :e ORDER BY plant_name"),
            {"s": start, "e": end},
        ).fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame.from_records([r[0] for r in rows])
    for c in df.columns:
        if c not in {"plant_name", "owner"}:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


@st.cache_data(ttl=60)
def _load_clusters(start: str, end: str) -> pd.DataFrame:
    engine = _make_engine()
    with engine.connect() as conn:
        return pd.read_sql(
            text("SELECT plant_name, signature, cluster_id, cluster_size, asset_type, inferred_mw "
                 "FROM marketdata.inner_mongolia_nodal_clusters "
                 "WHERE start_date = :s AND end_date = :e"),
            conn, params={"s": start, "e": end},
        )


# ---------------------------------------------------------------------------
# Excel export helper
# ---------------------------------------------------------------------------
def _to_excel(sheets: dict) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name[:31], index=False)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Main render function
# ---------------------------------------------------------------------------
def render(engine) -> None:
    """Render the BESS Market Ranking tab. Call from app.py with the SQLAlchemy engine."""
    from streamlit_autorefresh import st_autorefresh

    st.subheader("Inner Mongolia — All-BESS Market Performance")
    st.caption(
        "Ranks every BESS station by arbitrage profit, cycles, IRR, and efficiency "
        "over a selected period. Heavy computation runs as an async ECS task."
    )

    _ensure_station_table(engine)

    # ── Date range ────────────────────────────────────────────────────────────
    today = date.today()
    first_current = date(today.year, today.month, 1)
    first_prev = (
        date(today.year - 1, 12, 1) if today.month == 1
        else date(today.year, today.month - 1, 1)
    )

    dc1, dc2 = st.columns(2)
    start_date = dc1.date_input("Start Date", value=first_prev, key="bess_rank_start")
    end_date   = dc2.date_input("End Date (exclusive)", value=first_current, key="bess_rank_end")

    if start_date >= end_date:
        st.error("End date must be after start date.")
        return

    start = str(start_date)
    end   = str(end_date)

    # ── Station master upload ─────────────────────────────────────────────────
    with st.expander("Station master (upload 电站.xlsx to update)", expanded=False):
        uploaded = st.file_uploader("电站.xlsx", type=["xlsx"], key="bess_rank_upload")
        if uploaded is not None:
            df_excel = pd.read_excel(uploaded)
            df_excel.columns = df_excel.columns.str.strip()
            required = ["调度机组名称", "额定功率", "业主"]
            missing = [c for c in required if c not in df_excel.columns]
            if missing:
                st.error(f"Missing columns: {missing}")
            else:
                df_excel = df_excel.rename(columns={
                    "调度机组名称": "plant_name",
                    "额定功率":    "MW",
                    "业主":        "owner",
                })
                df_excel["plant_name"] = df_excel["plant_name"].astype(str).str.strip()
                df_excel["owner"]      = df_excel["owner"].astype(str).str.strip()
                df_excel = df_excel.drop_duplicates("plant_name")[["plant_name", "MW", "owner"]]
                _save_station_to_db(df_excel, engine)
                st.success(f"Station master updated — {len(df_excel)} stations saved.")

    station_df = _load_station_from_db(engine)
    if station_df.empty:
        st.warning("No station data. Upload 电站.xlsx above to enable BESS ranking.")
        return

    st.info(f"Station master: {len(station_df)} BESS stations loaded from DB.")

    # ── Advanced config ───────────────────────────────────────────────────────
    with st.expander("Pipeline configuration", expanded=False):
        cc1, cc2, cc3 = st.columns(3)
        is_15min       = cc1.checkbox("15-min data (÷4)", value=True, key="bess_rank_15min")
        conversion_factor = 4 if is_15min else 1
        duration          = cc1.number_input("BESS duration (h)", 1, 12, 4, key="bess_rank_dur")
        subsidy_per_mwh   = cc2.number_input("Subsidy (¥/MWh discharged)", 0, 2000, 350, step=10, key="bess_rank_sub")
        capex_yuan_per_kwh= cc2.number_input("Investment cost (¥/kWh)", 100, 2000, 600, step=50, key="bess_rank_capex")
        degradation_rate  = cc3.number_input("Degradation (%/yr)", 0.0, 20.0, 4.0, step=0.5, key="bess_rank_deg") / 100
        om_cost           = cc3.number_input("O&M (¥/MW/yr)", 0, 500000, 24000, step=10000, key="bess_rank_om")
        life_years        = cc3.number_input("Project life (yr)", 1, 30, 10, key="bess_rank_life")

    cfg = dict(
        conversion_factor=conversion_factor,
        duration=duration,
        subsidy_per_mwh=subsidy_per_mwh,
        capex_yuan_per_kwh=capex_yuan_per_kwh,
        degradation_rate=degradation_rate,
        om_cost_per_mw_per_year=om_cost,
        life_years=life_years,
    )

    # ── Run button ────────────────────────────────────────────────────────────
    btn_col, filter_col = st.columns([2, 3])
    run_clicked = btn_col.button("Run BESS Arbitrage", type="primary", key="bess_rank_run")
    show_yuanjing = filter_col.checkbox("Show only 远景 assets", value=False, key="bess_rank_yuanjing")

    if run_clicked:
        try:
            task_arn = _start_pipeline_task(start, end, cfg)
        except Exception as e:
            st.error(f"Failed to start ECS task: {e}")
            return
        st.session_state["bess_pipeline_task_arn"] = task_arn
        st.session_state["bess_pipeline_started_at"] = time.time()
        st.success("Pipeline started on ECS.")
        st.rerun()

    # ── Pipeline status ───────────────────────────────────────────────────────
    if "bess_pipeline_task_arn" in st.session_state:
        task_arn = st.session_state["bess_pipeline_task_arn"]
        status = _get_task_status(task_arn)
        st.info(f"Pipeline status: {status.get('lastStatus')}")

        if status.get("lastStatus") != "STOPPED":
            st_autorefresh(interval=5000, key="bess_pipeline_poll")

        if status.get("lastStatus") == "STOPPED":
            exit_code = status.get("exitCode")
            if exit_code not in (None, 0):
                st.error(
                    f"Pipeline STOPPED with exitCode={exit_code}. "
                    f"Reason: {status.get('reason') or status.get('stoppedReason')}"
                )
            else:
                st.success("Pipeline finished.")
                result  = _load_results(start, end)
                clusters = _load_clusters(start, end)
                if result.empty:
                    st.warning("No results for this date range in DB yet.")
                else:
                    st.session_state["bess_run"] = {"result": result, "clusters": clusters}

    # ── Display ───────────────────────────────────────────────────────────────
    if "bess_run" not in st.session_state:
        # Try to load last available results
        result  = _load_results(start, end)
        clusters = _load_clusters(start, end)
        if not result.empty:
            st.session_state["bess_run"] = {"result": result, "clusters": clusters}

    if "bess_run" not in st.session_state:
        st.info("No results yet for this date range. Click **Run BESS Arbitrage** above.")
        return

    result   = st.session_state["bess_run"]["result"]
    clusters = st.session_state["bess_run"].get("clusters", pd.DataFrame())

    if show_yuanjing:
        result = result[result["owner"].str.contains("远景", na=False)]

    start_dt = date.fromisoformat(start)
    end_dt   = date.fromisoformat(end)
    num_days = max((end_dt - start_dt).days, 1)

    # Derived metrics
    if "total_profit_per_installed_volume_per_day" not in result.columns:
        if "expected_total_profit_万元" in result.columns:
            mw_col = "MW" if "MW" in result.columns else "mw"
            result["total_profit_per_installed_volume_per_day"] = (
                result["expected_total_profit_万元"] * 10000
                / pd.to_numeric(result[mw_col], errors="coerce").fillna(0)
                / 365
            )

    if "expected_total_profit_万元" in result.columns:
        result["daily_avg_revenue_万元"] = result["expected_total_profit_万元"] / num_days

    # Portfolio IRR (远景 assets)
    yuanjing_df = result[result["owner"].str.contains("远景", na=False)]
    portfolio_irr = np.nan
    if not yuanjing_df.empty and "total_profit_per_installed_volume_per_day" in yuanjing_df.columns:
        mw_col = "MW" if "MW" in yuanjing_df.columns else "mw"
        mw_series = pd.to_numeric(yuanjing_df.get(mw_col, pd.Series(dtype=float)), errors="coerce").fillna(0)
        total_mw = mw_series.sum()
        weighted_profit = (
            yuanjing_df["total_profit_per_installed_volume_per_day"] * cfg["duration"] * 365 * mw_series
        ).sum()
        weighted_invest = cfg["capex_yuan_per_kwh"] * 1000 * cfg["duration"] * total_mw
        if weighted_invest > 0 and weighted_profit > 0:
            cashflows = [-weighted_invest]
            for y in range(1, cfg["life_years"] + 1):
                degraded = weighted_profit * ((1 - cfg["degradation_rate"]) ** (y - 1))
                net = degraded - cfg["om_cost_per_mw_per_year"] * total_mw
                cashflows.append(net)
            portfolio_irr = irr_from_cashflows(cashflows)

    m1, m2 = st.columns(2)
    period = f"{start_dt} → {end_dt}"
    m1.metric("远景 Portfolio IRR", f"{portfolio_irr*100:.2f}%" if not np.isnan(portfolio_irr) else "N/A", delta=period)
    m2.metric("远景 Total MW", f"{yuanjing_df.get('MW', yuanjing_df.get('mw')).sum():,.0f}" if not yuanjing_df.empty else "0")

    # Table
    fmt = {
        "discharge_mwh": "{:,.0f}",
        "charge_mwh": "{:,.0f}",
        "MW": "{:,.0f}",
        "arbitrage_profit_per_discharge_mwh": "{:,.0f}",
        "total_profit_per_discharge_mwh": "{:,.0f}",
        "arbitrage_per_installed_volume_per_day": "{:,.0f}",
        "total_profit_per_installed_volume_per_day": "{:,.0f}",
        "peer_MW_bess": "{:,.0f}", "peer_MW_solar": "{:,.0f}",
        "peer_MW_wind": "{:,.0f}", "peer_MW_thermal": "{:,.0f}",
        "peer_count_bess": "{:,.0f}", "peer_count_solar": "{:,.0f}",
        "peer_count_wind": "{:,.0f}", "peer_count_thermal": "{:,.0f}",
        "payback_years": "{:,.0f}",
        "irr": "{:.2%}",
        "efficiency": "{:.2%}",
    }
    for col in ["arbitrage_profit_万元","charge_cost_万元","energy_revenue_万元",
                "subsidy_万元","expected_total_profit_万元","daily_avg_revenue_万元"]:
        if col in result.columns:
            fmt[col] = "{:,.2f}"

    def _highlight(row):
        if "远景" in str(row.get("owner", "")):
            return ["background-color:#28a745;color:white;font-weight:bold"] * len(row)
        return [""] * len(row)

    display_cols = [
        "plant_name","owner",
        "rank_total_profit_per_installed_volume","rank_arbitrage_profit",
        "rank_cycles","rank_efficiency",
        "MW","irr","payback_years",
        "discharge_mwh","charge_mwh","efficiency","estimated_cycles_per_day",
        "arbitrage_profit_per_discharge_mwh","total_profit_per_discharge_mwh",
        "arbitrage_per_installed_volume_per_day","total_profit_per_installed_volume_per_day",
        "arbitrage_profit_万元","charge_cost_万元","energy_revenue_万元",
        "subsidy_万元","expected_total_profit_万元","daily_avg_revenue_万元",
        "peer_count_bess","peer_count_solar","peer_count_wind","peer_count_thermal",
        "peer_MW_bess","peer_MW_solar","peer_MW_wind","peer_MW_thermal",
    ]
    display_df = result[[c for c in display_cols if c in result.columns]].copy()
    styled = display_df.style.format(fmt).apply(_highlight, axis=1)
    st.dataframe(styled, use_container_width=True)

    # Peer explorer
    if not clusters.empty:
        st.markdown("---")
        st.subheader("Nodal Peer Explorer")
        selected_bess = st.selectbox(
            "Select BESS station:", sorted(display_df["plant_name"].unique()),
            key="bess_rank_peer_select",
        )
        peer_detail = build_peer_detail_table(selected_bess, clusters)
        st.dataframe(peer_detail, use_container_width=True)

    # Excel export
    excel_bytes = _to_excel({"bess_arbitrage": display_df})
    st.download_button(
        "Download Excel",
        excel_bytes,
        f"bess_arbitrage_{start}_to_{end}.xlsx",
        key="bess_rank_dl",
    )
