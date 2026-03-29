# -*- coding: utf-8 -*-
"""
apps/trading/bess/mengxi/pnl_attribution/app.py

Read-only Streamlit UI for Mengxi daily P&L attribution outputs.
"""
from __future__ import annotations

import datetime as dt
import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

st.set_page_config(page_title="Mengxi P&L Attribution", layout="wide")

st.markdown(
    """
    <style>
    .block-container {padding-top: 1.2rem; padding-bottom: 1rem;}
    .metric-card {
        border: 1px solid rgba(49, 51, 63, 0.2);
        border-radius: 14px;
        padding: 0.9rem 1rem;
        background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
        box-shadow: 0 1px 8px rgba(15, 23, 42, 0.06);
    }
    .section-header {
        margin-top: 0.3rem;
        margin-bottom: 0.5rem;
        padding: 0.55rem 0.8rem;
        border-left: 6px solid #2563eb;
        background: #f8fbff;
        border-radius: 8px;
        font-weight: 600;
        color: #0f172a;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

DB_URL = os.getenv("DB_DSN") or os.getenv("PGURL")
if not DB_URL:
    st.error("Missing DB_DSN / PGURL")
    st.stop()

@st.cache_resource
def get_engine():
    # Cache SQLAlchemy engine as a process-level resource (not per rerun).
    return create_engine(DB_URL, pool_pre_ping=True)


ENGINE = get_engine()
ASSET_CODES = ["suyou", "wulate", "wuhai", "wulanchabu", "hetao", "hangjinqi", "siziwangqi", "gushanliang"]
ASSET_DISPLAY = {
    "suyou": "苏右",
    "wulate": "乌拉特",
    "wuhai": "乌海",
    "wulanchabu": "乌兰察布",
    "hetao": "河套",
    "hangjinqi": "杭锦旗",
    "siziwangqi": "四子王旗",
    "gushanliang": "谷山梁",
}


def _safe_read_sql(engine: Engine, sql: str, params: dict | None = None, cols: list[str] | None = None) -> pd.DataFrame:
    try:
        return pd.read_sql(text(sql), engine, params=params)
    except Exception:
        return pd.DataFrame(columns=cols or [])


@st.cache_data(ttl=300)
def load_scenario_pnl(_engine: Engine, start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    # `_engine` is underscore-prefixed so Streamlit excludes it from cache hashing.
    sql = """
        SELECT
            trade_date,
            asset_code,
            scenario_name,
            scenario_available,
            compensation_yuan_per_mwh,
            compensation_blocked,
            compensation_block_reason,
            market_revenue_yuan,
            subsidy_revenue_yuan,
            total_revenue_yuan,
            discharge_mwh,
            charge_mwh,
            avg_daily_cycles
        FROM reports.bess_asset_daily_scenario_pnl
        WHERE trade_date >= :start_date
          AND trade_date <= :end_date
          AND asset_code = ANY(:asset_codes)
        ORDER BY trade_date, asset_code, scenario_name
    """
    df = _safe_read_sql(
        _engine,
        sql,
        params={"start_date": start_date, "end_date": end_date, "asset_codes": ASSET_CODES},
        cols=[
            "trade_date", "asset_code", "scenario_name", "scenario_available", "compensation_yuan_per_mwh",
            "compensation_blocked", "compensation_block_reason", "market_revenue_yuan", "subsidy_revenue_yuan",
            "total_revenue_yuan", "discharge_mwh", "charge_mwh", "avg_daily_cycles",
        ],
    )
    if df.empty:
        return df
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    for c in ["compensation_yuan_per_mwh", "market_revenue_yuan", "subsidy_revenue_yuan", "total_revenue_yuan", "discharge_mwh", "charge_mwh", "avg_daily_cycles"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["compensation_blocked"] = df["compensation_blocked"].fillna(False).astype(bool)
    df["asset_label"] = df["asset_code"].map(ASSET_DISPLAY).fillna(df["asset_code"])
    return df


@st.cache_data(ttl=300)
def load_attribution(_engine: Engine, start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    # `_engine` is underscore-prefixed so Streamlit excludes it from cache hashing.
    sql = """
        SELECT
            trade_date,
            asset_code,
            pf_unrestricted_pnl,
            pf_grid_feasible_pnl,
            cleared_actual_pnl,
            nominated_pnl,
            tt_forecast_optimal_pnl,
            tt_strategy_pnl,
            grid_restriction_loss,
            forecast_error_loss,
            strategy_error_loss,
            nomination_loss,
            execution_clearing_loss,
            realisation_gap_vs_pf,
            realisation_gap_vs_pf_grid
        FROM reports.bess_asset_daily_attribution
        WHERE trade_date >= :start_date
          AND trade_date <= :end_date
          AND asset_code = ANY(:asset_codes)
        ORDER BY trade_date, asset_code
    """
    df = _safe_read_sql(_engine, sql, params={"start_date": start_date, "end_date": end_date, "asset_codes": ASSET_CODES})
    if df.empty:
        return df
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    metric_cols = [c for c in df.columns if c not in ["trade_date", "asset_code"]]
    for c in metric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["asset_label"] = df["asset_code"].map(ASSET_DISPLAY).fillna(df["asset_code"])
    return df


@st.cache_data(ttl=300)
def load_monthly_status(_engine: Engine) -> tuple[pd.DataFrame, pd.DataFrame]:
    # `_engine` is underscore-prefixed so Streamlit excludes it from cache hashing.
    coverage_sql = """
        SELECT asset_code, effective_month, discharge_known, compensation_known, blocked_missing_compensation, notes
        FROM staging.mengxi_compensation_coverage_status
        WHERE asset_code = ANY(:asset_codes)
    """
    rate_sql = """
        SELECT asset_code, effective_month, compensation_yuan_per_mwh, source_system, notes
        FROM core.asset_monthly_compensation
        WHERE active_flag = TRUE
          AND asset_code = ANY(:asset_codes)
    """
    coverage_df = _safe_read_sql(
        _engine,
        coverage_sql,
        params={"asset_codes": ASSET_CODES},
        cols=["asset_code", "effective_month", "discharge_known", "compensation_known", "blocked_missing_compensation", "notes"],
    )
    rate_df = _safe_read_sql(
        _engine,
        rate_sql,
        params={"asset_codes": ASSET_CODES},
        cols=["asset_code", "effective_month", "compensation_yuan_per_mwh", "source_system", "notes"],
    )
    for df in (coverage_df, rate_df):
        if not df.empty:
            df["effective_month"] = pd.to_datetime(df["effective_month"], errors="coerce").dt.date
    if not coverage_df.empty:
        coverage_df["blocked_missing_compensation"] = coverage_df["blocked_missing_compensation"].fillna(False).astype(bool)
    return coverage_df, rate_df


def month_range(start_date: dt.date, end_date: dt.date) -> list[dt.date]:
    months = pd.date_range(pd.Timestamp(start_date).replace(day=1), pd.Timestamp(end_date).replace(day=1), freq="MS")
    return [d.date() for d in months]


st.sidebar.header("Filters")
default_end = dt.date.today()
default_start = default_end - dt.timedelta(days=30)
date_range = st.sidebar.date_input("Trade date range", value=(default_start, default_end))
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = default_start, default_end
if start_date > end_date:
    st.warning("Invalid date range: start date is after end date.")
    st.stop()

asset_choice = st.sidebar.selectbox(
    "Asset drilldown",
    options=["fleet"] + ASSET_CODES,
    format_func=lambda x: "Fleet" if x == "fleet" else f"{x} / {ASSET_DISPLAY.get(x, x)}",
)
scenario_choice = st.sidebar.selectbox(
    "Scenario",
    options=["cleared_actual", "perfect_foresight_unrestricted", "perfect_foresight_grid_feasible", "nominated_dispatch", "tt_forecast_optimal", "tt_strategy"],
)

scenario_df = load_scenario_pnl(ENGINE, start_date, end_date)
attribution_df = load_attribution(ENGINE, start_date, end_date)
coverage_df, rate_df = load_monthly_status(ENGINE)

st.title("Mengxi Trading — P&L Attribution")
st.caption("Read-only view over reporting outputs. No upstream calculation overrides are applied in this UI.")

if scenario_df.empty:
    st.warning("No scenario P&L rows found for selected range.")
    st.stop()

base_df = scenario_df[scenario_df["scenario_name"] == scenario_choice].copy()
if asset_choice != "fleet":
    base_df = base_df[base_df["asset_code"] == asset_choice].copy()

valid_total = base_df["total_revenue_yuan"].dropna()
blocked_rows = base_df[base_df["compensation_blocked"]]
missing_cov_rows = base_df[(~base_df["compensation_blocked"]) & (base_df["compensation_yuan_per_mwh"].isna())]

k1, k2, k3, k4 = st.columns(4)
with k1:
    st.markdown(f'<div class="metric-card">Scenario rows<br><b>{len(base_df):,}</b></div>', unsafe_allow_html=True)
with k2:
    st.markdown(f'<div class="metric-card">Computed P&L rows<br><b>{len(valid_total):,}</b></div>', unsafe_allow_html=True)
with k3:
    st.markdown(f'<div class="metric-card">Blocked rows<br><b>{len(blocked_rows):,}</b></div>', unsafe_allow_html=True)
with k4:
    st.markdown(f'<div class="metric-card">Missing coverage rows<br><b>{len(missing_cov_rows):,}</b></div>', unsafe_allow_html=True)

st.markdown('<div class="section-header">Fleet summary (selected scenario)</div>', unsafe_allow_html=True)
summary = (
    base_df.groupby(["asset_code", "asset_label"], as_index=False)
    .agg(
        rows=("trade_date", "count"),
        computed_rows=("total_revenue_yuan", lambda s: int(s.notna().sum())),
        blocked_rows=("compensation_blocked", "sum"),
        market_revenue_yuan=("market_revenue_yuan", "sum"),
        subsidy_revenue_yuan=("subsidy_revenue_yuan", "sum"),
        total_revenue_yuan=("total_revenue_yuan", "sum"),
    )
    .sort_values("asset_code")
)
st.dataframe(summary, use_container_width=True)

st.markdown('<div class="section-header">Asset/drilldown daily rows</div>', unsafe_allow_html=True)
show_cols = [
    "trade_date", "asset_code", "scenario_name", "scenario_available", "compensation_yuan_per_mwh",
    "compensation_blocked", "compensation_block_reason", "market_revenue_yuan", "subsidy_revenue_yuan", "total_revenue_yuan",
    "discharge_mwh", "charge_mwh",
]
st.dataframe(base_df[show_cols].sort_values(["trade_date", "asset_code"]).reset_index(drop=True), use_container_width=True)

st.markdown('<div class="section-header">Monthly compensation rates (evidence-derived)</div>', unsafe_allow_html=True)
if rate_df.empty:
    st.info("No active monthly compensation rates found.")
else:
    rates = rate_df.copy()
    if asset_choice != "fleet":
        rates = rates[rates["asset_code"] == asset_choice]
    st.dataframe(rates.sort_values(["effective_month", "asset_code"]), use_container_width=True)

st.markdown('<div class="section-header">Blocked months (discharge known, compensation missing)</div>', unsafe_allow_html=True)
if coverage_df.empty:
    st.info("Coverage status table is not available yet.")
else:
    blocked_months = coverage_df[coverage_df["blocked_missing_compensation"]].copy()
    if asset_choice != "fleet":
        blocked_months = blocked_months[blocked_months["asset_code"] == asset_choice]
    blocked_months = blocked_months.sort_values(["effective_month", "asset_code"])
    st.dataframe(blocked_months, use_container_width=True)

st.markdown('<div class="section-header">Missing monthly coverage</div>', unsafe_allow_html=True)
months = month_range(start_date, end_date)
expected = pd.MultiIndex.from_product([
    ASSET_CODES if asset_choice == "fleet" else [asset_choice],
    months,
], names=["asset_code", "effective_month"]).to_frame(index=False)

if not coverage_df.empty:
    present = coverage_df[["asset_code", "effective_month"]].drop_duplicates()
    missing = expected.merge(present, on=["asset_code", "effective_month"], how="left", indicator=True)
    missing = missing[missing["_merge"] == "left_only"].drop(columns=["_merge"])
else:
    missing = expected

missing["status"] = "no coverage record"
st.dataframe(missing.sort_values(["effective_month", "asset_code"]), use_container_width=True)

st.markdown('<div class="section-header">Daily attribution (no synthetic TT assumptions)</div>', unsafe_allow_html=True)
if attribution_df.empty:
    st.info("No attribution rows found in selected date range.")
else:
    attr = attribution_df.copy()
    if asset_choice != "fleet":
        attr = attr[attr["asset_code"] == asset_choice]
    st.dataframe(attr.sort_values(["trade_date", "asset_code"]), use_container_width=True)
