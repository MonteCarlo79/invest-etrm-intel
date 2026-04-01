# -*- coding: utf-8 -*-
"""
DB-backed Mengxi P&L dashboard with:
- schema-compatible readers (old/new column names)
- classic dashboard mode (weekly/monthly revenue/spread/cycles/efficiency)
- reporting mode (detailed operational tables)
"""
from __future__ import annotations

import datetime as dt
import os
from typing import Iterable

import numpy as np
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

st.set_page_config(page_title="Mengxi P&L Attribution", layout="wide")

DB_URL = os.getenv("DB_DSN") or os.getenv("PGURL")
if not DB_URL:
    st.error("Missing DB_DSN / PGURL")
    st.stop()


@st.cache_resource
def get_engine() -> Engine:
    return create_engine(DB_URL, pool_pre_ping=True)


ENGINE = get_engine()
ASSET_CODES = ["suyou", "wulate", "wuhai", "wulanchabu", "hetao", "hangjinqi", "siziwangqi", "gushanliang"]
NO_SUBSIDY_ASSETS = {"wulanchabu"}
ASSET_DISPLAY = {
    "suyou": "SuYou",
    "wulate": "WuLaTe",
    "wuhai": "WuHai",
    "wulanchabu": "WuLanChaBu",
    "hetao": "HeTao",
    "hangjinqi": "HangJinQi",
    "siziwangqi": "SiZiWangQi",
    "gushanliang": "GuShanLiang",
}


def _safe_read_sql(engine: Engine, sql: str, params: dict | None = None, cols: list[str] | None = None) -> pd.DataFrame:
    try:
        return pd.read_sql(text(sql), engine, params=params)
    except Exception:
        return pd.DataFrame(columns=cols or [])


def _display_df(df: pd.DataFrame) -> "pd.io.formats.style.Styler | pd.DataFrame":
    if df.empty:
        return df
    out = df.copy()
    fmt: dict[str, str] = {}
    for col in out.columns:
        if pd.api.types.is_numeric_dtype(out[col]):
            s = pd.to_numeric(out[col], errors="coerce")
            non_null = s.dropna()
            if len(non_null) == 0:
                fmt[col] = "{:,.0f}"
            elif np.all(np.isclose(non_null, np.round(non_null))):
                fmt[col] = "{:,.0f}"
            else:
                fmt[col] = "{:,.2f}"
    return out.style.format(fmt, na_rep="")


def _table_columns(engine: Engine, schema_name: str, table_name: str) -> set[str]:
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema_name
          AND table_name = :table_name
    """
    df = _safe_read_sql(engine, sql, params={"schema_name": schema_name, "table_name": table_name}, cols=["column_name"])
    if df.empty:
        return set()
    return set(df["column_name"].astype(str).str.lower().tolist())


def _pick_expr(cols: set[str], target_alias: str, candidates: Iterable[str], default_expr: str = "NULL") -> str:
    for c in candidates:
        c_lc = c.lower()
        if c_lc in cols:
            return f"{c_lc} AS {target_alias}"
    return f"{default_expr} AS {target_alias}"


@st.cache_data(ttl=300)
def load_scenario_pnl(_engine: Engine, start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    out_cols = [
        "trade_date",
        "asset_code",
        "scenario_name",
        "scenario_available",
        "compensation_yuan_per_mwh",
        "compensation_blocked",
        "compensation_block_reason",
        "market_revenue_yuan",
        "subsidy_revenue_yuan",
        "total_revenue_yuan",
        "discharge_mwh",
        "charge_mwh",
        "avg_daily_cycles",
        "asset_label",
    ]
    cols = _table_columns(_engine, "reports", "bess_asset_daily_scenario_pnl")
    if not cols:
        return pd.DataFrame(columns=out_cols)

    select_parts = [
        _pick_expr(cols, "trade_date", ["trade_date"]),
        _pick_expr(cols, "asset_code", ["asset_code"]),
        _pick_expr(cols, "scenario_name", ["scenario_name"]),
        _pick_expr(cols, "scenario_available", ["scenario_available"], default_expr="TRUE"),
        _pick_expr(cols, "compensation_yuan_per_mwh", ["compensation_yuan_per_mwh", "compensation_rate"]),
        _pick_expr(cols, "compensation_blocked", ["compensation_blocked"], default_expr="FALSE"),
        _pick_expr(cols, "compensation_block_reason", ["compensation_block_reason"]),
        _pick_expr(cols, "market_revenue_yuan", ["market_revenue_yuan", "market_revenue"]),
        _pick_expr(cols, "subsidy_revenue_yuan", ["subsidy_revenue_yuan", "compensation_revenue"]),
        _pick_expr(cols, "total_revenue_yuan", ["total_revenue_yuan", "total_pnl"]),
        _pick_expr(cols, "discharge_mwh", ["discharge_mwh"]),
        _pick_expr(cols, "charge_mwh", ["charge_mwh"]),
        _pick_expr(cols, "avg_daily_cycles", ["avg_daily_cycles"]),
    ]
    sql = f"""
        SELECT {", ".join(select_parts)}
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
        cols=out_cols,
    )
    if df.empty:
        return pd.DataFrame(columns=out_cols)

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["asset_code"] = df["asset_code"].astype(str).str.strip().str.lower()
    df["scenario_name"] = df["scenario_name"].astype(str).str.strip().str.lower()
    df["scenario_name"] = df["scenario_name"].replace(
        {
            "nominated": "nominated_dispatch",
            "tt_forecast": "tt_forecast_optimal",
            "tt_optimal": "tt_forecast_optimal",
        }
    )
    for c in ["compensation_yuan_per_mwh", "market_revenue_yuan", "subsidy_revenue_yuan", "total_revenue_yuan", "discharge_mwh", "charge_mwh", "avg_daily_cycles"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    no_subsidy_mask = df["asset_code"].isin(NO_SUBSIDY_ASSETS)
    df.loc[no_subsidy_mask, "subsidy_revenue_yuan"] = 0.0
    df.loc[no_subsidy_mask, "total_revenue_yuan"] = (
        df.loc[no_subsidy_mask, "market_revenue_yuan"].fillna(0.0)
        + df.loc[no_subsidy_mask, "subsidy_revenue_yuan"].fillna(0.0)
    )
    df["compensation_blocked"] = df["compensation_blocked"].fillna(False).astype(bool)
    df["asset_label"] = df["asset_code"].map(ASSET_DISPLAY).fillna(df["asset_code"])
    return df


@st.cache_data(ttl=300)
def load_attribution(_engine: Engine, start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    cols = _table_columns(_engine, "reports", "bess_asset_daily_attribution")
    if not cols:
        return pd.DataFrame()

    select_parts = [
        _pick_expr(cols, "trade_date", ["trade_date"]),
        _pick_expr(cols, "asset_code", ["asset_code"]),
        _pick_expr(cols, "pf_unrestricted_pnl", ["pf_unrestricted_pnl"]),
        _pick_expr(cols, "pf_grid_feasible_pnl", ["pf_grid_feasible_pnl"]),
        _pick_expr(cols, "cleared_actual_pnl", ["cleared_actual_pnl"]),
        _pick_expr(cols, "nominated_pnl", ["nominated_pnl", "nominated_dispatch_pnl"]),
        _pick_expr(cols, "tt_forecast_optimal_pnl", ["tt_forecast_optimal_pnl"]),
        _pick_expr(cols, "tt_strategy_pnl", ["tt_strategy_pnl"]),
        _pick_expr(cols, "grid_restriction_loss", ["grid_restriction_loss"]),
        _pick_expr(cols, "forecast_error_loss", ["forecast_error_loss"]),
        _pick_expr(cols, "strategy_error_loss", ["strategy_error_loss"]),
        _pick_expr(cols, "nomination_loss", ["nomination_loss"]),
        _pick_expr(cols, "execution_clearing_loss", ["execution_clearing_loss"]),
        _pick_expr(cols, "realisation_gap_vs_pf", ["realisation_gap_vs_pf", "unexplained_gap"]),
        _pick_expr(cols, "realisation_gap_vs_pf_grid", ["realisation_gap_vs_pf_grid", "total_explained_loss"]),
    ]
    sql = f"""
        SELECT {", ".join(select_parts)}
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
    return df


@st.cache_data(ttl=300)
def load_scenario_window(_engine: Engine) -> pd.DataFrame:
    sql = """
        SELECT
            MIN(trade_date) AS min_trade_date,
            MAX(trade_date) AS max_trade_date,
            COUNT(*) AS row_count
        FROM reports.bess_asset_daily_scenario_pnl
    """
    df = _safe_read_sql(_engine, sql, cols=["min_trade_date", "max_trade_date", "row_count"])
    if not df.empty:
        for c in ["min_trade_date", "max_trade_date"]:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df


def _safe_pct(n: pd.Series, d: pd.Series) -> pd.Series:
    n_num = pd.to_numeric(n, errors="coerce").astype(float)
    d_num = pd.to_numeric(d, errors="coerce").astype(float).replace(0, np.nan)
    return pd.to_numeric((n_num / d_num) * 100, errors="coerce").round(0)


def _safe_div_round(n: pd.Series, d: pd.Series, decimals: int = 0) -> pd.Series:
    n_num = pd.to_numeric(n, errors="coerce").astype(float)
    d_num = pd.to_numeric(d, errors="coerce").astype(float).replace(0, np.nan)
    return pd.to_numeric(n_num / d_num, errors="coerce").round(decimals)


def _fmt_pct(v: object) -> str:
    if pd.isna(v):
        return ""
    return f"{int(v)}%"


def _build_classic_tables(asset_df: pd.DataFrame, freq: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = asset_df.copy()
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    if freq == "W":
        df["period_start"] = pd.to_datetime(df["trade_date"]).dt.to_period("W-SUN").apply(lambda p: p.start_time.date())
    else:
        df["period_start"] = pd.to_datetime(df["trade_date"]).dt.to_period("M").apply(lambda p: p.start_time.date())

    grp = df.groupby(["period_start", "scenario_name"], as_index=False).agg(
        market_revenue_yuan=("market_revenue_yuan", "sum"),
        subsidy_revenue_yuan=("subsidy_revenue_yuan", "sum"),
        total_revenue_yuan=("total_revenue_yuan", "sum"),
        discharge_mwh=("discharge_mwh", "sum"),
        charge_mwh=("charge_mwh", "sum"),
        avg_daily_cycles=("avg_daily_cycles", "mean"),
    )

    idx = pd.Index(sorted(grp["period_start"].unique()), name="period_start")

    def pvt(col: str) -> pd.DataFrame:
        return grp.pivot(index="period_start", columns="scenario_name", values=col).reindex(idx)

    def _num_series(df: pd.DataFrame, col: str) -> pd.Series:
        if col not in df.columns:
            return pd.Series(np.nan, index=df.index, dtype="float64")
        return pd.to_numeric(df[col], errors="coerce").astype(float)

    p_total = pvt("total_revenue_yuan")
    p_market = pvt("market_revenue_yuan")
    p_subsidy = pvt("subsidy_revenue_yuan")
    p_discharge = pvt("discharge_mwh")
    p_charge = pvt("charge_mwh")
    p_cycles = pvt("avg_daily_cycles")

    opt = "perfect_foresight_grid_feasible" if "perfect_foresight_grid_feasible" in p_total.columns else "perfect_foresight_unrestricted"

    rev = pd.DataFrame({"period_start": idx})
    rev["actual_revenue"] = (_num_series(p_total, "cleared_actual") / 10000).round(0)
    rev["market_revenue"] = (_num_series(p_market, "cleared_actual") / 10000).round(0)
    rev["subsidy_revenue"] = (_num_series(p_subsidy, "cleared_actual") / 10000).round(0)
    rev["nominated_revenue"] = (_num_series(p_total, "nominated_dispatch") / 10000).round(0)
    rev["strategy_revenue"] = (_num_series(p_total, "tt_strategy") / 10000).round(0)
    rev["optimal_revenue"] = (_num_series(p_total, opt) / 10000).round(0)
    rev["discharged volume in MWh"] = _num_series(p_discharge, "cleared_actual").round(0)
    rev["actual/optimal"] = _safe_pct(rev["actual_revenue"], rev["optimal_revenue"]).map(_fmt_pct)
    rev["actual/nominated"] = _safe_pct(rev["actual_revenue"], rev["nominated_revenue"]).map(_fmt_pct)

    spread = pd.DataFrame({"period_start": idx})
    spread["actual_spread"] = _safe_div_round(_num_series(p_total, "cleared_actual"), _num_series(p_discharge, "cleared_actual"), 0)
    spread["market_spread"] = _safe_div_round(_num_series(p_market, "cleared_actual"), _num_series(p_discharge, "cleared_actual"), 0)
    spread["Subsidies"] = _safe_div_round(_num_series(p_subsidy, "cleared_actual"), _num_series(p_discharge, "cleared_actual"), 0)
    spread["nominated_spread"] = _safe_div_round(_num_series(p_total, "nominated_dispatch"), _num_series(p_discharge, "nominated_dispatch"), 0)
    spread["strategy_spread"] = _safe_div_round(_num_series(p_total, "tt_strategy"), _num_series(p_discharge, "tt_strategy"), 0)
    spread["optimal_spread"] = _safe_div_round(_num_series(p_total, opt), _num_series(p_discharge, opt), 0)
    spread["unit cycle spread"] = (spread["actual_spread"] * _num_series(p_cycles, "cleared_actual")).round(0)
    spread["expected unit cycle spread"] = ((spread["market_spread"] + 350) * _num_series(p_cycles, "cleared_actual")).round(0)

    cycles = pd.DataFrame({"period_start": idx})
    cycles["actual_avg_daily_cycles"] = _num_series(p_cycles, "cleared_actual").round(2)
    cycles["nominated_avg_daily_cycles"] = _num_series(p_cycles, "nominated_dispatch").round(2)
    cycles["strategy_avg_daily_cycles"] = _num_series(p_cycles, "tt_strategy").round(2)
    cycles["optimal_avg_daily_cycles"] = _num_series(p_cycles, opt).round(2)
    cycles["actual/optimal"] = _safe_pct(cycles["actual_avg_daily_cycles"], cycles["optimal_avg_daily_cycles"]).map(_fmt_pct)
    cycles["actual/nominated"] = _safe_pct(cycles["actual_avg_daily_cycles"], cycles["nominated_avg_daily_cycles"]).map(_fmt_pct)

    eff = pd.DataFrame({"period_start": idx})
    eff["actual_efficiency"] = (_safe_div_round(_num_series(p_discharge, "cleared_actual"), _num_series(p_charge, "cleared_actual"), 3) * 100).round(1).map(_fmt_pct)

    return rev, spread, cycles, eff


def _render_classic_asset(asset_df: pd.DataFrame, label: str) -> None:
    week_tab, month_tab = st.tabs(["Weekly", "Monthly"])
    for tab, freq in ((week_tab, "W"), (month_tab, "M")):
        with tab:
            rev, spr, cyc, eff = _build_classic_tables(asset_df, freq)
            if rev.empty:
                st.warning("No data for this asset in selected period.")
                continue
            st.subheader(f"Revenue (10k CNY) - {label}")
            st.dataframe(_display_df(rev), use_container_width=True)
            st.subheader("Spread (CNY/MWh)")
            st.dataframe(_display_df(spr), use_container_width=True)
            st.subheader("Cycles")
            st.dataframe(_display_df(cyc), use_container_width=True)
            st.subheader("Efficiency")
            st.dataframe(_display_df(eff), use_container_width=True)


# -------------------- UI --------------------
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
window_df = load_scenario_window(ENGINE)

st.title("Mengxi Trading - P&L Attribution")
st.caption("Read-only view over reporting outputs. No upstream calculation overrides are applied in this UI.")

view_mode = st.radio("View mode", ["Classic Dashboard", "Reporting Tables"], horizontal=True)

if scenario_df.empty:
    if not window_df.empty and pd.notna(window_df.loc[0, "min_trade_date"]) and pd.notna(window_df.loc[0, "max_trade_date"]):
        min_d = window_df.loc[0, "min_trade_date"].date()
        max_d = window_df.loc[0, "max_trade_date"].date()
        row_count = int(window_df.loc[0, "row_count"] or 0)
        st.warning(
            f"No scenario P&L rows found for selected range. "
            f"Available DB window: {min_d} to {max_d} (rows: {row_count:,})."
        )
    else:
        st.warning("No scenario P&L rows found for selected range.")

if view_mode == "Classic Dashboard":
    if "asset_code" not in scenario_df.columns:
        st.info("No assets available in selected range.")
        st.stop()

    if asset_choice == "fleet":
        focus_assets = [a for a in ["suyou", "wulate"] if a in scenario_df["asset_code"].unique().tolist()]
        if not focus_assets:
            focus_assets = sorted(scenario_df["asset_code"].dropna().unique().tolist())
    else:
        focus_assets = [asset_choice]

    if focus_assets:
        tabs = st.tabs([f"{a} / {ASSET_DISPLAY.get(a, a)}" for a in focus_assets])
        for tab, a in zip(tabs, focus_assets):
            with tab:
                _render_classic_asset(scenario_df[scenario_df["asset_code"] == a].copy(), ASSET_DISPLAY.get(a, a))
    else:
        st.info("No assets available in selected range.")
else:
    base_df = scenario_df[scenario_df["scenario_name"] == scenario_choice].copy()
    if asset_choice != "fleet":
        base_df = base_df[base_df["asset_code"] == asset_choice].copy()

    st.subheader("Daily rows")
    st.dataframe(_display_df(base_df.sort_values(["trade_date", "asset_code"])), use_container_width=True)

    st.subheader("Daily attribution")
    if attribution_df.empty:
        st.info("No attribution rows found in selected date range.")
    else:
        attr = attribution_df.copy()
        if asset_choice != "fleet":
            attr = attr[attr["asset_code"] == asset_choice]
        st.dataframe(_display_df(attr.sort_values(["trade_date", "asset_code"])), use_container_width=True)
