# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 12:37:57 2026

@author: dipeng.chen
"""

# apps/trading/bess/mengxi/market_monitor/app.py
from __future__ import annotations

import os
import datetime as dt
from typing import Dict, Iterable, List, Tuple

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

st.set_page_config(page_title="Mengxi Trading Monitor", layout="wide")

alt.data_transformers.disable_max_rows()
alt.themes.enable("none")
alt.renderers.set_embed_options(actions=False)

# -----------------------------
# Styling
# -----------------------------
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
    .source-badge {
        display: inline-block;
        padding: 0.2rem 0.5rem;
        border-radius: 999px;
        background: #eef2ff;
        color: #3730a3;
        font-size: 0.8rem;
        margin-right: 0.35rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------
# Config
# -----------------------------
DB_URL = os.getenv("DB_DSN") or os.getenv("PGURL")
DEFAULT_DAYS = int(os.getenv("MONITOR_DEFAULT_DAYS", "7"))

if not DB_URL:
    st.error("Missing DB_DSN / PGURL")
    st.stop()

engine = create_engine(DB_URL)

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

# -----------------------------
# Helpers
# -----------------------------
def load_series(eng: Engine, table_name: str, start_ts: dt.datetime, end_ts: dt.datetime) -> pd.DataFrame:
    sql = text(f"""
        SELECT time, price
        FROM "{table_name}"
        WHERE time >= :start_ts
          AND time < :end_ts
        ORDER BY time
    """)
    try:
        df = pd.read_sql(sql, eng, params={"start_ts": start_ts, "end_ts": end_ts})
    except Exception:
        return pd.DataFrame(columns=["time", "price"])

    if df.empty:
        return df

    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    return df.dropna(subset=["time"]).sort_values("time")


def load_hourly_avg(eng: Engine, table_name: str, start_ts: dt.datetime, end_ts: dt.datetime) -> pd.DataFrame:
    raw = load_series(eng, table_name, start_ts, end_ts)
    if raw.empty:
        return pd.DataFrame(columns=["hour_ts", "value"])
    raw["hour_ts"] = raw["time"].dt.floor("H")
    out = raw.groupby("hour_ts", as_index=False)["price"].mean().rename(columns={"price": "value"})
    return out


def compose_series_sum(
    eng: Engine,
    items: Iterable[Tuple[str, int]],
    start_ts: dt.datetime,
    end_ts: dt.datetime,
) -> pd.DataFrame:
    combined = None
    for table, sign in items:
        df = load_series(eng, table, start_ts, end_ts)
        if df.empty:
            continue
        df = df.rename(columns={"price": "value"})
        df["value"] = df["value"] * sign
        if combined is None:
            combined = df
        else:
            combined = combined.merge(df, on="time", how="outer", suffixes=("", "_r"))
            combined["value"] = combined["value"].fillna(0) + combined["value_r"].fillna(0)
            combined = combined.drop(columns=["value_r"])

    if combined is None or combined.empty:
        return pd.DataFrame(columns=["hour_ts", "value"])

    combined["hour_ts"] = pd.to_datetime(combined["time"]).dt.floor("H")
    out = combined.groupby("hour_ts", as_index=False)["value"].mean()
    return out


def max_time_for_table(eng: Engine, table_name: str) -> str:
    try:
        sql = text(f'SELECT MAX(time) AS max_time FROM "{table_name}"')
        val = pd.read_sql(sql, eng).iloc[0, 0]
        if pd.isna(val):
            return "N/A"
        return str(pd.to_datetime(val))
    except Exception:
        return "N/A"


def styled_line_two(
    left: pd.DataFrame,
    right: pd.DataFrame,
    left_label: str,
    right_label: str,
    title: str,
    y_title: str = "Value",
) -> alt.Chart:
    d1 = left.rename(columns={left.columns[0]: "ts", left.columns[1]: "value"}).copy()
    d1["series"] = left_label

    d2 = right.rename(columns={right.columns[0]: "ts", right.columns[1]: "value"}).copy()
    d2["series"] = right_label

    data = pd.concat([d1, d2], ignore_index=True)
    if data.empty:
        return alt.Chart(pd.DataFrame({"ts": [], "value": [], "series": []})).mark_line()

    return (
        alt.Chart(data)
        .mark_line(strokeWidth=2.2)
        .encode(
            x=alt.X("ts:T", title="Time"),
            y=alt.Y("value:Q", title=y_title),
            color=alt.Color(
                "series:N",
                scale=alt.Scale(
                    domain=[left_label, right_label],
                    range=["#1d4ed8", "#ef4444"],
                ),
                legend=alt.Legend(orient="top"),
            ),
            tooltip=["ts:T", "series:N", alt.Tooltip("value:Q", format=",.2f")],
        )
        .properties(height=260, title=title)
        .configure_axis(labelColor="#334155", titleColor="#334155")
        .configure_title(color="#0f172a", fontSize=15)
        .configure_view(strokeOpacity=0)
    )


def styled_overlay(series_map: Dict[str, pd.DataFrame], title: str, y_title: str = "Value") -> alt.Chart:
    frames = []
    palette = ["#1d4ed8", "#06b6d4", "#16a34a", "#ef4444", "#7c3aed", "#f59e0b"]
    domain = []

    for i, (label, df) in enumerate(series_map.items()):
        if df is None or df.empty:
            continue
        tmp = df.rename(columns={df.columns[0]: "ts", df.columns[1]: "value"}).copy()
        tmp["series"] = label
        frames.append(tmp)
        domain.append(label)

    if not frames:
        return alt.Chart(pd.DataFrame({"ts": [], "value": [], "series": []})).mark_line()

    data = pd.concat(frames, ignore_index=True)
    return (
        alt.Chart(data)
        .mark_line(strokeWidth=2.0)
        .encode(
            x=alt.X("ts:T", title="Time"),
            y=alt.Y("value:Q", title=y_title),
            color=alt.Color(
                "series:N",
                scale=alt.Scale(domain=domain, range=palette[: len(domain)]),
                legend=alt.Legend(orient="top"),
            ),
            tooltip=["ts:T", "series:N", alt.Tooltip("value:Q", format=",.2f")],
        )
        .properties(height=260, title=title)
        .configure_axis(labelColor="#334155", titleColor="#334155")
        .configure_title(color="#0f172a", fontSize=15)
        .configure_view(strokeOpacity=0)
    )


def chart_grid(charts: List[alt.Chart]) -> None:
    cols = st.columns(2)
    for i, chart in enumerate(charts):
        with cols[i % 2]:
            st.altair_chart(chart, use_container_width=True)
        if i % 2 == 1 and i < len(charts) - 1:
            cols = st.columns(2)

# -----------------------------
# Sidebar
# -----------------------------
st.sidebar.header("Date Range")
default_end = dt.date.today()
default_start = default_end - dt.timedelta(days=DEFAULT_DAYS)
date_range = st.sidebar.date_input("Display dates (inclusive)", value=(default_start, default_end))
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = default_start, default_end

start_ts = dt.datetime.combine(start_date, dt.time(0, 0))
end_ts = dt.datetime.combine(end_date + dt.timedelta(days=1), dt.time(0, 0))
st.sidebar.caption("Charts are hourly averages of 15-min source data where applicable.")

# -----------------------------
# Header
# -----------------------------
st.title("Mengxi Trading — Market Monitor")
st.markdown(
    '<span class="source-badge">TT ENOS</span>'
    '<span class="source-badge">marketdata.public</span>'
    '<span class="source-badge">RDS / ECS</span>',
    unsafe_allow_html=True,
)

k1, k2, k3, k4 = st.columns(4)
with k1:
    st.markdown('<div class="metric-card">省级实时价最新时间<br><b>%s</b></div>' % max_time_for_table(engine, "hist_mengxi_provincerealtimeclearprice_15min"), unsafe_allow_html=True)
with k2:
    st.markdown('<div class="metric-card">苏右RT最新时间<br><b>%s</b></div>' % max_time_for_table(engine, "hist_mengxi_suyou_clear_15min"), unsafe_allow_html=True)
with k3:
    st.markdown('<div class="metric-card">乌拉特RT最新时间<br><b>%s</b></div>' % max_time_for_table(engine, "hist_mengxi_wulate_clear_15min"), unsafe_allow_html=True)
with k4:
    st.markdown('<div class="metric-card">竞价空间最新时间<br><b>%s</b></div>' % max_time_for_table(engine, "hist_mengxi_biddingspacereal_15min"), unsafe_allow_html=True)

# -----------------------------
# Mengxi only for v1
# -----------------------------
st.markdown('<div class="section-header">蒙西 — 省级价格</div>', unsafe_allow_html=True)

mx_prov_rt = load_hourly_avg(engine, "hist_mengxi_provincerealtimeclearprice_15min", start_ts, end_ts)
mx_prov_fc = load_hourly_avg(engine, "hist_mengxi_provincerealtimepriceforecast_15min", start_ts, end_ts)
mx_hbd_rt = load_hourly_avg(engine, "hist_mengxi_hubaodongrealtimeclearprice_15min", start_ts, end_ts)
mx_hbd_fc = load_hourly_avg(engine, "hist_mengxi_hubaodongrealtimepriceforecast_15min", start_ts, end_ts)
mx_hbx_rt = load_hourly_avg(engine, "hist_mengxi_hubaoxirealtimeclearprice_15min", start_ts, end_ts)
mx_hbx_fc = load_hourly_avg(engine, "hist_mengxi_hubaoxirealtimepriceforecast_15min", start_ts, end_ts)

chart_grid([
    styled_line_two(mx_prov_rt, mx_prov_fc, "全网实时电价", "全网实时电价预测", "蒙西全网实时电价（实际 vs 预测）", "Price"),
    styled_line_two(mx_hbd_rt, mx_hbd_fc, "呼包东RT", "呼包东预测", "呼包东实时电价", "Price"),
    styled_line_two(mx_hbx_rt, mx_hbx_fc, "呼包西RT", "呼包西预测", "呼包西实时电价", "Price"),
])

st.markdown('<div class="section-header">蒙西 — 节点实时电价</div>', unsafe_allow_html=True)

wl_rt = load_hourly_avg(engine, "hist_mengxi_wulate_clear_15min", start_ts, end_ts)
wl_fc = load_hourly_avg(engine, "hist_mengxi_wulate_forecast_15min", start_ts, end_ts)
sy_rt = load_hourly_avg(engine, "hist_mengxi_suyou_clear_15min", start_ts, end_ts)
sy_fc = load_hourly_avg(engine, "hist_mengxi_suyou_forecast_15min", start_ts, end_ts)
wh_rt = load_hourly_avg(engine, "hist_mengxi_wuhai_clear_15min", start_ts, end_ts)
wh_fc = load_hourly_avg(engine, "hist_mengxi_wuhai_forecast_15min", start_ts, end_ts)
wlc_rt = load_hourly_avg(engine, "hist_mengxi_wulanchabu_clear_15min", start_ts, end_ts)
wlc_fc = load_hourly_avg(engine, "hist_mengxi_wulanchabu_forecast_15min", start_ts, end_ts)

chart_grid([
    styled_line_two(wl_rt, wl_fc, "乌拉特RT", "乌拉特预测", "乌拉特节点实时电价", "Price"),
    styled_line_two(sy_rt, sy_fc, "苏右RT", "苏右预测", "苏右节点实时电价", "Price"),
    styled_line_two(wh_rt, wh_fc, "乌海RT", "乌海预测", "乌海节点实时电价", "Price"),
    styled_line_two(wlc_rt, wlc_fc, "乌兰察布RT", "乌兰察布预测", "乌兰察布节点实时电价", "Price"),
])

st.markdown('<div class="section-header">蒙西 — 基础面</div>', unsafe_allow_html=True)

bs_real = load_hourly_avg(engine, "hist_mengxi_biddingspacereal_15min", start_ts, end_ts)
bs_fcst = load_hourly_avg(engine, "hist_mengxi_biddingspaceforecast_15min", start_ts, end_ts)
ne_real = load_hourly_avg(engine, "hist_mengxi_newenergyreal_15min", start_ts, end_ts)
ne_fcst = load_hourly_avg(engine, "hist_mengxi_newenergyforecast_15min", start_ts, end_ts)
wind_real = load_hourly_avg(engine, "hist_mengxi_windpowerreal_15min", start_ts, end_ts)
wind_fcst = load_hourly_avg(engine, "hist_mengxi_windpowerforecast_15min", start_ts, end_ts)
inhouse = load_hourly_avg(engine, "hist_mengxi_inhouse_windforecast_15min", start_ts, end_ts)

chart_grid([
    styled_line_two(bs_real, bs_fcst, "竞价空间(实)", "竞价空间(预)", "蒙西竞价空间", "MW"),
    styled_line_two(ne_real, ne_fcst, "新能源出力(实)", "新能源出力(预)", "蒙西新能源出力", "MW"),
    styled_overlay(
        {
            "自研风电预测": inhouse,
            "电网风电预测": wind_fcst,
            "风电实际": wind_real,
        },
        "蒙西风电：自研 vs 电网 vs 实际",
        "MW",
    ),
])