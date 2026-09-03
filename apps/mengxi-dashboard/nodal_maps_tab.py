"""Nodal Maps tab — Perfect Foresight spread ranking, precomputed daily.

Reads reports.nodal_pf_node_daily (written by scripts/run_nodal_pf_node_daily.py,
scheduled daily via EventBridge) — no LP runs in this session. Fixed config:
100 MW / 2h / 85% RTE.
"""
from __future__ import annotations

from datetime import date as _date

import pandas as pd
import plotly.express as px
import streamlit as st

from services.mengxi_nodal.pf_results import (
    CONFIG as _NM_CONFIG,
    aggregate_pf as _nm_aggregate,
    get_latest_date as _nm_latest,
    get_pf_results as _nm_results,
)

_PROVINCES = ["蒙西", "山西", "陕西", "湖南", "浙江", "云南", "贵州", "广东", "广西", "海南", "甘肃",
              "山东", "河北南网", "黑龙江", "辽宁", "湖北", "安徽", "江西"]


def render(get_engine) -> None:
    st.header("Nodal Investment Maps — Perfect Foresight Spread Ranking")
    st.caption(
        f"Precomputed daily per node ({_NM_CONFIG['power_mw']:.0f} MW / {_NM_CONFIG['duration_h']:.0f}h / "
        f"{_NM_CONFIG['rte_pct']:.0f}% RTE) into `reports.nodal_pf_node_daily` by the daily batch — "
        "this page only aggregates; nothing is optimised in-session."
    )

    engine = get_engine()

    _nm_c1, _nm_c2, _nm_c3 = st.columns(3)
    _nm_province = _nm_c1.selectbox("Province", _PROVINCES, key="nm_province")
    _nm_start = _nm_c2.date_input("Start date", value=_date(_date.today().year, 1, 1), key="nm_start")
    _nm_end = _nm_c3.date_input("End date", value=_date.today(), key="nm_end")
    _nm_top_n = st.slider("Top-N nodes to highlight", 5, 50, 20, key="nm_topn")

    if _nm_start > _nm_end:
        st.warning("Start date must be ≤ end date.")
        return

    _nm_latest_d = _nm_latest(engine, _nm_province)
    _nm_df = _nm_results(engine, _nm_province, _nm_start, _nm_end)

    if _nm_df.empty:
        st.info(
            f"No precomputed PF results for {_nm_province} in {_nm_start} → {_nm_end} yet. "
            "The daily batch computes yesterday's nodes every night; history is being backfilled."
        )
        return

    _nm_totals, _nm_monthly = _nm_aggregate(_nm_df, _NM_CONFIG["power_mw"])

    _k1, _k2, _k3 = st.columns(3)
    _k1.metric("nodes ranked", len(_nm_totals))
    _k2.metric("days covered", _nm_df["data_date"].nunique())
    _k3.metric("data through", str(_nm_latest_d))

    # ── Ranked bar chart ────────────────────────────────────────────────────
    st.subheader("Ranked nodes by PF revenue / MW")
    _nm_top_df = _nm_totals.head(_nm_top_n)
    _nm_bar = px.bar(
        _nm_top_df, x="node_name", y="rev_per_mw",
        labels={"node_name": "Node", "rev_per_mw": "Rev / MW (CNY)"},
        title=f"Top {_nm_top_n} nodes — {_nm_province} PF spread ({_nm_start} → {_nm_end})",
        color="rev_per_mw", color_continuous_scale="Blues",
    )
    _nm_bar.update_layout(xaxis_tickangle=-45, showlegend=False)
    st.plotly_chart(_nm_bar, use_container_width=True)

    # ── Heatmap: node × month ───────────────────────────────────────────────
    if not _nm_monthly.empty:
        st.subheader("Monthly PF revenue / MW heatmap")
        _nm_hm_df = _nm_monthly.reindex(_nm_totals["node_name"].tolist()).head(_nm_top_n).fillna(0.0)
        _nm_hm = px.imshow(
            _nm_hm_df.values, x=list(_nm_hm_df.columns), y=list(_nm_hm_df.index),
            labels={"x": "Month", "y": "Node", "color": "Rev/MW (CNY)"},
            title=f"Monthly PF revenue / MW — top {_nm_top_n} nodes",
            color_continuous_scale="RdYlGn", aspect="auto",
        )
        _nm_hm.update_layout(height=max(400, _nm_top_n * 20))
        st.plotly_chart(_nm_hm, use_container_width=True)

    # ── Top-N investment table ──────────────────────────────────────────────
    st.subheader(f"Top {_nm_top_n} node investment summary")
    _nm_disp = _nm_top_df[["rank", "node_name", "rev_per_mw", "total_profit_cny"]].copy()
    _nm_disp.columns = ["Rank", "Node", "Rev / MW (CNY)", f"Total Profit {_NM_CONFIG['power_mw']:.0f}MW (CNY)"]
    _nm_disp["Rev / MW (CNY)"] = _nm_disp["Rev / MW (CNY)"].map("{:,.0f}".format)
    _nm_disp[f"Total Profit {_NM_CONFIG['power_mw']:.0f}MW (CNY)"] = \
        _nm_disp[f"Total Profit {_NM_CONFIG['power_mw']:.0f}MW (CNY)"].map("{:,.0f}".format)
    st.dataframe(_nm_disp, use_container_width=True, hide_index=True)
