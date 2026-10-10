"""Nodal Maps tab — Perfect Foresight spread ranking, precomputed daily.

Reads reports.nodal_pf_node_daily (written by scripts/run_nodal_pf_node_daily.py,
scheduled daily via EventBridge) — no LP runs in this session. Fixed config:
100 MW / 2h / 85% RTE.
"""
from __future__ import annotations

from datetime import date as _date

import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st

from services.mengxi_nodal.pf_results import (
    CONFIG as _NM_CONFIG,
    aggregate_pf as _nm_aggregate,
    get_latest_date as _nm_latest,
    get_pf_results as _nm_results,
)
from services.openinfra.map_data import (
    extent_center as _oim_center,
    get_line_paths as _oim_lines,
    get_substations as _oim_subs,
    tag_for_province as _oim_tag,
)

_PROVINCES = ["蒙西", "山西", "陕西", "湖南", "浙江", "云南", "贵州", "广东", "广西", "海南", "甘肃",
              "山东", "河北南网", "黑龙江", "辽宁", "湖北", "安徽", "江西"]

# voltage class -> RGB for the grid underlay
_VOLT_COLORS = [
    (1000000, [120, 0, 0]),      # 1000 kV UHV
    (750000, [200, 60, 0]),      # 750 kV
    (500000, [210, 40, 40]),     # 500 kV
    (330000, [230, 150, 40]),    # 330 kV
    (0, [150, 150, 150]),
]


def _volt_rgb(v):
    for floor, rgb in _VOLT_COLORS:
        if (v or 0) >= floor:
            return rgb
    return _VOLT_COLORS[-1][1]


@st.cache_data(ttl=3600, show_spinner=False)
def _grid_payload(_engine, tag: str):
    subs = _oim_subs(_engine, tag)
    lines = _oim_lines(_engine, tag)
    return subs, lines


def _render_grid_map(engine, province: str, top_df: pd.DataFrame) -> None:
    """OpenInfraMap grid underlay: 500/750 kV lines + stations; exact-name
    highlights for ranked PF nodes when they match a substation name."""
    tag = _oim_tag(province)
    if tag is None:
        return
    subs, lines = _grid_payload(engine, tag)
    if subs.empty and not lines:
        st.caption("Grid underlay not loaded for this province yet "
                   "(staging.openinfra_* empty — run the OpenInfraMap extract task).")
        return

    st.subheader(f"Grid map — {province} (500 kV+ backbone)")
    _show_labels = st.checkbox("站名标注 (station labels)", value=True,
                               key=f"nm_labels_{tag}")
    lon0, lat0 = _oim_center(tag)
    layers = []
    if lines:
        line_data = [{"path": p["path"], "color": _volt_rgb(p["max_voltage"])}
                     for p in lines]
        layers.append(pdk.Layer(
            "PathLayer", line_data, get_path="path", get_color="color",
            get_width=2, width_min_pixels=1, width_max_pixels=3, pickable=False))
    if not subs.empty:
        sdf = subs.copy()
        sdf["color"] = sdf["max_voltage"].map(_volt_rgb)
        sdf["radius"] = sdf["max_voltage"].fillna(0).map(
            lambda v: 9000 if v >= 750000 else (6000 if v >= 500000 else 3500))
        layers.append(pdk.Layer(
            "ScatterplotLayer", sdf, get_position=["lon", "lat"],
            get_fill_color="color", get_radius="radius", stroked=True,
            get_line_color=[30, 30, 30], line_width_min_pixels=0.5,
            pickable=True, auto_highlight=True))
        # ranked PF nodes whose name matches a station → gold highlight
        ranked = set(top_df["node_name"]) if not top_df.empty else set()
        if ranked:
            hit = sdf[sdf["name"].map(
                lambda n: any(r in n or n in r for r in ranked if len(r) >= 2))]
            if not hit.empty:
                layers.append(pdk.Layer(
                    "ScatterplotLayer", hit, get_position=["lon", "lat"],
                    get_fill_color=[255, 190, 0], get_radius=14000,
                    stroked=True, get_line_color=[120, 60, 0],
                    line_width_min_pixels=1.5, pickable=True))
        # Chinese station-name labels for the 500 kV+ backbone stations
        if _show_labels:
            ldf = sdf[sdf["max_voltage"].fillna(0) >= 500000]
            if not ldf.empty:
                layers.append(pdk.Layer(
                    "TextLayer", ldf, get_position=["lon", "lat"],
                    get_text="name", get_size=12, get_color=[35, 35, 35],
                    get_pixel_offset=[0, -10], font_family="sans-serif",
                    get_text_anchor="'middle'", get_alignment_baseline="'bottom'",
                    pickable=False))
    st.pydeck_chart(pdk.Deck(
        initial_view_state=pdk.ViewState(longitude=lon0, latitude=lat0, zoom=5.2),
        layers=layers, map_style=None,
        tooltip={"text": "{name}\n{voltages}"}))
    st.caption(
        f"{len(subs)} named stations · {len(lines)} line paths (≥330/500 kV) · "
        "gold = ranked PF node matched to a station · "
        "Grid geometry © OpenStreetMap contributors, ODbL (via OpenInfraMap).")



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

    _nm_top_df = _nm_totals.head(_nm_top_n)

    # ── Geographic grid underlay (OpenInfraMap staging, if loaded) ──────────
    _render_grid_map(engine, _nm_province, _nm_top_df)

    # ── Ranked bar chart ────────────────────────────────────────────────────
    st.subheader("Ranked nodes by PF revenue / MW")
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
