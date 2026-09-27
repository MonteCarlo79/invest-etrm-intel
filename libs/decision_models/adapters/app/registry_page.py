"""
libs/decision_models/adapters/app/registry_page.py

Asset Registry — canonical lifecycle view (Ardian Opta lesson: assets as
first-class entities with a screening → upcoming → operating lifecycle).

Read-mostly presentation of marketdata.asset_registry: lifecycle groups,
capacity by zone, per-asset detail (substation, price nodes, COD, ops data
start), and telemetry-attachment notes for upcoming assets.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st


def _engine():
    from libs.decision_models.adapters.app.dispatch_pnl_page import _engine as _e
    return _e()


def render_registry_page() -> None:
    import plotly.graph_objects as go

    st.subheader("Asset Registry · 资产注册表")
    engine = _engine()
    try:
        from services.common.asset_registry import load_assets, seed_if_empty
        seed_if_empty(engine)
        df = load_assets(engine)
    except Exception as exc:
        st.error(f"asset_registry 读取失败：{exc}")
        return

    if df.empty:
        st.info("注册表为空。")
        return

    # ── Headline ─────────────────────────────────────────────────────────────
    op = df[df["status"] == "operating"]
    up = df[df["status"] == "upcoming"]
    c1, c2, c3 = st.columns(3)
    c1.metric("在运资产", f"{len(op)} 座 / {op['capacity_mw'].sum():,.0f} MW")
    c2.metric("储备/在建", f"{len(up)} 座 / {up['capacity_mw'].sum():,.0f} MW")
    c3.metric("区域数", f"{df['zone'].nunique()} 个")

    # ── Capacity by zone ──────────────────────────────────────────────────────
    zc = (df.groupby(["zone", "status"], as_index=False)["capacity_mw"].sum())
    fig = go.Figure()
    for status, color in (("operating", "#1565C0"), ("upcoming", "#E53935")):
        z = zc[zc["status"] == status]
        if not z.empty:
            fig.add_bar(x=z["zone"], y=z["capacity_mw"], name=status,
                        marker_color=color)
    fig.update_layout(barmode="group", height=280, margin=dict(t=20, b=20),
                      yaxis_title="容量 (MW)", xaxis_title="")
    st.plotly_chart(fig, use_container_width=True)

    # ── Per-asset detail ─────────────────────────────────────────────────────
    for status, label in (("operating", "在运"), ("upcoming", "储备/在建")):
        sub = df[df["status"] == status]
        if sub.empty:
            continue
        st.markdown(f"#### {label}（{len(sub)}）")
        show = sub[["asset_code", "plant_name", "zone", "capacity_mw",
                    "duration_h", "substation", "zone_price_node",
                    "ops_data_since", "notes"]].copy()
        show.columns = ["代码", "电站", "区域", "容量 MW", "时长 h",
                        "母站", "价格节点", "运营数据起始", "备注"]
        st.dataframe(show, use_container_width=True, hide_index=True)
        if status == "upcoming":
            st.caption(
                "投运时自动接入的数据链：节点电价（自有节点建成后补全）、出清电量、"
                "调度日报、结算单 — 注册表行已就位，COD 后填充 cod_date 即可。"
            )
