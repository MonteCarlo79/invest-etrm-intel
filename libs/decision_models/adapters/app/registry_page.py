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

    # ── Add candidate (screening) ──────────────────────────────────────────────
    with st.expander("➕ 新增候选资产（screening — 自动进入 Deal Structurer 新资产筛选）"):
        _render_add_candidate_form(engine, df)

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


_KV_CHOICES = ("110", "220", "500")


def _render_add_candidate_form(engine, existing_df) -> None:
    """Insert a screening asset into marketdata.asset_registry.

    The deal-structurer 新资产筛选 tab reads status IN (upcoming, screening),
    so any row added here appears there automatically on next view.
    """
    from sqlalchemy import text as _t
    import re as _re

    existing_codes = set(existing_df["asset_code"].tolist()) if not existing_df.empty else set()
    existing_zones = sorted(z for z in existing_df["zone"].dropna().unique()) if not existing_df.empty else []
    existing_nodes = sorted(n for n in existing_df["zone_price_node"].dropna().unique()) if not existing_df.empty else []

    with st.form("add_candidate_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            asset_code = st.text_input("资产代码 (英文小写+数字, 例: bayin)", key="ac_code").strip()
            plant_name = st.text_input("电站名称 (中文)", key="ac_name").strip()
            zone = st.selectbox("区域", options=[""] + existing_zones + ["<新区域>"], key="ac_zone")
            if zone == "<新区域>":
                zone = st.text_input("新区域名称", key="ac_zone_new").strip()
            capacity_mw = st.number_input("容量 MW", min_value=0.0, value=100.0, step=50.0, key="ac_cap")
            duration_h = st.number_input("时长 h", min_value=0.0, value=2.0, step=0.5, key="ac_dur")
        with c2:
            substation = st.text_input("母站 (例: 某某500kV变电站)", key="ac_sub").strip()
            conn_kv = st.selectbox("并网电压 kV", options=_KV_CHOICES, index=1, key="ac_kv")
            node_mode = st.radio("价格节点", ["选择已有节点作代理", "输入新节点", "暂无(后续补)"],
                                 index=0, key="ac_nmode")
            zone_price_node = None
            if node_mode == "选择已有节点作代理":
                zone_price_node = st.selectbox("代理节点", options=[""] + existing_nodes, key="ac_node_sel") or None
            elif node_mode == "输入新节点":
                zone_price_node = st.text_input("节点名 (例: 内蒙.某某站/220kV.1M)", key="ac_node_txt").strip() or None
            status = st.selectbox("状态", ["screening", "upcoming"], index=0, key="ac_status")
            notes = st.text_input("备注", key="ac_notes").strip()
        submitted = st.form_submit_button("加入筛选池", type="primary")

    if not submitted:
        return

    # ── validation ────────────────────────────────────────────────────────────
    errs = []
    if not _re.fullmatch(r"[a-z][a-z0-9_]{1,30}", asset_code or ""):
        errs.append("资产代码须为英文小写字母开头，仅小写字母/数字/下划线")
    if asset_code in existing_codes:
        errs.append(f"资产代码 {asset_code} 已存在")
    if not plant_name:
        errs.append("电站名称必填")
    if not zone:
        errs.append("区域必填")
    if capacity_mw <= 0:
        errs.append("容量必须 > 0")
    if duration_h <= 0:
        errs.append("时长必须 > 0")
    if node_mode == "选择已有节点作代理" and not zone_price_node:
        errs.append("请选择一个代理节点（或改选 暂无）")
    for e in errs:
        st.error(e)
    if errs:
        return

    with engine.begin() as conn:
        conn.execute(_t("""
            INSERT INTO marketdata.asset_registry
                (asset_code, plant_name, status, zone, capacity_mw, duration_h,
                 capacity_source, substation, conn_kv, zone_price_node, notes)
            VALUES (:code, :name, :status, :zone, :cap, :dur, :src, :sub, :kv, :node, :notes)
        """), {"code": asset_code, "name": plant_name, "status": status, "zone": zone,
               "cap": float(capacity_mw), "dur": float(duration_h), "src": "UI added (screening)",
               "sub": substation or None, "kv": conn_kv, "node": zone_price_node,
               "notes": notes or None})
    st.success(f"✅ {plant_name} ({asset_code}) 已加入 {status} 池 — 打开 Deal Structurer → 7·新资产筛选 即可评估。")
    st.cache_data.clear()
