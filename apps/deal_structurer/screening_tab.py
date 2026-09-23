"""apps/deal_structurer/screening_tab.py — New Asset Screening.

Marginal portfolio contribution for candidate assets (Ardian Opta lesson):
before committing capital, quantify what a new asset does to the existing
fleet's cashflow volatility and zone diversification.

Per candidate (from marketdata.asset_registry, upcoming/screening):
  - pairwise daily-price correlation with the fleet proxy
  - portfolio σ per MW before → after
  - diversification benefit (vol-per-MW reduction)
  - zone concentration map (existing fleet throughput weights)
"""
from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

from libs.deal_models.workflows.portfolio_contribution import compute_contribution

_PROXY_FALLBACK = {"alashan": "内蒙.德岭山站/220kV.1M"}


def _engine():
    import os
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    return create_engine(url, pool_pre_ping=True)


def _fmt_pct(v, digits=1):
    return "—" if v is None or pd.isna(v) else f"{v:.{digits}%}"


def render() -> None:
    import plotly.graph_objects as go

    st.header("新资产筛选 · Marginal Portfolio Contribution")
    st.caption(
        "口径：组合现金流代理 = Σ 容量×时长×区域节点日均价（120 日窗口）；"
        "波动率/MW 下降 = 多元化收益。价格代理为节点日均价，未含预测偏差与运维风险。"
    )

    engine = _engine()
    from services.common.asset_registry import fleet_by_zone, load_assets, seed_if_empty
    seed_if_empty(engine)

    fleet = fleet_by_zone(engine)
    candidates = load_assets(engine, status=["upcoming", "screening"])
    if candidates.empty:
        st.info("注册表中没有 upcoming/screening 状态的候选资产。")
        return
    if fleet.empty:
        st.warning("在运资产缺少容量或价格节点，无法计算组合代理。")

    choice = st.selectbox(
        "候选资产",
        candidates["asset_code"].tolist(),
        format_func=lambda c: f"{c} — {candidates[candidates['asset_code'] == c]['plant_name'].iloc[0]}",
    )
    cand = candidates[candidates["asset_code"] == choice].iloc[0].to_dict()

    res = compute_contribution(engine, cand, fleet,
                               proxy_node=_PROXY_FALLBACK.get(choice))
    for w in res["warnings"]:
        st.warning(w)
    if res.get("used_proxy_node"):
        st.info(f"⚠️ 该资产尚无自有价格节点，暂用 {res['candidate_node']} 作区域价格代理。")

    m = res.get("metrics") or {}
    c1, c2, c3 = st.columns(3)
    c1.metric("与组合相关性", "—" if m.get("corr") is None else f"{m['corr']:.2f}")
    vpm_b, vpm_a = m.get("vol_per_mw_before"), m.get("vol_per_mw_after")
    c2.metric("组合波动率/MW",
              "—" if vpm_b is None else f"{vpm_b:,.0f} → {vpm_a:,.0f}")
    div = m.get("diversification")
    c3.metric("多元化收益", _fmt_pct(div),
              delta=_fmt_pct(div) if div else None,
              delta_color="normal" if (div or 0) >= 0 else "inverse")

    if m.get("corr") is not None:
        if m["corr"] < 0.4:
            st.caption("🟢 低相关 — 该区域对组合有显著对冲/分散价值")
        elif m["corr"] < 0.75:
            st.caption("🟡 中等相关 — 分散价值一般")
        else:
            st.caption("🔴 高相关 — 与现有组合同向波动，多元化价值有限")

    # ── Zone concentration map ────────────────────────────────────────────────
    st.markdown("#### 区域集中度（在运组合吞吐权重 = 容量×时长）")
    fw = res.get("fleet_weights", {})
    if fw:
        zones = pd.DataFrame({"node": list(fw), "weight": list(fw.values())})
        zones["candidate"] = zones["node"] == res.get("candidate_node")
        fig = go.Figure()
        for is_cand, color in ((False, "#1565C0"), (True, "#E53935")):
            z = zones[zones["candidate"] == is_cand]
            if not z.empty:
                fig.add_bar(x=z["node"], y=z["weight"],
                            name="候选区域" if is_cand else "现有组合",
                            marker_color=color)
        fig.update_layout(height=280, margin=dict(t=20, b=20),
                          yaxis_title="吞吐权重 (MW·h)", xaxis_title="")
        st.plotly_chart(fig, use_container_width=True)
        cand_w = (cand.get("capacity_mw") or 0) * (cand.get("duration_h") or 1)
        total_w = sum(fw.values())
        if total_w > 0:
            st.caption(
                f"候选区域现有权重 {(fw.get(res.get('candidate_node'), 0) / total_w):.1%}；"
                f"本资产新增吞吐权重 {cand_w:,.0f} MW·h（组合现有 {total_w:,.0f}）。"
            )

    # ── Before/after daily proxy curves ───────────────────────────────────────
    fs, cs = res.get("fleet_series"), res.get("candidate_series")
    if fs is not None and cs is not None and not fs.empty:
        combined = fs + cs * ((cand.get("capacity_mw") or 0) * (cand.get("duration_h") or 1) / 1.0)
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=fs.index, y=fs, name="现有组合代理", mode="lines"))
        fig2.add_trace(go.Scatter(x=combined.index, y=combined,
                                  name="组合+候选", mode="lines",
                                  line=dict(dash="dot")))
        fig2.update_layout(height=280, margin=dict(t=20, b=20),
                           yaxis_title="日现金流代理 (¥)", xaxis_title="")
        st.plotly_chart(fig2, use_container_width=True)
