"""复盘 Review — monthly review-deck metrics: upload, browse, reconcile.

Reviewed figures are an overlay/reconciliation only. Settlement and waterfall 实际 come from
settlement bills (no LP recompute here).
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import text

from libs.asset_risk.review import store
from libs.asset_risk.review.parse_bess import parse_bess_deck
from libs.asset_risk.review.recon import reconcile
from libs.asset_risk.waterfall import _load_settlement_actuals

_REPO = Path(__file__).resolve().parents[2]
SCAN_REL = Path("assets/operating/复盘/储能/蒙西")


def scan_dir() -> Path:
    env = os.environ.get("REVIEW_DECK_DIR")
    return Path(env) if env else _REPO / SCAN_REL


def _actuals_by_code(engine) -> pd.DataFrame:
    """Settlement-bill 实际 components as (asset_code, month, arb/cap/fee/other_cny)."""
    from libs.asset_risk.review.recon import ACTUAL_COLUMNS
    with engine.connect() as conn:
        assets = pd.read_sql(text("SELECT id, name, asset_code FROM marketdata.rm_assets"), conn)
    assets = assets[assets["asset_code"].notna()]
    empty = pd.DataFrame(columns=ACTUAL_COLUMNS)
    if assets.empty:
        return empty
    try:
        act = _load_settlement_actuals(engine, assets["id"].astype(int).tolist(), None, None)
    except Exception as e:  # noqa: BLE001 — reconciliation is advisory
        st.caption(f"结算数据不可用: {e}")
        return empty
    if act.empty:
        return empty
    act = act.merge(assets[["name", "asset_code"]], left_on="asset", right_on="name")
    return act[ACTUAL_COLUMNS]


def _parse_many(items: list[tuple[str, bytes]]) -> None:
    parsed = []
    for name, data in items:
        try:
            df, notes = parse_bess_deck(data, name)
        except Exception as e:  # noqa: BLE001 — one bad deck must not stop the batch
            st.error(f"{name}: 解析失败 — {e}")
            continue
        parsed.append({"name": name, "hash": hashlib.sha256(data).hexdigest(), "df": df, "notes": notes})
    st.session_state["review_parsed"] = parsed


@st.cache_resource(show_spinner=False)
def _ensure_review_table(_engine):
    store.ensure_table(_engine)
    return True


RECON_CN = {
    "asset_code": "资产",
    "month": "月份",
    "reviewed_executed_cny": "复盘实际执行收益(¥)",
    "actual_arb_cap_cny": "结算套利+容量补偿(¥)",
    "diff_exec_cny": "执行收益差异(¥)",
    "diff_exec_pct": "执行收益差异%",
    "reviewed_total_cny": "复盘月度总收益(¥)",
    "actual_total_cny": "瀑布实际总额(¥)",
    "diff_total_cny": "总收益差异(¥)",
    "diff_total_pct": "总收益差异%",
    "spot_only_cny": "结算现货收益·参考(¥)",
    "flag": "标记",
}
_STAGES = [("strategy_revenue_cny", "策略申报收益", "#9ecae1"),
           ("cleared_revenue_cny", "实时出清收益", "#4292c6"),
           ("executed_revenue_cny", "实际执行收益", "#08519c")]


def _asset_names(engine) -> dict:
    try:
        with engine.connect() as c:
            rows = c.execute(text("SELECT asset_code, name FROM marketdata.rm_assets "
                                  "WHERE asset_code IS NOT NULL")).fetchall()
        return {r[0]: r[1] for r in rows}
    except Exception:  # noqa: BLE001 — labels only
        return {}


def _revenue_chart(sub: pd.DataFrame, names: dict) -> None:
    """Grouped bars per month: 策略申报 → 实时出清 → 实际执行 (spot + capcomp, pre-fee, 万元),
    with 月度总收益 (after fees) as markers. One chart per selected asset."""
    import plotly.graph_objects as go
    for code in sorted(sub["asset_code"].unique()):
        a = sub[sub["asset_code"] == code]
        months = sorted(a["month_s"].unique())
        if not months or a[a["metric"].isin([m for m, _, _ in _STAGES])].empty:
            continue
        val = a.pivot_table(index="month_s", columns="metric", values="value", aggfunc="first").reindex(months)
        fig = go.Figure()
        for metric, label, color in _STAGES:
            if metric in val:
                y = val[metric] / 1e4
                fig.add_bar(x=months, y=y, name=label, marker_color=color,
                            text=[f"{v:,.0f}" if pd.notna(v) else "" for v in y], textposition="outside",
                            hovertemplate="%{x} " + label + ": %{y:,.1f} 万元<extra></extra>")
        if "total_revenue_cny" in val:
            fig.add_scatter(x=months, y=val["total_revenue_cny"] / 1e4, name="月度总收益（扣费后）",
                            mode="markers+lines", marker=dict(symbol="diamond", size=10, color="#e6550d"),
                            line=dict(dash="dot", color="#e6550d"),
                            hovertemplate="%{x} 月度总收益: %{y:,.1f} 万元<extra></extra>")
        fig.update_layout(barmode="group", title=f"{names.get(code, code)} · 复盘收益（万元）",
                          xaxis=dict(type="category", title="月份"),
                          yaxis=dict(title="万元", tickformat=",.0f"),
                          legend=dict(orientation="h", y=-0.2), height=420, margin=dict(t=60))
        st.plotly_chart(fig, use_container_width=True)


def render_review(engine) -> None:
    st.subheader("复盘 Review")
    _ensure_review_table(engine)
    st.caption("最新上传覆盖同月数据")

    with st.expander("复盘 deck 上传 (BESS pptx)", expanded=False):
        files = st.file_uploader("上传月度复盘 pptx", type=["pptx"], accept_multiple_files=True,
                                 key="review_upload")
        if files and st.button("解析", key="review_parse_btn"):
            _parse_many([(f.name, f.getvalue()) for f in files])
        folder = scan_dir()
        if folder.is_dir():
            if st.button(f"Scan folder ({folder.name})", key="review_scan_btn"):
                _parse_many([(p.name, p.read_bytes()) for p in sorted(folder.glob("*.pptx"))])
        else:
            st.caption("Scan folder 仅限本地运行（本环境无复盘文件夹）。")
        parsed = st.session_state.get("review_parsed") or []
        for p in parsed:
            st.markdown(f"**{p['name']}** — {len(p['df'])} rows")
            for n in p["notes"]:
                st.caption(n)
            if not p["df"].empty:
                st.dataframe(p["df"], use_container_width=True, hide_index=True)
        if parsed and st.button("保存", key="review_save_btn"):
            for p in parsed:
                n = store.save_review(engine, p["df"], p["name"], p["hash"])
                st.success(f"{p['name']}: {n} rows saved") if n else st.info(
                    f"{p['name']}: skipped (already stored or empty)")
            st.session_state["review_parsed"] = []

    data = store.load_review(engine)
    if data.empty:
        st.info("暂无复盘数据。上传月度复盘 pptx 后保存。")
        return
    codes = sorted(data["asset_code"].unique())
    sel = st.multiselect("Asset", codes, default=codes[:1], key="review_assets")
    if not sel:
        st.info("Select one or more assets.")
        return
    names = _asset_names(engine)
    sub = data[data["asset_code"].isin(sel)].copy()
    sub["month_s"] = sub["month"].astype(str).str[:7]
    for code in sel:
        st.markdown(f"##### {code}")
        piv = (sub[sub["asset_code"] == code].pivot_table(index="metric", columns="month_s",
                                                          values="value", aggfunc="first"))
        st.dataframe(piv, use_container_width=True)

    _revenue_chart(sub, names)

    st.markdown("##### 对账 Reconciliation")
    st.caption("两组口径一致的对比：(1) 复盘实际执行收益(现货+容量补偿，费用前) vs 结算单 套利+容量补偿；"
               "(2) 复盘月度总收益(扣系统运行费/线损/调频分摊) vs 瀑布「实际」总额(套利+容量补偿+费用+其他)。"
               "无结算单显示 —；|差异| > 5% 标记。现货收益仅供参考，不标记。不触发 LP 计算。")
    rec = reconcile(sub, _actuals_by_code(engine))
    if rec.empty:
        st.info("所选资产无 executed/total 复盘数据。")
        return
    show = rec.drop(columns=["flag_exec", "flag_total"]).copy()
    show["asset_code"] = show["asset_code"].map(lambda c: names.get(c, c))
    show["flag"] = show["flag"].map({True: "差异>5%", False: ""})
    show = show.rename(columns=RECON_CN)
    show = show[[c for c in RECON_CN.values() if c in show.columns]]
    money = [c for c in show.columns if c.endswith("(¥)")]
    pct = [c for c in show.columns if c.endswith("%")]
    st.dataframe(show.style.format("{:,.0f}", subset=money, na_rep="—")
                           .format("{:+.2%}", subset=pct, na_rep="—"),
                 use_container_width=True, hide_index=True)
