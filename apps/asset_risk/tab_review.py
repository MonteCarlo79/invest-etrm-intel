"""复盘 Review — monthly review-deck metrics: upload, browse, reconcile.

Reviewed figures are an overlay/reconciliation only. Settlement and waterfall 实际 come from
settlement bills (no LP recompute here).
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pandas as pd
import plotly.express as px
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
    sub = data[data["asset_code"].isin(sel)].copy()
    sub["month_s"] = sub["month"].astype(str).str[:7]
    for code in sel:
        st.markdown(f"##### {code}")
        piv = (sub[sub["asset_code"] == code].pivot_table(index="metric", columns="month_s",
                                                          values="value", aggfunc="first"))
        st.dataframe(piv, use_container_width=True)

    ex = sub[sub["metric"] == "executed_revenue_cny"]
    if not ex.empty:
        fig = px.line(ex.sort_values("month_s"), x="month_s", y="value", color="asset_code", markers=True,
                      labels={"month_s": "月份", "value": "实际执行收益 (¥)", "asset_code": "资产"},
                      title="实际执行收益 executed_revenue_cny")
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("##### 对账 Reconciliation")
    st.caption("两组口径一致的对比：(1) 复盘实际执行收益(现货+容量补偿，费用前) vs 结算单 套利+容量补偿；"
               "(2) 复盘月度总收益(扣系统运行费/线损/调频分摊) vs 瀑布「实际」总额(套利+容量补偿+费用+其他)。"
               "无结算单显示 —；|差异| > 5% 标记。现货收益仅供参考，不标记。不触发 LP 计算。")
    rec = reconcile(sub, _actuals_by_code(engine))
    if rec.empty:
        st.info("所选资产无 executed/total 复盘数据。")
        return
    show = rec.drop(columns=["flag_exec", "flag_total"]).copy()
    show["flag"] = show["flag"].map({True: "FLAG >5%", False: ""})
    st.dataframe(show.fillna("—"), use_container_width=True, hide_index=True)
