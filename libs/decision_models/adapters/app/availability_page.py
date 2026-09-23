"""
libs/decision_models/adapters/app/availability_page.py

Revenue-weighted availability (Ardian Opta lesson) for IM BESS assets.

Flat delivery % treats every interval alike; revenue-weighted availability
weights shortfalls by the value of the interval in which they occur — an
offline hour at 19:00 peak costs far more than at 03:00.

Sections:
  - Headline tiles per asset: flat delivery %, RWA %, foregone ¥
  - Monthly RWA trend
  - Worst foregone-value intervals
  - O&M window economics (hour-of-day foregone-value score; cheapest windows)

Data quality caveats are shown when present (stale ops data, assets whose
nominated-vs-actual alignment looks broken).

Presentation only; all compute is in
libs/decision_models/workflows/availability.py.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

from libs.decision_models.workflows.availability import (
    compute_intervals, data_coverage, fetch_dispatch, om_window_table,
    summarize, worst_intervals,
)

_IM_ASSET_CODES = ["suyou", "hangjinqi", "siziwangqi", "gushanliang"]
_IM_ASSET_DISPLAY = {
    "suyou": "SuYou (景蓝乌尔图)",
    "hangjinqi": "HangJinQi (悦杭独贵)",
    "siziwangqi": "SiZiWangQi (景通四益堂储)",
    "gushanliang": "GuShanLiang (裕昭沙子坝)",
}
# Assets whose nominated-vs-actual alignment is suspect (median deviation >> 50%
# in coverage audit) — flag rather than silently show misleading numbers.
_SUSPECT_ALIGNMENT = {"suyou"}


def _engine():
    from libs.decision_models.adapters.app.dispatch_pnl_page import _engine as _e
    return _e()


def render_availability_page() -> None:
    import plotly.graph_objects as go

    st.subheader("Revenue-Weighted Availability · 收入加权可用率")
    st.caption(
        "口径：被调用时段（|调度指令|>0.1MW）内，欠发能量按当期价值计价（放电欠发按正价、"
        "充电欠发按负价）。RWA = 1 − 欠发价值 / 调用价值。数据来源 ops_bess_dispatch_15min。"
    )

    engine = _engine()
    cov = data_coverage(engine, _IM_ASSET_CODES)
    if cov.empty:
        st.info("ops_bess_dispatch_15min 暂无数据。")
        return

    # Coverage + staleness banner
    last_dates = cov.set_index("asset_code")["last_date"].to_dict()
    max_last = max(last_dates.values())
    if (pd.Timestamp.today().date() - max_last).days > 30:
        st.warning(
            f"⚠️ 调度数据最新至 {max_last}（已 {((pd.Timestamp.today().date() - max_last).days)} 天未更新）"
            " — 指标反映历史窗口，请通过 Data Management 上传最新调度日报。"
        )

    assets = st.multiselect(
        "资产", options=_IM_ASSET_CODES,
        default=_IM_ASSET_CODES,
        format_func=lambda c: _IM_ASSET_DISPLAY.get(c, c),
        key="avail_assets",
    )
    if not assets:
        return

    df = fetch_dispatch(engine, assets)
    if df.empty:
        st.info("所选资产无调度数据。")
        return
    df = compute_intervals(df)

    # ── Headline tiles ────────────────────────────────────────────────────────
    summ = summarize(df)
    cols = st.columns(len(assets))
    for i, code in enumerate(assets):
        row = summ[summ["asset_code"] == code]
        with cols[i]:
            st.markdown(f"**{_IM_ASSET_DISPLAY.get(code, code)}**")
            if row.empty:
                st.caption("无数据")
                continue
            r = row.iloc[0]
            if code in _SUSPECT_ALIGNMENT:
                st.caption("🚩 调度指令与实际出力对不上（疑似数据源口径问题），数值仅供参考")
            flat = r["flat_delivery"]
            rwa = r["rwa"]
            st.metric("容量交付率", f"{flat:.1%}" if pd.notna(flat) else "—")
            st.metric("收入加权可用率", f"{rwa:.1%}" if pd.notna(rwa) else "—",
                      delta=None if pd.isna(rwa) or pd.isna(flat) else f"{(rwa-flat)*100:+.1f}pp")
            st.metric("欠发损失", f"¥{r['foregone_cny']:,.0f}")

    # ── Monthly RWA trend ─────────────────────────────────────────────────────
    df["month"] = pd.to_datetime(df["interval_start"]).dt.strftime("%Y-%m")
    monthly = summarize(df, group_cols=("asset_code", "month"))
    if not monthly.empty:
        fig = go.Figure()
        for code in assets:
            m = monthly[monthly["asset_code"] == code].sort_values("month")
            if m.empty:
                continue
            fig.add_trace(go.Scatter(
                x=m["month"], y=m["rwa"] * 100, mode="lines+markers",
                name=_IM_ASSET_DISPLAY.get(code, code),
            ))
        fig.update_layout(height=300, margin=dict(t=20, b=20),
                          yaxis_title="RWA (%)", xaxis_title="")
        st.plotly_chart(fig, use_container_width=True)

    # ── O&M window economics ─────────────────────────────────────────────────
    st.markdown("#### O&M 窗口经济性（低价值时段 = 检修窗口）")
    om = om_window_table(df)
    if not om.empty:
        for code in assets:
            w = om[om["asset_code"] == code].reset_index(drop=True)
            if w.empty:
                continue
            best_n = min(3, len(w))
            with st.expander(f"{_IM_ASSET_DISPLAY.get(code, code)} — 窗口评分（越低越适合检修）", expanded=False):
                show = w[["hour", "avg_price", "avg_nominated_mw", "window_score", "n_intervals"]].copy()
                show.columns = ["时段", "均价 ¥/MWh", "平均调用 MW", "窗口评分", "样本数"]
                show["均价 ¥/MWh"] = show["均价 ¥/MWh"].round(1)
                show["平均调用 MW"] = show["平均调用 MW"].round(1)
                show["窗口评分"] = show["窗口评分"].round(1)
                best_hours = set(w.head(best_n)["hour"].tolist())
                st.caption(f"建议检修窗口：{sorted(best_hours)} 时（评分最低的 {best_n} 个时段）")
                st.dataframe(show, use_container_width=True, hide_index=True)

    # ── Worst foregone intervals ──────────────────────────────────────────────
    st.markdown("#### 欠发损失最大的时段")
    worst = worst_intervals(df, top_n=5)
    if not worst.empty:
        show = worst.copy()
        show["asset_code"] = show["asset_code"].map(_IM_ASSET_DISPLAY)
        show["foregone_cny"] = show["foregone_cny"].round(0)
        show["nominated_dispatch_mw"] = show["nominated_dispatch_mw"].round(1)
        show["actual_dispatch_mw"] = show["actual_dispatch_mw"].round(1)
        show["nodal_price_excel"] = show["nodal_price_excel"].round(1)
        show.columns = ["资产", "时段", "指令 MW", "实际 MW", "节点价 ¥/MWh", "欠发损失 ¥"]
        st.dataframe(show, use_container_width=True, hide_index=True)
