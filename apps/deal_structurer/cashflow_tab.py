"""Tab 3 · Project Cash Flow — deterministic P&L + finance-distribution layer.

Two sections:

1. Deterministic cashflow form (unchanged behavior): project + financing
   inputs → ``compute_cashflow`` → KPI metrics, P&L table, equity-FCF chart.
   The base-revenue default now comes from the Option Value tab's
   ``ov_result`` (capture_adj [¥/kW/yr] × power_mw × 1000 → absolute ¥/yr)
   when set — the old dispatch-tab p50 source is retired with the
   pipeline tabs; wind/solar cells (no capture_adj) fall back to
   ``total_yuan_kw``; no ``ov_result`` → the historical 15.0 ¥M default.

2. NEW 分布 / Distribution section: per-path ``compute_cashflow`` over
   ``ov_result["revenue_paths_yuan"]`` (absolute ¥/yr per simulated year) →
   equity-IRR P10/P50/P90 histogram, P(IRR < hurdle), revenue VaR/CVaR 5%,
   and a ±10% one-at-a-time NPV tornado on {capex, interest, O&M, revenue
   scale} computed on the P50-revenue cashflow (pure cashflow recomputes,
   no MC re-runs — replaces the old mc_tab tornado).

Graceful degradation (never a traceback): no ``ov_result`` → guidance to
compute a cell on the Option Value tab first; ``revenue_paths_yuan`` None
(snapshot-sourced cell) → guidance to run the on-demand recompute there.

Pure helpers (``distribution_metrics``, ``npv_tornado``,
``base_revenue_default_m``) carry no streamlit dependency and are
unit-tested; rendering is verified by import smoke.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from libs.deal_models.contracts import ProjectFinancials
from libs.deal_models.project_cashflow import compute_cashflow
from apps.deal_structurer import session_cache


# ---------------------------------------------------------------------------
# Pure helpers (no streamlit) — unit-tested
# ---------------------------------------------------------------------------

def distribution_metrics(irr_paths: np.ndarray, revenue_paths: np.ndarray,
                         hurdle: float) -> dict:
    """Distribution stats over per-path equity IRRs and annual revenues.

    Returns {irr_p10, irr_p50, irr_p90, prob_below_hurdle, rev_var5,
    rev_cvar5}. Revenue VaR 5% uses the floor order statistic
    (``method="lower"``) — an observed path revenue — so the CVaR tail mean
    (paths ≤ VaR) is exactly comparable to it.
    """
    irr = np.asarray(irr_paths, dtype=float)
    rev = np.asarray(revenue_paths, dtype=float)
    rv5 = float(np.percentile(rev, 5, method="lower"))
    tail = rev[rev <= rv5]
    return {
        "irr_p10": float(np.percentile(irr, 10)),
        "irr_p50": float(np.percentile(irr, 50)),
        "irr_p90": float(np.percentile(irr, 90)),
        "prob_below_hurdle": float((irr < float(hurdle)).mean()),
        "rev_var5": rv5,
        "rev_cvar5": float(tail.mean()) if tail.size else rv5,
    }


_TORNADO_PARAMS = [
    ("capex", "capex_total_yuan"),
    ("interest_rate", "interest_rate"),
    ("om_cost", "annual_om_yuan"),
    ("revenue_scale", None),  # scales the P50 base revenue itself
]


def npv_tornado(fin: ProjectFinancials, base_rev: float,
                pct: float = 0.10) -> list[dict]:
    """±10% one-at-a-time NPV sensitivity on the P50-revenue cashflow.

    Pure ``compute_cashflow`` recomputes — deterministic, no MC re-runs.
    Each row: {param, npv_plus, npv_minus, d_plus, d_minus, swing} where
    d_* are NPV changes vs the base case (¥). Sorted by swing descending.
    """
    n = fin.project_life_years
    base_fin = fin.model_copy(
        update={"annual_revenue_yuan": [float(base_rev)] * n})
    base_npv = compute_cashflow(base_fin).npv
    rows = []
    for label, field in _TORNADO_PARAMS:
        npvs = {}
        for sign, key in ((+1.0, "plus"), (-1.0, "minus")):
            if field is None:
                update = {"annual_revenue_yuan":
                          [float(base_rev) * (1.0 + sign * pct)] * n}
            else:
                update = {field: getattr(fin, field) * (1.0 + sign * pct),
                          "annual_revenue_yuan": [float(base_rev)] * n}
            npvs[key] = compute_cashflow(fin.model_copy(update=update)).npv
        rows.append({
            "param": label,
            "npv_plus": npvs["plus"], "npv_minus": npvs["minus"],
            "d_plus": npvs["plus"] - base_npv,
            "d_minus": npvs["minus"] - base_npv,
            "swing": abs(npvs["plus"] - npvs["minus"]),
        })
    return sorted(rows, key=lambda r: r["swing"], reverse=True)


def base_revenue_default_m(ov: dict | None) -> tuple[float, str | None]:
    """Form default for base annual revenue (¥M) + basis label.

    ov_result contract: ¥/kW/yr × power_mw × 1000 → absolute ¥/yr (the
    contract's power_mw is the authority) → ¥M for the widget.
    capture_adj is the primary basis; wind/solar cells (capture_adj None)
    fall back to total_yuan_kw. No ov_result → historical 15.0 default.
    """
    if not ov:
        return 15.0, None
    power = float(ov.get("power_mw") or 0.0)
    for key, label in (("capture_adj_yuan_kw", "捕获率调整后 capture_adj"),
                       ("total_yuan_kw", "合计 total")):
        v = ov.get(key)
        if v is not None and power > 0:
            return float(v) * power * 1000.0 / 1e6, label
    return 15.0, None


# ---------------------------------------------------------------------------
# Streamlit rendering
# ---------------------------------------------------------------------------

def render() -> None:
    st.header("3 · Project Cash Flow")
    ov = st.session_state.get("ov_result")
    base_rev_default, rev_basis = base_revenue_default_m(ov)

    col1, col2 = st.columns([1, 2])
    with col1:
        st.subheader("Project Inputs")
        capex = st.number_input("Capex (¥M)", 10.0, 5000.0, 100.0, key="cf_capex") * 1e6
        om = st.number_input("Annual O&M (¥M)", 0.1, 100.0, 3.0, key="cf_om") * 1e6
        base_rev_input = st.number_input("Base Annual Revenue (¥M)", 0.1, 5000.0, float(base_rev_default), key="cf_rev") * 1e6
        if rev_basis:
            st.caption(f"默认值来源：期权价值页 {rev_basis} × "
                       f"{ov.get('power_mw')} MW × 1000（{ov.get('province')} "
                       f"{ov.get('asset_type')} 快照 {ov.get('asof_date')}）。")
        life = st.slider("Project Life (years)", 10, 30, 20, key="cf_life")
        st.subheader("Financing")
        debt_ratio = st.slider("Debt Ratio", 0.0, 0.90, 0.70, 0.05, key="cf_debt")
        interest = st.slider("Interest Rate", 0.02, 0.12, 0.05, 0.005, format="%.3f", key="cf_rate")
        loan_term = st.slider("Loan Term (years)", 5, 20, 10, key="cf_term")
        grace = st.slider("Grace Period (years)", 0, 3, 1, key="cf_grace")
        hurdle = st.slider("Hurdle Rate", 0.04, 0.20, 0.08, 0.01, format="%.2f", key="cf_hurdle")
        run_btn = st.button("▶ Calculate", type="primary", key="cf_run")

    with col2:
        if run_btn:
            fin = ProjectFinancials(
                capex_total_yuan=capex, annual_revenue_yuan=[base_rev_input] * life,
                annual_om_yuan=om, project_life_years=life,
                debt_ratio=debt_ratio, interest_rate=interest,
                loan_term_years=loan_term, grace_years=grace, hurdle_rate=hurdle,
            )
            cf = compute_cashflow(fin)
            st.session_state["last_financials"] = fin
            st.session_state["last_cf_result"] = cf
            session_cache.save(st.session_state)

        cf = st.session_state.get("last_cf_result")
        if cf is None:
            st.info("Configure project parameters and click **▶ Calculate**.")
        else:
            m = st.columns(4)
            m[0].metric("Equity IRR", f"{cf.equity_irr:.1%}")
            m[1].metric("Project IRR", f"{cf.project_irr:.1%}")
            m[2].metric("DSCR (min)", f"{cf.dscr_min:.2f}x" if cf.dscr_min == cf.dscr_min else "—")
            m[3].metric("NPV (¥M)", f"{cf.npv/1e6:.1f}")
            m2 = st.columns(3)
            m2[0].metric("ROACE", f"{cf.roace:.1%}")
            m2[1].metric("Payback", f"{cf.payback_years:.1f}yr" if cf.payback_years == cf.payback_years else "—")
            m2[2].metric("DSCR (avg)", f"{cf.dscr_avg:.2f}x" if cf.dscr_avg == cf.dscr_avg else "—")

            rows = [
                {"Year": r.year, "Revenue (¥M)": r.revenue/1e6, "Opex (¥M)": r.opex/1e6,
                 "EBITDA (¥M)": r.ebitda/1e6, "EBIT (¥M)": r.ebit/1e6,
                 "Net Income (¥M)": r.net_income/1e6, "Equity FCF (¥M)": r.equity_fcf/1e6,
                 "Debt Service (¥M)": r.debt_service/1e6}
                for r in cf.annual
            ]
            df = pd.DataFrame(rows).set_index("Year")
            st.dataframe(df.style.format("{:.2f}"), use_container_width=True)

            fig = go.Figure(go.Bar(
                x=[f"Y{r.year}" for r in cf.annual],
                y=[r.equity_fcf / 1e6 for r in cf.annual],
                marker_color=["green" if r.equity_fcf >= 0 else "red" for r in cf.annual],
            ))
            fig.update_layout(title="Equity Free Cash Flow (¥M)", height=300)
            st.plotly_chart(fig, use_container_width=True)

    st.divider()
    _render_distribution(ov, capex=capex, om=om, life=life,
                         debt_ratio=debt_ratio, interest=interest,
                         loan_term=loan_term, grace=grace, hurdle=hurdle)


def _render_distribution(ov: dict | None, *, capex: float, om: float,
                         life: int, debt_ratio: float, interest: float,
                         loan_term: int, grace: int, hurdle: float) -> None:
    """分布 / Distribution: ov_result revenue paths → IRR distribution,
    VaR/CVaR, NPV tornado. Degrades to guidance, never a traceback."""
    st.subheader("分布 · Distribution（收入路径 → 股权 IRR 分布）")

    if ov is None:
        st.info("请先在「期权价值 Option Value」页选择一个单元并获得其价值"
                "（快照读取或按需重算）— 分布层以该单元的收入路径为输入。")
        return
    paths = ov.get("revenue_paths_yuan")
    if paths is None:
        st.info("该单元的收入路径不在会话中（快照来源不含路径）— 请先在"
                "「期权价值 Option Value」页执行「按需重算 on-demand "
                "recompute」生成路径分布。")
        return
    paths = np.asarray(paths, dtype=float).ravel()
    if paths.size == 0:
        st.info("收入路径为空 — 请在「期权价值 Option Value」页重新执行"
                "「按需重算」。")
        return

    st.caption(
        f"单元：{ov.get('province')} · {ov.get('asset_type')} · "
        f"{ov.get('duration_h')}h · {ov.get('power_mw')} MW · 快照 "
        f"{ov.get('asof_date')} · 路径数 {paths.size}。"
        "财务参数取自上方表单当前值（点击按钮时固化）。"
    )
    run = st.button("▶ 计算分布", type="primary", key="cf_dist_run")
    if run:
        p50_rev = float(np.median(paths))
        fin = ProjectFinancials(
            capex_total_yuan=capex,
            annual_revenue_yuan=[p50_rev] * life,
            annual_om_yuan=om, project_life_years=life,
            debt_ratio=debt_ratio, interest_rate=interest,
            loan_term_years=loan_term, grace_years=grace,
            hurdle_rate=hurdle,
        )
        with st.spinner(f"逐路径重算现金流（{paths.size} 条）…"):
            irrs = np.array([
                compute_cashflow(fin.model_copy(
                    update={"annual_revenue_yuan": [float(r)] * life})
                ).equity_irr
                for r in paths
            ])
            metrics = distribution_metrics(irrs, paths, hurdle)
            tornado = npv_tornado(fin, p50_rev)
        st.session_state["cf_dist_result"] = {
            "irr_paths": irrs, "metrics": metrics, "tornado": tornado,
            "p50_rev": p50_rev, "n_paths": int(paths.size),
            "fin_echo": {
                "capex_m": capex / 1e6, "om_m": om / 1e6, "life": life,
                "debt_ratio": debt_ratio, "interest": interest,
                "loan_term": loan_term, "grace": grace, "hurdle": hurdle,
            },
        }

    dist = st.session_state.get("cf_dist_result")
    if dist is None:
        st.info("点击 **▶ 计算分布** 生成股权 IRR 分布、收入 VaR/CVaR 与 "
                "NPV 敏感性龙卷风图。")
        return

    mt = dist["metrics"]
    m = st.columns(4)
    m[0].metric("Equity IRR P50", f"{mt['irr_p50']:.1%}")
    m[1].metric("IRR P10 – P90", f"{mt['irr_p10']:.1%} – {mt['irr_p90']:.1%}")
    m[2].metric("P(IRR < hurdle)", f"{mt['prob_below_hurdle']:.1%}")
    m[3].metric("Revenue P50 (¥M)", f"{dist['p50_rev']/1e6:.1f}")
    m2 = st.columns(2)
    m2[0].metric("Revenue VaR 5% (¥M)", f"{mt['rev_var5']/1e6:.1f}")
    m2[1].metric("Revenue CVaR 5% (¥M)", f"{mt['rev_cvar5']/1e6:.1f}")

    c1, c2 = st.columns(2)
    with c1:
        fig = go.Figure(go.Histogram(
            x=dist["irr_paths"] * 100, nbinsx=40,
            marker_color="rgb(99,110,250)", name="Equity IRR %",
        ))
        fig.add_vline(x=mt["irr_p10"] * 100, line_dash="dot",
                      annotation_text="P10")
        fig.add_vline(x=mt["irr_p50"] * 100, line_dash="dash",
                      line_color="green", annotation_text="P50")
        fig.add_vline(x=mt["irr_p90"] * 100, line_dash="dot",
                      annotation_text="P90")
        fig.add_vline(x=dist["fin_echo"]["hurdle"] * 100, line_dash="dash",
                      line_color="red", annotation_text="hurdle")
        fig.update_layout(title="Equity IRR Distribution",
                          xaxis_title="IRR (%)", height=350)
        st.plotly_chart(fig, use_container_width=True)
        st.caption("−99.99% 处为 IRR 求解器哨兵（现金流无符号变化 / 深度亏损"
                   "路径）— 如实保留，未剔除。")

    with c2:
        tn = dist["tornado"]
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            y=[t["param"] for t in tn],
            x=[t["d_plus"] / 1e6 for t in tn],
            name="+10%", orientation="h", marker_color="rgb(239,85,59)",
        ))
        fig2.add_trace(go.Bar(
            y=[t["param"] for t in tn],
            x=[t["d_minus"] / 1e6 for t in tn],
            name="−10%", orientation="h", marker_color="rgb(99,110,250)",
        ))
        fig2.update_layout(
            barmode="relative",
            title="NPV 敏感性（±10% 单因素，P50 收入基准）",
            xaxis_title="ΔNPV (¥M)", height=350,
        )
        st.plotly_chart(fig2, use_container_width=True)
        st.caption("龙卷风图为 P50 收入现金流上的纯现金流重算（无 MC 重跑）；"
                   "revenue_scale = 整体收入 ±10%。interest_rate 零摆动属预期："
                   "引擎 NPV 为无杠杆口径（税基 EBIT、无债息税盾），项目 NPV "
                   "不随融资利率变化。")

    e = dist["fin_echo"]
    st.caption(
        f"计算参数（点击时固化）：capex {e['capex_m']:.1f} ¥M · O&M "
        f"{e['om_m']:.1f} ¥M · 寿命 {e['life']}y · 债比 {e['debt_ratio']:.0%} · "
        f"利率 {e['interest']:.3f} · 贷款 {e['loan_term']}y（宽限 {e['grace']}y）· "
        f"hurdle {e['hurdle']:.2f} · 路径 {dist['n_paths']} 条 — "
        "修改表单后请重新点击 ▶ 计算分布。"
    )
