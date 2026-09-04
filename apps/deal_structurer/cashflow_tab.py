"""Tab 3 — Project Cash Flow: P&L table, KPI summary, waterfall."""
from __future__ import annotations
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from libs.deal_models.contracts import ProjectFinancials
from libs.deal_models.project_cashflow import compute_cashflow
from apps.deal_structurer import session_cache


def render() -> None:
    st.header("3 · Project Cash Flow")
    dr = st.session_state.get("dispatch_result")
    base_rev_default = (dr.p50 / 1e6) if dr is not None else 15.0

    col1, col2 = st.columns([1, 2])
    with col1:
        st.subheader("Project Inputs")
        capex = st.number_input("Capex (¥M)", 10.0, 5000.0, 100.0, key="cf_capex") * 1e6
        om = st.number_input("Annual O&M (¥M)", 0.1, 100.0, 3.0, key="cf_om") * 1e6
        base_rev_input = st.number_input("Base Annual Revenue (¥M)", 1.0, 500.0, float(base_rev_default), key="cf_rev") * 1e6
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
            return

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
