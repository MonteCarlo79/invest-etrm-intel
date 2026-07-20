"""Tab 4 — Monte Carlo: IRR distribution, VaR table, tornado chart."""
from __future__ import annotations
import numpy as np
import plotly.graph_objects as go
import streamlit as st
from libs.deal_models.contracts import MCRequest
from services.deal_engine.batch_runner import run_batch
from apps.deal_structurer import session_cache


def render() -> None:
    st.header("4 · Monte Carlo Results")

    fin = st.session_state.get("last_financials")
    dispatch_req = st.session_state.get("last_dispatch_req")
    price_req = st.session_state.get("price_sim_req")

    if fin is None or dispatch_req is None or price_req is None:
        st.warning("Complete Tabs 1, 2, and 3 first.")
        return

    col1, col2 = st.columns([1, 3])
    with col1:
        n_sim = st.select_slider("Simulations", [500, 1000, 2000, 5000], 1000, key="mc_nsim")
        run_btn = st.button("▶ Run Monte Carlo", type="primary", key="mc_run")

    with col2:
        if run_btn:
            req = MCRequest(price_sim=price_req, dispatch=dispatch_req, financials=fin, n_simulations=n_sim)
            bar = st.progress(0.0, text="Running simulations…")
            mc = run_batch(req, progress_callback=bar.progress)
            st.session_state["mc_result"] = mc
            session_cache.save(st.session_state)
            bar.empty()

        mc = st.session_state.get("mc_result")
        if mc is None:
            st.info("Click **▶ Run Monte Carlo** to compute distributions.")
            return

        m = st.columns(4)
        m[0].metric("Revenue P50", f"¥{mc.revenue_p50/1e6:.1f}M")
        m[1].metric("Revenue VaR (5%)", f"¥{mc.revenue_var_5pct/1e6:.1f}M")
        m[2].metric("Equity IRR P50", f"{mc.equity_irr_p50:.1%}")
        m[3].metric("P(IRR < hurdle)", f"{mc.irr_prob_below_hurdle:.1%}")

        c1, c2 = st.columns(2)
        with c1:
            fig = go.Figure()
            fig.add_trace(go.Histogram(x=mc.equity_irr_paths * 100, nbinsx=40, name="Equity IRR %", marker_color="rgb(99,110,250)"))
            fig.add_vline(x=mc.equity_irr_p10 * 100, line_dash="dot", annotation_text="P10")
            fig.add_vline(x=mc.equity_irr_p50 * 100, line_dash="dash", line_color="green", annotation_text="P50")
            fig.add_vline(x=mc.equity_irr_p90 * 100, line_dash="dot", annotation_text="P90")
            fig.update_layout(title="Equity IRR Distribution", xaxis_title="IRR (%)", height=350)
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            params = [t["param"] for t in mc.tornado]
            swings = [t["swing"] * 100 for t in mc.tornado]
            fig2 = go.Figure(go.Bar(x=swings, y=params, orientation="h", marker_color="rgb(239,85,59)"))
            fig2.update_layout(title="IRR Sensitivity (Tornado)", xaxis_title="IRR Swing (pp)", height=350)
            st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Risk Metrics")
        st.table({
            "Metric": ["Revenue P10", "Revenue P50", "Revenue P90", "Revenue VaR (5%)", "Revenue CVaR (5%)"],
            "Value": [
                f"¥{mc.revenue_p10/1e6:.1f}M", f"¥{mc.revenue_p50/1e6:.1f}M",
                f"¥{mc.revenue_p90/1e6:.1f}M", f"¥{mc.revenue_var_5pct/1e6:.1f}M",
                f"¥{mc.revenue_cvar_5pct/1e6:.1f}M",
            ],
        })
