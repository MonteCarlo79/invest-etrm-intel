"""Tab 2 — Dispatch Revenue: P10/P50/P90 bar + histogram + decomposition."""
from __future__ import annotations
import numpy as np
import plotly.graph_objects as go
import streamlit as st
from libs.deal_models.contracts import DispatchRequest
from libs.deal_models.dispatch_valuation import dispatch_annual


def render() -> None:
    st.header("2 · Dispatch Revenue")
    paths = st.session_state.get("price_paths")
    if paths is None:
        st.warning("Run **Tab 1 · Price Simulation** first to generate price paths.")
        return

    col1, col2 = st.columns([1, 2])
    with col1:
        asset_type = st.selectbox("Asset Type", ["bess", "wind", "wind_bess"], key="dr_asset")
        if asset_type in ("bess", "wind_bess"):
            st.subheader("BESS Parameters")
            power_mw = st.number_input("Power (MW)", 1.0, 500.0, 50.0, key="dr_power")
            capacity_mwh = st.number_input("Capacity (MWh)", 1.0, 2000.0, 100.0, key="dr_cap")
            roundtrip_eff = st.slider("Roundtrip Efficiency", 0.70, 0.95, 0.85, 0.01, key="dr_eff")
            cycles = st.slider("Cycles/day", 0.5, 2.0, 1.0, 0.5, key="dr_cycles")
            om = st.number_input("O&M ¥/MWh discharged", 0.0, 50.0, 10.0, key="dr_om")
        else:
            power_mw = capacity_mwh = roundtrip_eff = om = 0.0; cycles = 1.0
        if asset_type in ("wind", "wind_bess"):
            st.subheader("Wind Parameters")
            installed_mw = st.number_input("Installed MW", 1.0, 2000.0, 100.0, key="dr_wind_mw")
        else:
            installed_mw = 0.0

        run_btn = st.button("▶ Calculate Revenue", type="primary", key="dr_run")

    with col2:
        if run_btn:
            req = DispatchRequest(
                asset_type=asset_type,
                capacity_mwh=capacity_mwh, power_mw=power_mw,
                roundtrip_eff=roundtrip_eff, cycles_per_day=cycles,
                om_cost_yuan_per_mwh=om, installed_mw=installed_mw,
            )
            with st.spinner("Calculating dispatch revenue…"):
                result = dispatch_annual(paths, req)
            st.session_state["dispatch_result"] = result
            st.session_state["last_dispatch_req"] = req

        result = st.session_state.get("dispatch_result")
        if result is not None:
            m_cols = st.columns(3)
            m_cols[0].metric("P10 Annual Revenue", f"¥{result.p10/1e6:.1f}M")
            m_cols[1].metric("P50 Annual Revenue", f"¥{result.p50/1e6:.1f}M")
            m_cols[2].metric("P90 Annual Revenue", f"¥{result.p90/1e6:.1f}M")

            fig = go.Figure()
            fig.add_trace(go.Histogram(
                x=result.revenue_paths / 1e6, nbinsx=40,
                name="Revenue (¥M)", marker_color="rgb(99,110,250)",
            ))
            fig.add_vline(x=result.p10 / 1e6, line_dash="dot", line_color="orange", annotation_text="P10")
            fig.add_vline(x=result.p50 / 1e6, line_dash="dash", line_color="green", annotation_text="P50")
            fig.add_vline(x=result.p90 / 1e6, line_dash="dot", line_color="blue", annotation_text="P90")
            fig.update_layout(title="Annual Revenue Distribution", xaxis_title="Revenue (¥M)", height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Configure asset parameters and click **▶ Calculate Revenue**.")
