"""Tab 1 — Price Simulation: province selector, OU/PCA params, price path chart."""
from __future__ import annotations
from datetime import date, timedelta
import numpy as np
import plotly.graph_objects as go
import streamlit as st
from libs.deal_models.contracts import OUParams, PriceSimRequest
from libs.deal_models.price_simulator import simulate_prices
from services.deal_engine.price_data import fetch_price_history
from apps.deal_structurer import session_cache


def render() -> None:
    st.header("1 · Price Simulation")
    st.caption("Simulate forward hourly price paths for the target province.")

    col1, col2 = st.columns([1, 2])
    with col1:
        province = st.selectbox("Province", ["蒙西", "蒙东", "山西", "河北南网", "山东", "陕西", "甘肃", "新疆"], key="ps_province")
        model = st.radio("Price Model", ["OU (Ornstein-Uhlenbeck)", "PCA (Distribution Sliders)"], key="ps_model")
        n_sim = st.slider("Simulations", 100, 2000, 500, 100, key="ps_nsim")
        n_years = st.slider("Horizon (years)", 1, 10, 1, key="ps_nyears")

        use_ou = model.startswith("OU")
        if use_ou:
            st.subheader("OU Parameters")
            mu = st.number_input("Long-run mean (¥/MWh)", 50.0, 800.0, 300.0, key="ou_mu")
            kappa = st.number_input("Mean-reversion κ", 0.1, 20.0, 2.0, key="ou_kappa")
            sigma = st.number_input("Volatility σ (¥/MWh ann.)", 10.0, 300.0, 80.0, key="ou_sigma")
        else:
            price_type = st.radio("Price type", ["DA (day-ahead)", "RT (real-time)"], horizontal=True, key="ps_price_type")
            lookback = st.slider("History window (months)", 3, 24, 12, key="ps_lookback")
            with st.expander("Paste custom data instead"):
                history_text = st.text_area("Hourly prices (one per line, ¥/MWh)", height=100, key="ps_history")

        run_btn = st.button("▶ Run Simulation", type="primary", key="ps_run")

    with col2:
        if run_btn:
            with st.spinner("Simulating price paths…"):
                try:
                    if use_ou:
                        req = PriceSimRequest(
                            province=province, n_simulations=n_sim, n_years=n_years,
                            model="ou",
                            ou_params=OUParams(kappa=kappa, mu=mu, sigma=sigma),
                        )
                    else:
                        custom = st.session_state.get("ps_history", "").strip()
                        if custom:
                            raw = [float(x) for x in custom.splitlines() if x.strip()]
                        else:
                            end_dt = date.today()
                            start_dt = end_dt - timedelta(days=lookback * 30)
                            price_col = "rt_price" if st.session_state.get("ps_price_type", "").startswith("RT") else "da_price"
                            raw = fetch_price_history(
                                province, str(start_dt), str(end_dt), price_col=price_col
                            )
                        if len(raw) < 168:
                            st.error("Need at least 168 hours of history for PCA fitting.")
                            return
                        req = PriceSimRequest(
                            province=province, n_simulations=n_sim, n_years=n_years,
                            model="pca", price_history_yuan_mwh=raw,
                        )
                    paths = simulate_prices(req)
                    st.session_state["price_paths"] = paths
                    st.session_state["price_sim_req"] = req
                    session_cache.save(st.session_state)
                except Exception as e:
                    st.error(f"Simulation failed: {e}")
                    return

        paths = st.session_state.get("price_paths")
        if paths is not None:
            # Plot fan chart: P10/P50/P90 + sample paths
            hours = np.arange(paths.shape[1])
            p10 = np.percentile(paths, 10, axis=0)
            p50 = np.percentile(paths, 50, axis=0)
            p90 = np.percentile(paths, 90, axis=0)
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=hours, y=p90, name="P90", line=dict(color="rgba(99,110,250,0.3)"), showlegend=True))
            fig.add_trace(go.Scatter(x=hours, y=p10, name="P10", fill="tonexty", line=dict(color="rgba(99,110,250,0.3)"), fillcolor="rgba(99,110,250,0.08)"))
            fig.add_trace(go.Scatter(x=hours, y=p50, name="P50", line=dict(color="rgb(99,110,250)", width=2)))
            for i in range(min(8, paths.shape[0])):
                fig.add_trace(go.Scatter(x=hours, y=paths[i], line=dict(color="rgba(200,200,200,0.4)", width=0.5), showlegend=False))
            province_label = st.session_state.get("ps_province", province)
            fig.update_layout(title=f"Price Paths — {province_label}", xaxis_title="Hour", yaxis_title="¥/MWh", height=450)
            st.plotly_chart(fig, use_container_width=True)

            summary_cols = st.columns(3)
            summary_cols[0].metric("P10 Mean Price", f"¥{p10.mean():.0f}/MWh")
            summary_cols[1].metric("P50 Mean Price", f"¥{p50.mean():.0f}/MWh")
            summary_cols[2].metric("P90 Mean Price", f"¥{p90.mean():.0f}/MWh")
        else:
            st.info("Configure parameters and click **▶ Run Simulation** to generate price paths.")
