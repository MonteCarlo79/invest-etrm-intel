"""Tab 2 — Dispatch Revenue: P10/P50/P90 bar + histogram + decomposition."""
from __future__ import annotations
import numpy as np
import plotly.graph_objects as go
import streamlit as st
from libs.deal_models.contracts import DispatchRequest
from libs.deal_models.dispatch_valuation import dispatch_annual, _dispatch_wind, _dispatch_bess
from services.deal_engine.price_data import fetch_price_wind_correlation
from apps.deal_structurer import session_cache


@st.cache_data(ttl=3600, show_spinner="Fetching correlation data from DB…")
def _load_corr():
    return fetch_price_wind_correlation()


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
            power_mw = capacity_mwh = om = 0.0; roundtrip_eff = 0.85; cycles = 1.0
        if asset_type in ("wind", "wind_bess"):
            st.subheader("Wind Parameters")
            installed_mw = st.number_input("Installed MW", 1.0, 2000.0, 100.0, key="dr_wind_mw")
            st.subheader("Cannibalization Model")
            price_wind_corr = st.slider(
                "Price-Wind Correlation ρ",
                min_value=-1.0, max_value=1.0, value=0.0, step=0.05,
                key="dr_corr",
                help="Correlation between annual avg price and wind CF. "
                     "Negative = cannibalization (more wind → lower prices). "
                     "蒙西 ≈ -0.38 · 山东 ≈ -0.13 · 安徽 ≈ +0.26",
            )
            cf_volatility = st.slider(
                "CF Annual Volatility σ",
                min_value=0.0, max_value=0.30, value=0.10, step=0.01,
                key="dr_cf_vol",
                help="Year-to-year spread in wind capacity factor. "
                     "0 = fixed CF every year. 0.10 = ±10% CF variation across years.",
            )
        else:
            installed_mw = 0.0
            price_wind_corr = 0.0
            cf_volatility = 0.0

        run_btn = st.button("▶ Calculate Revenue", type="primary", key="dr_run")

    with col2:
        if run_btn:
            req = DispatchRequest(
                asset_type=asset_type,
                capacity_mwh=capacity_mwh, power_mw=power_mw,
                roundtrip_eff=roundtrip_eff, cycles_per_day=cycles,
                om_cost_yuan_per_mwh=om, installed_mw=installed_mw,
                price_wind_corr=price_wind_corr,
                cf_volatility=cf_volatility,
            )
            with st.spinner("Calculating dispatch revenue…"):
                result = dispatch_annual(paths, req)
            st.session_state["dispatch_result"] = result
            st.session_state["last_dispatch_req"] = req
            session_cache.save(st.session_state)

        result = st.session_state.get("dispatch_result")
        if result is not None:
            # ── Revenue statistics ────────────────────────────────────────────
            rev = result.revenue_paths
            r_cols = st.columns(4)
            r_cols[0].metric("P10 Revenue", f"¥{result.p10/1e6:.1f}M")
            r_cols[1].metric("P50 Revenue", f"¥{result.p50/1e6:.1f}M")
            r_cols[2].metric("P90 Revenue", f"¥{result.p90/1e6:.1f}M")
            r_cols[3].metric("Mean Revenue", f"¥{result.mean/1e6:.1f}M")

            # ── Unit revenue statistics ───────────────────────────────────────
            saved_req = st.session_state.get("last_dispatch_req")
            unit_rev_paths = None
            vol_label = ""
            if saved_req is not None and paths is not None:
                n_hours = paths.shape[1]
                n_days = n_hours // 24

                if saved_req.asset_type == "wind":
                    cf_arr = saved_req.capacity_factor_profile
                    wind_gen = saved_req.installed_mw * (
                        float(np.sum(cf_arr[:n_hours])) if cf_arr else 0.30 * n_hours
                    )
                    vol_label = f"Total generation: {wind_gen/1e3:.1f} GWh/yr"
                    if wind_gen > 0:
                        unit_rev_paths = rev / wind_gen

                elif saved_req.asset_type == "bess":
                    n_cyc = max(1, int(saved_req.cycles_per_day))
                    energy = min(saved_req.power_mw, saved_req.capacity_mwh / n_cyc)
                    charge_vol = energy * n_cyc * n_days
                    vol_label = f"Annual throughput: {charge_vol/1e3:.1f} GWh/yr"
                    if charge_vol > 0:
                        unit_rev_paths = rev / charge_vol

                elif saved_req.asset_type == "wind_bess":
                    cf_arr = saved_req.capacity_factor_profile
                    wind_gen = saved_req.installed_mw * (
                        float(np.sum(cf_arr[:n_hours])) if cf_arr else 0.30 * n_hours
                    )
                    n_cyc = max(1, int(saved_req.cycles_per_day))
                    energy = min(saved_req.power_mw, saved_req.capacity_mwh / n_cyc)
                    charge_vol = energy * n_cyc * n_days
                    combined_vol = wind_gen + charge_vol
                    vol_label = f"Wind {wind_gen/1e3:.1f} GWh + BESS {charge_vol/1e3:.1f} GWh/yr"
                    if combined_vol > 0:
                        unit_rev_paths = rev / combined_vol

            if unit_rev_paths is not None:
                u_cols = st.columns(4)
                u_cols[0].metric("P10 Unit Revenue", f"¥{float(np.percentile(unit_rev_paths,10)):.1f}/MWh")
                u_cols[1].metric("P50 Unit Revenue", f"¥{float(np.percentile(unit_rev_paths,50)):.1f}/MWh")
                u_cols[2].metric("P90 Unit Revenue", f"¥{float(np.percentile(unit_rev_paths,90)):.1f}/MWh")
                u_cols[3].metric("Mean Unit Revenue", f"¥{float(unit_rev_paths.mean()):.1f}/MWh")
                if vol_label:
                    st.caption(vol_label)

            # ── Histograms ────────────────────────────────────────────────────
            hist_cols = st.columns(2 if unit_rev_paths is not None else 1)

            with hist_cols[0]:
                fig_rev = go.Figure()
                fig_rev.add_trace(go.Histogram(
                    x=rev / 1e6, nbinsx=40,
                    name="Revenue", marker_color="rgb(99,110,250)",
                ))
                fig_rev.add_vline(x=result.p10/1e6, line_dash="dot", line_color="orange", annotation_text="P10")
                fig_rev.add_vline(x=result.p50/1e6, line_dash="dash", line_color="green", annotation_text="P50")
                fig_rev.add_vline(x=result.p90/1e6, line_dash="dot", line_color="royalblue", annotation_text="P90")
                fig_rev.add_vline(x=result.mean/1e6, line_dash="dash", line_color="red", annotation_text="Mean")
                fig_rev.update_layout(title="Annual Revenue Distribution", xaxis_title="Revenue (¥M)", height=380, showlegend=False)
                st.plotly_chart(fig_rev, use_container_width=True)

            if unit_rev_paths is not None:
                with hist_cols[1]:
                    fig_unit = go.Figure()
                    fig_unit.add_trace(go.Histogram(
                        x=unit_rev_paths, nbinsx=40,
                        name="Unit Revenue", marker_color="rgb(239,85,59)",
                    ))
                    fig_unit.add_vline(x=float(np.percentile(unit_rev_paths,10)), line_dash="dot", line_color="orange", annotation_text="P10")
                    fig_unit.add_vline(x=float(np.percentile(unit_rev_paths,50)), line_dash="dash", line_color="green", annotation_text="P50")
                    fig_unit.add_vline(x=float(np.percentile(unit_rev_paths,90)), line_dash="dot", line_color="royalblue", annotation_text="P90")
                    fig_unit.add_vline(x=float(unit_rev_paths.mean()), line_dash="dash", line_color="red", annotation_text="Mean")
                    fig_unit.update_layout(title="Unit Revenue Distribution", xaxis_title="¥/MWh", height=380, showlegend=False)
                    st.plotly_chart(fig_unit, use_container_width=True)
        else:
            st.info("Configure asset parameters and click **▶ Calculate Revenue**.")

    # ── Price-Wind Correlation ─────────────────────────────────────────────────
    if asset_type in ("wind", "wind_bess"):
        st.divider()
        st.subheader("Price–Wind Correlation by Province")
        st.caption(
            "Pearson correlation between monthly average DA spot price and monthly wind capacity factor. "
            "Negative values indicate wind cannibalization (higher generation → lower prices)."
        )

        if st.button("Load Correlation Data", key="dr_load_corr"):
            try:
                corr_df = _load_corr()
                st.session_state["_corr_df"] = corr_df
            except Exception as e:
                st.error(f"Could not load correlation data: {e}")

        corr_df = st.session_state.get("_corr_df")
        if corr_df is not None and not corr_df.empty:
            st.dataframe(corr_df, hide_index=True)
        elif corr_df is not None:
            st.info("No overlapping price + wind data found in DB.")
