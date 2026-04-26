"""
apps/options-cockpit/app.py

Standard Options Trading Cockpit — 4-tab Streamlit app.

Tabs
----
1. Single Option   — BS/Black-76 price + Greeks + P&L heatmap
2. Structures      — vanilla, straddle, strangle, spreads, butterfly, condor
3. Vol Smile       — SVI calibration from manual grid or uploaded CSV
4. Historical Vol  — rolling HV from uploaded price CSV vs current IV

No DB, no LLM, no auth required — pure computation tool.

Run:
    streamlit run apps/options-cockpit/app.py
"""
from __future__ import annotations

import io
import math
import os
import sys

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# Ensure repo root is importable when launched from this directory
_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from libs.options.black_scholes import bs_price, b76_price, bs_greeks, b76_greeks, implied_vol
from libs.options.structures import build_structure
from libs.options.smile import SVIParams, fit_svi, svi_vol, calibrate_from_quotes

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Options Cockpit",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Session state defaults
# ---------------------------------------------------------------------------
if "implied_vol_tab1" not in st.session_state:
    st.session_state.implied_vol_tab1 = 0.20   # shared between Tab 1 and Tab 4

# ---------------------------------------------------------------------------
# Sidebar — shared parameters
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title("📊 Options Cockpit")
    st.markdown("---")
    st.subheader("Underlying")
    underlying_type = st.radio(
        "Pricing model",
        ["Equity (Black-Scholes)", "Futures / Forward (Black-76)"],
        index=0,
        horizontal=False,
    )
    mode = "b76" if "Black-76" in underlying_type else "bs"

    st.markdown("---")
    st.subheader("Market Parameters")
    S_label = "Futures Price (F)" if mode == "b76" else "Spot Price (S)"
    S = st.number_input(S_label, min_value=0.01, value=100.0, step=1.0, format="%.4f")
    r_pct = st.number_input("Risk-free rate (%)", min_value=0.0, max_value=50.0, value=5.0, step=0.1, format="%.2f")
    r = r_pct / 100.0
    q_pct = 0.0
    if mode == "bs":
        q_pct = st.number_input("Dividend yield (%)", min_value=0.0, max_value=20.0, value=0.0, step=0.1, format="%.2f")
    q = q_pct / 100.0

    T_days = st.number_input("Time to expiry (calendar days)", min_value=1, max_value=3650, value=30, step=1)
    T = T_days / 365.0

    sigma_pct = st.number_input("Implied vol (%)", min_value=0.1, max_value=500.0, value=20.0, step=0.5, format="%.2f")
    sigma = sigma_pct / 100.0
    st.session_state.implied_vol_tab1 = sigma  # share with Tab 4

    st.markdown("---")
    st.caption("All tabs share the market parameters above unless overridden.")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _price(S_or_F: float, K: float, flag: str) -> float:
    if mode == "b76":
        return b76_price(S_or_F, K, T, r, sigma, flag)
    return bs_price(S_or_F, K, T, r, sigma, q, flag)


def _greeks(S_or_F: float, K: float, flag: str) -> dict:
    if mode == "b76":
        return b76_greeks(S_or_F, K, T, r, sigma, flag)
    return bs_greeks(S_or_F, K, T, r, sigma, q, flag)


def _moneyness_label(S_or_F: float, K: float, flag: str) -> str:
    diff_pct = (S_or_F - K) / K * 100.0
    atm_tol = 1.0  # within 1% = ATM
    if abs(diff_pct) <= atm_tol:
        return "ATM"
    if flag == "c":
        return "ITM" if diff_pct > 0 else "OTM"
    else:
        return "ITM" if diff_pct < 0 else "OTM"


def _colour_for_label(label: str) -> str:
    return {"ITM": "green", "ATM": "orange", "OTM": "red"}.get(label, "gray")


def _greeks_df(g: dict) -> pd.DataFrame:
    rows = [
        ("Delta (Δ)", f"{g['delta']:+.4f}", "dV/dS — change in option value per $1 move in underlying"),
        ("Gamma (Γ)", f"{g['gamma']:.6f}", "dΔ/dS — rate of change of delta per $1 move"),
        ("Vega (ν)", f"{g['vega']:+.4f}", "dV/d(σ%) — value change per 1% rise in implied vol"),
        ("Theta (Θ)", f"{g['theta']:+.4f}", "dV/dt — value decay per calendar day (negative for long options)"),
        ("Rho (ρ)", f"{g['rho']:+.4f}", "dV/dr% — value change per 1% rise in risk-free rate"),
    ]
    return pd.DataFrame(rows, columns=["Greek", "Value", "Interpretation"])


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs(
    ["Single Option", "Structures", "Vol Smile", "Historical Vol"]
)


# ============================================================
# TAB 1 — Single Option
# ============================================================
with tab1:
    st.subheader("Single Option Pricer")

    col_params, col_results = st.columns([1, 2])

    with col_params:
        flag = st.radio("Option type", ["Call", "Put"], index=0, horizontal=True)
        flag_code = "c" if flag == "Call" else "p"
        K = st.number_input("Strike (K)", min_value=0.01, value=float(round(S)), step=1.0, format="%.4f", key="K_tab1")

    price_val = _price(S, K, flag_code)
    g = _greeks(S, K, flag_code)
    label = _moneyness_label(S, K, flag_code)

    with col_results:
        m1, m2, m3 = st.columns(3)
        m1.metric("Option Price", f"{price_val:.4f}")
        m2.metric("Moneyness", label)
        m3.metric("Intrinsic Value", f"{max(S - K, 0.0) if flag_code == 'c' else max(K - S, 0.0):.4f}")

    st.markdown("##### Greeks")
    st.dataframe(_greeks_df(g), use_container_width=True, hide_index=True)

    # Implied vol from market price input
    st.markdown("---")
    st.markdown("##### Implied Vol Solver")
    col_iv1, col_iv2, col_iv3 = st.columns(3)
    mkt_price = col_iv1.number_input("Market price", min_value=0.0, value=price_val, step=0.01, format="%.4f", key="mkt_price_iv")
    if col_iv2.button("Solve IV", key="solve_iv"):
        iv = implied_vol(mkt_price, S, K, T, r, flag=flag_code, mode=mode, q=q)
        if math.isnan(iv):
            col_iv3.error("No solution (below intrinsic or zero vega)")
        else:
            col_iv3.metric("Implied Vol", f"{iv*100:.2f}%")

    # P&L heatmap: spot × vol bump
    st.markdown("---")
    st.markdown("##### P&L Heatmap (vs current price)")
    spot_range_pct = np.linspace(-30, 30, 13)  # -30% to +30% spot moves
    vol_bumps_pct  = np.linspace(-10, 10, 9)    # -10% to +10% vol bumps

    heatmap_z = []
    for dv in vol_bumps_pct:
        row = []
        for ds in spot_range_pct:
            S_new = S * (1 + ds / 100.0)
            sig_new = max(sigma + dv / 100.0, 0.001)
            if mode == "b76":
                p_new = b76_price(S_new, K, T, r, sig_new, flag_code)
            else:
                p_new = bs_price(S_new, K, T, r, sig_new, q, flag_code)
            row.append(p_new - price_val)
        heatmap_z.append(row)

    fig_hm = go.Figure(go.Heatmap(
        z=heatmap_z,
        x=[f"{x:+.0f}%" for x in spot_range_pct],
        y=[f"{y:+.0f}%" for y in vol_bumps_pct],
        colorscale=[
            [0.0, "#d73027"], [0.4, "#fee090"], [0.5, "#ffffbf"],
            [0.6, "#e0f3f8"], [1.0, "#1a9850"]
        ],
        zmid=0,
        colorbar=dict(title="P&L"),
        hovertemplate="Spot %{x}<br>Vol %{y}<br>P&L: %{z:.4f}<extra></extra>",
    ))
    fig_hm.update_layout(
        height=350,
        margin=dict(l=10, r=10, t=30, b=10),
        xaxis_title="Spot move",
        yaxis_title="Vol bump",
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    st.plotly_chart(fig_hm, use_container_width=True, key="heatmap_tab1")


# ============================================================
# TAB 2 — Structures
# ============================================================
with tab2:
    st.subheader("Multi-Leg Option Structures")

    STRUCTURES = [
        "vanilla", "straddle", "strangle",
        "bull_spread", "bear_spread", "butterfly", "condor"
    ]
    STRUCT_LABELS = {
        "vanilla": "Vanilla (single leg)",
        "straddle": "Straddle",
        "strangle": "Strangle",
        "bull_spread": "Bull Spread (call debit)",
        "bear_spread": "Bear Spread (put debit)",
        "butterfly": "Butterfly (call)",
        "condor": "Condor (call)",
    }

    struct_name = st.selectbox(
        "Structure",
        STRUCTURES,
        format_func=lambda x: STRUCT_LABELS[x],
        key="struct_select",
    )

    # Per-structure strike inputs
    col_s1, col_s2, col_s3, col_s4 = st.columns(4)

    strike = None
    flag_struct = "c"
    strike_atm = None
    strike_low = None
    strike_high = None
    strike_lo2 = None
    strike_hi2 = None

    ATM = float(round(S))

    if struct_name == "vanilla":
        strike = col_s1.number_input("Strike", value=ATM, step=1.0, format="%.4f", key="v_strike")
        flag_struct = col_s2.radio("Type", ["Call", "Put"], horizontal=True, key="v_flag")
        flag_struct = "c" if flag_struct == "Call" else "p"

    elif struct_name == "straddle":
        strike_atm = col_s1.number_input("ATM Strike", value=ATM, step=1.0, format="%.4f", key="str_atm")

    elif struct_name == "strangle":
        strike_low  = col_s1.number_input("Put Strike (low)", value=ATM * 0.95, step=1.0, format="%.4f", key="stng_lo")
        strike_high = col_s2.number_input("Call Strike (high)", value=ATM * 1.05, step=1.0, format="%.4f", key="stng_hi")

    elif struct_name in ("bull_spread", "bear_spread"):
        strike_low  = col_s1.number_input("Lower Strike", value=ATM * 0.95, step=1.0, format="%.4f", key="sp_lo")
        strike_high = col_s2.number_input("Upper Strike", value=ATM * 1.05, step=1.0, format="%.4f", key="sp_hi")

    elif struct_name == "butterfly":
        strike_low  = col_s1.number_input("Lower Wing", value=ATM * 0.95, step=1.0, format="%.4f", key="bf_lo")
        strike_atm  = col_s2.number_input("Body (ATM)", value=ATM, step=1.0, format="%.4f", key="bf_atm")
        strike_high = col_s3.number_input("Upper Wing", value=ATM * 1.05, step=1.0, format="%.4f", key="bf_hi")

    elif struct_name == "condor":
        strike_low  = col_s1.number_input("K1 (outer low)",  value=ATM * 0.90, step=1.0, format="%.4f", key="co_k1")
        strike_lo2  = col_s2.number_input("K2 (inner low)",  value=ATM * 0.95, step=1.0, format="%.4f", key="co_k2")
        strike_hi2  = col_s3.number_input("K3 (inner high)", value=ATM * 1.05, step=1.0, format="%.4f", key="co_k3")
        strike_high = col_s4.number_input("K4 (outer high)", value=ATM * 1.10, step=1.0, format="%.4f", key="co_k4")

    try:
        result = build_structure(
            name=struct_name,
            S_or_F=S, T=T, r=r, sigma=sigma, q=q, mode=mode,
            strike=strike, flag=flag_struct,
            strike_atm=strike_atm,
            strike_low=strike_low, strike_high=strike_high,
            strike_lo2=strike_lo2, strike_hi2=strike_hi2,
        )

        st.markdown(f"#### {result.name}")

        # Summary metrics
        mc1, mc2, mc3, mc4, mc5 = st.columns(5)
        direction = "Debit (pay)" if result.net_premium >= 0 else "Credit (receive)"
        mc1.metric("Net Premium", f"{abs(result.net_premium):.4f}", delta=direction,
                   delta_color="inverse" if result.net_premium >= 0 else "normal")
        mc2.metric("Delta", f"{result.delta:+.4f}")
        mc3.metric("Gamma", f"{result.gamma:.6f}")
        mc4.metric("Vega (per 1%)", f"{result.vega:+.4f}")
        mc5.metric("Theta (per day)", f"{result.theta:+.4f}")

        if result.breakeven_lower or result.breakeven_upper:
            be_str = ""
            if result.breakeven_lower:
                be_str += f"Lower: **{result.breakeven_lower:.2f}**"
            if result.breakeven_upper:
                be_str += f"   Upper: **{result.breakeven_upper:.2f}**"
            st.markdown(f"Breakeven(s): {be_str}")

        # Payoff diagram
        spots = [pt[0] for pt in result.payoff_at_expiry]
        pnls  = [pt[1] for pt in result.payoff_at_expiry]
        fig_payoff = go.Figure()
        fig_payoff.add_trace(go.Scatter(
            x=spots, y=pnls, mode="lines", name="P&L at expiry",
            line=dict(color="#1f77b4", width=2),
            fill="tozeroy",
            fillcolor="rgba(31,119,180,0.10)",
        ))
        fig_payoff.add_hline(y=0, line_dash="dash", line_color="gray", line_width=1)
        fig_payoff.add_vline(x=S, line_dash="dot", line_color="orange",
                             annotation_text="Current", annotation_position="top right")
        fig_payoff.update_layout(
            height=350,
            margin=dict(l=10, r=10, t=30, b=10),
            xaxis_title="Spot at expiry",
            yaxis_title="P&L",
            plot_bgcolor="white", paper_bgcolor="white",
            legend=dict(orientation="h"),
        )
        st.plotly_chart(fig_payoff, use_container_width=True, key="payoff_chart_tab2")

        # Per-leg table
        st.markdown("##### Leg Detail")
        leg_rows = []
        for leg in result.legs:
            direction_leg = "Long" if leg.quantity > 0 else "Short"
            leg_type = "Call" if leg.flag == "c" else "Put"
            g_leg = _greeks(S, leg.strike, leg.flag)
            leg_rows.append({
                "Direction": direction_leg,
                "Type": leg_type,
                "Strike": f"{leg.strike:.4f}",
                "Premium": f"{leg.price:.4f}",
                "Delta": f"{g_leg['delta'] * leg.quantity:+.4f}",
                "Vega/1%": f"{g_leg['vega'] * leg.quantity:+.4f}",
            })
        st.dataframe(pd.DataFrame(leg_rows), use_container_width=True, hide_index=True)

    except Exception as exc:
        st.error(f"Structure error: {exc}")


# ============================================================
# TAB 3 — Vol Smile
# ============================================================
with tab3:
    st.subheader("Volatility Smile — SVI Calibration")

    smile_mode = st.radio(
        "Input mode",
        ["Manual grid", "Upload CSV"],
        horizontal=True,
        key="smile_mode",
    )

    F_smile = st.number_input(
        "Forward / ATM reference (F)",
        min_value=0.01, value=float(S), step=1.0, format="%.4f",
        key="F_smile",
    )
    T_smile_days = st.number_input(
        "Time to expiry (calendar days)",
        min_value=1, max_value=3650, value=T_days,
        key="T_smile_days",
    )
    T_smile = T_smile_days / 365.0

    svi_result = None
    plot_df = None

    if smile_mode == "Manual grid":
        st.markdown("Enter strikes and implied vols (in %):")
        default_grid = pd.DataFrame({
            "strike": [S * 0.85, S * 0.90, S * 0.95, S * 1.00, S * 1.05, S * 1.10, S * 1.15],
            "mid_vol": [30.0, 28.0, 25.0, 23.0, 24.0, 26.0, 29.0],
        })
        edited = st.data_editor(default_grid, num_rows="dynamic", key="smile_grid", width=400)

        if st.button("Fit SVI", key="fit_svi_manual"):
            try:
                df_in = edited.dropna().copy()
                df_in["mid_vol"] = df_in["mid_vol"] / 100.0  # pct → decimal
                df_in["bid_vol"] = df_in["mid_vol"] * 0.97
                df_in["ask_vol"] = df_in["mid_vol"] * 1.03
                params, aug_df = calibrate_from_quotes(df_in, F_smile, T_smile)
                svi_result = params
                plot_df = aug_df
            except Exception as exc:
                st.error(f"SVI calibration failed: {exc}")

    else:  # Upload CSV
        st.markdown(
            "Upload CSV with columns: `strike`, `bid_vol`, `ask_vol` "
            "(vols in %, e.g. 25.0 = 25%). Optional: `mid_vol`."
        )
        uploaded = st.file_uploader("Choose CSV file", type=["csv"], key="smile_csv")
        if uploaded is not None:
            try:
                df_up = pd.read_csv(uploaded)
                # Convert vol columns from pct to decimal
                for col in ["bid_vol", "ask_vol", "mid_vol"]:
                    if col in df_up.columns:
                        df_up[col] = df_up[col] / 100.0
                st.dataframe(df_up.head(10), use_container_width=True, hide_index=True)
                if st.button("Fit SVI", key="fit_svi_upload"):
                    params, aug_df = calibrate_from_quotes(df_up, F_smile, T_smile)
                    svi_result = params
                    plot_df = aug_df
            except Exception as exc:
                st.error(f"CSV error: {exc}")

    # --- Results ---
    if svi_result is not None and plot_df is not None:
        st.markdown("---")
        st.markdown("##### Fitted SVI Parameters")
        pc1, pc2, pc3, pc4, pc5 = st.columns(5)
        pc1.metric("a", f"{svi_result.a:.5f}")
        pc2.metric("b", f"{svi_result.b:.5f}")
        pc3.metric("ρ (skew)", f"{svi_result.rho:.4f}")
        pc4.metric("m", f"{svi_result.m:.5f}")
        pc5.metric("σ (curvature)", f"{svi_result.sigma:.5f}")

        # Smile chart
        k_dense = np.linspace(plot_df["log_k"].min() - 0.05,
                              plot_df["log_k"].max() + 0.05, 200)
        svi_dense = [svi_vol(k, svi_result, T_smile) * 100 for k in k_dense]
        strike_dense = [F_smile * math.exp(k) for k in k_dense]

        fig_smile = go.Figure()
        # Bid-ask band
        if "bid_vol" in plot_df.columns and "ask_vol" in plot_df.columns:
            fig_smile.add_trace(go.Scatter(
                x=plot_df["strike"], y=plot_df["ask_vol"] * 100,
                mode="lines", line=dict(width=0), showlegend=False,
            ))
            fig_smile.add_trace(go.Scatter(
                x=plot_df["strike"], y=plot_df["bid_vol"] * 100,
                fill="tonexty", fillcolor="rgba(31,119,180,0.15)",
                mode="lines", line=dict(width=0), name="Bid-Ask band",
            ))
        # Market mid dots
        fig_smile.add_trace(go.Scatter(
            x=plot_df["strike"], y=plot_df["mid_vol"] * 100,
            mode="markers", marker=dict(color="#1f77b4", size=8), name="Market mid",
        ))
        # SVI curve
        fig_smile.add_trace(go.Scatter(
            x=strike_dense, y=svi_dense,
            mode="lines", line=dict(color="#d62728", width=2), name="SVI fit",
        ))
        fig_smile.add_vline(x=F_smile, line_dash="dot", line_color="gray",
                            annotation_text="ATM", annotation_position="top right")
        fig_smile.update_layout(
            height=400,
            margin=dict(l=10, r=10, t=30, b=10),
            xaxis_title="Strike", yaxis_title="Implied Vol (%)",
            plot_bgcolor="white", paper_bgcolor="white",
            legend=dict(orientation="h", yanchor="bottom", y=1.01),
        )
        st.plotly_chart(fig_smile, use_container_width=True, key="smile_chart_tab3")

        # Mispricing table
        st.markdown("##### Mispricing vs SVI Model")
        st.caption(
            "Mispricing > +1: model prices above ask (market **cheap** to buy). "
            "Mispricing < −1: model prices below bid (market **expensive** to sell)."
        )
        disp_cols = ["strike", "bid_vol", "ask_vol", "mid_vol", "svi_vol",
                     "model_price_call", "mid_price_call", "mispricing"]
        disp_cols = [c for c in disp_cols if c in plot_df.columns]
        disp_df = plot_df[disp_cols].copy()
        for vc in ["bid_vol", "ask_vol", "mid_vol", "svi_vol"]:
            if vc in disp_df.columns:
                disp_df[vc] = (disp_df[vc] * 100).round(2).astype(str) + "%"

        def _style_mispricing(row):
            try:
                v = float(row["mispricing"])
            except Exception:
                return [""] * len(row)
            color = ""
            if v > 1.0:
                color = "background-color: #c8f7c5"  # green — market cheap
            elif v < -1.0:
                color = "background-color: #f7c5c5"  # red — market expensive
            return [color] * len(row)

        styled = disp_df.style.apply(_style_mispricing, axis=1)
        st.dataframe(styled, use_container_width=True, hide_index=True)

        # Download augmented CSV
        csv_bytes = plot_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download augmented CSV",
            data=csv_bytes,
            file_name="svi_calibration.csv",
            mime="text/csv",
        )


# ============================================================
# TAB 4 — Historical Vol
# ============================================================
with tab4:
    st.subheader("Historical Volatility Analysis")

    st.markdown(
        "Upload a CSV with columns `date` and `price` (daily closing prices). "
        "Rolling HV windows: 10 / 21 / 63 / 126 days."
    )
    hv_file = st.file_uploader("Choose CSV file", type=["csv"], key="hv_csv")

    if hv_file is not None:
        try:
            df_hv = pd.read_csv(hv_file, parse_dates=["date"])
            df_hv = df_hv.sort_values("date").reset_index(drop=True)

            if "price" not in df_hv.columns:
                st.error("CSV must contain a 'price' column.")
            elif len(df_hv) < 5:
                st.error("Need at least 5 rows to compute rolling HV.")
            else:
                # Log returns
                df_hv = df_hv[df_hv["price"] > 0].copy()
                df_hv["log_ret"] = np.log(df_hv["price"] / df_hv["price"].shift(1))

                windows = [10, 21, 63, 126]
                window_labels = {10: "HV-10d", 21: "HV-21d", 63: "HV-63d", 126: "HV-126d"}

                for w in windows:
                    col_name = window_labels[w]
                    df_hv[col_name] = (
                        df_hv["log_ret"]
                        .rolling(window=w, min_periods=w)
                        .std()
                        * math.sqrt(252)
                        * 100  # as percentage
                    )

                # Rolling vol chart
                fig_hv = go.Figure()
                colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"]
                for w, col_name, color in zip(windows, [window_labels[w] for w in windows], colors):
                    mask = df_hv[col_name].notna()
                    fig_hv.add_trace(go.Scatter(
                        x=df_hv.loc[mask, "date"],
                        y=df_hv.loc[mask, col_name],
                        mode="lines",
                        name=col_name,
                        line=dict(color=color, width=1.5),
                    ))

                # Current IV horizontal line
                iv_pct = st.session_state.implied_vol_tab1 * 100
                fig_hv.add_hline(
                    y=iv_pct,
                    line_dash="dash", line_color="purple",
                    annotation_text=f"Current IV ({iv_pct:.1f}%)",
                    annotation_position="right",
                )

                fig_hv.update_layout(
                    height=400,
                    margin=dict(l=10, r=10, t=30, b=10),
                    xaxis_title="Date",
                    yaxis_title="Historical Vol (%)",
                    plot_bgcolor="white", paper_bgcolor="white",
                    legend=dict(orientation="h", yanchor="bottom", y=1.01),
                    hovermode="x unified",
                )
                st.plotly_chart(fig_hv, use_container_width=True, key="hv_chart_tab4")

                # Summary table
                st.markdown("##### HV Summary")
                summary_rows = []
                for w in windows:
                    col_name = window_labels[w]
                    series = df_hv[col_name].dropna()
                    if len(series) == 0:
                        continue
                    current = series.iloc[-1]
                    summary_rows.append({
                        "Window": col_name,
                        "Current (%)": f"{current:.2f}",
                        "Min (%)": f"{series.min():.2f}",
                        "Mean (%)": f"{series.mean():.2f}",
                        "Max (%)": f"{series.max():.2f}",
                    })
                st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)

                # HV vs IV comparison
                st.markdown("---")
                st.markdown("##### HV vs Current IV")
                hv21_series = df_hv[window_labels[21]].dropna()
                if len(hv21_series) > 0:
                    hv21_current = hv21_series.iloc[-1]
                    diff = iv_pct - hv21_current
                    badge = "IV > HV" if diff > 0 else "IV < HV"
                    badge_color = "orange" if diff > 0 else "blue"
                    c1, c2, c3 = st.columns(3)
                    c1.metric("21d HV (%)", f"{hv21_current:.2f}")
                    c2.metric("Current IV (%)", f"{iv_pct:.2f}")
                    c3.metric(
                        "IV - HV (vol premium)",
                        f"{diff:+.2f}%",
                        delta=badge,
                        delta_color="inverse" if diff < 0 else "off",
                    )
                    if abs(diff) < 1.0:
                        st.info("IV and 21d HV are near equal — options fairly priced vs recent realised vol.")
                    elif diff > 5.0:
                        st.warning(f"IV is {diff:.1f}% above 21d HV — options appear rich vs recent realised vol.")
                    elif diff < -5.0:
                        st.success(f"IV is {abs(diff):.1f}% below 21d HV — options appear cheap vs recent realised vol.")

        except Exception as exc:
            st.error(f"Error reading file: {exc}")
    else:
        st.info("Upload a daily price CSV to compute rolling historical volatility.")
