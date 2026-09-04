"""Tab 5 — VaR & Greeks: risk metrics dashboard."""
from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text
from libs.risk.var import historical_var, parametric_var
from libs.risk.greeks import compute_book_greeks


def render_var(engine):
    """Render VaR & Greeks tab."""
    with engine.connect() as conn:
        books = pd.read_sql(text("SELECT id, name FROM marketdata.rm_books ORDER BY name"), conn)

    if books.empty:
        st.warning("No books found.")
        return

    book_id = st.selectbox("Book", books["id"].tolist(),
                           format_func=lambda x: books[books["id"] == x]["name"].iloc[0], key="var_book")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Greeks")
        with engine.connect() as conn:
            positions = pd.read_sql(text("""
                SELECT direction, volume_mwh, status FROM marketdata.rm_positions WHERE book_id = :bid
            """), conn, params={"bid": book_id})
        if positions.empty:
            st.info("No positions.")
        else:
            greeks = compute_book_greeks(positions.to_dict("records"))
            st.metric("Delta (net MWh)", f"{greeks['delta_mwh']:,.1f}")
            st.metric("Gamma", f"{greeks['gamma']:.4f}")
            st.metric("Vega", f"{greeks['vega']:.4f}")

    with col2:
        st.subheader("Value at Risk")
        with engine.connect() as conn:
            open_pos = pd.read_sql(text("""
                SELECT direction, volume_mwh, status FROM marketdata.rm_positions
                WHERE book_id = :bid AND status = 'open'
            """), conn, params={"bid": book_id})
            prices = pd.read_sql(text("""
                SELECT delivery_date, AVG(market_price_cny_mwh) as price
                FROM marketdata.rm_position_volumes
                WHERE book_id = :bid AND market_price_cny_mwh IS NOT NULL
                GROUP BY delivery_date ORDER BY delivery_date
            """), conn, params={"bid": book_id})

        if open_pos.empty:
            st.info("No open positions for VaR.")
        elif len(prices) < 20:
            st.warning(f"Need 20+ price observations for VaR (have {len(prices)}).")
        else:
            greeks = compute_book_greeks(open_pos.to_dict("records"))
            delta = greeks["delta_mwh"]
            returns = prices["price"].diff().dropna()
            sigma = float(returns.tail(20).std())

            hist_95 = historical_var(returns, delta, 0.95)
            hist_99 = historical_var(returns, delta, 0.99)
            param_95 = parametric_var(delta, sigma, 0.95, 1)
            param_99 = parametric_var(delta, sigma, 0.99, 1)
            param_10d = parametric_var(delta, sigma, 0.95, 10)

            var_df = pd.DataFrame({
                "Metric": ["1D 95% VaR", "1D 99% VaR", "10D 95% VaR"],
                "Historical": [f"¥{hist_95:,.0f}", f"¥{hist_99:,.0f}", f"¥{hist_95*np.sqrt(10):,.0f}"],
                "Parametric": [f"¥{param_95:,.0f}", f"¥{param_99:,.0f}", f"¥{param_10d:,.0f}"],
            })
            st.dataframe(var_df, use_container_width=True, hide_index=True)

    # Stress scenarios
    st.divider()
    st.subheader("Stress Scenarios")
    c1, c2 = st.columns(2)
    with c1:
        spot_shock = st.slider("Spot price shock (%)", -50, 50, 0, key="spot_shock")
    with c2:
        bilateral_shock = st.slider("Bilateral benchmark shock (%)", -50, 50, 0, key="bi_shock")

    if spot_shock != 0 or bilateral_shock != 0:
        with engine.connect() as conn:
            stress_pos = pd.read_sql(text("""
                SELECT direction, volume_mwh, price_cny_mwh, channel
                FROM marketdata.rm_positions WHERE book_id = :bid AND status = 'open'
            """), conn, params={"bid": book_id})
        if not stress_pos.empty:
            scenario_pnl = 0.0
            for _, pos in stress_pos.iterrows():
                vol = float(pos["volume_mwh"])
                price = float(pos["price_cny_mwh"] or 0)
                shock = spot_shock / 100.0 if pos["channel"] in ("DA", "RT") else bilateral_shock / 100.0
                price_change = price * shock
                if pos["direction"] == "buy":
                    scenario_pnl += vol * price_change
                else:
                    scenario_pnl -= vol * price_change
            st.metric("Scenario P&L Impact", f"¥{scenario_pnl:,.0f}",
                      delta_color="inverse" if scenario_pnl < 0 else "normal")
