"""Tab 4 — Positions & MtM: position volumes, contract register, forward curves, MtM."""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text
from libs.risk.mtm import compute_mtm


def render_positions(engine):
    """Render Positions & MtM tab."""
    with engine.connect() as conn:
        books = pd.read_sql(text("SELECT id, name FROM marketdata.rm_books ORDER BY name"), conn)

    if books.empty:
        st.warning("No books found.")
        return

    book_id = st.selectbox("Book", books["id"].tolist(),
                           format_func=lambda x: books[books["id"] == x]["name"].iloc[0], key="pos_book")

    subtab1, subtab2, subtab3, subtab4 = st.tabs(["Hourly Volumes", "Contract Register", "Forward Curves", "MtM"])

    with subtab1:
        with engine.connect() as conn:
            df = pd.read_sql(text("""
                SELECT delivery_date, hour, da_price_cny_mwh, rt_price_cny_mwh,
                       da_volume_mwh, rt_volume_mwh, market_price_cny_mwh, pnl_cny,
                       nominated_mwh, cleared_mwh, settled_mwh
                FROM marketdata.rm_position_volumes WHERE book_id = :bid
                ORDER BY delivery_date DESC, hour LIMIT 500
            """), conn, params={"bid": book_id})
        if df.empty:
            st.info("No position volume data yet.")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.download_button("Export CSV", df.to_csv(index=False), "position_volumes.csv", "text/csv")

    with subtab2:
        with engine.connect() as conn:
            positions = pd.read_sql(text("""
                SELECT id, channel, instrument_type, direction, volume_mwh,
                       price_cny_mwh, start_date, end_date, counterparty, status
                FROM marketdata.rm_positions WHERE book_id = :bid ORDER BY start_date DESC
            """), conn, params={"bid": book_id})
        if positions.empty:
            st.info("No positions recorded.")
        else:
            st.dataframe(positions, use_container_width=True, hide_index=True)

    with subtab3:
        with engine.connect() as conn:
            curves = pd.read_sql(text("""
                SELECT province, product, delivery_date, price_cny_kwh * 1000 as price_cny_mwh, source, curve_date
                FROM marketdata.rm_forward_curves ORDER BY delivery_date LIMIT 1000
            """), conn)
        if curves.empty:
            st.info("No forward curves loaded.")
        else:
            fig = go.Figure()
            for source in curves["source"].unique():
                subset = curves[curves["source"] == source]
                fig.add_trace(go.Scatter(x=subset["delivery_date"], y=subset["price_cny_mwh"],
                                         mode="lines", name=source))
            fig.update_layout(title="Forward Curve", xaxis_title="Delivery Date", yaxis_title="CNY/MWh", height=350)
            st.plotly_chart(fig, use_container_width=True)

        uploaded = st.file_uploader("Upload Curve CSV", type=["csv"], key="curve_upload")
        if uploaded and st.button("Upload Curve"):
            from services.forward_curve.manual_upload import validate_curve_csv, upload_manual_curve
            df_curve = pd.read_csv(uploaded)
            errors = validate_curve_csv(df_curve)
            if errors:
                st.error(f"Validation errors: {errors}")
            else:
                n = upload_manual_curve(df_curve)
                st.success(f"Uploaded {n} curve points.")
                st.rerun()

    with subtab4:
        with engine.connect() as conn:
            pos_df = pd.read_sql(text("""
                SELECT direction, volume_mwh, price_cny_mwh, province, start_date, end_date, channel
                FROM marketdata.rm_positions WHERE book_id = :bid AND status = 'open'
            """), conn, params={"bid": book_id})
            fwd = pd.read_sql(text("""
                SELECT DISTINCT ON (province) province, price_cny_kwh * 1000 as price
                FROM marketdata.rm_forward_curves ORDER BY province, curve_date DESC, delivery_date DESC
            """), conn)
        if pos_df.empty:
            st.info("No open positions for MtM.")
        else:
            forward_prices = dict(zip(fwd["province"], fwd["price"])) if not fwd.empty else {}
            mtm_results = compute_mtm(pos_df.to_dict("records"), forward_prices)
            mtm_df = pd.DataFrame(mtm_results)
            st.metric("Total Unrealised P&L", f"¥{mtm_df['unrealized_pnl_cny'].sum():,.0f}")
            st.dataframe(mtm_df[["channel", "direction", "volume_mwh", "price_cny_mwh",
                                  "forward_price_cny_mwh", "unrealized_pnl_cny"]],
                         use_container_width=True, hide_index=True)
