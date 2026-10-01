"""Tab 4 — Positions & MtM: hourly volumes, procurement coverage, open exposure, MtM."""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text


def render_positions(engine):
    """Render Positions & MtM tab."""
    st.subheader("Positions & MtM")

    with engine.connect() as conn:
        books = pd.read_sql(text(
            "SELECT id, name FROM marketdata.rm_books WHERE book_type = 'load' ORDER BY name"
        ), conn)

    if books.empty:
        st.warning("No retail load books found. Run the retail backfill or upload data first.")
        return

    book_id = st.selectbox(
        "Book",
        books["id"].tolist(),
        format_func=lambda x: books[books["id"] == x]["name"].iloc[0],
        key="retail_pos_book",
    )

    with engine.connect() as conn:
        snap = pd.read_sql(text("""
            SELECT SUM(realized_cny) AS ytd,
                   (SELECT unrealized_mtm_cny FROM marketdata.rm_pnl_snapshots s2
                    WHERE s2.book_id = :b ORDER BY snapshot_date DESC LIMIT 1) AS mtm
            FROM marketdata.rm_pnl_snapshots WHERE book_id = :b
        """), conn, params={"b": book_id}).iloc[0]
    c1, c2 = st.columns(2)
    c1.metric("Realised P&L YTD", f"¥{snap['ytd']:,.0f}" if pd.notna(snap["ytd"]) else "N/A")
    c2.metric("Latest Unrealised MtM", f"¥{snap['mtm']:,.0f}" if pd.notna(snap["mtm"]) else "N/A")

    subtab1, subtab2, subtab3, subtab4 = st.tabs([
        "Hourly Volumes", "Procurement Coverage", "Open Exposure", "MtM"
    ])

    with subtab1:
        _render_hourly_volumes(book_id, engine)

    with subtab2:
        _render_procurement_coverage(book_id, engine)

    with subtab3:
        _render_open_exposure(book_id, engine)

    with subtab4:
        _render_mtm(book_id, engine)


def _render_hourly_volumes(book_id: int, engine):
    """Show hourly position volumes for book (all channel columns, not just DA/RT)."""
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT delivery_date, hour,
                   annual_volume_mwh, annual_price_cny_mwh,
                   monthly_auction_volume_mwh, monthly_auction_price_cny_mwh,
                   monthly_listed_volume_mwh, monthly_listed_price_cny_mwh,
                   intramonth_match_volume_mwh, intramonth_match_price_cny_mwh,
                   da_volume_mwh, rt_volume_mwh,
                   nominated_mwh, settled_mwh
            FROM marketdata.rm_position_volumes
            WHERE book_id = :bid
            ORDER BY delivery_date DESC, hour
            LIMIT 500
        """), conn, params={"bid": book_id})

    if df.empty:
        st.info("No hourly position data yet for this book.")
        return

    # collapse the all-NULL DA/RT columns for midlong books
    df = df.dropna(axis=1, how="all")
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.download_button("Export CSV", df.to_csv(index=False), "position_volumes.csv", "text/csv")


def _render_procurement_coverage(book_id: int, engine):
    """Procurement coverage ratio: forward-bought / contracted load."""
    with engine.connect() as conn:
        # Contracted load from customer profiles
        contracted = pd.read_sql(text("""
            SELECT cp.profile_date,
                   SUM(cp.nominated_mwh) AS contracted_mwh,
                   SUM(cp.load_mwh) AS actual_load_mwh
            FROM marketdata.rm_customer_profiles cp
            JOIN marketdata.rm_customer_contracts cc ON cc.customer_id = cp.customer_id
            WHERE cc.bound_asset_id IN (
                SELECT asset_id FROM marketdata.rm_books WHERE id = :bid
            )
            AND cc.contract_status = 'active'
            GROUP BY cp.profile_date
            ORDER BY cp.profile_date DESC
            LIMIT 90
        """), conn, params={"bid": book_id})

        # Forward-bought positions (buy side)
        forward_bought = pd.read_sql(text("""
            SELECT DATE(start_date) AS pos_date,
                   SUM(volume_mwh) AS forward_mwh
            FROM marketdata.rm_positions
            WHERE book_id = :bid AND direction = 'buy' AND status = 'open'
            GROUP BY DATE(start_date)
            ORDER BY pos_date DESC
            LIMIT 90
        """), conn, params={"bid": book_id})

    if contracted.empty:
        st.info("No customer profile data linked to this book.")
        return

    # Merge
    if not forward_bought.empty:
        merged = contracted.merge(
            forward_bought, left_on="profile_date", right_on="pos_date", how="left"
        )
        merged["coverage_ratio"] = (
            merged["forward_mwh"].fillna(0) / merged["contracted_mwh"].replace(0, float("nan"))
        )
    else:
        merged = contracted.copy()
        merged["forward_mwh"] = 0.0
        merged["coverage_ratio"] = 0.0

    # Metrics
    avg_coverage = merged["coverage_ratio"].mean()
    col1, col2, col3 = st.columns(3)
    col1.metric("Avg Coverage Ratio", f"{avg_coverage:.1%}" if pd.notna(avg_coverage) else "N/A")
    col2.metric("Total Contracted MWh", f"{merged['contracted_mwh'].sum():,.1f}")
    col3.metric("Total Forward Bought MWh", f"{merged['forward_mwh'].sum():,.1f}")

    # Chart
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=merged["profile_date"], y=merged["contracted_mwh"],
        mode="lines", name="Contracted Load", line=dict(color="#3498db")
    ))
    fig.add_trace(go.Scatter(
        x=merged["profile_date"], y=merged["forward_mwh"],
        mode="lines", name="Forward Bought", line=dict(color="#2ecc71")
    ))
    fig.update_layout(
        title="Procurement Coverage (Contracted vs Forward Bought)",
        xaxis_title="Date", yaxis_title="MWh", height=350
    )
    st.plotly_chart(fig, use_container_width=True)

    # Coverage ratio chart
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=merged["profile_date"], y=merged["coverage_ratio"],
        mode="lines+markers", name="Coverage Ratio", line=dict(color="#e67e22")
    ))
    fig2.add_hline(y=1.0, line_dash="dash", line_color="red", annotation_text="100% coverage")
    fig2.update_layout(title="Coverage Ratio", yaxis_title="Ratio", height=250)
    st.plotly_chart(fig2, use_container_width=True)


def _render_open_exposure(book_id: int, engine):
    """Show unhedged (open) exposure."""
    with engine.connect() as conn:
        open_pos = pd.read_sql(text("""
            SELECT id, channel, instrument_type, direction, volume_mwh,
                   price_cny_mwh, start_date, end_date, counterparty, province
            FROM marketdata.rm_positions
            WHERE book_id = :bid AND status = 'open'
            ORDER BY start_date DESC
        """), conn, params={"bid": book_id})

    if open_pos.empty:
        st.info("No open positions.")
        return

    buy_mwh = open_pos[open_pos["direction"] == "buy"]["volume_mwh"].sum()
    sell_mwh = open_pos[open_pos["direction"] == "sell"]["volume_mwh"].sum()
    net_mwh = buy_mwh - sell_mwh

    col1, col2, col3 = st.columns(3)
    col1.metric("Buy Volume", f"{buy_mwh:,.1f} MWh")
    col2.metric("Sell Volume", f"{sell_mwh:,.1f} MWh")
    col3.metric("Net Exposure", f"{net_mwh:,.1f} MWh",
                delta_color="inverse" if net_mwh < 0 else "normal")

    st.dataframe(open_pos, use_container_width=True, hide_index=True)


def _render_mtm(book_id: int, engine):
    """Per-book MtM: procurement (open positions x scenario curve) + retail contracts."""
    from services.retail_risk import mtm as mm

    scenario = st.selectbox("Scenario", ["spot_base", "spot_p10", "spot_m10"],
                            format_func={"spot_base": "Base", "spot_p10": "+10",
                                         "spot_m10": "−10"}.get,
                            key="mtm_scenario")
    with engine.connect() as conn:
        result = mm.book_mtm(conn, book_id, scenario=scenario)

    col1, col2, col3 = st.columns(3)
    col1.metric("Procurement MtM", f"¥{result.procurement_mtm_cny:,.0f}")
    col2.metric("Retail-contract MtM", f"¥{result.retail_mtm_cny:,.0f}")
    col3.metric("Total Unrealised", f"¥{result.total_mtm_cny:,.0f}")

    if not result.positions_df.empty:
        st.caption("Open positions (procurement)")
        cols = [c for c in ["channel", "direction", "volume_mwh", "price_cny_mwh",
                            "forward_price_cny_mwh", "unrealized_pnl_cny"]
                if c in result.positions_df.columns]
        st.dataframe(result.positions_df[cols], use_container_width=True, hide_index=True)
    if not result.contracts_df.empty:
        st.caption("Active retail contracts (remaining volume)")
        cols = [c for c in ["contract_type", "price_cny_mwh", "remaining_mwh",
                            "forward_price", "mtm_cny"] if c in result.contracts_df.columns]
        st.dataframe(result.contracts_df[cols], use_container_width=True, hide_index=True)
    if result.positions_df.empty and result.contracts_df.empty:
        st.info("No open positions or active contracts for MtM.")
