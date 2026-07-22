"""Tab 1 — Asset Configuration: CRUD for rm_assets and rm_books."""
from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import text


def render_asset_config(engine):
    """Render asset configuration tab."""
    st.subheader("Asset Registry")

    with engine.connect() as conn:
        assets_df = pd.read_sql(text("""
            SELECT a.id, a.name, a.asset_type, a.province, a.capacity_mw,
                   a.bess_duration_h, a.bess_dod_pct, a.status, a.commission_date,
                   b.id as book_id, b.name as book_name
            FROM marketdata.rm_assets a
            LEFT JOIN marketdata.rm_books b ON b.asset_id = a.id
            ORDER BY a.name
        """), conn)

    if not assets_df.empty:
        st.dataframe(assets_df, use_container_width=True, hide_index=True)
    else:
        st.info("No assets registered yet. Add one below.")

    st.subheader("Add Asset")
    with st.form("add_asset"):
        col1, col2, col3 = st.columns(3)
        with col1:
            name = st.text_input("Asset Name")
            asset_type = st.selectbox("Type", ["wind", "solar", "bess", "thermal"])
        with col2:
            province = st.text_input("Province", value="inner_mongolia_mengxi")
            capacity = st.number_input("Capacity (MW)", min_value=0.0, step=0.5)
        with col3:
            commission_date = st.date_input("Commission Date")
            bess_duration = st.number_input("BESS Duration (h)", min_value=0.0, step=0.5)
            bess_dod = st.number_input("BESS DoD (%)", min_value=0.0, max_value=100.0, step=1.0)

        notes = st.text_area("Notes", height=68)
        submitted = st.form_submit_button("Create Asset + Book")

        if submitted and name:
            with engine.begin() as conn:
                result = conn.execute(text("""
                    INSERT INTO marketdata.rm_assets
                        (name, asset_type, province, capacity_mw, bess_duration_h,
                         bess_dod_pct, commission_date, notes)
                    VALUES (:name, :type, :prov, :cap, :dur, :dod, :cd, :notes)
                    RETURNING id
                """), {
                    "name": name, "type": asset_type, "prov": province,
                    "cap": capacity, "dur": bess_duration if asset_type == "bess" else None,
                    "dod": bess_dod if asset_type == "bess" else None,
                    "cd": commission_date, "notes": notes,
                })
                asset_id = result.scalar()
                conn.execute(text("""
                    INSERT INTO marketdata.rm_books (name, book_type, asset_id)
                    VALUES (:name, 'asset', :aid)
                """), {"name": f"{name} Book", "aid": asset_id})
            st.success(f"Created asset '{name}' (ID: {asset_id}) with linked book.")
            st.rerun()
