"""Tab 1 — Asset Configuration: editable registry table + Add Asset."""
from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import text


def _norm_folder(val) -> str | None:
    """Normalize an invoice-folder input: blank / NaN / literal 'nan'/'none' → None."""
    if val is None or pd.isna(val):
        return None
    s = str(val).strip()
    return None if not s or s.lower() in ("nan", "none") else s


# Columns users may edit inline in the registry table (id/name/book_* are read-only).
_EDITABLE_COLS = {"asset_type", "province", "capacity_mw", "bess_duration_h",
                  "bess_dod_pct", "status", "commission_date", "invoice_folder"}


def _save_table_edits(engine, orig_df: pd.DataFrame):
    """Write st.data_editor edits back to rm_assets (whitelisted columns only)."""
    state = st.session_state.get("asset_editor", {})
    edited_rows = state.get("edited_rows", {})
    if not edited_rows:
        st.info("No changes to save.")
        return

    n = 0
    for idx_str, cols in edited_rows.items():
        row_id = int(orig_df.iloc[int(idx_str)]["id"])
        clean = {}
        for col, val in cols.items():
            if col not in _EDITABLE_COLS:
                continue
            if col == "invoice_folder":
                clean[col] = _norm_folder(val)
            elif col == "commission_date":
                clean[col] = pd.to_datetime(val).date() if val is not None and not pd.isna(val) else None
            elif val is None or (isinstance(val, float) and pd.isna(val)):
                clean[col] = None
            else:
                clean[col] = val
        if not clean:
            continue
        sets = ", ".join(f"{c} = :{c}" for c in clean)
        with engine.begin() as conn:
            conn.execute(text(f"UPDATE marketdata.rm_assets SET {sets} WHERE id = :id"),
                         {**clean, "id": row_id})
        n += 1
    st.success(f"Saved changes to {n} asset(s).")


def render_asset_config(engine):
    """Render asset configuration tab."""
    st.subheader("Asset Registry")

    with engine.connect() as conn:
        assets_df = pd.read_sql(text("""
            SELECT a.id, a.name, a.asset_type, a.province, a.capacity_mw,
                   a.bess_duration_h, a.bess_dod_pct, a.status, a.commission_date,
                   a.invoice_folder, b.id as book_id, b.name as book_name
            FROM marketdata.rm_assets a
            LEFT JOIN marketdata.rm_books b ON b.asset_id = a.id
            ORDER BY a.name
        """), conn)

    if assets_df.empty:
        st.info("No assets registered yet. Add one below.")
    else:
        st.caption("Edit cells directly in the table, then click **Save Table Changes**. "
                   "Type and status are dropdowns; id/name/book are read-only.")
        st.data_editor(
            assets_df,
            column_config={
                "id": st.column_config.NumberColumn(disabled=True, width="small"),
                "name": st.column_config.TextColumn(disabled=True),
                "asset_type": st.column_config.SelectboxColumn(
                    options=["wind", "solar", "bess", "thermal"], width="small"),
                "status": st.column_config.SelectboxColumn(
                    options=["active", "retired"], width="small"),
                "capacity_mw": st.column_config.NumberColumn(width="small"),
                "bess_duration_h": st.column_config.NumberColumn(width="small"),
                "bess_dod_pct": st.column_config.NumberColumn(width="small"),
                "commission_date": st.column_config.DateColumn(width="medium"),
                "province": st.column_config.TextColumn(),
                "invoice_folder": st.column_config.TextColumn(),
                "book_id": st.column_config.NumberColumn(disabled=True, width="small"),
                "book_name": st.column_config.TextColumn(disabled=True),
            },
            use_container_width=True,
            hide_index=True,
            key="asset_editor",
        )
        if st.button("Save Table Changes", type="primary"):
            _save_table_edits(engine, assets_df)
            st.rerun()

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

        invoice_folder = st.text_input(
            "Invoice Folder (source file directory)",
            placeholder="e.g. B-8 内蒙杭锦旗",
            help="Folder under the settlement invoices root that this asset's bills live in. "
                 "Used by Settlement tab → Scan & Ingest. Can also be set later via the table.",
        )
        notes = st.text_area("Notes", height=68)
        submitted = st.form_submit_button("Create Asset + Book")

        if submitted and name:
            with engine.begin() as conn:
                result = conn.execute(text("""
                    INSERT INTO marketdata.rm_assets
                        (name, asset_type, province, capacity_mw, bess_duration_h,
                         bess_dod_pct, commission_date, notes, invoice_folder)
                    VALUES (:name, :type, :prov, :cap, :dur, :dod, :cd, :notes, :folder)
                    RETURNING id
                """), {
                    "name": name, "type": asset_type, "prov": province,
                    "cap": capacity, "dur": bess_duration if asset_type == "bess" else None,
                    "dod": bess_dod if asset_type == "bess" else None,
                    "cd": commission_date, "notes": notes,
                    "folder": _norm_folder(invoice_folder),
                })
                asset_id = result.scalar()
                conn.execute(text("""
                    INSERT INTO marketdata.rm_books (name, book_type, asset_id)
                    VALUES (:name, 'asset', :aid)
                """), {"name": f"{name} Book", "aid": asset_id})
            st.success(f"Created asset '{name}' (ID: {asset_id}) with linked book.")
            st.rerun()
