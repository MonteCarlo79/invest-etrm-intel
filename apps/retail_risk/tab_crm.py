"""Tab 1 — CRM: Customer registry, add customer form, CRM import upload."""
from __future__ import annotations

import io
import json

import pandas as pd
import streamlit as st
from sqlalchemy import text


def render_crm(engine):
    """Render CRM tab: customer table, add form, CRM import."""
    st.subheader("Customer Registry")

    # Portfolio summary metrics
    with engine.connect() as conn:
        summary = conn.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE status = 'active') AS active_customers,
                COUNT(*) FILTER (WHERE status = 'prospect') AS prospects,
                COUNT(*) FILTER (WHERE status = 'churned') AS churned
            FROM marketdata.rm_customers
        """)).fetchone()
        contracted_mwh = conn.execute(text("""
            SELECT COALESCE(SUM(annual_forecast_mwh), 0) AS total_mwh
            FROM marketdata.rm_customer_contracts
            WHERE contract_status = 'active'
        """)).scalar()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Active Customers", int(summary[0]) if summary else 0)
    c2.metric("Prospects", int(summary[1]) if summary else 0)
    c3.metric("Churned", int(summary[2]) if summary else 0)
    c4.metric("Contracted MWh/yr", f"{float(contracted_mwh or 0):,.0f}")

    st.divider()

    # Filters
    col1, col2 = st.columns(2)
    with col1:
        with engine.connect() as conn:
            provinces = pd.read_sql(
                text("SELECT DISTINCT province FROM marketdata.rm_customers ORDER BY province"), conn
            )["province"].tolist()
        province_filter = st.multiselect("Filter by Province", provinces, key="crm_province")
    with col2:
        status_filter = st.multiselect(
            "Filter by Status", ["active", "prospect", "churned"],
            default=["active", "prospect"], key="crm_status"
        )

    # Build query
    where_clauses = []
    params: dict = {}
    if province_filter:
        where_clauses.append("province = ANY(:provinces)")
        params["provinces"] = province_filter
    if status_filter:
        where_clauses.append("status = ANY(:statuses)")
        params["statuses"] = status_filter

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    with engine.connect() as conn:
        customers = pd.read_sql(text(f"""
            SELECT id, name, province, district, customer_type, voltage_level,
                   contracted_capacity_kva, bd_name, channel_name,
                   fixed_spread_cny_mwh, revenue_share_ratio, status, created_at
            FROM marketdata.rm_customers
            {where_sql}
            ORDER BY province, name
        """), conn, params=params)

    if customers.empty:
        st.info("No customers found matching filters.")
    else:
        st.dataframe(customers, use_container_width=True, hide_index=True)
        st.download_button(
            "Export CSV", customers.to_csv(index=False), "customers.csv", "text/csv"
        )

    # Add customer form
    st.divider()
    with st.expander("Add New Customer"):
        with st.form("add_customer_form"):
            col1, col2 = st.columns(2)
            with col1:
                name = st.text_input("Customer Name *")
                province = st.text_input("Province *")
                district = st.text_input("District")
                customer_type = st.selectbox(
                    "Customer Type", ["industrial", "commercial", "residential"]
                )
                voltage_level = st.text_input("Voltage Level (e.g. 10kV, 35kV)")
            with col2:
                contracted_capacity_kva = st.number_input(
                    "Contracted Capacity (kVA)", min_value=0.0, step=100.0
                )
                bd_name = st.text_input("BD Name")
                customer_source = st.text_input("Customer Source")
                channel_name = st.text_input("Channel Name")
                fixed_spread = st.number_input("Fixed Spread (CNY/MWh)", step=0.1)
                revenue_share = st.number_input(
                    "Revenue Share Ratio", min_value=0.0, max_value=1.0, step=0.01
                )
            notes = st.text_area("Notes")
            submitted = st.form_submit_button("Add Customer")

        if submitted:
            if not name or not province:
                st.error("Customer Name and Province are required.")
            else:
                with engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO marketdata.rm_customers
                            (name, province, district, customer_type, voltage_level,
                             contracted_capacity_kva, bd_name, customer_source, channel_name,
                             fixed_spread_cny_mwh, revenue_share_ratio, notes)
                        VALUES
                            (:name, :province, :district, :ctype, :vlevel,
                             :capacity, :bd, :source, :channel,
                             :spread, :share, :notes)
                    """), {
                        "name": name, "province": province, "district": district or None,
                        "ctype": customer_type, "vlevel": voltage_level or None,
                        "capacity": contracted_capacity_kva or None,
                        "bd": bd_name or None, "source": customer_source or None,
                        "channel": channel_name or None,
                        "spread": fixed_spread or None, "share": revenue_share or None,
                        "notes": notes or None,
                    })
                st.success(f"Customer '{name}' added.")
                st.rerun()

    # CRM Import
    st.divider()
    st.subheader("CRM Import (各省份台账.xlsx)")

    with engine.connect() as conn:
        configs = pd.read_sql(
            text("SELECT province, column_map, notes FROM marketdata.rm_crm_import_configs ORDER BY province"),
            conn
        )

    if configs.empty:
        st.info("No CRM import configs found. Add a province config below.")
    else:
        st.dataframe(configs, use_container_width=True, hide_index=True)

    uploaded = st.file_uploader(
        "Upload 台账.xlsx", type=["xlsx", "xls"], key="crm_upload"
    )
    if uploaded:
        province_key = st.text_input("Province key for this file (must match config)", key="crm_prov_key")
        if st.button("Import CRM File"):
            if not province_key:
                st.error("Province key required.")
            else:
                _import_crm_file(uploaded, province_key, engine)

    # Add/update CRM config
    with st.expander("Add / Update CRM Import Config"):
        with st.form("crm_config_form"):
            cfg_province = st.text_input("Province")
            cfg_column_map = st.text_area(
                "Column Map (JSON)",
                value='{"customer_name": "客户名称", "contracted_capacity_kva": "签约容量"}',
                height=120,
            )
            cfg_notes = st.text_input("Notes")
            cfg_submitted = st.form_submit_button("Save Config")

        if cfg_submitted:
            try:
                col_map = json.loads(cfg_column_map)
            except json.JSONDecodeError as e:
                st.error(f"Invalid JSON: {e}")
            else:
                with engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO marketdata.rm_crm_import_configs (province, column_map, notes)
                        VALUES (:prov, :colmap, :notes)
                        ON CONFLICT (province) DO UPDATE
                            SET column_map = EXCLUDED.column_map,
                                notes = EXCLUDED.notes,
                                updated_at = NOW()
                    """), {"prov": cfg_province, "colmap": json.dumps(col_map), "notes": cfg_notes or None})
                st.success(f"Config saved for province: {cfg_province}")
                st.rerun()


def _import_crm_file(uploaded, province_key: str, engine):
    """Parse and import CRM xlsx file using province column map config."""
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT column_map FROM marketdata.rm_crm_import_configs WHERE province = :prov
        """), {"prov": province_key}).fetchone()

    if not row:
        st.error(f"No CRM import config found for province '{province_key}'. Please add one first.")
        return

    col_map: dict = row[0]
    xl = pd.ExcelFile(io.BytesIO(uploaded.read()))
    df = xl.parse(xl.sheet_names[0])

    # Rename columns using config map (reverse: config key → actual col name in file)
    rename_map = {v: k for k, v in col_map.items()}
    df = df.rename(columns=rename_map)

    inserted = 0
    errors = []
    for _, row_data in df.iterrows():
        name = str(row_data.get("customer_name", "")).strip()
        if not name or name == "nan":
            continue
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO marketdata.rm_customers
                        (name, province, contracted_capacity_kva, voltage_level,
                         bd_name, channel_name, customer_type)
                    VALUES (:name, :prov, :cap, :vlevel, :bd, :channel, :ctype)
                    ON CONFLICT DO NOTHING
                """), {
                    "name": name,
                    "prov": province_key,
                    "cap": _safe_float(row_data.get("contracted_capacity_kva")),
                    "vlevel": str(row_data.get("voltage_level", "")).strip() or None,
                    "bd": str(row_data.get("bd_name", "")).strip() or None,
                    "channel": str(row_data.get("channel_name", "")).strip() or None,
                    "ctype": str(row_data.get("customer_type", "")).strip() or None,
                })
            inserted += 1
        except Exception as e:
            errors.append(str(e))

    st.success(f"Imported {inserted} customers from '{uploaded.name}'.")
    if errors:
        st.warning(f"{len(errors)} rows had errors: {errors[:3]}")


def _safe_float(val) -> float | None:
    try:
        return float(val) if val is not None and str(val).strip() not in ("", "nan") else None
    except (ValueError, TypeError):
        return None
