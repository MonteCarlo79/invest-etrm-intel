"""Tab 2 — Settlement: retail settlement upload, processing, per-customer P&L contribution."""
from __future__ import annotations

import io

import pandas as pd
import streamlit as st
from sqlalchemy import text

# Retail settlement categories (superset of asset-risk)
_RETAIL_CATEGORIES = [
    "retail_revenue",
    "energy_procurement",
    "capacity_compensation",
    "transmission_distribution",
    "system_service_fee",
    "ancillary_service",
    "imbalance_penalty",
    "other",
]


def render_settlement(engine):
    """Render retail settlement tab: upload + analytics."""
    st.subheader("Retail Settlement Upload")

    with engine.connect() as conn:
        customers = pd.read_sql(text("""
            SELECT id, name, province FROM marketdata.rm_customers
            WHERE status = 'active' ORDER BY name
        """), conn)

    col1, col2 = st.columns(2)
    with col1:
        if customers.empty:
            st.warning("No active customers. Add customers in Tab 1 first.")
            customer_id = None
        else:
            customer_id = st.selectbox(
                "Customer",
                customers["id"].tolist(),
                format_func=lambda x: customers[customers["id"] == x]["name"].iloc[0],
                key="settlement_customer",
            )
    with col2:
        settlement_month = st.date_input("Settlement Month (1st of month)", key="settlement_month")

    uploaded = st.file_uploader(
        "Upload settlement file (Excel or PDF)", type=["xlsx", "xls", "csv", "pdf"],
        key="settlement_upload"
    )

    if uploaded and customer_id and st.button("Process File"):
        ext = uploaded.name.rsplit(".", 1)[-1].lower()
        if ext == "pdf":
            st.warning("PDF parsing not yet implemented — requires pdfplumber integration.")
        else:
            _process_excel(uploaded, customer_id, settlement_month, engine)

    st.divider()
    st.subheader("Settlement Analytics")

    if customers.empty:
        return

    _render_analytics(customer_id, engine)


def _process_excel(uploaded, customer_id: int, settlement_month, engine):
    """Parse retail settlement Excel and store items."""
    raw = uploaded.read()
    xl = pd.ExcelFile(io.BytesIO(raw))

    # Detect sheet — try common names
    sheet_names = xl.sheet_names
    target = next(
        (s for s in sheet_names if any(k in s for k in ("结算", "Settlement", "settlement", "Sheet1"))),
        sheet_names[0]
    )
    df = xl.parse(target)

    # Auto-detect category column
    cat_col = next((c for c in df.columns if "category" in str(c).lower() or "类别" in str(c)), None)
    amt_col = next((c for c in df.columns if "amount" in str(c).lower() or "金额" in str(c)), None)
    vol_col = next((c for c in df.columns if "volume" in str(c).lower() or "电量" in str(c)), None)

    if amt_col is None:
        st.error("Cannot find amount column. Ensure the file has an 'amount' or '金额' column.")
        return

    items = []
    for _, row in df.iterrows():
        amt = _safe_float(row.get(amt_col))
        if amt is None:
            continue
        items.append({
            "category": str(row.get(cat_col, "other")).strip() if cat_col else "other",
            "volume_mwh": _safe_float(row.get(vol_col)) if vol_col else None,
            "amount_cny": amt,
        })

    if not items:
        st.error("No valid rows parsed from file.")
        return

    with engine.begin() as conn:
        sid = conn.execute(text("""
            INSERT INTO marketdata.rm_retail_settlements (customer_id, settlement_month, file_name, file_type, status)
            VALUES (:cid, :month, :fname, 'excel', 'processed')
            RETURNING id
        """), {"cid": customer_id, "month": settlement_month, "fname": uploaded.name}).scalar()

        for item in items:
            # Map any unknown category to 'other'
            cat = item["category"] if item["category"] in (
                "retail_revenue", "energy_procurement", "capacity_compensation",
                "transmission_distribution", "system_service_fee", "ancillary_service",
                "imbalance_penalty",
            ) else "other"
            conn.execute(text("""
                INSERT INTO marketdata.rm_retail_settlement_items
                    (settlement_id, category, volume_mwh, amount_cny)
                VALUES (:sid, :cat, :vol, :amt)
            """), {"sid": sid, "cat": cat, "vol": item["volume_mwh"], "amt": item["amount_cny"]})

    st.success(f"Processed {len(items)} settlement items for settlement id={sid}.")


def _render_analytics(customer_id, engine):
    """Render settlement analytics grouped by retail category."""
    if customer_id is None:
        return

    with engine.connect() as conn:
        items_df = pd.read_sql(text("""
            SELECT si.category, si.volume_mwh, si.amount_cny,
                   s.settlement_month
            FROM marketdata.rm_retail_settlement_items si
            JOIN marketdata.rm_retail_settlements s ON s.id = si.settlement_id
            WHERE s.customer_id = :cid
            ORDER BY s.settlement_month DESC, si.category
        """), conn, params={"cid": customer_id})

    if items_df.empty:
        st.info("No settlement data yet for this customer. Upload a settlement file above.")
        return

    col1, col2, col3 = st.columns(3)
    revenue = items_df[items_df["category"] == "retail_revenue"]["amount_cny"].sum()
    procurement = items_df[items_df["category"] == "energy_procurement"]["amount_cny"].sum()
    net = items_df["amount_cny"].sum()
    col1.metric("Retail Revenue", f"¥{revenue:,.0f}")
    col2.metric("Energy Procurement", f"¥{procurement:,.0f}")
    col3.metric("Net Settlement", f"¥{net:,.0f}")

    st.subheader("By Category")
    cat_df = (
        items_df.groupby("category")
        .agg(total_amount=("amount_cny", "sum"), total_volume=("volume_mwh", "sum"))
        .sort_values("total_amount", ascending=False)
    )
    st.dataframe(cat_df, use_container_width=True)

    st.subheader("Monthly Trend")
    monthly = (
        items_df.groupby("settlement_month")
        .agg(total_amount=("amount_cny", "sum"))
        .reset_index()
    )
    st.line_chart(monthly.set_index("settlement_month")["total_amount"])

    # Per-customer P&L contribution across all customers
    st.subheader("All-Customer P&L Contribution")
    with engine.connect() as conn:
        all_customers_df = pd.read_sql(text("""
            SELECT c.name, c.province,
                   SUM(si.amount_cny) FILTER (WHERE si.category = 'retail_revenue') AS revenue,
                   SUM(si.amount_cny) FILTER (WHERE si.category = 'energy_procurement') AS procurement,
                   SUM(si.amount_cny) AS net_settlement
            FROM marketdata.rm_customers c
            JOIN marketdata.rm_retail_settlements s ON s.customer_id = c.id
            JOIN marketdata.rm_retail_settlement_items si ON si.settlement_id = s.id
            GROUP BY c.id, c.name, c.province
            ORDER BY net_settlement DESC
        """), conn)

    if not all_customers_df.empty:
        st.dataframe(all_customers_df, use_container_width=True, hide_index=True)


def _safe_float(val) -> float | None:
    try:
        return float(val) if val is not None and str(val).strip() not in ("", "nan") else None
    except (ValueError, TypeError):
        return None
