"""Tab 2 — Settlement: file upload, parsing, analytics."""
from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import text


def render_settlement(engine):
    """Render settlement tab with upload panel and analytics."""
    st.subheader("Settlement Upload")

    with engine.connect() as conn:
        books = pd.read_sql(text(
            "SELECT id, name FROM marketdata.rm_books ORDER BY name"
        ), conn)

    if books.empty:
        st.warning("No books found. Create an asset in Tab 1 first.")
        return

    book_id = st.selectbox("Book", books["id"].tolist(),
                           format_func=lambda x: books[books["id"] == x]["name"].iloc[0])
    settlement_month = st.date_input("Settlement Month (1st of month)")

    uploaded = st.file_uploader("Upload settlement file", type=["xlsx", "xls", "csv", "pdf"])

    if uploaded and st.button("Process File"):
        file_type = uploaded.name.split(".")[-1].lower()
        if file_type == "pdf":
            _process_pdf(uploaded, book_id, settlement_month, engine)
        else:
            _process_excel(uploaded, book_id, settlement_month, engine)

    st.divider()
    st.subheader("Settlement Analytics")
    _render_analytics(book_id, engine)


def _process_pdf(uploaded, book_id: int, settlement_month, engine):
    """Process uploaded PDF settlement (上网电费结算单)."""
    import io
    from libs.settlement.parser import parse_pdf_settlement

    items = parse_pdf_settlement(io.BytesIO(uploaded.read()))
    if not items:
        st.warning("No settlement items extracted from PDF. Check file format.")
        return

    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO marketdata.rm_settlements (book_id, settlement_month, file_name, file_type, status)
            VALUES (:bid, :month, :fname, 'pdf', 'processed')
            RETURNING id
        """), {"bid": book_id, "month": settlement_month, "fname": uploaded.name})
        settlement_id = result.scalar()

        for item in items:
            conn.execute(text("""
                INSERT INTO marketdata.rm_settlement_items
                    (settlement_id, category, volume_mwh, price_cny_kwh,
                     amount_cny, peak_period, notes)
                VALUES (:sid, :cat, :vol, :price, :amt, :peak, :notes)
            """), {
                "sid": settlement_id, "cat": item["category"],
                "vol": item.get("volume_mwh"), "price": item.get("price_cny_kwh"),
                "amt": item["amount_cny"], "peak": item.get("peak_period"),
                "notes": item.get("notes"),
            })

    st.success(f"Processed {len(items)} settlement items from PDF.")


def _process_excel(uploaded, book_id: int, settlement_month, engine):
    """Process uploaded Excel settlement file."""
    import io
    from libs.settlement.parser import detect_format, parse_trade_capture, parse_capacity_compensation

    xl = pd.ExcelFile(io.BytesIO(uploaded.read()))
    fmt = detect_format(xl)
    st.info(f"Detected format: **{fmt}**")

    if fmt == "trade_capture":
        df = xl.parse("Trades")
        items = parse_trade_capture(df)
    elif fmt == "capacity_compensation":
        items = parse_capacity_compensation(xl)
    elif fmt == "wind_farm_ops":
        import tempfile, os
        from services.operating_assets.ingest import ingest_file
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            uploaded.seek(0)
            tmp.write(uploaded.read())
            tmp_path = tmp.name
        result = ingest_file(tmp_path)
        os.unlink(tmp_path)
        st.success(f"Wind farm data ingested: {result['rows_written']} rows. Errors: {result['errors']}")
        return
    else:
        st.error(f"Unknown format: {fmt}. Please use manual column mapping.")
        return

    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO marketdata.rm_settlements (book_id, settlement_month, file_name, file_type, status)
            VALUES (:bid, :month, :fname, :ftype, 'processed')
            RETURNING id
        """), {"bid": book_id, "month": settlement_month, "fname": uploaded.name, "ftype": "excel"})
        settlement_id = result.scalar()

        for item in items:
            conn.execute(text("""
                INSERT INTO marketdata.rm_settlement_items
                    (settlement_id, category, delivery_date, volume_mwh,
                     price_cny_kwh, amount_cny, amount_receivable_cny,
                     amount_settled_cny, amount_diff_cny, counterparty, notes)
                VALUES (:sid, :cat, :dd, :vol, :price, :amt, :recv, :settled, :diff, :cp, :notes)
            """), {
                "sid": settlement_id, "cat": item["category"],
                "dd": item.get("delivery_date"), "vol": item.get("volume_mwh"),
                "price": item.get("price_cny_kwh"), "amt": item["amount_cny"],
                "recv": item.get("amount_receivable_cny"),
                "settled": item.get("amount_settled_cny"),
                "diff": item.get("amount_diff_cny"),
                "cp": item.get("counterparty"), "notes": item.get("notes"),
            })

    st.success(f"Processed {len(items)} settlement items.")


def _render_analytics(book_id: int, engine):
    """Render settlement analytics for selected book."""
    with engine.connect() as conn:
        items_df = pd.read_sql(text("""
            SELECT si.category, si.peak_period, si.volume_mwh, si.amount_cny,
                   si.amount_receivable_cny, si.amount_settled_cny, si.amount_diff_cny,
                   s.settlement_month
            FROM marketdata.rm_settlement_items si
            JOIN marketdata.rm_settlements s ON s.id = si.settlement_id
            WHERE s.book_id = :bid
            ORDER BY s.settlement_month DESC, si.category
        """), conn, params={"bid": book_id})

    if items_df.empty:
        st.info("No settlement data yet for this book.")
        return

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Amount", f"¥{items_df['amount_cny'].sum():,.0f}")
    col2.metric("Total Volume", f"{items_df['volume_mwh'].sum():,.1f} MWh")
    if items_df["volume_mwh"].sum() > 0:
        avg_price = items_df["amount_cny"].sum() / items_df["volume_mwh"].sum()
        col3.metric("Avg Price", f"¥{avg_price:,.1f}/MWh")

    st.dataframe(
        items_df.groupby("category").agg(
            total_amount=("amount_cny", "sum"),
            total_volume=("volume_mwh", "sum"),
        ).sort_values("total_amount", ascending=False),
        use_container_width=True,
    )

    recon = items_df[items_df["amount_diff_cny"].notna() & (items_df["amount_diff_cny"] != 0)]
    if not recon.empty:
        st.subheader("Reconciliation (应收 vs 实际结算)")
        st.dataframe(recon[["category", "amount_receivable_cny", "amount_settled_cny", "amount_diff_cny"]],
                     use_container_width=True, hide_index=True)
