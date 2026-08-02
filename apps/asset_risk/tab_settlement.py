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

    uploaded = st.file_uploader("Upload settlement file(s)", type=["xlsx", "xls", "csv", "pdf"],
                                accept_multiple_files=True)

    if uploaded and st.button("Process File(s)"):
        for f in uploaded:
            st.caption(f"Processing: **{f.name}**")
            file_type = f.name.split(".")[-1].lower()
            if file_type == "pdf":
                _process_pdf(f, book_id, settlement_month, engine)
            else:
                _process_excel(f, book_id, settlement_month, engine)

    # Auto-scan from invoice folders
    st.divider()
    st.subheader("Auto-Scan Invoice Folders")
    st.caption("Scan `data/raw/settlement/invoices/` for new PDFs and ingest automatically.")
    col_scan1, col_scan2 = st.columns([1, 1])
    with col_scan1:
        dry_run = st.checkbox("Dry run (preview only)", value=True)
    with col_scan2:
        if st.button("Scan & Ingest", type="primary"):
            with st.spinner("Scanning invoice folders..."):
                from services.settlement_ingest.scanner import scan_and_ingest
                results = scan_and_ingest(dry_run=dry_run)
                ingested = [r for r in results if r.get("status") == "ingested"]
                already = [r for r in results if r.get("status") == "already_ingested"]
                errors = [r for r in results if r.get("status") == "error"]
                skipped = [r for r in results if r.get("status") == "skipped"]
                dry = [r for r in results if r.get("status") == "dry_run"]

                if dry_run:
                    st.info(f"Dry run: {len(dry)} new files found, {len(already)} already ingested, {len(skipped)} skipped.")
                    if dry:
                        st.dataframe(pd.DataFrame(dry)[["path", "asset", "month", "type"]],
                                     use_container_width=True, hide_index=True)
                else:
                    st.success(f"Ingested {len(ingested)} files. Already done: {len(already)}. Errors: {len(errors)}.")
                    if ingested:
                        st.dataframe(pd.DataFrame(ingested)[["path", "asset", "month", "type", "items"]],
                                     use_container_width=True, hide_index=True)
                    if errors:
                        st.error("Errors:")
                        st.dataframe(pd.DataFrame(errors)[["path", "asset", "error"]],
                                     use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Settlement Analytics")
    _render_analytics(book_id, engine)


def _process_pdf(uploaded, book_id: int, settlement_month, engine):
    """Process uploaded PDF settlement — auto-detects text vs scanned image."""
    import io
    import pdfplumber

    pdf_bytes = uploaded.read()
    buf = io.BytesIO(pdf_bytes)

    # Detect if PDF is scanned (image-based) or has extractable text
    pdf = pdfplumber.open(buf)
    page = pdf.pages[0]
    is_scanned = len(page.chars) == 0 and len(page.images) > 0
    pdf.close()

    if is_scanned:
        # Use Claude Vision parser for scanned discharge settlement PDFs
        st.info("Detected scanned PDF — using AI Vision to extract data...")
        try:
            import tempfile, os
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                tmp.write(pdf_bytes)
                tmp_path = tmp.name
            from services.settlement_ingest.parser_discharge import parse_discharge_settlement_pdf
            items = parse_discharge_settlement_pdf(tmp_path)
            os.unlink(tmp_path)
        except Exception as e:
            st.error(f"Vision parsing failed: {e}")
            return
    else:
        # Use text-based parser for charging cost PDFs
        buf.seek(0)
        from libs.settlement.parser import parse_pdf_settlement
        items = parse_pdf_settlement(buf)

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
    """Render settlement analytics for selected book — monthly breakdown."""
    import plotly.graph_objects as go

    with engine.connect() as conn:
        items_df = pd.read_sql(text("""
            SELECT si.category, si.volume_mwh, si.amount_cny,
                   si.amount_receivable_cny, si.amount_settled_cny, si.amount_diff_cny,
                   s.settlement_month
            FROM marketdata.rm_settlement_items si
            JOIN marketdata.rm_settlements s ON s.id = si.settlement_id
            WHERE s.book_id = :bid
            ORDER BY s.settlement_month, si.category
        """), conn, params={"bid": book_id})

    if items_df.empty:
        st.info("No settlement data yet for this book.")
        return

    # Convert settlement_month to period string for display
    items_df["month"] = pd.to_datetime(items_df["settlement_month"]).dt.strftime("%Y-%m")

    # Summary metrics (all time)
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Amount", f"¥{items_df['amount_cny'].sum():,.0f}")
    total_vol = items_df["volume_mwh"].sum()
    col2.metric("Total Volume", f"{total_vol:,.1f} MWh")
    if total_vol and total_vol != 0:
        col3.metric("Avg Price", f"¥{items_df['amount_cny'].sum() / total_vol:,.1f}/MWh")

    # Monthly summary table (pivot: month × category)
    st.subheader("Monthly Breakdown")
    monthly = items_df.groupby(["month", "category"]).agg(
        amount=("amount_cny", "sum"),
        volume=("volume_mwh", "sum"),
    ).reset_index()

    # Pivot for display
    pivot = monthly.pivot_table(index="month", columns="category", values="amount", aggfunc="sum", fill_value=0)
    pivot["NET TOTAL"] = pivot.sum(axis=1)
    pivot = pivot.sort_index()
    st.dataframe(pivot.style.format("¥{:,.0f}"), use_container_width=True)

    # Monthly bar chart (stacked by category)
    fig = go.Figure()
    categories = [c for c in pivot.columns if c != "NET TOTAL"]
    for cat in categories:
        fig.add_trace(go.Bar(
            x=pivot.index, y=pivot[cat], name=cat,
        ))
    fig.add_trace(go.Scatter(
        x=pivot.index, y=pivot["NET TOTAL"], name="Net Total",
        mode="lines+markers", line=dict(color="black", width=2),
    ))
    fig.update_layout(
        barmode="relative", title="Settlement by Month & Category",
        xaxis_title="Month", yaxis_title="CNY", height=400, legend=dict(orientation="h", y=-0.2),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Category totals (all months combined)
    st.subheader("Category Totals (All Months)")
    cat_totals = items_df.groupby("category").agg(
        total_amount=("amount_cny", "sum"),
        total_volume=("volume_mwh", "sum"),
    ).sort_values("total_amount", ascending=False)
    st.dataframe(cat_totals.style.format({"total_amount": "¥{:,.0f}", "total_volume": "{:,.1f}"}),
                 use_container_width=True)

    # Reconciliation
    recon = items_df[items_df["amount_diff_cny"].notna() & (items_df["amount_diff_cny"] != 0)]
    if not recon.empty:
        st.subheader("Reconciliation (应收 vs 实际结算)")
        st.dataframe(recon[["month", "category", "amount_receivable_cny", "amount_settled_cny", "amount_diff_cny"]],
                     use_container_width=True, hide_index=True)
