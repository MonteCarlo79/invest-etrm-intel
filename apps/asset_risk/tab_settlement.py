"""Tab 2 — Settlement: file upload, parsing, analytics."""
from __future__ import annotations

import hashlib
import json

import pandas as pd
import streamlit as st
from sqlalchemy import text


def _content_sha256(data: bytes) -> str:
    """SHA-256 of uploaded file bytes (content follows bytes, not filename)."""
    return hashlib.sha256(data).hexdigest()


def _already_ingested(engine, book_id: int, settlement_month: str, file_hash: str) -> bool:
    """True if an identical-content settlement exists at this book+month.

    Guards the rename-reupload duplication: tab uploads historically stored no
    hash, so a renamed re-upload bypassed both overwrite (name match) and the
    scanner's hash check (observed 2026-08-17: 苏右/裕昭沙子坝/远景乌拉特 dupes).
    """
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT 1 FROM marketdata.rm_settlements
            WHERE book_id = :bid AND settlement_month = :month
              AND raw_data->>'file_hash' = :h LIMIT 1
        """), {"bid": book_id, "month": settlement_month, "h": file_hash}).first()
    return row is not None


def _has_category(engine, book_id: int, settlement_month: str, category: str) -> bool:
    """True if any settlement item of this category exists at this book+month."""
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT 1 FROM marketdata.rm_settlement_items i
            JOIN marketdata.rm_settlements s ON s.id = i.settlement_id
            WHERE s.book_id = :bid AND s.settlement_month = :month
              AND i.category = :cat LIMIT 1
        """), {"bid": book_id, "month": settlement_month, "cat": category}).first()
    return row is not None


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

    month_mode = st.radio("Month detection", ["Auto-detect from filename", "Manual override"],
                          horizontal=True, key="month_mode")
    manual_month = None
    if month_mode == "Manual override":
        manual_month = st.date_input("Settlement Month (1st of month)")

    overwrite = st.checkbox("Overwrite existing data for same book+month", value=True)

    uploaded = st.file_uploader("Upload settlement file(s)", type=["xlsx", "xls", "csv", "pdf"],
                                accept_multiple_files=True, key=f"upload_{book_id}")

    if uploaded and st.button("Process File(s)"):
        for f in uploaded:
            try:
                _process_one_upload(f, book_id, month_mode, manual_month, overwrite, engine)
            except Exception as e:
                # One bad file must not kill the batch (observed 2026-08-21:
                # a DatetimeFieldOverflow aborted the whole upload loop)
                st.error(f"**{f.name}** processing failed: {e}")
                continue


    # Auto-scan from invoice folder for selected asset
    st.divider()
    # Get invoice_folder for selected book's asset
    with engine.connect() as conn:
        folder_df = pd.read_sql(text("""
            SELECT a.invoice_folder, a.name as asset_name FROM marketdata.rm_assets a
            JOIN marketdata.rm_books b ON b.asset_id = a.id
            WHERE b.id = :bid
        """), conn, params={"bid": book_id})
    invoice_folder = folder_df["invoice_folder"].iloc[0] if (not folder_df.empty and folder_df["invoice_folder"].iloc[0]) else None
    asset_name = folder_df["asset_name"].iloc[0] if not folder_df.empty else ""

    st.subheader(f"Scan & Ingest: {asset_name}")
    if invoice_folder:
        import os
        from services.settlement_ingest.scanner import INVOICE_ROOT
        scan_path = os.path.join(INVOICE_ROOT, invoice_folder)
        st.caption(f"Invoice folder: `{invoice_folder}/`")

        col_scan1, col_scan2 = st.columns([1, 1])
        with col_scan1:
            dry_run = st.checkbox("Dry run (preview only)", value=False)
        with col_scan2:
            if st.button("Scan & Ingest", type="primary"):
                with st.spinner(f"Scanning {invoice_folder}..."):
                    from services.settlement_ingest.scanner import scan_and_ingest
                    results = scan_and_ingest(root=scan_path, dry_run=dry_run)
                    ingested = [r for r in results if r.get("status") == "ingested"]
                    already = [r for r in results if r.get("status") == "already_ingested"]
                    errors = [r for r in results if r.get("status") == "error"]
                    skipped = [r for r in results if r.get("status") == "skipped"]
                    dry = [r for r in results if r.get("status") == "dry_run"]

                    if dry_run:
                        st.info(f"Dry run: {len(dry)} new files, {len(already)} already ingested, {len(skipped)} skipped.")
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
    else:
        st.warning("No invoice folder configured for this asset. Set it in the Asset Config tab.")

    st.divider()
    st.subheader("Settlement Analytics")
    _render_analytics(book_id, engine)


def _process_one_upload(f, book_id, month_mode, manual_month, overwrite, engine):
    # Template files are not settlement data
    if "模版" in f.name or "模板" in f.name:
        st.info(f"**{f.name}**: template file — skipped (not settlement data).")
        return

    # Auto-detect month from filename
    if month_mode == "Auto-detect from filename":
        from services.settlement_ingest.scanner import extract_month_from_filename
        detected = extract_month_from_filename(f.name)
        if detected and not detected.startswith("NEED_YEAR"):
            settlement_month = detected
        elif detected:
            # Month found but no year — try extracting billing period from PDF content
            period = None
            if f.name.lower().endswith(".pdf"):
                try:
                    import tempfile, os
                    from services.settlement_ingest.parser_charge import extract_billing_period
                    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                        tmp.write(f.read())
                        tmp_path = tmp.name
                    f.seek(0)
                    period = extract_billing_period(tmp_path)
                    os.unlink(tmp_path)
                except Exception:
                    period = None
            if period:
                settlement_month = period
                st.caption(f"  Detected period from PDF content: {period}")
            else:
                # Never stamp the current year onto a yearless file — a 2025
                # invoice scanned in 2026 would silently land on 2026.
                st.warning(
                    f"Cannot determine settlement YEAR for '{f.name}' — skipped. "
                    "Rename the file to include the year (e.g. 2025-06 / 2025年6月) "
                    "or use Manual override."
                )
                return
        else:
            st.warning(f"Cannot detect month from '{f.name}'. Skipping.")
            return
    else:
        settlement_month = manual_month.strftime("%Y-%m-%d") if manual_month else None
        if not settlement_month:
            st.warning("Please select a settlement month.")
            return

    # Validate before any DB use (a bad month string crashed the batch on
    # DatetimeFieldOverflow — e.g. "1001-20-01" from a serial-number filename)
    import re as _re
    _mv = _re.match(r'^(\d{4})-(\d{2})-\d{2}$', settlement_month or "")
    if not _mv or not (2015 <= int(_mv.group(1)) <= 2100) or not (1 <= int(_mv.group(2)) <= 12):
        st.warning(f"**{f.name}**: invalid settlement month '{settlement_month}' — skipped.")
        return

    st.caption(f"Processing: **{f.name}** → month: {settlement_month}")

    # Content-hash dedup: identical bytes already at this book+month → skip.
    # Catches renamed re-uploads (filename guards can't see those).
    fhash = _content_sha256(f.getvalue())
    if _already_ingested(engine, book_id, settlement_month, fhash):
        st.warning(f"**{f.name}**: identical content already ingested for {settlement_month} — skipped.")
        return

    # Overwrite: delete existing settlement for this book+month+filename pattern
    if overwrite:
        with engine.begin() as conn:
            # Delete items first (FK), then settlement record
            conn.execute(text("""
                DELETE FROM marketdata.rm_settlement_items
                WHERE settlement_id IN (
                    SELECT id FROM marketdata.rm_settlements
                    WHERE book_id = :bid AND settlement_month = :month
                    AND file_name = :fname
                )
            """), {"bid": book_id, "month": settlement_month, "fname": f.name})
            conn.execute(text("""
                DELETE FROM marketdata.rm_settlements
                WHERE book_id = :bid AND settlement_month = :month AND file_name = :fname
            """), {"bid": book_id, "month": settlement_month, "fname": f.name})

    file_type = f.name.split(".")[-1].lower()
    if file_type == "pdf":
        _process_pdf(f, book_id, settlement_month, engine, fhash)
    else:
        _process_excel(f, book_id, settlement_month, engine, fhash)



def _process_pdf(uploaded, book_id: int, settlement_month, engine, file_hash: str | None = None):
    """Process uploaded PDF settlement.

    Routing: filename semantics (上/下网) decide the parser — never send a
    下网 charge bill to the discharge vision parser (observed 2026-08: a scanned
    kWh-denominated 下网 bill was mis-extracted as discharge, 1000x unit error).
    parser_charge needs a text layer; parser_discharge (vision) handles any PDF.
    """
    import io
    import pdfplumber
    from services.settlement_ingest.scanner import classify_pdf

    pdf_bytes = uploaded.read()

    kind = classify_pdf(uploaded.name)
    if kind == "skip":
        st.info(f"{uploaded.name}: 发票/核查 file — skipped (not settlement data).")
        return

    voucher_fallback = False
    if kind == "voucher":
        # 发电侧凭证 fallback: if this book+month has no discharge data yet
        # (no 上网结算单), the voucher is the only discharge source — use it.
        # (observed 乌海 2026-01/02: vouchers are the sole discharge documents)
        if "发电侧" in uploaded.name and not _has_category(engine, book_id, settlement_month, "discharge_energy"):
            voucher_fallback = True
        else:
            st.info(f"**{uploaded.name}**: 结算凭证 (trading-center voucher) — skipped. "
                    "Its content duplicates the 上网/下网结算单; ingesting both would double-count. "
                    "No data written.")
            return

    # Detect if PDF is scanned (image-based) or has extractable text
    buf = io.BytesIO(pdf_bytes)
    pdf = pdfplumber.open(buf)
    page = pdf.pages[0]
    is_scanned = len(page.chars) == 0 and len(page.images) > 0
    pdf.close()

    import tempfile, os
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(pdf_bytes)
        tmp_path = tmp.name

    try:
        if voucher_fallback:
            st.info("发电侧结算凭证 — no 上网结算单 for this month; using voucher as discharge source.")
            from services.settlement_ingest.parser_voucher import parse_generation_voucher
            items = parse_generation_voucher(tmp_path)
        elif kind == "discharge" or (kind == "unknown" and is_scanned):
            # Discharge (上网) — vision parser handles scanned + text PDFs
            if is_scanned:
                st.info("Detected scanned PDF — using AI Vision to extract data...")
            from services.settlement_ingest.parser_discharge import parse_discharge_settlement_pdf
            items = parse_discharge_settlement_pdf(tmp_path)
        elif kind == "charge" or kind == "unknown":
            if is_scanned:
                st.warning(
                    f"**{uploaded.name}** looks like a 下网 charge bill but is a scanned image — "
                    "the charge parser needs a text-layer PDF. Skipped: no data written. "
                    "Use the scanner path or obtain the text version."
                )
                return
            from services.settlement_ingest.parser_charge import parse_charging_cost_pdf
            items = parse_charging_cost_pdf(tmp_path)
        else:
            items = []
    except Exception as e:
        st.error(f"PDF parsing failed: {e}")
        return
    finally:
        os.unlink(tmp_path)

    if not items:
        st.warning("No settlement items extracted from PDF. Check file format.")
        return

    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO marketdata.rm_settlements (book_id, settlement_month, file_name, file_type, status, raw_data)
            VALUES (:bid, :month, :fname, 'pdf', 'processed', :rd)
            RETURNING id
        """), {"bid": book_id, "month": settlement_month, "fname": uploaded.name,
               "rd": json.dumps({"file_hash": file_hash}) if file_hash else None})
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


def _process_excel(uploaded, book_id: int, settlement_month, engine, file_hash: str | None = None):
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
            INSERT INTO marketdata.rm_settlements (book_id, settlement_month, file_name, file_type, status, raw_data)
            VALUES (:bid, :month, :fname, :ftype, 'processed', :rd)
            RETURNING id
        """), {"bid": book_id, "month": settlement_month, "fname": uploaded.name, "ftype": "excel",
               "rd": json.dumps({"file_hash": file_hash}) if file_hash else None})
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


def _year_subtotal_row(pivot, monthly, year, days_in_month, energy_per_cycle_mwh,
                       discharge_col, charge_col, cap_col):
    """Build one year's YTD subtotal row with per-unit metrics recalculated
    from that year's own volumes. Returns a Series named "{year} YTD",
    or None if the year has no months in pivot."""
    months = [m for m in pivot.index if m.startswith(year)]
    if not months:
        return None
    row = pivot.loc[months].sum(axis=0)
    row.name = f"{year} YTD"

    yr_monthly = monthly[monthly["month"].str.startswith(year)]
    total_discharge_vol = yr_monthly[yr_monthly["category_cn"] == "放电收入"]["volume"].sum()
    total_charge_vol = yr_monthly[yr_monthly["category_cn"] == "充电电费"]["volume"].sum()
    if total_discharge_vol > 0 and "价差收入" in pivot.columns:
        # 价差收入 = 放电 + 充电 (不含容量补偿); 度电总价差 = (价差收入 + 容量补偿) / 放电量
        cap_total = row[cap_col] if (cap_col and cap_col in pivot.columns) else 0
        row["度电总价差"] = (row["价差收入"] + cap_total) / total_discharge_vol
        if cap_col and "容量补偿价差" in pivot.columns:
            row["容量补偿价差"] = cap_total / total_discharge_vol
        if "套利价差" in pivot.columns:
            row["套利价差"] = row["价差收入"] / total_discharge_vol
    if energy_per_cycle_mwh and energy_per_cycle_mwh > 0 and "日均充放次数" in pivot.columns:
        yr_days = sum(d for m, d in zip(pivot.index, days_in_month) if m.startswith(year))
        row["日均充放次数"] = round(total_charge_vol / energy_per_cycle_mwh / yr_days, 2) if yr_days > 0 else 0
    if total_charge_vol > 0:
        row["转化率"] = total_discharge_vol / total_charge_vol
    return row


def _insert_year_subtotals(pivot, monthly, days_in_month, energy_per_cycle_mwh,
                           discharge_col, charge_col, cap_col):
    """Insert a "YYYY YTD" subtotal row after each year's last month row.
    Extends automatically: any year present in the data gets a subtotal."""
    years = sorted({m[:4] for m in pivot.index})
    parts = []
    for year in years:
        months = [m for m in pivot.index if m.startswith(year)]
        parts.append(pivot.loc[months])
        row = _year_subtotal_row(pivot, monthly, year, days_in_month, energy_per_cycle_mwh,
                                 discharge_col, charge_col, cap_col)
        if row is not None:
            parts.append(row.to_frame().T)
    return pd.concat(parts)


def _render_analytics(book_id: int, engine):
    """Render settlement analytics for selected book — monthly breakdown."""
    import plotly.graph_objects as go

    # Chinese category labels
    CATEGORY_CN = {
        "charge_energy": "充电电费",
        "discharge_energy": "放电收入",
        "capacity_compensation": "容量补偿/非市场化",
        "transmission": "输配电费",
        "system_operation": "上网线损费",
        "coal_capacity_charge": "系统运行费",
        "basic_fee": "基本电费/力调",
        "govt_surcharges": "政府基金及附加",
        "frequency": "调频",
        "penalty": "偏差费用",
        "subsidy": "补贴",
        "rebate": "返还",
        "other": "其他",
        "generation_revenue": "发电收入",
    }

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
        st.info("暂无结算数据，请先上传结算文件。")
        return

    # Map category to Chinese
    items_df["category_cn"] = items_df["category"].map(CATEGORY_CN).fillna(items_df["category"])

    # Convert settlement_month to period string for display
    items_df["month"] = pd.to_datetime(items_df["settlement_month"]).dt.strftime("%Y-%m")

    # Summary metrics
    revenue_vol = items_df[items_df["category"].isin(["discharge_energy", "generation_revenue"])]["volume_mwh"].sum()
    charge_vol_total = items_df[items_df["category"] == "charge_energy"]["volume_mwh"].sum()
    charge_amt_total = items_df[items_df["category"] == "charge_energy"]["amount_cny"].sum()
    discharge_amt_total = items_df[items_df["category"].isin(["discharge_energy", "generation_revenue"])]["amount_cny"].sum()

    # Conversion efficiency
    conversion_rate = revenue_vol / charge_vol_total if charge_vol_total else None

    col1, col2, col3 = st.columns(3)
    col1.metric("结算总额", f"¥{items_df['amount_cny'].sum():,.0f}")
    col2.metric("放电总量", f"{revenue_vol:,.1f} MWh")
    col3.metric("充电总量", f"{charge_vol_total:,.1f} MWh")

    col4, col5, col6 = st.columns(3)
    col4.metric("放电均价", f"¥{discharge_amt_total / revenue_vol:,.1f}/MWh" if revenue_vol else "N/A")
    col5.metric("充电均价", f"¥{abs(charge_amt_total / charge_vol_total):,.1f}/MWh" if charge_vol_total else "N/A")
    col6.metric("电能转化率", f"{conversion_rate:.2%}" if conversion_rate else "N/A")

    # Get asset capacity for cycle calculation
    with engine.connect() as conn:
        cap_df = pd.read_sql(text("""
            SELECT a.capacity_mw, a.bess_duration_h FROM marketdata.rm_assets a
            JOIN marketdata.rm_books b ON b.asset_id = a.id
            WHERE b.id = :bid
        """), conn, params={"bid": book_id})
    capacity_mw = float(cap_df["capacity_mw"].iloc[0]) if not cap_df.empty else None
    duration_h = float(cap_df["bess_duration_h"].iloc[0]) if (not cap_df.empty and cap_df["bess_duration_h"].iloc[0]) else 4.0
    # Energy per cycle (MWh) = MW × hours
    energy_per_cycle_mwh = capacity_mw * duration_h if capacity_mw else None

    # Monthly summary table (pivot: month × category_cn)
    st.subheader("月度明细")
    monthly = items_df.groupby(["month", "category_cn"]).agg(
        amount=("amount_cny", "sum"),
        volume=("volume_mwh", "sum"),
    ).reset_index()

    # Pivot for display
    pivot = monthly.pivot_table(index="month", columns="category_cn", values="amount", aggfunc="sum", fill_value=0)
    pivot["净利润"] = pivot.sum(axis=1)

    # Add spread columns
    discharge_col = "放电收入" if "放电收入" in pivot.columns else None
    charge_col = "充电电费" if "充电电费" in pivot.columns else None
    cap_col = "容量补偿/非市场化" if "容量补偿/非市场化" in pivot.columns else None

    # Get monthly volumes for calculations
    discharge_vol = monthly[monthly["category_cn"] == "放电收入"].set_index(
        monthly[monthly["category_cn"] == "放电收入"]["month"])["volume"].reindex(pivot.index).fillna(0)
    charge_vol_monthly = monthly[monthly["category_cn"] == "充电电费"].set_index(
        monthly[monthly["category_cn"] == "充电电费"]["month"])["volume"].reindex(pivot.index).fillna(0)

    # Add volume columns
    pivot["放电量(MWh)"] = discharge_vol.values
    pivot["充电量(MWh)"] = charge_vol_monthly.values

    if discharge_col and charge_col:
        # 价差收入 = 放电收入 + 充电电费 (充电为负) — 容量补偿不计入 (2026-08-11: 容量补偿单列)
        cap_amount = pivot[cap_col] if cap_col else 0
        pivot["价差收入"] = pivot[discharge_col] + pivot[charge_col]

        # 度电总价差 = (价差收入 + 容量补偿) / 放电电量 — 含容量补偿的综合度电收益
        pivot["度电总价差"] = ((pivot["价差收入"] + cap_amount) / discharge_vol.values).replace([float("inf"), float("-inf")], 0).fillna(0)

        # 容量补偿价差 = 容量补偿 / 放电电量
        if cap_col:
            pivot["容量补偿价差"] = (cap_amount / discharge_vol.values).replace([float("inf"), float("-inf")], 0).fillna(0)

        # 套利价差 = 价差收入 / 放电电量 (纯市场化充放价差)
        pivot["套利价差"] = (pivot["价差收入"] / discharge_vol.values).replace([float("inf"), float("-inf")], 0).fillna(0)

    # 日均充放次数 = 月充电总量 / 装机容量 / 当月天数
    import calendar
    days_in_month = []
    for m in pivot.index:
        try:
            year, mon = int(m[:4]), int(m[5:7])
            days_in_month.append(calendar.monthrange(year, mon)[1])
        except (ValueError, IndexError):
            days_in_month.append(30)
    if energy_per_cycle_mwh and energy_per_cycle_mwh > 0:
        pivot["日均充放次数"] = (charge_vol_monthly.values / energy_per_cycle_mwh / pd.Series(days_in_month, index=pivot.index).values).round(2)
        pivot["日均充放次数"] = pivot["日均充放次数"].replace([float("inf"), float("-inf")], 0).fillna(0)

    # Add 电能转化率 per month
    import numpy as np
    with np.errstate(divide='ignore', invalid='ignore'):
        conversion_arr = np.where(charge_vol_monthly.values > 0, discharge_vol.values / charge_vol_monthly.values, 0)
    pivot["转化率"] = conversion_arr

    pivot = pivot.sort_index()

    # Per-year YTD subtotal rows ("2025 YTD", "2026 YTD", ...)
    pivot = _insert_year_subtotals(pivot, monthly, days_in_month, energy_per_cycle_mwh,
                                   discharge_col, charge_col, cap_col)

    # Reorder columns
    desired_order = [
        "净利润", "容量补偿/非市场化", "价差收入", "调频", "系统运行费", "上网线损费",
        "基本电费/力调", "放电收入", "充电电费", "放电量(MWh)", "充电量(MWh)",
        "度电总价差", "容量补偿价差", "套利价差", "日均充放次数", "转化率",
    ]
    ordered_cols = [c for c in desired_order if c in pivot.columns]
    remaining = [c for c in pivot.columns if c not in ordered_cols]
    pivot = pivot[ordered_cols + remaining]

    # Format: ¥ for money columns, plain number for others
    money_cols = ["净利润", "容量补偿/非市场化", "价差收入", "调频", "系统运行费", "上网线损费",
                  "基本电费/力调", "放电收入", "充电电费", "度电总价差", "容量补偿价差", "套利价差"]
    vol_cols = ["放电量(MWh)", "充电量(MWh)"]
    fmt = {}
    for c in pivot.columns:
        if c in money_cols:
            fmt[c] = "¥{:,.0f}"
        elif c in vol_cols:
            fmt[c] = "{:,.0f}"
        elif c == "日均充放次数":
            fmt[c] = "{:.2f}"
        elif c == "转化率":
            fmt[c] = "{:.1%}"
        else:
            fmt[c] = "¥{:,.0f}"

    # Color: red headers for cost, green for revenue, neutral for derived
    cost_cols = ["充电电费", "系统运行费", "上网线损费", "基本电费/力调", "调频"]
    revenue_cols = ["放电收入", "容量补偿/非市场化"]

    def color_headers(styler):
        styles = []
        for col in styler.columns:
            if col in cost_cols:
                styles.append(f"th.col_heading:contains('{col}') {{ color: red; }}")
            elif col in revenue_cols:
                styles.append(f"th.col_heading:contains('{col}') {{ color: green; }}")
        return styler

    styled = pivot.style.format(fmt)
    # Apply column header colors via set_table_styles
    header_styles = []
    for i, col in enumerate(pivot.columns):
        if col in cost_cols:
            header_styles.append({"selector": f"th.col{i}", "props": [("color", "#e74c3c")]})
        elif col in revenue_cols:
            header_styles.append({"selector": f"th.col{i}", "props": [("color", "#27ae60")]})
    styled = styled.set_table_styles(header_styles, overwrite=False)

    st.dataframe(styled, use_container_width=True)

    # Monthly bar chart (stacked by money category; YTD subtotal rows excluded).
    # 放电收入/充电电费 are excluded: 价差收入 already = 放电收入 + 充电电费 —
    # showing all three triple-counts (user note 2026-08-22).
    money_order = ["容量补偿/非市场化", "价差收入", "调频", "其他",
                   "系统运行费", "上网线损费", "基本电费/力调"]
    chart_pivot = pivot[[c for c in money_order if c in pivot.columns] + ["净利润"]]
    chart_pivot = chart_pivot[~chart_pivot.index.astype(str).str.contains("YTD")]

    fig = go.Figure()
    categories = [c for c in money_order if c in chart_pivot.columns]
    for cat in categories:
        fig.add_trace(go.Bar(
            x=chart_pivot.index, y=chart_pivot[cat], name=cat,
        ))
    fig.add_trace(go.Scatter(
        x=chart_pivot.index, y=chart_pivot["净利润"], name="净利润",
        mode="lines+markers", line=dict(color="black", width=2),
    ))
    fig.update_layout(
        barmode="relative", title="月度结算分类",
        xaxis_title="月份", yaxis_title="元 (CNY)", height=400,
        legend=dict(orientation="h", y=-0.2),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Category totals (all months combined)
    st.subheader("分类汇总 (全部月份)")
    cat_totals = items_df.groupby("category_cn").agg(
        total_amount=("amount_cny", "sum"),
        total_volume=("volume_mwh", "sum"),
    ).sort_values("total_amount", ascending=False)
    cat_totals.columns = ["金额 (元)", "电量 (MWh)"]
    st.dataframe(cat_totals.style.format({"金额 (元)": "¥{:,.0f}", "电量 (MWh)": "{:,.1f}"}),
                 use_container_width=True)

    # Reconciliation
    recon = items_df[items_df["amount_diff_cny"].notna() & (items_df["amount_diff_cny"] != 0)]
    if not recon.empty:
        st.subheader("对账 (应收 vs 实际结算)")
        st.dataframe(recon[["month", "category_cn", "amount_receivable_cny", "amount_settled_cny", "amount_diff_cny"]],
                     use_container_width=True, hide_index=True)
