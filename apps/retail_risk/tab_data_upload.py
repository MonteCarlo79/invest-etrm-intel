# apps/retail_risk/tab_data_upload.py
"""Data Upload: parse a file with the retail parsers, preview, write to DB."""
from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

from services.retail_risk import loader, schemas
from services.retail_risk.parsers import (
    INVOICE_GLOBS, LOAD_PARSERS, TRADES_PARSERS, benchmark_infohub, mtm_workbook,
)
from services.retail_risk.run_backfill import _shandong_shapes, trades_to_volumes

_KINDS = ["trades", "invoice", "mtm_workbook", "benchmark"]


def _save_tmp(uploaded) -> Path:
    tmp = Path(tempfile.mkdtemp()) / uploaded.name
    tmp.write_bytes(uploaded.read())
    return tmp


def _parse_trades_single(province: str, path: Path):
    """Parse one uploaded trades file by building a minimal fake root for the
    province parser (parsers glob under <root>/<province>/...)."""
    fake = Path(tempfile.mkdtemp())
    if province == "冀南":
        dest = fake / "冀南" / "中长期交易结果" / "2026年1月"
    elif province == "浙江":
        dest = fake / "浙江" / "交易记录"
    elif province == "山东":
        dest = fake / "山东" / "202601" / "景融"
    else:
        dest = fake / "安徽" / "交易记录"
    dest.mkdir(parents=True)
    shutil.copy(path, dest / path.name)
    if province == "山东":
        # shapes come from the local data root's 日用电曲线 (uploaded file is 持仓明细)
        trades = TRADES_PARSERS["山东"](fake, _shandong_shapes(Path("data/trading")))
    else:
        trades = TRADES_PARSERS[province](fake)
    load = LOAD_PARSERS[province](fake) if province in LOAD_PARSERS else None
    return {"trades": trades, "volumes": trades_to_volumes(trades, load)}


def render_upload(engine):
    st.subheader("Data Upload")
    st.caption("Parse a single file with the retail parsers, preview the frames, then write.")

    col1, col2 = st.columns(2)
    with col1:
        province = st.selectbox("Province", schemas.LOAD_BOOK_PROVINCES, key="up_prov")
    with col2:
        kind = st.selectbox("Data type", _KINDS, key="up_kind")

    uploaded = st.file_uploader("File", type=["xlsx", "xls", "csv", "pdf"], key="up_file")
    if not uploaded:
        return

    if st.button("Parse", key="up_parse"):
        path = _save_tmp(uploaded)
        st.session_state["up_path"] = str(path)
        try:
            if kind == "trades":
                st.session_state["up_result"] = _parse_trades_single(province, path)
            elif kind == "invoice":
                doc = INVOICE_GLOBS[province][1](path)
                st.session_state["up_result"] = {"invoice": doc}
            elif kind == "mtm_workbook":
                st.session_state["up_result"] = mtm_workbook.parse_mtm_workbook(path)
            else:
                st.session_state["up_result"] = {
                    "benchmarks": benchmark_infohub.parse_infohub_benchmarks(path)}
        except Exception as e:  # noqa: BLE001
            st.error(f"Parse failed: {e}")
            return

    result = st.session_state.get("up_result")
    if not result:
        return

    if kind == "trades":
        trades, volumes = result["trades"], result["volumes"]
        st.write(f"trades rows: **{len(trades)}**, volumes rows: **{len(volumes)}**")
        st.dataframe(trades.head(50), use_container_width=True, hide_index=True)
        if st.button("Write trades to DB", key="up_write_trades"):
            with engine.begin() as conn:
                bid = loader.get_or_create_book(conn, province)
                ym = pd.Timestamp.today().strftime("%Y%m")
                n1 = loader.write_trades(conn, bid, trades, f"{province}_{ym}_upload", province)
                n2 = loader.write_volumes(conn, bid, volumes, f"{province}_{ym}_upload")
            st.success(f"Wrote {n1} positions, {n2} volume rows (book {bid}).")

    elif kind == "invoice":
        doc = result["invoice"]
        st.write(f"month: **{doc.settlement_month}**, items: **{len(doc.items)}**, "
                 f"printed total: **{doc.total_amount_cny}**")
        st.dataframe(pd.DataFrame(doc.items), use_container_width=True, hide_index=True)
        if st.button("Write invoice to DB", key="up_write_invoice"):
            with engine.begin() as conn:
                bid = loader.get_or_create_book(conn, province)
                sid = loader.write_invoice(conn, bid, doc, Path(st.session_state["up_path"]).name,
                                           loader.file_sha256(st.session_state["up_path"]))
            st.success(f"Invoice written (settlement id {sid})." if sid else "Already ingested (hash match).")

    elif kind == "mtm_workbook":
        st.write(f"province: **{result['province']}**, product: **{result['product']}**, "
                 f"curves: **{len(result['curves'])}**, contracts: **{len(result['contracts'])}**")
        st.dataframe(result["curves"].head(50), use_container_width=True, hide_index=True)
        if st.button("Write curves + contracts to DB", key="up_write_mtm"):
            with engine.begin() as conn:
                nc = loader.write_curves(conn, result["curves"])
                ncust = 0
                if not result["contracts"].empty:
                    _, ncust = loader.write_contracts(conn, result["province"], result["contracts"])
            st.success(f"Wrote {nc} curve rows, {ncust} contracts.")

    else:
        df = result["benchmarks"]
        st.write(f"benchmark rows: **{len(df)}**")
        st.dataframe(df, use_container_width=True, hide_index=True)
        if st.button("Write benchmarks to DB", key="up_write_bench"):
            with engine.begin() as conn:
                n = loader.write_benchmarks(conn, df)
            st.success(f"Wrote {n} benchmark rows.")
