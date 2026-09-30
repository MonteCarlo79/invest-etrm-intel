# apps/retail_risk/tab_reconciliation.py
"""Reconciliation tab: trades vs settlement invoices, per book x month (Goal 1)."""
from __future__ import annotations

import datetime

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

from services.retail_risk import reconcile as rc


@st.cache_data(ttl=300)
def _recon(_engine_url: str, book_id: int, month: str) -> dict:
    eng = create_engine(_engine_url, pool_pre_ping=True)
    with eng.connect() as conn:
        r = rc.reconcile_month(conn, book_id, datetime.date.fromisoformat(month))
    return {"status": r.status, "tolerance": r.tolerance_cny,
            "lines": [vars(l) for l in r.lines]}


def render_reconciliation(engine):
    st.subheader("Trade ↔ Invoice Reconciliation")

    with engine.connect() as conn:
        books = pd.read_sql(text(
            "SELECT id, name FROM marketdata.rm_books WHERE book_type = 'load' ORDER BY name"
        ), conn)
        months = pd.read_sql(text("""
            SELECT DISTINCT settlement_month FROM marketdata.rm_settlements
            ORDER BY settlement_month DESC
        """), conn)

    if books.empty:
        st.info("No load books yet. Run the backfill or upload data first.")
        return

    book_id = st.selectbox("Book", books["id"].tolist(),
                           format_func=lambda x: books[books["id"] == x]["name"].iloc[0],
                           key="recon_book")
    if months.empty:
        st.info("No invoices ingested for reconciliation yet.")
        return

    month_list = [m.isoformat() for m in months["settlement_month"]]
    status_rows = []
    for m in month_list:
        r = _recon(str(engine.url), book_id, m)
        status_rows.append({"month": m, "status": r["status"], "tolerance_cny": r["tolerance"]})
    status_df = pd.DataFrame(status_rows)

    def _color(s):
        return {"matched": "🟢", "explained": "🟡", "flagged": "🔴"}.get(s, "⚪")
    status_df["status"] = status_df["status"].map(lambda s: f"{_color(s)} {s}")
    st.dataframe(status_df, use_container_width=True, hide_index=True)

    sel = st.selectbox("Drill into month", month_list, key="recon_month")
    r = _recon(str(engine.url), book_id, sel)
    lines = pd.DataFrame(r["lines"])
    st.dataframe(lines, use_container_width=True, hide_index=True)
