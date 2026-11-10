# apps/retail_risk/tab_reconciliation.py
"""Reconciliation tab: trades vs settlement invoices, per book x month (Goal 1)."""
from __future__ import annotations

import datetime

import pandas as pd
import streamlit as st
from sqlalchemy import text

from services.retail_risk import reconcile as rc


@st.cache_data(ttl=300)
def _recon(_engine, book_id: int, month: str) -> dict:
    """Reuses the app's engine (underscore arg excluded from cache key).
    A second engine built from env/engine.url either masks the password or
    inherits a stale process env — there must be exactly one engine."""
    with _engine.connect() as conn:
        r = rc.reconcile_month(conn, book_id, datetime.date.fromisoformat(month))
    return {"status": r.status, "tolerance": r.tolerance_cny,
            "lines": [vars(l) for l in r.lines]}


def render_reconciliation(engine):
    st.subheader("Trade ↔ Invoice Reconciliation")

    with engine.connect() as conn:
        books = pd.read_sql(text(
            "SELECT id, name FROM marketdata.rm_books WHERE book_type = 'load' ORDER BY name"
        ), conn)

    if books.empty:
        st.info("No load books yet. Run the backfill or upload data first.")
        return

    book_id = st.selectbox("Book", books["id"].tolist(),
                           format_func=lambda x: books[books["id"] == x]["name"].iloc[0],
                           key="recon_book")
    with engine.connect() as conn:
        # this book's months only: an invoice month OR a month with trades
        months = pd.read_sql(text("""
            SELECT settlement_month FROM marketdata.rm_settlements WHERE book_id = :b
            UNION
            SELECT DISTINCT CAST(date_trunc('month', start_date) AS date)
            FROM marketdata.rm_positions WHERE book_id = :b
              AND start_date < date_trunc('month', CURRENT_DATE)
            ORDER BY 1 DESC
        """), conn, params={"b": book_id})
    if months.empty:
        st.info("No invoices ingested for reconciliation yet.")
        return

    month_list = [pd.Timestamp(m).date().isoformat() for m in months.iloc[:, 0]]
    status_rows = []
    for m in month_list:
        r = _recon(engine, book_id, m)
        status_rows.append({"month": m, "status": r["status"], "tolerance_cny": r["tolerance"]})
    status_df = pd.DataFrame(status_rows)

    def _color(s):
        return {"matched": "🟢", "explained": "🟡", "flagged": "🔴"}.get(s, "⚪")
    status_df["status"] = status_df["status"].map(lambda s: f"{_color(s)} {s}")
    st.dataframe(status_df, use_container_width=True, hide_index=True)
    st.caption("🟢 matched: 中长期 and total tie · 🟡 explained: 中长期 ties, spot line is "
               "an estimate (exposure × mean RT) · 🔴 flagged: trades disagree with the "
               "invoice 中长期 line · ⚪ no_invoice / no_trades / no_prices: data missing, "
               "not a mismatch. 浙江/山东 settle 中长期 as a difference vs DA price.")

    sel = st.selectbox("Drill into month", month_list, key="recon_month")
    r = _recon(engine, book_id, sel)
    lines = pd.DataFrame(r["lines"])
    st.dataframe(lines, use_container_width=True, hide_index=True)
