"""Tab 2 — Settlement: retail settlements by load book (rm_settlements / rm_settlement_items).

Per-book view replaces the old per-customer upload (user request 2026-11-16): retail
invoices are ingested per book through the Data Upload tab (services.retail_risk.loader.
write_invoice), so this tab reads the same book-level tables the Realised P&L tab uses.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import text

CATEGORY_CN = {
    "retail_revenue": "零售收入",
    "midlong_energy": "中长期电量",
    "spot_energy": "现货电量",
    "green_premium": "绿电溢价",
    "imbalance": "偏差/考核",
    "frequency": "调频分摊",
    "market_redistribution": "市场化分摊/返还",
    "rule_charges": "规则性费用",
    "other": "其他",
}


def _load_books(engine) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(
            "SELECT id, name FROM marketdata.rm_books WHERE book_type = 'load' ORDER BY name"), conn)


def _load_items(engine, book_ids: list[int]) -> pd.DataFrame:
    if not book_ids:
        return pd.DataFrame(columns=["book_id", "settlement_month", "category", "volume_mwh", "amount_cny"])
    ids = ",".join(str(int(b)) for b in book_ids)
    with engine.connect() as conn:
        return pd.read_sql(text(f"""
            SELECT s.book_id, s.settlement_month, si.category, si.volume_mwh, si.amount_cny
            FROM marketdata.rm_settlement_items si
            JOIN marketdata.rm_settlements s ON s.id = si.settlement_id
            WHERE s.book_id IN ({ids})
        """), conn)


def render_settlement(engine):
    st.subheader("Retail Settlement · 售电结算（按账簿）")
    st.caption("Upload retail invoices per book in the **Data Upload** tab; they land here.")

    books = _load_books(engine)
    if books.empty:
        st.info("No load books yet.")
        return

    book_id = st.selectbox("Book", books["id"].tolist(),
                           format_func=lambda x: books.loc[books["id"] == x, "name"].iloc[0],
                           key="settlement_book")

    with engine.connect() as conn:
        headers = pd.read_sql(text("""
            SELECT settlement_month, file_name, file_type, status, total_amount_cny, upload_date
            FROM marketdata.rm_settlements WHERE book_id = :b ORDER BY settlement_month DESC, id DESC
        """), conn, params={"b": book_id})

    if headers.empty:
        st.info("No settlements for this book yet.")
    else:
        items = _load_items(engine, [book_id])
        items["month"] = pd.to_datetime(items["settlement_month"]).dt.strftime("%Y-%m")
        items["类别"] = items["category"].map(CATEGORY_CN).fillna(items["category"])

        st.markdown("**By category × month · 金额 (¥)**")
        amt = items.pivot_table(index="类别", columns="month", values="amount_cny",
                                aggfunc="sum").sort_index(axis=1)
        st.dataframe(amt.style.format("{:,.0f}", na_rep="—"), use_container_width=True)

        st.markdown("**By category × month · 电量 (MWh)**")
        vol = items.pivot_table(index="类别", columns="month", values="volume_mwh",
                                aggfunc="sum").sort_index(axis=1)
        st.dataframe(vol.style.format("{:,.1f}", na_rep="—"), use_container_width=True)

        with st.expander(f"Settlement files ({len(headers)})"):
            st.dataframe(headers, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("All load books · 各账簿零售收入 (¥)")
    all_items = _load_items(engine, books["id"].tolist())
    rev = all_items[all_items["category"] == "retail_revenue"].copy()
    if rev.empty:
        st.info("No retail revenue settled yet.")
        return
    rev["month"] = pd.to_datetime(rev["settlement_month"]).dt.strftime("%Y-%m")
    rev["Book"] = rev["book_id"].map(dict(zip(books["id"], books["name"])))
    summary = rev.pivot_table(index="Book", columns="month", values="amount_cny",
                              aggfunc="sum").sort_index(axis=1)
    summary["合计"] = summary.sum(axis=1)
    st.dataframe(summary.style.format("{:,.0f}", na_rep="—"), use_container_width=True)
