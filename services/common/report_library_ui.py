"""Streamlit rendering helper for the Report Library tab.

Import and call render_library_tab() inside a `with tab_library:` block.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st


@st.cache_data(ttl=300, show_spinner=False)
def _cached_list_reports(market_code: str) -> pd.DataFrame:
    from services.common.report_library import list_reports
    return list_reports(market_code)


@st.cache_data(ttl=3600, show_spinner=False, max_entries=20)
def _cached_get_pdf(report_id: int) -> bytes | None:
    from services.common.report_library import get_report_pdf
    return get_report_pdf(report_id)


def render_library_tab(market_code: str, market_name: str, key_prefix: str) -> None:
    """Render the full Library tab content."""
    st.header(f"{market_name} — Report Library")
    st.caption(
        "Historical daily reports — saved automatically each morning after delivery. "
        "Reports are available from the next scheduled run onwards."
    )

    col_refresh, _ = st.columns([1, 5])
    if col_refresh.button("🔄 Refresh", key=f"{key_prefix}_lib_refresh"):
        _cached_list_reports.clear()
        st.rerun()

    df = _cached_list_reports(market_code)

    if df.empty:
        st.info(
            "No reports in library yet. Reports are saved automatically at 06:00 SGT "
            "each morning. You can also trigger a manual report from the Data Management tab."
        )
        return

    # ── Filters ──────────────────────────────────────────────────────────────
    df["report_date"] = pd.to_datetime(df["report_date"])
    years = sorted(df["report_date"].dt.year.unique().tolist(), reverse=True)

    f1, f2, _ = st.columns([1, 1, 4])
    sel_year = f1.selectbox(
        "Year", ["All"] + [str(y) for y in years], key=f"{key_prefix}_lib_year"
    )
    sel_type = f2.selectbox(
        "Type", ["All", "daily", "weekly", "monthly"], key=f"{key_prefix}_lib_type"
    )

    filtered = df.copy()
    if sel_year != "All":
        filtered = filtered[filtered["report_date"].dt.year == int(sel_year)]
    if sel_type != "All":
        filtered = filtered[filtered["report_type"] == sel_type]

    st.caption(f"**{len(filtered)}** report(s) found")

    # ── Table (metadata only, no PDF data) ───────────────────────────────────
    if filtered.empty:
        st.info("No reports match the selected filters.")
        return

    display = filtered[["report_date", "report_type", "filename", "file_size_kb"]].copy()
    display["report_date"] = display["report_date"].dt.strftime("%Y-%m-%d")
    display.columns = ["Date", "Type", "Filename", "Size (KB)"]
    st.dataframe(display, use_container_width=True, hide_index=True)

    # ── Download ─────────────────────────────────────────────────────────────
    st.divider()
    st.subheader("Download")

    options = [
        f"{str(row['report_date'])[:10]}  ·  {row['report_type']}  ·  {row['file_size_kb']} KB"
        for _, row in filtered.iterrows()
    ]
    sel_idx = st.selectbox(
        "Select report",
        range(len(options)),
        format_func=lambda i: options[i],
        key=f"{key_prefix}_lib_sel",
    )
    sel_row = filtered.iloc[sel_idx]

    with st.spinner("Loading PDF…"):
        pdf_data = _cached_get_pdf(int(sel_row["id"]))

    if pdf_data:
        st.download_button(
            "⬇ Download PDF",
            data=pdf_data,
            file_name=sel_row["filename"],
            mime="application/pdf",
            key=f"{key_prefix}_lib_dl",
            type="primary",
        )
        st.caption(f"{len(pdf_data) // 1024} KB · {sel_row['filename']}")
    else:
        st.error("PDF not found in database.")
