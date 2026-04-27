"""
libs/decision_models/adapters/app/attribution_page.py

Streamlit page: BESS dispatch P&L attribution.

Pattern C (table-first, model-fallback):
  - Primary: read pre-computed rows from reports.bess_asset_daily_attribution
  - Fallback: call dispatch_pnl_attribution model if table is empty for the
    requested date (requires scenario PnL data to be loaded separately)

Drop into any Streamlit app:
    from libs.decision_models.adapters.app.attribution_page import render_attribution_page
    render_attribution_page()
"""
from __future__ import annotations

import datetime
from typing import List, Optional

ASSET_CODES: List[str] = [
    "suyou", "wulate", "wuhai", "wulanchabu",
    "hetao", "hangjinqi", "siziwangqi", "gushanliang",
]
ASSET_DISPLAY = {
    "suyou": "SuYou", "wulate": "WuLaTe", "wuhai": "WuHai",
    "wulanchabu": "WuLanChaBu", "hetao": "HeTao",
    "hangjinqi": "HangJinQi", "siziwangqi": "SiZiWangQi",
    "gushanliang": "GuShanLiang",
}

_LOSS_COLS = [
    "grid_restriction_loss",
    "forecast_error_loss",
    "strategy_error_loss",
    "nomination_loss",
    "execution_clearing_loss",
]
_SCENARIO_COLS = [
    "pf_unrestricted_pnl",
    "pf_grid_feasible_pnl",
    "tt_forecast_optimal_pnl",
    "tt_strategy_pnl",
    "nominated_pnl",
    "cleared_actual_pnl",
]


def _load_attribution_from_db(asset_code: str, date_from: datetime.date, date_to: datetime.date):
    """Load attribution rows from reports.bess_asset_daily_attribution."""
    import pandas as pd
    from sqlalchemy import text
    from services.common.db_utils import get_engine

    sql = text("""
        SELECT trade_date, asset_code,
               pf_unrestricted_pnl, pf_grid_feasible_pnl,
               cleared_actual_pnl, nominated_pnl,
               tt_forecast_optimal_pnl, tt_strategy_pnl,
               grid_restriction_loss, forecast_error_loss,
               strategy_error_loss, nomination_loss, execution_clearing_loss,
               realisation_gap_vs_pf, realisation_gap_vs_pf_grid
        FROM reports.bess_asset_daily_attribution
        WHERE asset_code = :asset
          AND trade_date BETWEEN :d1 AND :d2
        ORDER BY trade_date
    """)
    try:
        engine = get_engine()
        return pd.read_sql(sql, engine, params={"asset": asset_code, "d1": date_from, "d2": date_to})
    except Exception:
        return pd.DataFrame()


def render_attribution_page() -> None:
    import pandas as pd
    import streamlit as st

    st.header("BESS Dispatch P&L Attribution")
    st.caption(
        "Daily P&L waterfall from perfect foresight to cleared actual. "
        "Shows where value is lost in the dispatch chain."
    )

    # --- Sidebar controls ---
    with st.sidebar:
        st.subheader("Attribution settings")
        asset_code = st.selectbox(
            "Asset",
            options=ASSET_CODES,
            format_func=lambda c: ASSET_DISPLAY.get(c, c),
        )
        today = datetime.date.today()
        date_from = st.date_input("From", value=today - datetime.timedelta(days=30))
        date_to = st.date_input("To", value=today - datetime.timedelta(days=1))

    if date_from > date_to:
        st.error("'From' date must be before 'To' date.")
        return

    # --- Load data ---
    with st.spinner("Loading attribution data…"):
        df = _load_attribution_from_db(asset_code, date_from, date_to)

    if df.empty:
        st.info(
            f"No pre-computed attribution found for {ASSET_DISPLAY.get(asset_code, asset_code)} "
            f"between {date_from} and {date_to}. "
            "Run `python -m services.monitoring.run_daily_attribution` to populate."
        )
        return

    # --- Summary metrics ---
    st.subheader("Period summary")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Days with data", len(df))
    with col2:
        avg_actual = df["cleared_actual_pnl"].mean()
        st.metric("Avg actual PnL / day", f"¥{avg_actual:,.0f}" if pd.notna(avg_actual) else "—")
    with col3:
        avg_gap_pf = df["realisation_gap_vs_pf"].mean()
        st.metric("Avg gap vs PF", f"¥{avg_gap_pf:,.0f}" if pd.notna(avg_gap_pf) else "—")
    with col4:
        avg_gap_grid = df["realisation_gap_vs_pf_grid"].mean()
        st.metric("Avg gap vs PF grid", f"¥{avg_gap_grid:,.0f}" if pd.notna(avg_gap_grid) else "—")

    # --- Tabs ---
    tab_waterfall, tab_table, tab_trend = st.tabs(["Waterfall", "Daily table", "Trend"])

    with tab_waterfall:
        st.subheader("Average daily attribution waterfall")
        loss_means = df[_LOSS_COLS].mean().dropna()
        if loss_means.empty:
            st.info("No loss data available for the selected period.")
        else:
            waterfall_df = pd.DataFrame({
                "Attribution bucket": [c.replace("_", " ").title() for c in loss_means.index],
                "Average daily loss (¥)": loss_means.values,
            })
            st.dataframe(waterfall_df, use_container_width=True, hide_index=True)

    with tab_table:
        st.subheader("Daily attribution detail")
        display_cols = ["trade_date"] + _SCENARIO_COLS + _LOSS_COLS + [
            "realisation_gap_vs_pf", "realisation_gap_vs_pf_grid"
        ]
        available = [c for c in display_cols if c in df.columns]
        st.dataframe(
            df[available].sort_values("trade_date", ascending=False),
            use_container_width=True,
            hide_index=True,
        )

    with tab_trend:
        st.subheader("Realization ratio over time")
        if "cleared_actual_pnl" in df.columns and "pf_grid_feasible_pnl" in df.columns:
            trend_df = df[["trade_date", "cleared_actual_pnl", "pf_grid_feasible_pnl"]].copy()
            trend_df["realization_ratio"] = (
                trend_df["cleared_actual_pnl"] / trend_df["pf_grid_feasible_pnl"]
            ).where(trend_df["pf_grid_feasible_pnl"] != 0)
            st.line_chart(
                trend_df.set_index("trade_date")["realization_ratio"],
                use_container_width=True,
            )
            st.caption(
                "Realization ratio = cleared_actual_pnl / pf_grid_feasible_pnl. "
                "1.0 = perfect execution given grid constraints."
            )
        else:
            st.info("Realization ratio requires both cleared_actual_pnl and pf_grid_feasible_pnl.")

    # ── Export ────────────────────────────────────────────────────────────────
    from libs.decision_models.adapters.app.export_utils import (
        reportlab_available, to_excel_bytes, to_pdf_bytes_from_tables,
    )
    with st.expander("📥 Download report", expanded=False):
        col_pdf, col_xl = st.columns(2)

        asset_label = ASSET_DISPLAY.get(asset_code, asset_code)
        display_cols = ["trade_date"] + _SCENARIO_COLS + _LOSS_COLS + [
            "realisation_gap_vs_pf", "realisation_gap_vs_pf_grid"
        ]
        available_cols = [c for c in display_cols if c in df.columns]
        detail_df = df[available_cols].sort_values("trade_date", ascending=False)

        loss_means = df[_LOSS_COLS].mean().dropna()
        waterfall_df = pd.DataFrame({
            "Attribution bucket": [c.replace("_", " ").title() for c in loss_means.index],
            "Average daily loss (¥)": loss_means.values,
        }) if not loss_means.empty else pd.DataFrame()

        # PDF
        if reportlab_available():
            try:
                pdf_bytes = to_pdf_bytes_from_tables(
                    f"Attribution Report — {asset_label}  {date_from} → {date_to}",
                    sections=[
                        {"heading": "Waterfall Summary (Average Daily)", "df": waterfall_df},
                        {"heading": "Daily Attribution Detail", "df": detail_df},
                    ],
                    landscape=True,
                )
                if pdf_bytes:
                    col_pdf.download_button(
                        "⬇ PDF Report",
                        data=pdf_bytes,
                        file_name=f"attribution_{asset_code}_{date_from}_{date_to}.pdf",
                        mime="application/pdf",
                        key=f"attr_pdf_{asset_code}",
                    )
            except Exception as exc:
                col_pdf.caption(f"PDF error: {exc}")
        else:
            col_pdf.caption("Install `reportlab` to enable PDF export.")

        # Excel
        try:
            sheets: dict = {}
            if not detail_df.empty:
                sheets["Daily Attribution"] = detail_df.reset_index(drop=True)
            if not waterfall_df.empty:
                sheets["Waterfall Summary"] = waterfall_df

            if sheets:
                xl_bytes = to_excel_bytes(sheets)
                col_xl.download_button(
                    "⬇ Excel Tables",
                    data=xl_bytes,
                    file_name=f"attribution_{asset_code}_{date_from}_{date_to}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"attr_xl_{asset_code}",
                )
            else:
                col_xl.caption("No data to export.")
        except Exception as exc:
            col_xl.caption(f"Excel error: {exc}")
