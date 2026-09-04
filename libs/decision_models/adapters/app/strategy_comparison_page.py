"""
libs/decision_models/adapters/app/strategy_comparison_page.py

Streamlit page for the BESS dispatch strategy comparison workflow.

Drop into any Streamlit app:
    from libs.decision_models.adapters.app.strategy_comparison_page import render_strategy_comparison_page
    render_strategy_comparison_page()

What it does
------------
1. User selects asset, date range, forecast models.
2. Load context (prices, dispatch, metadata) from DB.
3. Run perfect foresight benchmark.
4. Run forecast-driven dispatch suite.
5. Rank strategies by realised P&L.
6. Show discrepancy attribution waterfall.
7. Generate and display daily/weekly/monthly report.

All model and DB logic is delegated to workflows/strategy_comparison.py
and resources/bess_context.py — this file is pure presentation.
"""
from __future__ import annotations

import datetime
from typing import List, Optional

ASSET_CODES = [
    "suyou", "wulate", "wuhai", "wulanchabu",
    "hetao", "hangjinqi", "siziwangqi", "gushanliang",
]
ASSET_DISPLAY = {
    "suyou": "SuYou", "wulate": "WuLaTe", "wuhai": "WuHai",
    "wulanchabu": "WuLanChaBu", "hetao": "HeTao",
    "hangjinqi": "HangJinQi", "siziwangqi": "SiZiWangQi",
    "gushanliang": "GuShanLiang",
}
FORECAST_MODELS = ["ols_rt_time_v1", "naive_rt_lag1", "naive_rt_lag7", "ols_da_time_v1", "naive_da"]


def render_strategy_comparison_page() -> None:
    import pandas as pd
    import streamlit as st

    from libs.decision_models.workflows.strategy_comparison import (
        attribute_dispatch_discrepancy,
        generate_asset_strategy_report,
        load_bess_strategy_comparison_context,
        rank_dispatch_strategies,
        run_forecast_dispatch_suite,
        run_perfect_foresight_dispatch,
    )
    from libs.decision_models.resources.bess_context import (
        load_precomputed_scenario_pnl,
        load_precomputed_attribution,
    )

    st.header("BESS Dispatch Strategy Comparison")
    st.caption(
        "Compare perfect foresight / forecast / nominated / actual strategies. "
        "Ranks realised P&L and decomposes discrepancy into attribution buckets."
    )

    # ── Sidebar controls ──────────────────────────────────────────────────────
    with st.sidebar:
        st.subheader("Parameters")
        asset_code = st.selectbox(
            "Asset",
            ASSET_CODES,
            format_func=lambda x: f"{x} / {ASSET_DISPLAY.get(x, x)}",
        )
        default_end = datetime.date.today()
        default_start = default_end.replace(day=1)
        date_range = st.date_input(
            "Date range",
            value=(default_start, default_end),
        )
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            date_from_d, date_to_d = date_range
        else:
            date_from_d = date_to_d = default_end

        date_from = str(date_from_d)
        date_to = str(date_to_d)

        forecast_model_choice = st.multiselect(
            "Forecast models to run",
            FORECAST_MODELS,
            default=["ols_rt_time_v1"],
        )
        period_type = st.radio(
            "Report period",
            ["daily", "weekly", "monthly"],
            index=2,
            horizontal=True,
        )
        window_days = st.number_input(
            "LP window (days)",
            min_value=1,
            max_value=30,
            value=1,
            step=1,
            help=(
                "Number of consecutive days to optimise in one LP solve. "
                "1 = each day independently (SOC resets daily). "
                ">1 = SOC carries over across day boundaries within each window, "
                "giving the optimiser cross-day flexibility at the cost of a larger LP."
            ),
        )
        run_btn = st.button("Run comparison", type="primary")

    if not run_btn:
        st.info("Select an asset and date range, then click **Run comparison**.")
        return

    if date_from_d > date_to_d:
        st.error("Start date must be before end date.")
        return

    # ── Run all steps inside a collapsible status block ───────────────────────
    _asset_label = ASSET_DISPLAY.get(asset_code, asset_code)
    _data_quality_notes: list = []
    _no_prices = False

    with st.status(
        f"Running strategy comparison — {_asset_label} {date_from} → {date_to}…",
        expanded=True,
    ) as _run_status:

        # Step 1
        st.write("Step 1/6 — Loading context (prices, dispatch, metadata) from DB…")
        context = load_bess_strategy_comparison_context(asset_code, date_from, date_to)
        _data_quality_notes = context.get("data_quality_notes", [])

        if not context.get("actual_prices_hourly"):
            _run_status.update(
                label="No price data — cannot run comparison", state="error"
            )
            _no_prices = True
        else:
            # Step 2
            _pf_label = (
                "Step 2/6 — Perfect foresight LP…"
                if window_days == 1
                else f"Step 2/6 — Perfect foresight LP ({window_days}-day windows)…"
            )
            st.write(_pf_label)
            pf_result = run_perfect_foresight_dispatch(context, window_days=int(window_days))

            # Step 3
            if forecast_model_choice:
                st.write(
                    f"Step 3/6 — Forecast suite ({', '.join(forecast_model_choice)})…"
                )
                forecast_suite = run_forecast_dispatch_suite(
                    context, forecast_model_choice, window_days=int(window_days)
                )
            else:
                st.write("Step 3/6 — No forecast models selected, skipping.")
                forecast_suite = {"strategies": [], "requested_models": [], "suite_caveats": []}

            # Step 4
            st.write("Step 4/6 — Loading pre-computed DB P&L…")
            db_pnl_df, _ = load_precomputed_scenario_pnl(asset_code, date_from_d, date_to_d)
            db_attr_df, _ = load_precomputed_attribution(asset_code, date_from_d, date_to_d)

            # Step 5
            st.write("Step 5/6 — Ranking strategies & attributing discrepancy…")
            ranking = rank_dispatch_strategies(
                context, pf_result, forecast_suite,
                db_pnl_df=db_pnl_df if not db_pnl_df.empty else None,
            )
            attribution = attribute_dispatch_discrepancy(
                context, ranking,
                attribution_df=db_attr_df if not db_attr_df.empty else None,
            )

            # Step 6
            st.write("Step 6/6 — Generating report…")
            report = generate_asset_strategy_report(
                asset_code, date_from, date_to,
                period_type=period_type,
                context=context,
                ranking=ranking,
                attribution=attribution,
                db_pnl_df=db_pnl_df if not db_pnl_df.empty else None,
                db_attr_df=db_attr_df if not db_attr_df.empty else None,
            )

            _n_ranked = len([r for r in ranking.get("rows", []) if r.get("data_available")])
            _run_status.update(
                label=f"Complete — {_n_ranked} strategies ranked", state="complete"
            )

    _show_data_quality(st, _data_quality_notes)

    if _no_prices:
        st.error(
            f"No actual price data for {asset_code} between {date_from} and {date_to}. "
            "Cannot run comparison. Check DB_DSN and canon.nodal_rt_price_15min."
        )
        return

    # ── Display ───────────────────────────────────────────────────────────────
    tabs = st.tabs([
        "Summary", "Strategy Ranking", "P&L Comparison",
        "Discrepancy Attribution", "YTD / Year-End", "Report Markdown",
    ])

    with tabs[0]:
        st.subheader("Executive Summary")
        st.code(report["sections"].get("executive_summary", ""), language=None)

    with tabs[1]:
        st.subheader("Strategy Ranking")
        rows = ranking.get("rows", [])
        if rows:
            rank_df = pd.DataFrame([
                {
                    "Rank": r["rank"],
                    "Strategy": r["strategy_name"],
                    "Total P&L (CNY)": _fmt_yuan(r["pnl_total_yuan"]),
                    "Market P&L (CNY)": _fmt_yuan(r.get("pnl_market_yuan")),
                    "Subsidy (CNY)": _fmt_yuan(r.get("pnl_compensation_yuan")),
                    "Gap vs PF": _fmt_yuan(r["gap_vs_perfect_foresight_yuan"]),
                    "Gap vs Nominated": _fmt_yuan(r["gap_vs_nominated_yuan"]),
                    "Capture vs PF": _fmt_pct(r["capture_rate_vs_pf"]),
                    "Granularity": r["granularity"],
                    "Available": "✓" if r["data_available"] else "—",
                }
                for r in rows
            ])
            st.dataframe(rank_df, use_container_width=True)
        else:
            st.info("No ranking data.")

        if ranking.get("caveats"):
            with st.expander("Ranking caveats"):
                for c in ranking["caveats"]:
                    st.caption(f"• {c}")

    with tabs[2]:
        st.subheader("Realised P&L Comparison")
        pnl_table = report.get("pnl_comparison", {})
        if pnl_table.get("rows"):
            pnl_df = pd.DataFrame(pnl_table["rows"], columns=pnl_table["headers"])
            st.dataframe(pnl_df, use_container_width=True)

        period_rows_key = f"{period_type}_rows"
        period_rows = report.get(period_rows_key, [])
        if period_rows:
            st.subheader(f"{period_type.title()} P&L breakdown (from DB)")
            st.dataframe(pd.DataFrame(period_rows), use_container_width=True)

    with tabs[3]:
        st.subheader("Discrepancy Attribution Waterfall")
        st.caption(
            "Rules-based waterfall attribution — not causal proof. "
            "asset_issue is always None until an outage table is implemented."
        )
        buckets = attribution.get("buckets", {})
        total_gap = attribution.get("total_gap")
        if total_gap is not None:
            st.metric("Total gap (PF − actual)", f"{total_gap:,.0f} CNY")

        bucket_rows = []
        for key, label in [
            ("grid_restriction", "Grid restriction"),
            ("forecast_error", "Forecast error"),
            ("execution_nomination", "Execution / nomination"),
            ("execution_clearing", "Execution / clearing"),
            ("asset_issue", "Asset issue"),
            ("residual", "Residual"),
        ]:
            val = buckets.get(key)
            bucket_rows.append({
                "Bucket": label,
                "Loss (CNY)": _fmt_yuan(val),
                "% of total gap": _fmt_pct(val / total_gap if (val and total_gap) else None),
            })
        st.dataframe(pd.DataFrame(bucket_rows), use_container_width=True)

        daily_rows = attribution.get("daily_rows", [])
        if daily_rows:
            st.subheader("Daily attribution (from DB)")
            st.dataframe(pd.DataFrame(daily_rows), use_container_width=True)

        if attribution.get("caveats"):
            with st.expander("Attribution caveats"):
                for c in attribution["caveats"]:
                    st.caption(f"• {c}")

    with tabs[4]:
        st.subheader("YTD Summary")
        ytd = report.get("ytd_summary")
        if ytd:
            col1, col2, col3 = st.columns(3)
            col1.metric("YTD Actual P&L", _fmt_yuan(ytd.get("ytd_actual_pnl")))
            col2.metric("YTD PF Benchmark", _fmt_yuan(ytd.get("ytd_pf_pnl")))
            col3.metric("YTD Capture Rate", _fmt_pct(ytd.get("ytd_capture_rate")))
            st.caption(f"Data through: {ytd.get('data_through', '—')}")
        else:
            st.info("No YTD data.")

        st.subheader("Forecast to Year-End")
        fy = report.get("forecast_to_year_end")
        if fy and fy.get("projected_total") is not None:
            col1, col2, col3 = st.columns(3)
            col1.metric("Realized YTD", _fmt_yuan(fy.get("realized_ytd")))
            col2.metric("Projected Remainder", _fmt_yuan(fy.get("projected_remainder")))
            col3.metric("Projected Full Year", _fmt_yuan(fy.get("projected_total")))
            st.caption(f"Method: {fy.get('projection_method', '—')}")
            if fy.get("caveats"):
                st.caption(f"⚠ {fy['caveats'][0]}")
        else:
            st.info("Insufficient data for year-end projection.")

    with tabs[5]:
        st.subheader("Report Markdown")
        st.caption("Copy this for Slack / email distribution.")
        st.text_area("Markdown", value=report.get("markdown", ""), height=500)

    # ── Export ────────────────────────────────────────────────────────────────
    from libs.decision_models.adapters.app.export_utils import (
        reportlab_available, to_excel_bytes, to_pdf_bytes_from_markdown,
    )
    with st.expander("📥 Download report", expanded=False):
        col_pdf, col_xl = st.columns(2)

        # PDF from report markdown
        if reportlab_available():
            md_str = report.get("markdown", "")
            if md_str:
                try:
                    pdf_bytes = to_pdf_bytes_from_markdown(
                        f"BESS Strategy Report — {asset_code} {date_from}→{date_to}",
                        md_str,
                    )
                    if pdf_bytes:
                        col_pdf.download_button(
                            "⬇ PDF Report",
                            data=pdf_bytes,
                            file_name=f"strategy_{asset_code}_{date_from}_{date_to}.pdf",
                            mime="application/pdf",
                            key=f"sc_pdf_{asset_code}_{date_from}",
                        )
                except Exception as exc:
                    col_pdf.caption(f"PDF error: {exc}")
            else:
                col_pdf.caption("No report markdown generated yet.")
        else:
            col_pdf.caption("Install `reportlab` to enable PDF export.")

        # Excel — ranking, period rows, attribution
        try:
            sheets: dict = {}

            rank_rows = ranking.get("rows", [])
            if rank_rows:
                sheets["Strategy Ranking"] = pd.DataFrame([
                    {
                        "Rank": r["rank"],
                        "Strategy": r["strategy_name"],
                        "Total P&L (CNY)": r["pnl_total_yuan"],
                        "Market P&L (CNY)": r.get("pnl_market_yuan"),
                        "Subsidy (CNY)": r.get("pnl_compensation_yuan"),
                        "Gap vs PF (CNY)": r["gap_vs_perfect_foresight_yuan"],
                        "Capture vs PF": r["capture_rate_vs_pf"],
                        "Granularity": r["granularity"],
                        "Available": r["data_available"],
                    }
                    for r in rank_rows
                ])

            pnl_tbl = report.get("pnl_comparison", {})
            if pnl_tbl.get("rows"):
                sheets["P&L Comparison"] = pd.DataFrame(
                    pnl_tbl["rows"], columns=pnl_tbl["headers"]
                )

            period_rows_key = f"{period_type}_rows"
            period_rows = report.get(period_rows_key, [])
            if period_rows:
                sheets[f"{period_type.title()} P&L"] = pd.DataFrame(period_rows)

            daily_attr = attribution.get("daily_rows", [])
            if daily_attr:
                sheets["Attribution Daily"] = pd.DataFrame(daily_attr)

            if sheets:
                xl_bytes = to_excel_bytes(sheets)
                col_xl.download_button(
                    "⬇ Excel Tables",
                    data=xl_bytes,
                    file_name=f"strategy_{asset_code}_{date_from}_{date_to}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"sc_xl_{asset_code}_{date_from}",
                )
            else:
                col_xl.caption("No table data to export.")
        except Exception as exc:
            col_xl.caption(f"Excel error: {exc}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_yuan(v) -> str:
    if v is None:
        return "—"
    try:
        return f"{float(v):,.0f}"
    except (TypeError, ValueError):
        return "—"


def _fmt_pct(v) -> str:
    if v is None:
        return "—"
    try:
        return f"{float(v):.1%}"
    except (TypeError, ValueError):
        return "—"


def _show_data_quality(st_module, notes: list) -> None:
    if not notes:
        return
    # Show only warnings / errors (lines mentioning "failed", "no data", "null", "missing")
    important = [n for n in notes if any(
        kw in n.lower() for kw in ["failed", "no data", "null", "missing", "not yet", "todo"]
    )]
    if important:
        with st_module.expander(f"⚠ Data quality notes ({len(important)})", expanded=False):
            for n in important:
                st_module.caption(f"• {n}")
