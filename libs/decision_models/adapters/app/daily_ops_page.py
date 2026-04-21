"""
libs/decision_models/adapters/app/daily_ops_page.py

Streamlit page for the Inner Mongolia BESS daily operations strategy view.

Shows the 4 Inner Mongolia assets side-by-side for a selected date:
  - Executive summary cards
  - Strategy ranking table
  - Dispatch comparison (nominated vs actual)
  - Price chart
  - Discrepancy waterfall
  - Data quality caveats

Drop into any Streamlit app:
    from libs.decision_models.adapters.app.daily_ops_page import render_daily_ops_page
    render_daily_ops_page()

All model and DB logic is delegated to:
  - libs/decision_models/workflows/daily_strategy_report.py
  - libs/decision_models/workflows/strategy_comparison.py
  - libs/decision_models/resources/bess_context.py
This file is pure presentation.
"""
from __future__ import annotations

import datetime
from typing import Dict, Any, Optional

# The 4 Inner Mongolia assets from the ops ingestion pipeline
_IM_ASSET_CODES = ["suyou", "hangjinqi", "siziwangqi", "gushanliang"]
_IM_ASSET_DISPLAY = {
    "suyou": "SuYou (景蓝乌尔图)",
    "hangjinqi": "HangJinQi (悦杭独贵)",
    "siziwangqi": "SiZiWangQi (景通四益堂储)",
    "gushanliang": "GuShanLiang (裕昭沙子坝)",
}


def render_daily_ops_page() -> None:
    import pandas as pd
    import streamlit as st

    from libs.decision_models.workflows.daily_strategy_report import (
        render_bess_strategy_dashboard_payload,
        run_all_assets_daily_strategy_analysis,
        run_bess_daily_strategy_analysis,
        generate_bess_daily_strategy_report,
    )

    st.header("Inner Mongolia BESS — Daily Operations Strategy")
    st.caption(
        "Daily dispatch strategy comparison for the 4 Inner Mongolia assets. "
        "Sources: Excel ops files (nominated/actual) + canonical prices + LP benchmark."
    )

    # ── Sidebar controls ────────────────────────────────────────────────────
    with st.sidebar:
        st.subheader("Parameters")
        report_date = st.date_input(
            "Report date",
            value=datetime.date.today() - datetime.timedelta(days=1),
        )
        date_str = str(report_date)

        asset_mode = st.radio(
            "Asset scope",
            ["All 4 Inner Mongolia assets", "Single asset"],
            index=0,
        )
        selected_asset = None
        if asset_mode == "Single asset":
            selected_asset = st.selectbox(
                "Asset",
                _IM_ASSET_CODES,
                format_func=lambda x: f"{x} / {_IM_ASSET_DISPLAY.get(x, x)}",
            )

        use_ops = st.checkbox("Prefer ops dispatch data (recommended)", value=True)
        run_btn = st.button("Run daily analysis", type="primary")

    if not run_btn:
        st.info(
            "Select a date and click **Run daily analysis** to compare strategies. "
            "Ops data is loaded from `marketdata.ops_bess_dispatch_15min`."
        )
        return

    # ── Single-asset mode ───────────────────────────────────────────────────
    if asset_mode == "Single asset" and selected_asset:
        with st.spinner(f"Running analysis for {selected_asset} on {date_str}…"):
            payload = render_bess_strategy_dashboard_payload(
                selected_asset, date_str, use_ops_dispatch=use_ops,
            )
        _render_single_asset(st, selected_asset, date_str, payload)
        return

    # ── All-assets mode ──────────────────────────────────────────────────────
    with st.spinner(f"Running analysis for all 4 assets on {date_str}…"):
        all_results = run_all_assets_daily_strategy_analysis(
            date_str, use_ops_dispatch=use_ops,
        )

    errors = all_results.get("errors", {})
    if errors:
        for asset, err in errors.items():
            st.error(f"{asset}: {err}")

    summary = all_results.get("summary", {})
    _render_portfolio_summary(st, summary)

    # Per-asset tabs
    tabs = st.tabs([_IM_ASSET_DISPLAY.get(c, c) for c in _IM_ASSET_CODES])
    for tab, asset_code in zip(tabs, _IM_ASSET_CODES):
        with tab:
            result = all_results.get("asset_results", {}).get(asset_code)
            if result is None:
                st.warning(f"No result for {asset_code}.")
                continue
            # Re-render the payload from the pre-computed analysis
            payload = render_bess_strategy_dashboard_payload(
                asset_code, date_str, analysis=result,
            )
            _render_single_asset(st, asset_code, date_str, payload)


def _render_portfolio_summary(st_module, summary: Dict[str, Any]) -> None:
    """Render a cross-asset portfolio summary row."""
    import pandas as pd
    st = st_module

    st.subheader("Portfolio Summary")
    total_actual = summary.get("portfolio_total_actual_pnl")
    total_pf = summary.get("portfolio_total_pf_pnl")
    capture = summary.get("portfolio_capture_rate")
    n_ops = summary.get("n_assets_with_ops_data", 0)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Portfolio Actual P&L", _fmt_yuan(total_actual) + " CNY")
    col2.metric("Portfolio PF Benchmark", _fmt_yuan(total_pf) + " CNY")
    col3.metric("Portfolio Capture Rate", _fmt_pct(capture))
    col4.metric("Assets with Ops Data", f"{n_ops} / {len(_IM_ASSET_CODES)}")

    # Per-asset summary table
    asset_rows = summary.get("asset_rows", [])
    if asset_rows:
        df = pd.DataFrame([
            {
                "Asset": r["asset_code"],
                "Display": _IM_ASSET_DISPLAY.get(r["asset_code"], r["asset_code"]),
                "Actual P&L (CNY)": _fmt_yuan(r.get("actual_pnl")),
                "PF Benchmark (CNY)": _fmt_yuan(r.get("pf_pnl")),
                "Capture Rate": _fmt_pct(r.get("capture_rate")),
                "Ops Data": "✓" if r.get("ops_dispatch_available") else "—",
                "Best Strategy": r.get("best_strategy", "—"),
            }
            for r in asset_rows
        ])
        st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()


def _render_single_asset(
    st_module,
    asset_code: str,
    date_str: str,
    payload: Dict[str, Any],
) -> None:
    """Render the full strategy view for one asset."""
    import pandas as pd
    st = st_module

    display = _IM_ASSET_DISPLAY.get(asset_code, asset_code)
    ops_available = payload.get("ops_dispatch_available", False)

    st.subheader(f"{display} — {date_str}")
    if ops_available:
        st.caption("✓ Ops dispatch data available from Excel ingestion pipeline")
    else:
        st.caption(
            "⚠ No ops dispatch data — check that Excel file for this date has been ingested "
            "(`marketdata.ops_bess_dispatch_15min`)"
        )

    _show_data_quality(st, payload.get("caveats", []))

    # Summary metrics
    cards = payload.get("summary_cards", [])
    if cards:
        cols = st.columns(len(cards))
        for col, card in zip(cols, cards):
            col.metric(
                label=card["label"],
                value=card["value"],
                delta=card.get("delta"),
            )
    st.markdown("---")

    tabs = st.tabs(["Strategy Ranking", "Dispatch Chart", "Price Chart",
                    "Discrepancy Waterfall", "P&L Comparison", "Report"])

    # ── Tab: Strategy Ranking ────────────────────────────────────────────────
    with tabs[0]:
        st.subheader("Strategy Ranking")
        table = payload.get("strategy_table", [])
        if table:
            df = pd.DataFrame(table)
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No strategy ranking data.")
        st.caption(
            "Note: Hourly (PF / forecast) and 15-min (nominated / actual) P&L are not "
            "directly comparable in absolute terms — use gaps directionally."
        )

    # ── Tab: Dispatch Chart ──────────────────────────────────────────────────
    with tabs[1]:
        st.subheader("Nominated vs Actual Dispatch (MW)")
        chart_data = payload.get("dispatch_chart_data", {})
        timestamps = chart_data.get("timestamps", [])
        nominated = chart_data.get("nominated_mw", [])
        actual = chart_data.get("actual_mw", [])
        source = chart_data.get("source", "—")

        if timestamps:
            try:
                chart_df = pd.DataFrame({
                    "time": pd.to_datetime(timestamps),
                    "Nominated (MW)": nominated,
                    "Actual (MW)": actual,
                }).set_index("time")
                st.line_chart(chart_df)
            except Exception:
                st.info("Could not render dispatch chart.")
            st.caption(f"Source: {source}")
        else:
            st.info("No dispatch data available for this date.")

    # ── Tab: Price Chart ─────────────────────────────────────────────────────
    with tabs[2]:
        st.subheader("RT Price (CNY/MWh)")
        price_data = payload.get("price_chart_data", {})
        ts_15 = price_data.get("timestamps_15min", [])
        p_15 = price_data.get("prices_15min", [])
        if ts_15:
            try:
                price_df = pd.DataFrame({
                    "time": pd.to_datetime(ts_15),
                    "RT Price 15min (CNY/MWh)": p_15,
                }).set_index("time")
                st.line_chart(price_df)
            except Exception:
                st.info("Could not render price chart.")
        else:
            st.info("No price data available for this date.")

    # ── Tab: Discrepancy Waterfall ───────────────────────────────────────────
    with tabs[3]:
        st.subheader("Discrepancy Attribution Waterfall")
        st.caption(
            "Rules-based waterfall — not causal proof. "
            "asset_issue is always None until an outage table is implemented."
        )
        wf = payload.get("waterfall_data", {})
        total_gap = wf.get("total_gap")
        if total_gap is not None:
            st.metric("Total gap (PF − actual)", f"{float(total_gap):,.0f} CNY")

        bucket_list = wf.get("buckets", [])
        if bucket_list:
            bucket_rows = []
            for b in bucket_list:
                val = b.get("value_yuan")
                bucket_rows.append({
                    "Bucket": b["label"],
                    "Loss (CNY)": _fmt_yuan(val),
                    "% of total gap": (
                        _fmt_pct(val / total_gap)
                        if (val is not None and total_gap and total_gap != 0)
                        else "—"
                    ),
                })
            st.dataframe(
                pd.DataFrame(bucket_rows),
                use_container_width=True,
                hide_index=True,
            )

    # ── Tab: P&L Comparison ──────────────────────────────────────────────────
    with tabs[4]:
        st.subheader("P&L Comparison Table")
        pnl_comp = payload.get("pnl_comparison", {})
        if pnl_comp.get("rows"):
            pnl_df = pd.DataFrame(pnl_comp["rows"], columns=pnl_comp["headers"])
            st.dataframe(pnl_df, use_container_width=True, hide_index=True)
        else:
            st.info("No P&L comparison data.")

    # ── Tab: Report (markdown) ────────────────────────────────────────────────
    with tabs[5]:
        st.subheader("Report (Markdown)")
        # Regenerate markdown from the stored analysis if needed
        # (payload does not store the full markdown to keep it lean)
        st.caption(
            "Use `generate_bess_daily_strategy_report(asset_code, date, output_format='markdown')` "
            "to get the full markdown text, or 'pdf' / 'html' for other formats."
        )
        st.code(
            f"from libs.decision_models.workflows.daily_strategy_report import "
            f"generate_bess_daily_strategy_report\n"
            f"report = generate_bess_daily_strategy_report('{asset_code}', '{date_str}')",
            language="python",
        )


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
    important = [n for n in notes if any(
        kw in n.lower() for kw in [
            "failed", "no data", "null", "missing", "not yet", "todo",
            "no ops", "ops ingestion", "check",
        ]
    )]
    if important:
        with st_module.expander(f"⚠ Data quality notes ({len(important)})", expanded=False):
            for n in important:
                st_module.caption(f"• {n}")
