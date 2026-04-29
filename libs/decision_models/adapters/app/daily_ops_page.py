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
from typing import Any, Dict, Optional

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

    from libs.decision_models.adapters.app.export_utils import (
        reportlab_available, to_excel_bytes, to_pdf_bytes_from_markdown,
    )
    from libs.decision_models.workflows.daily_strategy_report import (
        build_cross_asset_summary,
        generate_bess_daily_strategy_report,
        render_bess_strategy_dashboard_payload,
        run_bess_daily_strategy_analysis,
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

    # ── Session state: run on button click; keep results until next click ──────
    _SS_KEY = "daily_ops_cached"
    if run_btn:
        # Single-asset mode
        if asset_mode == "Single asset" and selected_asset:
            _display = _IM_ASSET_DISPLAY.get(selected_asset, selected_asset)
            with st.status(
                f"Analysing {_display} on {date_str}…", expanded=True
            ) as _run_status:
                st.write("Step 1/2 — Running strategy analysis (DB queries + LP benchmark)…")
                analysis = run_bess_daily_strategy_analysis(
                    selected_asset, date_str, use_ops_dispatch=use_ops,
                )
                st.write("Step 2/2 — Building dashboard payload…")
                payload = render_bess_strategy_dashboard_payload(
                    selected_asset, date_str, analysis=analysis,
                )
                _run_status.update(label=f"{_display} — complete", state="complete")
            st.session_state[_SS_KEY] = {
                "mode": "single",
                "date_str": date_str,
                "asset": selected_asset,
                "analysis": analysis,
                "payload": payload,
            }
        else:
            # All-assets mode — run all 4 in parallel; update badges as each finishes.
            # Hard wall-clock limit: _TIMEOUT_S seconds per asset before marking as timed-out.
            import time as _time
            from concurrent.futures import (
                ThreadPoolExecutor as _TPE,
                wait as _wait,
                FIRST_COMPLETED as _FIRST_COMPLETED,
            )

            _TIMEOUT_S = 360  # 6-minute wall-clock limit per asset

            _n = len(_IM_ASSET_CODES)
            _badge_cols = st.columns(_n)
            _slots = {
                code: col.empty()
                for code, col in zip(_IM_ASSET_CODES, _badge_cols)
            }
            for _code in _IM_ASSET_CODES:
                _slots[_code].info(f"⏳ {_IM_ASSET_DISPLAY.get(_code, _code)}")
            _progress = st.progress(0.0, text="Running 4 assets in parallel…")

            _asset_results: dict = {}
            _errors: dict = {}
            _deadline = _time.monotonic() + _TIMEOUT_S
            with _TPE(max_workers=_n) as _executor:
                _future_to_code = {
                    _executor.submit(
                        run_bess_daily_strategy_analysis,
                        _code, date_str, use_ops_dispatch=use_ops,
                    ): _code
                    for _code in _IM_ASSET_CODES
                }
                _pending = set(_future_to_code.keys())
                _n_done = 0
                while _pending:
                    _remaining = max(0.0, _deadline - _time.monotonic())
                    _done_set, _pending = _wait(
                        _pending, timeout=_remaining, return_when=_FIRST_COMPLETED
                    )
                    # Process completed futures
                    for _future in _done_set:
                        _code = _future_to_code[_future]
                        _disp = _IM_ASSET_DISPLAY.get(_code, _code)
                        _n_done += 1
                        try:
                            _asset_results[_code] = _future.result()
                            _slots[_code].success(f"✓ {_disp}")
                        except Exception as _exc:
                            _errors[_code] = str(_exc)
                            _slots[_code].error(f"✗ {_disp}: {_exc}")
                        _progress.progress(
                            _n_done / _n,
                            text=f"{_n_done}/{_n} assets complete"
                            + (f" ({len(_errors)} errors)" if _errors else ""),
                        )
                    # Deadline exceeded — cancel remaining futures
                    if _pending and _time.monotonic() >= _deadline:
                        for _future in _pending:
                            _future.cancel()
                            _code = _future_to_code[_future]
                            _disp = _IM_ASSET_DISPLAY.get(_code, _code)
                            _errors[_code] = f"timed out after {_TIMEOUT_S}s"
                            _slots[_code].warning(f"⏱ {_disp}: timed out")
                            _n_done += 1
                            _progress.progress(
                                _n_done / _n,
                                text=f"{_n_done}/{_n} assets complete ({len(_errors)} errors/timeouts)",
                            )
                        _pending.clear()
                        st.warning(
                            f"Analysis timed out after {_TIMEOUT_S}s. "
                            "Some assets were not processed. Check LP solver or DB connection."
                        )
                        break
            all_results = {
                "date": date_str,
                "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "asset_results": _asset_results,
                "summary": build_cross_asset_summary(date_str, _asset_results),
                "errors": _errors,
            }
            st.session_state[_SS_KEY] = {
                "mode": "all",
                "date_str": date_str,
                "all_results": all_results,
            }

    cached = st.session_state.get(_SS_KEY)
    if not cached:
        st.info(
            "Select a date and click **Run daily analysis** to compare strategies. "
            "Ops data is loaded from `marketdata.ops_bess_dispatch_15min`."
        )
        return

    # ── Render from cached state ─────────────────────────────────────────────
    if cached["mode"] == "single":
        _render_single_asset(st, cached["asset"], cached["date_str"], cached["payload"])
        _render_export_section(st, cached["asset"], cached["date_str"],
                               cached["analysis"], cached["payload"])
        return

    # All-assets mode
    all_results = cached["all_results"]
    date_str = cached["date_str"]

    errors = all_results.get("errors", {})
    if errors:
        for asset, err in errors.items():
            st.error(f"{asset}: {err}")

    summary = all_results.get("summary", {})
    _render_portfolio_summary(st, summary)
    _render_portfolio_export(st, all_results, date_str)

    # Per-asset tabs
    tabs = st.tabs([_IM_ASSET_DISPLAY.get(c, c) for c in _IM_ASSET_CODES])
    for tab, asset_code in zip(tabs, _IM_ASSET_CODES):
        with tab:
            result = all_results.get("asset_results", {}).get(asset_code)
            if result is None:
                st.warning(f"No result for {asset_code}.")
                continue
            payload = render_bess_strategy_dashboard_payload(
                asset_code, date_str, analysis=result,
            )
            _render_single_asset(st, asset_code, date_str, payload)
            _render_export_section(st, asset_code, date_str, result, payload)


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
                "Asset": _IM_ASSET_DISPLAY.get(r["asset_code"], r["asset_code"]),
                "Actual P&L (CNY)": _fmt_yuan(r.get("actual_pnl")),
                "Avg Daily P&L (CNY)": _fmt_yuan(r.get("avg_daily_pnl")),
                "PF Benchmark (CNY)": _fmt_yuan(r.get("pf_pnl")),
                "Capture Rate": _fmt_pct(r.get("capture_rate")),
                "Avg Daily Cycles": (
                    f"{r['avg_daily_cycles']:.2f}"
                    if r.get("avg_daily_cycles") is not None else "—"
                ),
                "Price Spread (CNY/MWh)": (
                    f"{r['captured_spread_yuan_per_mwh']:,.0f}"
                    if r.get("captured_spread_yuan_per_mwh") is not None else "—"
                ),
                "Cycle Efficiency": (
                    f"{r['cycle_efficiency']:.3f}"
                    if r.get("cycle_efficiency") is not None else "—"
                ),
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

    tabs = st.tabs(["Strategy Ranking", "Dispatch Chart",
                    "Discrepancy Waterfall", "Report"])

    # ── Tab: Strategy Ranking ────────────────────────────────────────────────
    with tabs[0]:
        st.subheader("Strategy Ranking")
        table = payload.get("strategy_table", [])
        if table:
            available_rows = [r for r in table if r.get("Available", False)]
            unavailable_rows = [r for r in table if not r.get("Available", False)]
            _display_cols = [c for c in table[0].keys() if c != "Available"]
            if available_rows:
                df_avail = pd.DataFrame(available_rows)[_display_cols]
                st.dataframe(df_avail, use_container_width=True, hide_index=True)
            else:
                st.info("No strategy ranking data with available results.")
            if unavailable_rows:
                with st.expander(f"Unavailable strategies ({len(unavailable_rows)})", expanded=False):
                    df_unavail = pd.DataFrame(unavailable_rows)[_display_cols]
                    st.dataframe(df_unavail, use_container_width=True, hide_index=True)
        else:
            st.info("No strategy ranking data.")
        st.caption(
            "Note: Hourly (PF / forecast) and 15-min (nominated / actual) P&L are not "
            "directly comparable in absolute terms — use gaps directionally."
        )

    # ── Tab: Dispatch Chart ──────────────────────────────────────────────────
    with tabs[1]:
        chart_data = payload.get("dispatch_chart_data", {})
        timestamps = chart_data.get("timestamps", [])
        nominated = chart_data.get("nominated_mwh", [])
        actual = chart_data.get("actual_mwh", [])
        pf_timestamps = chart_data.get("pf_timestamps", [])
        pf_dispatch = chart_data.get("pf_dispatch_mwh", [])
        source = chart_data.get("source", "—")

        id_cleared_timestamps = chart_data.get("id_cleared_timestamps", [])
        id_cleared = chart_data.get("id_cleared_mwh", [])

        price_data = payload.get("price_chart_data", {})
        price_ts = price_data.get("timestamps_15min", [])
        price_vals = price_data.get("prices_15min", [])

        if timestamps or pf_timestamps or id_cleared_timestamps or price_ts:
            try:
                import plotly.graph_objects as go
                from plotly.subplots import make_subplots

                def _strip_tz(idx):
                    """Convert TZ-aware index to CST naive. Naive assumed already CST."""
                    if idx.tz is not None:
                        return pd.DatetimeIndex(
                            [t.replace(tzinfo=None) for t in idx.tz_convert("Asia/Shanghai")]
                        )
                    # Naive timestamps: TIMESTAMP columns from psycopg2 already return
                    # wall-clock CST, and LP-generated timestamps are also CST-naive.
                    return idx

                # ── Subplot: row 1 = dispatch, row 2 = RT price ────────────────
                fig = make_subplots(
                    rows=2, cols=1,
                    shared_xaxes=True,
                    row_heights=[0.65, 0.35],
                    vertical_spacing=0.06,
                    subplot_titles=("Dispatch (MWh per 15-min interval)", "RT Price (CNY/MWh)"),
                )

                # ── Actual dispatch: solid semi-transparent bar (row 1) ────────
                if timestamps and actual:
                    _act_idx = _strip_tz(pd.to_datetime(timestamps))
                    # Bar width: 14 min in ms so adjacent bars touch but don't overlap
                    _bar_width_ms = 14 * 60 * 1000
                    fig.add_trace(
                        go.Bar(
                            x=_act_idx,
                            y=actual,
                            name="Actual",
                            marker=dict(color="#DD8452", opacity=0.45),
                            width=_bar_width_ms,
                            hovertemplate="%{y:.3f} MWh<extra>Actual</extra>",
                        ),
                        row=1, col=1,
                    )

                # ── Other dispatch series: step lines on top (row 1) ──────────
                _step_series = [
                    ("Nominated",   timestamps,            nominated,   "#4C72B0", "solid"),
                    ("PF Dispatch", pf_timestamps,         pf_dispatch, "#55A868", "dash"),
                    ("DA Cleared",  id_cleared_timestamps, id_cleared,  "#C44E52", "dot"),
                ]
                for _label, _ts, _vals, _color, _dash in _step_series:
                    if _ts and _vals:
                        _idx = _strip_tz(pd.to_datetime(_ts))
                        fig.add_trace(
                            go.Scatter(
                                x=_idx,
                                y=_vals,
                                mode="lines",
                                name=_label,
                                line=dict(color=_color, dash=_dash, width=1.8, shape="hv"),
                                hovertemplate="%{y:.3f} MWh<extra>" + _label + "</extra>",
                            ),
                            row=1, col=1,
                        )

                # ── RT Price line (row 2) ──────────────────────────────────────
                if price_ts and price_vals:
                    _p_idx = _strip_tz(pd.to_datetime(price_ts))
                    fig.add_trace(
                        go.Scatter(
                            x=_p_idx,
                            y=price_vals,
                            mode="lines",
                            name="RT Price",
                            line=dict(color="#9467BD", width=1.5),
                            hovertemplate="%{y:,.0f} CNY/MWh<extra>RT Price</extra>",
                            showlegend=True,
                        ),
                        row=2, col=1,
                    )

                fig.update_layout(
                    height=520,
                    hovermode="x unified",
                    legend=dict(
                        orientation="h",
                        yanchor="bottom", y=1.04,
                        xanchor="left", x=0,
                    ),
                    margin=dict(l=60, r=20, t=70, b=40),
                    barmode="overlay",
                )
                fig.update_yaxes(title_text="MWh / 15-min", row=1, col=1)
                fig.update_yaxes(title_text="CNY/MWh", title_font=dict(color="#9467BD"),
                                 tickfont=dict(color="#9467BD"), row=2, col=1)
                fig.update_xaxes(title_text="Time (CST)", row=2, col=1)
                st.plotly_chart(fig, use_container_width=True)

            except Exception as _e:
                st.info(f"Could not render dispatch chart: {_e}")
            st.caption(
                f"Source: {source}. "
                "Dispatch: bars=Actual (semi-transparent), step lines=Nominated/PF/DA Cleared. "
                "Positive=discharge, negative=charge. "
                "PF Dispatch = LP perfect-foresight MW ÷ 4. "
                "DA Cleared = md_id_cleared_energy (DA market award, not physical dispatch)."
            )
        else:
            st.info("No dispatch data available for this date.")

    # ── Tab: Discrepancy Waterfall ───────────────────────────────────────────
    with tabs[2]:
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

    # ── Tab: Report (markdown) ────────────────────────────────────────────────
    with tabs[3]:
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
# Portfolio export section (all-4-assets mode)
# ---------------------------------------------------------------------------

def _render_portfolio_export(
    st_module,
    all_results: Dict[str, Any],
    date_str: str,
) -> None:
    """PDF + Excel download for the 4-asset portfolio view."""
    import pandas as pd
    from libs.decision_models.adapters.app.export_utils import (
        reportlab_available, to_excel_bytes, to_pdf_bytes_from_tables,
    )
    st = st_module

    with st.expander("📥 Download portfolio report", expanded=False):
        col_pdf, col_xl = st.columns(2)

        summary = all_results.get("summary", {})
        asset_rows = summary.get("asset_rows", [])

        # Build summary DataFrame
        summary_df = pd.DataFrame([
            {
                "Asset": r["asset_code"],
                "Display": _IM_ASSET_DISPLAY.get(r["asset_code"], r["asset_code"]),
                "Actual P&L (CNY)": r.get("actual_pnl"),
                "PF Benchmark (CNY)": r.get("pf_pnl"),
                "Capture Rate": r.get("capture_rate"),
                "Ops Data": "Yes" if r.get("ops_dispatch_available") else "No",
                "Best Strategy": r.get("best_strategy", "—"),
            }
            for r in asset_rows
        ]) if asset_rows else pd.DataFrame()

        # Per-asset strategy tables
        per_asset_sheets: dict = {}
        for asset_code in _IM_ASSET_CODES:
            result = all_results.get("asset_results", {}).get(asset_code)
            if result is None:
                continue
            ranking_rows = result.get("ranking", {}).get("rows", [])
            if ranking_rows:
                per_asset_sheets[f"{asset_code[:12]}_ranking"] = pd.DataFrame([
                    {
                        "Strategy": r["strategy_name"],
                        "Total P&L (CNY)": r.get("pnl_total_yuan"),
                        "Market P&L (CNY)": r.get("pnl_market_yuan"),
                        "Subsidy (CNY)": r.get("pnl_compensation_yuan"),
                        "Gap vs PF (CNY)": r.get("gap_vs_perfect_foresight_yuan"),
                        "Capture vs PF": r.get("capture_rate_vs_pf"),
                        "Available": r.get("data_available"),
                    }
                    for r in ranking_rows
                ])

        # PDF
        if reportlab_available():
            try:
                sections = []
                if not summary_df.empty:
                    sections.append({"heading": "Portfolio Summary", "df": summary_df})
                for sheet_name, df in per_asset_sheets.items():
                    sections.append({"heading": sheet_name.replace("_", " ").title(), "df": df})
                if sections:
                    from libs.decision_models.adapters.app.export_utils import to_pdf_bytes_from_tables
                    pdf_bytes = to_pdf_bytes_from_tables(
                        f"IM BESS Portfolio Daily Report — {date_str}",
                        sections=sections,
                    )
                    if pdf_bytes:
                        col_pdf.download_button(
                            "⬇ PDF Portfolio Report",
                            data=pdf_bytes,
                            file_name=f"portfolio_{date_str}.pdf",
                            mime="application/pdf",
                            key=f"portfolio_pdf_{date_str}",
                        )
            except Exception as exc:
                col_pdf.caption(f"PDF error: {exc}")
        else:
            col_pdf.caption("Install `reportlab` to enable PDF export.")

        # Excel
        try:
            sheets: dict = {}
            if not summary_df.empty:
                sheets["Portfolio Summary"] = summary_df
            sheets.update(per_asset_sheets)
            if sheets:
                xl_bytes = to_excel_bytes(sheets)
                col_xl.download_button(
                    "⬇ Excel Portfolio Tables",
                    data=xl_bytes,
                    file_name=f"portfolio_{date_str}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"portfolio_xl_{date_str}",
                )
            else:
                col_xl.caption("No data to export.")
        except Exception as exc:
            col_xl.caption(f"Excel error: {exc}")


# ---------------------------------------------------------------------------
# Export section
# ---------------------------------------------------------------------------

def _render_export_section(
    st_module,
    asset_code: str,
    date_str: str,
    analysis: Dict[str, Any],
    payload: Dict[str, Any],
) -> None:
    """Render PDF + Excel download buttons below the strategy view."""
    import pandas as pd
    from libs.decision_models.adapters.app.export_utils import (
        reportlab_available, to_excel_bytes, to_pdf_bytes_from_markdown,
    )
    from libs.decision_models.workflows.daily_strategy_report import (
        generate_bess_daily_strategy_report,
    )

    st = st_module
    with st.expander("📥 Download report", expanded=False):
        col_pdf, col_xl = st.columns(2)

        # ── PDF ──────────────────────────────────────────────────────────
        if reportlab_available():
            try:
                pdf_bytes = generate_bess_daily_strategy_report(
                    asset_code, date_str,
                    output_format="pdf",
                    analysis=analysis,
                )
                col_pdf.download_button(
                    "⬇ PDF Report",
                    data=pdf_bytes,
                    file_name=f"daily_ops_{asset_code}_{date_str}.pdf",
                    mime="application/pdf",
                    key=f"pdf_{asset_code}_{date_str}",
                )
            except Exception as exc:
                col_pdf.caption(f"PDF error: {exc}")
        else:
            col_pdf.caption("Install `reportlab` to enable PDF export.")

        # ── Excel ─────────────────────────────────────────────────────────
        try:
            sheets: dict = {}

            if payload.get("strategy_table"):
                sheets["Strategy Ranking"] = pd.DataFrame(payload["strategy_table"])

            pnl_comp = payload.get("pnl_comparison", {})
            if pnl_comp.get("rows"):
                sheets["P&L Comparison"] = pd.DataFrame(
                    pnl_comp["rows"], columns=pnl_comp["headers"]
                )

            wf = payload.get("waterfall_data", {})
            buckets = wf.get("buckets", [])
            if buckets:
                sheets["Waterfall"] = pd.DataFrame([
                    {"Bucket": b["label"], "Value (CNY)": b.get("value_yuan")}
                    for b in buckets
                ])

            chart = payload.get("dispatch_chart_data", {})
            if chart.get("timestamps"):
                dispatch_dict: dict = {"time": chart["timestamps"]}
                if chart.get("nominated_mwh"):
                    dispatch_dict["Nominated (MWh)"] = chart["nominated_mwh"]
                if chart.get("actual_mwh"):
                    dispatch_dict["Actual (MWh)"] = chart["actual_mwh"]
                if chart.get("id_cleared_mwh"):
                    dispatch_dict["DA Cleared Energy (MWh)"] = chart["id_cleared_mwh"]
                sheets["Dispatch 15min"] = pd.DataFrame(dispatch_dict)
            if chart.get("pf_timestamps"):
                sheets["PF Dispatch Hourly"] = pd.DataFrame({
                    "time": chart["pf_timestamps"],
                    "PF Dispatch (MWh per 15min)": chart["pf_dispatch_mwh"],
                })

            price = payload.get("price_chart_data", {})
            if price.get("timestamps_15min"):
                sheets["RT Prices 15min"] = pd.DataFrame({
                    "time": price["timestamps_15min"],
                    "RT Price (CNY/MWh)": price["prices_15min"],
                })

            if sheets:
                xl_bytes = to_excel_bytes(sheets)
                col_xl.download_button(
                    "⬇ Excel Tables",
                    data=xl_bytes,
                    file_name=f"daily_ops_{asset_code}_{date_str}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"xl_{asset_code}_{date_str}",
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
