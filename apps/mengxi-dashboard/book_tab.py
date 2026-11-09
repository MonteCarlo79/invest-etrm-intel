"""Trader Book tab — one 蒙西 station's asset book (trading-book plan Task 6,
2026-10-31). Rendered inside apps/mengxi-dashboard/app.py as the "Book" tab.

Sections:
  (a) Dispatch chain — one day: strategy vs cleared vs actual MW, with the
      cleared price on a secondary axis.
  (b) Attribution waterfall — strategy P&L → 5 named deltas → settled P&L
      over a date range (libs.trading_book.waterfall.build_waterfall via
      AssetBook.attribution). The settlement_residual line is a BALANCING
      line: it absorbs the deliberate overlap between decompositions plus
      settlement-only items (capacity compensation, rebates, assessment
      charges) so the chain lands exactly on settled — the copy says
      "balancing", never "settlement items" (carried review ruling).
  (c) Settled rows — reports.book_settled_daily table + bill-vs-accrual
      reconciliation (daily stacked by accrual_grade, cumulative overlay).
  (d) Backtest / 回测 — the per-day value-loss ladder from
      reports.bess_asset_daily_attribution over its own date range (default
      last 30 days): cumulative P&L chart (nominated vs DP-optimal vs cleared
      actual vs PF grid-feasible), per-day ladder table, and a summary strip
      (strategy-selection cost, realisation gap, capture rate). Reads only
      the attribution table, so it sits BEFORE the settled-table gate and
      renders regardless of reports.book_settled_daily. settlement_only
      books get an st.info note (no dispatch chain to backtest);
      registry_only books skip the section entirely.

Degenerate-waterfall guard (carried review ruling): build_waterfall prints a
stdout warning when a range has settled rows but ZERO attribution rows
(residual = settled). stdout is invisible in Streamlit, so the tab re-checks
the same condition (attribution count via the library's own params — alias
applied there — and settled count from the section (c) frame) and surfaces
it as st.warning.

Settled-table gate: reports.book_settled_daily is created by Task 4's
refresh_settled_daily writer, not by migration — until that first ops run
happens the table may not exist at all. The tab probes once per render
(to_regclass) and degrades sections (b)/(c) to a warning with the
remediation command instead of dying on UndefinedTable; the dispatch chain
renders regardless.

Data sources (all via libs.trading_book, Tasks 2/3/4/5):
  marketdata.md_id_cleared_energy, marketdata.ops_bess_dispatch_15min,
  marketdata.nodal_strategy_daily, reports.bess_asset_daily_attribution,
  reports.book_settled_daily.
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

from libs.trading_book import backtest as _backtest
from libs.trading_book.asset_book import AssetBook
from libs.trading_book.models import Waterfall
from libs.trading_book.registry import StationRef, get_stations
# Private import, deliberate: the registry->attribution asset-code alias
# (bameng->hetao) lives only in waterfall._attribution_params. Reusing it
# keeps the degenerate check on the exact same filter build_waterfall ran.
from libs.trading_book.waterfall import _attribution_params as _wf_attr_params

# Human labels for the waterfall's named lines. settlement_residual must read
# as the balancing line (carried ruling) — never "settlement items".
_LINE_LABELS = {
    "forecast_error": "Forecast error",
    "strategy_selection": "Strategy selection",
    "clearing": "Clearing",
    "curtailment_so": "Curtailment / SO",
    "equipment_site": "Equipment / site",
    "settlement_residual": "Balancing (residual)",
}

_ATTR_COUNT_SQL = """
    SELECT COUNT(*) AS n
    FROM reports.bess_asset_daily_attribution
    WHERE asset_code = :ac
      AND trade_date >= CAST(:s AS date)
      AND trade_date <= CAST(:e AS date)
"""

# to_regclass returns NULL when the relation is missing — one cheap probe, no
# exception path, for the settled-table gate (see module docstring).
_SETTLED_PROBE_SQL = "SELECT to_regclass('reports.book_settled_daily') AS r"

_SETTLED_SQL = """
    SELECT data_date, source, accrual_grade, amount_cny
    FROM reports.book_settled_daily
    WHERE asset_code = :ac
      AND data_date >= CAST(:s AS date)
      AND data_date <= CAST(:e AS date)
    ORDER BY data_date, source
"""

_SETTLED_GRADE_LABELS = {"bill": "Bill (settled)", "accrual": "Accrual (pro-rata)"}


def _waterfall_fig_data(w: Waterfall) -> tuple[list[str], list[float]]:
    """Waterfall -> plotly (labels, values): absolute strategy P&L, the named
    signed deltas in order, absolute settled P&L."""
    labels = ["Strategy P&L"]
    labels += [_LINE_LABELS.get(line.name, line.name.replace("_", " "))
               for line in w.lines]
    labels.append("Settled P&L")
    values = [w.strategy_pnl_cny]
    values += [line.delta_cny for line in w.lines]
    values.append(w.settled_pnl_cny)
    return labels, values


def _is_degenerate(n_attr_rows: int, settled_row_count: int) -> bool:
    """build_waterfall's loud-guard condition: settled rows exist but zero
    attribution rows — the waterfall degenerates to residual = settled."""
    return n_attr_rows == 0 and settled_row_count > 0


def _slot_labels() -> list[str]:
    """96 quarter-hour wall-clock labels (00:00 … 23:45)."""
    return [f"{h:02d}:{m:02d}" for h in range(24) for m in (0, 15, 30, 45)]


def _chain_fig(book: AssetBook, day: date) -> go.Figure:
    """One day's strategy / cleared / actual MW + cleared price (y2)."""
    cleared = book.cleared(day)
    actual = book.actual(day)
    strategies = book.strategies(day)
    x = _slot_labels()
    fig = go.Figure()
    if strategies:
        primary = next((s for s in strategies if s.primary), strategies[0])
        fig.add_trace(go.Scatter(x=x, y=primary.series, name="Strategy MW",
                                 line={"color": "#9467bd"}))
    fig.add_trace(go.Scatter(x=x, y=cleared.cleared_mw, name="Cleared MW",
                             line={"color": "#1f77b4"}))
    fig.add_trace(go.Scatter(x=x, y=actual.actual_mw, name="Actual MW",
                             line={"color": "#2ca02c"}))
    fig.add_trace(go.Scatter(x=x, y=cleared.cleared_price,
                             name="Cleared price (CNY/MWh)", yaxis="y2",
                             line={"color": "#ff7f0e", "dash": "dash"}))
    fig.update_layout(
        height=380, margin={"l": 10, "r": 10, "t": 30, "b": 10},
        legend={"orientation": "h", "y": 1.12},
        yaxis={"title": "MW"},
        yaxis2={"title": "CNY/MWh", "overlaying": "y", "side": "right",
                "showgrid": False},
    )
    return fig


def _waterfall_fig(w: Waterfall) -> go.Figure:
    labels, values = _waterfall_fig_data(w)
    measure = ["absolute"] + ["relative"] * (len(labels) - 2) + ["absolute"]
    fig = go.Figure(go.Waterfall(
        x=labels, y=values, measure=measure,
        connector={"line": {"color": "#9e9e9e"}},
        increasing={"marker": {"color": "#2ca02c"}},
        decreasing={"marker": {"color": "#d62728"}},
        totals={"marker": {"color": "#1f77b4"}},
    ))
    fig.update_layout(height=420, margin={"l": 10, "r": 10, "t": 30, "b": 10},
                      yaxis={"title": "CNY"})
    return fig


def _settled_table_available(engine) -> bool:
    """True once Task 4's refresh writer has created reports.book_settled_daily."""
    df = pd.read_sql(text(_SETTLED_PROBE_SQL), engine)
    return bool(df["r"].iloc[0] is not None)


def _load_settled_rows(engine, asset_code: str, start: date,
                       end: date) -> pd.DataFrame:
    df = pd.read_sql(text(_SETTLED_SQL), engine,
                     params={"ac": asset_code, "s": start.isoformat(),
                             "e": end.isoformat()})
    if not df.empty:
        df["amount_cny"] = df["amount_cny"].astype(float)
    return df


def _settled_recon_fig(settled_df: pd.DataFrame) -> go.Figure:
    """Daily settled CNY stacked by accrual_grade + cumulative (y2)."""
    daily = (settled_df.groupby(["data_date", "accrual_grade"])["amount_cny"]
             .sum().unstack(fill_value=0.0).sort_index())
    fig = go.Figure()
    for grade in ("bill", "accrual"):
        if grade in daily.columns:
            fig.add_trace(go.Bar(
                x=daily.index, y=daily[grade],
                name=_SETTLED_GRADE_LABELS.get(grade, grade),
            ))
    cum = daily.sum(axis=1).cumsum()
    fig.add_trace(go.Scatter(x=daily.index, y=cum, name="Cumulative settled",
                             yaxis="y2",
                             line={"color": "#1f77b4", "dash": "dash"}))
    fig.update_layout(
        height=340, barmode="stack",
        margin={"l": 10, "r": 10, "t": 30, "b": 10},
        legend={"orientation": "h", "y": 1.12},
        yaxis={"title": "CNY/day"},
        yaxis2={"title": "Cumulative CNY", "overlaying": "y", "side": "right",
                "showgrid": False},
    )
    return fig


# Backtest cumulative chart series — the brief's rung order: nominated vs
# tt_strategy (DP-optimal) vs cleared_actual vs pf_grid_feasible.
_BACKTEST_CUM_SERIES = [
    ("nominated_pnl", "Nominated", "#1f77b4"),
    ("tt_strategy_pnl", "DP-optimal (tt_strategy)", "#9467bd"),
    ("cleared_actual_pnl", "Cleared actual", "#2ca02c"),
    ("pf_grid_feasible_pnl", "PF grid-feasible", "#ff7f0e"),
]


def _backtest_cum_fig(df: pd.DataFrame) -> go.Figure:
    """Cumulative CNY per ladder rung over the backtest range. cumsum keeps
    running sums across NaN cells while the NaN days themselves stay gaps —
    partial scenario coverage shows as a hole, not a fake zero."""
    fig = go.Figure()
    for col, name, color in _BACKTEST_CUM_SERIES:
        fig.add_trace(go.Scatter(
            x=df["trade_date"], y=df[col].cumsum(), name=name, mode="lines",
            line={"color": color}))
    fig.update_layout(
        height=380, margin={"l": 10, "r": 10, "t": 30, "b": 10},
        legend={"orientation": "h", "y": 1.12},
        xaxis={"title": "Trade date"}, yaxis={"title": "Cumulative CNY"})
    return fig


def _render_backtest_section(engine, station: StationRef) -> None:
    """Backtest body for full-path stations: own 30-day-default range picker,
    coverage caption, summary strip, cumulative chart, per-day ladder table."""
    today = date.today()
    b1, b2 = st.columns(2)
    with b1:
        bt_start = st.date_input("Backtest start",
                                 value=today - timedelta(days=30),
                                 key="book_bt_start")
    with b2:
        bt_end = st.date_input("Backtest end", value=today,
                               key="book_bt_end")
    if bt_start > bt_end:
        st.error("Backtest start must be on or before backtest end.")
        return
    bt_df = _backtest.get_backtest(engine, station.asset_code,
                                   bt_start, bt_end)
    if bt_df.empty:
        st.info(f"No rows in reports.bess_asset_daily_attribution for "
                f"{station.plant_name} over {bt_start}..{bt_end} — check the "
                f"attribution pipeline "
                f"(services.monitoring.run_daily_attribution) coverage.")
        return
    summary = _backtest.summarise(bt_df)
    span_days = (bt_end - bt_start).days + 1
    st.caption(
        f"Coverage: **{summary['days']}** of {span_days} calendar days have "
        f"attribution rows; missing days are skipped — totals and ratios "
        f"cover present days only.")
    g1, g2, g3 = st.columns(3)
    ssc = summary["strategy_selection_cost"]
    g1.metric("Strategy-selection cost (nominated − DP-optimal)",
              "n/a" if ssc is None else f"¥{ssc:,.0f}")
    gap = summary["realisation_gap_vs_pf_grid"]
    g2.metric("Realisation gap (PF-grid − cleared actual)",
              "n/a" if gap is None else f"¥{gap:,.0f}")
    cap = summary["capture_nominated_vs_pf_grid"]
    g3.metric("Capture rate (nominated / PF-grid)",
              "n/a" if cap is None else f"{cap:.1%}")
    st.plotly_chart(_backtest_cum_fig(bt_df), use_container_width=True)
    st.dataframe(bt_df, use_container_width=True, hide_index=True)


def _render_freshness(book: AssetBook) -> None:
    """Top-of-tab layer health: one warning naming every stale / missing /
    unavailable layer, plus the full per-layer table in an expander."""
    rows = book.freshness()
    if not rows:
        return
    bad = [r for r in rows if r.status != "ok"]
    if bad:
        st.warning("Stale book layers: " + "; ".join(
            f"**{r.layer}** {r.status}"
            + (f" (latest {r.latest}, {r.lag_days}d behind, tolerance "
               f"{r.max_lag_days}d)" if r.latest else "")
            for r in bad))
    with st.expander("Data freshness by layer", expanded=False):
        st.dataframe(pd.DataFrame(
            [{"layer": r.layer, "latest": r.latest, "lag_days": r.lag_days,
              "tolerance_days": r.max_lag_days, "status": r.status,
              "note": r.note} for r in rows]),
            use_container_width=True, hide_index=True)


def render_book_tab(engine) -> None:
    st.title("Asset Book — 蒙西 Stations")
    st.caption(
        "One station's book: dispatch chain (strategy → cleared → actual), "
        "P&L attribution waterfall down to bill-anchored settled, and the "
        "settled rows themselves. Sources: libs.trading_book readers over "
        "md_id_cleared_energy / ops_bess_dispatch_15min / nodal_strategy_daily "
        "/ bess_asset_daily_attribution / book_settled_daily."
    )

    stations = get_stations()
    c1, c2, c3, c4 = st.columns([3, 2, 2, 2])
    with c1:
        station: StationRef = st.selectbox(
            "Station", stations, key="book_station",
            format_func=lambda s: f"{s.plant_name} ({s.asset_code})")
    today = date.today()
    with c2:
        chain_day = st.date_input("Chain day", value=today - timedelta(days=1),
                                  key="book_chain_day")
    with c3:
        start = st.date_input("Range start", value=today - timedelta(days=30),
                              key="book_start")
    with c4:
        end = st.date_input("Range end", value=today, key="book_end")

    if start > end:
        st.error("Range start must be on or before range end.")
        return

    book = AssetBook(engine, station)
    _render_freshness(book)

    # ── (a) Dispatch chain ──────────────────────────────────────────────────
    st.header("Dispatch chain — one day")
    if station.data_path != "full":
        # Task 11: wind / registry_only books have no strategy-dispatch layer —
        # say so instead of dying on AssetBook's NotImplementedError guard.
        st.info(f"{station.plant_name} is a {station.data_path} book "
                f"({station.asset_type}) — no dispatch-chain data path. "
                f"Settled rows and the attribution waterfall below render "
                f"from whatever data exists.")
    else:
        if not book.strategies(chain_day):
            st.info(f"No nodal-agent strategy row for {chain_day} "
                    f"({station.plant_name}) — chain shows cleared vs actual only.")
        st.plotly_chart(_chain_fig(book, chain_day), use_container_width=True)

    # ── (d) Backtest / 回测 ─────────────────────────────────────────────────
    # Placed before the settled-table gate: the backtest reads only
    # reports.bess_asset_daily_attribution (no dependency on
    # reports.book_settled_daily), so it renders regardless, like the chain.
    # registry_only books skip the section entirely (carried convention).
    if station.data_path != "registry_only":
        st.header("Backtest / 回测")
        if station.data_path != "full":
            st.info(f"{station.plant_name} is a {station.data_path} book "
                    f"({station.asset_type}) — the backtest needs the "
                    f"dispatch chain (the bess_asset_daily_attribution "
                    f"ladder); there is nothing to compare for this station.")
        else:
            _render_backtest_section(engine, station)

    # Settled-table gate (see module docstring): sections (b)/(c) both read
    # reports.book_settled_daily, which exists only after Task 4's writer has
    # run at least once. Degrade loudly rather than die on UndefinedTable.
    if not _settled_table_available(engine):
        st.warning(
            "reports.book_settled_daily is not initialised yet — the settled "
            "layer has no table. Run services.mengxi_nodal.settlement_bridge."
            "refresh_settled_daily for the relevant months, then reload; the "
            "attribution waterfall and settled rows sections render once the "
            "table exists.")
        return

    # ── (b) Attribution waterfall ───────────────────────────────────────────
    st.header(f"Attribution waterfall — {start} → {end}")
    settled_df = _load_settled_rows(engine, station.asset_code, start, end)
    n_attr = int(pd.read_sql(
        text(_ATTR_COUNT_SQL), engine,
        params=_wf_attr_params(book, start, end))["n"].iloc[0])
    waterfall = book.attribution(start, end)

    if _is_degenerate(n_attr, len(settled_df)):
        st.warning(
            f"Zero attribution rows in reports.bess_asset_daily_attribution "
            f"for {station.plant_name} over {start}..{end} while "
            f"{len(settled_df)} settled rows exist — the waterfall degenerates "
            f"to Balancing = Settled. Check the attribution-code alias or "
            f"attribution-pipeline lag before reading the named deltas.")

    m1, m2, m3 = st.columns(3)
    m1.metric("Strategy P&L", f"¥{waterfall.strategy_pnl_cny:,.0f}")
    m2.metric("Settled P&L", f"¥{waterfall.settled_pnl_cny:,.0f}")
    m3.metric("Attributed delta",
              f"¥{waterfall.settled_pnl_cny - waterfall.strategy_pnl_cny:,.0f}")
    st.plotly_chart(_waterfall_fig(waterfall), use_container_width=True)
    st.caption(
        "The **Balancing (residual)** line is a balancing figure: it absorbs "
        "the deliberate overlap between the named decompositions plus "
        "settlement-only items (capacity compensation, rebates, assessment "
        "charges) so the chain lands exactly on settled P&L.")

    with st.expander("Waterfall line detail", expanded=False):
        st.dataframe(pd.DataFrame(
            [{"line": _LINE_LABELS.get(l.name, l.name), "name": l.name,
              "delta_cny": l.delta_cny, "note": l.note}
             for l in waterfall.lines]),
            use_container_width=True, hide_index=True)

    # ── (c) Settled rows ────────────────────────────────────────────────────
    st.header(f"Settled rows — {start} → {end}")
    if settled_df.empty:
        st.info("No rows in reports.book_settled_daily for this station and "
                "range. Run services.mengxi_nodal.settlement_bridge."
                "refresh_settled_daily for the relevant months.")
    else:
        n_bill = float(settled_df.loc[settled_df["accrual_grade"] == "bill",
                                      "amount_cny"].sum())
        n_acc = float(settled_df.loc[settled_df["accrual_grade"] == "accrual",
                                     "amount_cny"].sum())
        s1, s2, s3 = st.columns(3)
        s1.metric("Settled total", f"¥{n_bill + n_acc:,.0f}")
        s2.metric("Bill-anchored", f"¥{n_bill:,.0f}")
        s3.metric("Accrual (pro-rata)", f"¥{n_acc:,.0f}")
        st.plotly_chart(_settled_recon_fig(settled_df),
                        use_container_width=True)
        st.dataframe(settled_df, use_container_width=True, hide_index=True)
