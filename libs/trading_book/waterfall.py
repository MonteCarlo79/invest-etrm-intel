"""Attribution waterfall assembler — Task 5 of the trading-book plan (2026-10-31).

Chains one book's DP-optimal strategy P&L down to bill-anchored settled P&L
over a date range:

    strategy_pnl → forecast_error → strategy_selection → clearing
                 → curtailment_so → settlement_residual → settled

Delta mapping from reports.bess_asset_daily_attribution. Its loss columns are
stored as positive magnitudes (upstream − downstream), so the waterfall
deltas negate them; the strategy_selection spread is stored signed:

    forecast_error      = −Σ forecast_error_loss
    strategy_selection  =  Σ (nominated_pnl − tt_strategy_pnl)   (nominated vs
                          DP-optimal spread; per-day spread, NULL days skipped)
    clearing            = −Σ execution_clearing_loss
    curtailment_so      = −Σ realisation_gap_vs_pf_grid — lumped: the brief's
                          cleared-vs-actual / actual-vs-capability split into
                          curtailment_so + equipment_site is not separable
                          from daily attribution rows alone, so v1 reports the
                          single curtailment_so line (brief's fallback)
    settlement_residual =  settled − strategy_pnl − Σ(other deltas)

strategy_pnl = Σ tt_strategy_pnl. settled = Σ reports.book_settled_daily
amounts (bill + accrual grades) with data_date inside [start, end], read via
the Task 4 facade book.settlement(period) month by month and filtered to the
range.

The lines deliberately mix decompositions: strategy_selection + clearing walk
the tt_strategy → nominated → cleared chain, forecast_error is the PF-grid →
forecast-optimal step, and curtailment spans the whole PF-grid → cleared
ladder — so they overlap, and settlement_residual absorbs both the overlap
and true settlement-only items (capacity compensation, rebates, assessment
charges) to land exactly on settled.

Attribution asset_code values match the registry codes (suyou, hangjinqi, …)
— same codes the pnl_attribution writer uses.
"""
from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy import text

from libs.trading_book.asset_book import AssetBook
from libs.trading_book.models import Day, Waterfall, WaterfallLine

_ATTRIBUTION_SQL = """
    SELECT SUM(tt_strategy_pnl)                   AS strategy_pnl,
           SUM(forecast_error_loss)               AS forecast_error_loss,
           SUM(nominated_pnl - tt_strategy_pnl)   AS strategy_selection,
           SUM(execution_clearing_loss)           AS execution_clearing_loss,
           SUM(realisation_gap_vs_pf_grid)        AS realisation_gap
    FROM reports.bess_asset_daily_attribution
    WHERE asset_code = :ac
      AND trade_date >= CAST(:s AS date)
      AND trade_date <= CAST(:e AS date)
"""


def _map_lines(strategy_pnl: float, forecast_err: float, strat_sel: float,
               clearing: float, curtail: float, settled: float) -> Waterfall:
    """Pure assembly: signed deltas -> 5 named lines + balancing residual.

    residual = settled − strategy_pnl − Σ(other deltas), so
    strategy_pnl + Σ lines == settled exactly (up to float rounding).
    """
    residual = settled - strategy_pnl - (forecast_err + strat_sel
                                         + clearing + curtail)
    lines = (
        WaterfallLine("forecast_error", forecast_err,
                      "−Σ forecast_error_loss (PF-grid → forecast-optimal)"),
        WaterfallLine("strategy_selection", strat_sel,
                      "Σ(nominated_pnl − tt_strategy_pnl), nominated vs DP-optimal"),
        WaterfallLine("clearing", clearing,
                      "−Σ execution_clearing_loss (nominated → cleared)"),
        WaterfallLine("curtailment_so", curtail,
                      "−Σ realisation_gap_vs_pf_grid (lumped; equipment_site "
                      "not separable from daily rows)"),
        WaterfallLine("settlement_residual", residual,
                      "balancing: settled − strategy_pnl − Σ(other lines)"),
    )
    return Waterfall(strategy_pnl_cny=float(strategy_pnl), lines=lines,
                     settled_pnl_cny=float(settled))


def _month_periods(start: date, end: date) -> list[str]:
    """'YYYY-MM' period labels for every month [start, end] touches."""
    periods: list[str] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        periods.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return periods


def _settled_in_range(book: AssetBook, start: Day, end: Day) -> float:
    """Settled CNY inside [start, end] via the Task 4 facade, day-filtered."""
    total = 0.0
    for period in _month_periods(start, end):
        s = book.settlement(period)
        total += sum(amount for day, amount, _ in s.daily
                     if start <= day <= end)
    return total


def _f(value) -> float:
    """SUM over an all-NULL/empty set comes back None/NaN -> 0.0."""
    return 0.0 if value is None or pd.isna(value) else float(value)


def build_waterfall(engine, book: AssetBook, start: Day, end: Day) -> Waterfall:
    """Aggregate attribution + settled rows over [start, end] -> Waterfall."""
    df = pd.read_sql(
        text(_ATTRIBUTION_SQL), engine,
        params={"ac": book.station.asset_code,
                "s": start.isoformat(), "e": end.isoformat()})
    row = df.iloc[0] if not df.empty else {}
    return _map_lines(
        strategy_pnl=_f(row.get("strategy_pnl")),
        forecast_err=-_f(row.get("forecast_error_loss")),
        strat_sel=_f(row.get("strategy_selection")),
        clearing=-_f(row.get("execution_clearing_loss")),
        curtail=-_f(row.get("realisation_gap")),
        settled=_settled_in_range(book, start, end))
