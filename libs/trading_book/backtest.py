"""Backtest ladder reader + summariser — Task 14 of the trading-book plan
(2026-10-31).

Reads the per-day value-loss ladder from
reports.bess_asset_daily_attribution over a date range and summarises it:

    PF unrestricted → PF grid-feasible → forecast-optimal → DP strategy
    → nominated → cleared actual

with the stored loss buckets between rungs. Loss columns are stored as
POSITIVE magnitudes (upstream − downstream); the ladder P&L columns are
signed CNY. Derived metrics:

    strategy_selection_cost     = Σ(nominated_pnl − tt_strategy_pnl)
                                  (row-wise difference summed, NULL rows
                                  skipped — same semantics as the waterfall's
                                  SUM(nominated_pnl - tt_strategy_pnl))
    nomination_loss             = Σ nomination_loss
    execution_clearing_loss     = Σ execution_clearing_loss
    realisation_gap_vs_pf_grid  = Σ realisation_gap_vs_pf_grid
    capture_actual_vs_nominated = Σ cleared_actual_pnl / Σ nominated_pnl
    capture_nominated_vs_pf_grid= Σ nominated_pnl / Σ pf_grid_feasible_pnl

Capture ratios are ratio-of-column-totals (portfolio capture); None when the
denominator total is 0 or missing — never a fabricated 0% or 100%.

Frame: CST-naive everywhere — date bounds are naive literals compared via
CAST(:s AS date); no '+08' suffix, no AT TIME ZONE rendering.

Missing days are honest: reports.bess_asset_daily_attribution has one row per
(asset_code, trade_date) with data, so absent days simply do not appear in the
frame; summarise() reports the covered-day count and skips NULL cells within
present rows (partial scenario coverage).
"""
from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy import text

# Private import, deliberate: the registry->attribution asset-code alias
# (bameng->hetao) lives only in waterfall._attribution_code. Reusing it keeps
# the backtest ladder on the exact same code mapping as the waterfall and the
# Book tab's attribution count probe.
from libs.trading_book.waterfall import _attribution_code

# The full ladder: P&L rungs then the stored loss buckets (positive
# magnitudes). Order here is the order returned / displayed.
LADDER_COLUMNS: list[str] = [
    "pf_unrestricted_pnl",
    "pf_grid_feasible_pnl",
    "tt_forecast_optimal_pnl",
    "tt_strategy_pnl",
    "nominated_pnl",
    "cleared_actual_pnl",
    "grid_restriction_loss",
    "forecast_error_loss",
    "strategy_error_loss",
    "nomination_loss",
    "execution_clearing_loss",
    "realisation_gap_vs_pf",
    "realisation_gap_vs_pf_grid",
]

# Chart rungs (subset of LADDER_COLUMNS), in the brief's display order:
# nominated vs tt_strategy (DP-optimal) vs cleared_actual vs pf_grid_feasible.
CHART_COLUMNS: list[str] = [
    "nominated_pnl",
    "tt_strategy_pnl",
    "cleared_actual_pnl",
    "pf_grid_feasible_pnl",
]

_BACKTEST_SQL = """
    SELECT trade_date,
           pf_unrestricted_pnl, pf_grid_feasible_pnl,
           tt_forecast_optimal_pnl, tt_strategy_pnl,
           nominated_pnl, cleared_actual_pnl,
           grid_restriction_loss, forecast_error_loss, strategy_error_loss,
           nomination_loss, execution_clearing_loss,
           realisation_gap_vs_pf, realisation_gap_vs_pf_grid
    FROM reports.bess_asset_daily_attribution
    WHERE asset_code = :ac
      AND trade_date >= CAST(:s AS date)
      AND trade_date <= CAST(:e AS date)
    ORDER BY trade_date
"""


def get_backtest(engine, asset_code: str, start: date, end: date) -> pd.DataFrame:
    """Per-day attribution ladder for one station over [start, end].

    asset_code is the REGISTRY code; the attribution-stack alias
    (bameng->hetao) is applied here. Returns columns trade_date +
    LADDER_COLUMNS, trade_date as datetime64, ladder columns as float64 (DB
    numeric -> Decimal would break plotly; to_numeric coerces). Empty frame
    keeps the full column set so callers can rely on column presence.
    """
    df = pd.read_sql(
        text(_BACKTEST_SQL), engine,
        params={"ac": _attribution_code(asset_code),
                "s": start.isoformat(), "e": end.isoformat()})
    if df.empty:
        return pd.DataFrame(columns=["trade_date"] + LADDER_COLUMNS)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    for col in LADDER_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _total(df: pd.DataFrame, col: str) -> float | None:
    """Column total over present rows, NaN cells skipped. None when the column
    is absent entirely; 0.0 for an empty/all-NaN column (same convention as
    waterfall._f: SUM over an all-NULL set -> 0.0)."""
    if col not in df.columns:
        return None
    v = df[col].sum()
    return 0.0 if pd.isna(v) else float(v)


def _ratio(num: float | None, den: float | None) -> float | None:
    """Ratio of totals; None when either side is missing or the denominator
    total is 0 (capture is undefined, never fabricated)."""
    if num is None or den is None or den == 0.0:
        return None
    return num / den


def summarise(df: pd.DataFrame) -> dict:
    """Ladder frame -> totals + derived metrics (see module docstring).

    'days' is the covered-day count (rows present in the range; absent days
    never reach the frame). strategy_selection_cost sums the ROW-WISE
    nominated − tt_strategy difference with NULL rows skipped — identical to
    the waterfall's SUM(nominated_pnl - tt_strategy_pnl), which totals of
    columns would not be when some days carry only one of the two rungs.
    """
    totals = {col: _total(df, col) for col in LADDER_COLUMNS}
    if {"nominated_pnl", "tt_strategy_pnl"} <= set(df.columns):
        ssc = (df["nominated_pnl"] - df["tt_strategy_pnl"]).sum()
        strategy_selection_cost = 0.0 if pd.isna(ssc) else float(ssc)
    else:
        strategy_selection_cost = None
    return {
        "days": int(len(df)),
        "totals": totals,
        "strategy_selection_cost": strategy_selection_cost,
        "nomination_loss": totals["nomination_loss"],
        "execution_clearing_loss": totals["execution_clearing_loss"],
        "realisation_gap_vs_pf_grid": totals["realisation_gap_vs_pf_grid"],
        "capture_actual_vs_nominated": _ratio(
            totals["cleared_actual_pnl"], totals["nominated_pnl"]),
        "capture_nominated_vs_pf_grid": _ratio(
            totals["nominated_pnl"], totals["pf_grid_feasible_pnl"]),
    }
