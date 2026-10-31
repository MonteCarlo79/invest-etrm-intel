"""Settlement facade — Task 4 of the trading-book plan (2026-10-31).

Contract consumed by libs.trading_book.asset_book.AssetBook.settlement()
(call-time import; an ImportError here surfaces there as NotImplementedError):
    module:    libs.trading_book.settlement_bridge
    function:  get_settlement(engine, station: StationRef, period: str) -> Settlement
    period:    'YYYY-MM' (one calendar month, CST)

Reads reports.book_settled_daily — populated by
services.mengxi_nodal.settlement_bridge.refresh_settled_daily — and aggregates
the month's per-(day, charge-stream) rows into
Settlement(period, settled_cny, daily=[(day, amount_cny, accrual_grade)]),
merging entries that share (day, accrual_grade) and ordering by day.
settled_cny is the straight sum over daily (bills are authoritative for the
settled layer, spec decision 3); a month with no rows settles to 0.0/[].
"""
from __future__ import annotations

import calendar
from datetime import date

import pandas as pd
from sqlalchemy import text

from libs.trading_book.models import Settlement
from libs.trading_book.registry import StationRef

_SQL = """
    SELECT data_date, amount_cny, accrual_grade
    FROM reports.book_settled_daily
    WHERE asset_code = :ac
      AND data_date >= CAST(:s AS date)
      AND data_date <= CAST(:e AS date)
    ORDER BY data_date
"""


def month_bounds(period: str) -> tuple[date, date]:
    """'YYYY-MM' -> (first, last) calendar day of the month."""
    y, m = int(period[:4]), int(period[5:7])
    return date(y, m, 1), date(y, m, calendar.monthrange(y, m)[1])


def _aggregate(rows: list[tuple[date, float, str]], period: str) -> Settlement:
    """(day, amount_cny, accrual_grade) rows -> Settlement.

    A day can carry several charge streams of one grade (e.g. 'discharge_energy'
    and 'charge_energy' bill rows) — those merge; different grades stay separate
    entries. Ordered by day; settled_cny = sum over daily.
    """
    merged: dict[tuple[date, str], float] = {}
    for day, amount, grade in rows:
        key = (day, grade)
        merged[key] = merged.get(key, 0.0) + float(amount)
    daily = [(day, amt, grade) for (day, grade), amt in sorted(merged.items())]
    return Settlement(period=period,
                      settled_cny=sum(amt for _, amt, _ in daily),
                      daily=daily)


def get_settlement(engine, station: StationRef, period: str) -> Settlement:
    """One station book's monthly settled P&L — see module docstring contract."""
    s, e = month_bounds(period)
    df = pd.read_sql(text(_SQL), engine,
                     params={"ac": station.asset_code,
                             "s": s.isoformat(), "e": e.isoformat()})
    rows = [(r.data_date, float(r.amount_cny), r.accrual_grade)
            for r in df.itertuples()]
    return _aggregate(rows, period)
