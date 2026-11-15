"""Uncached position assembly shared by the market-maker app and the Risk Agent tools.

R13: load-book physical rows are never stored, so the full book is stored rm_positions rows
plus in-memory load-book physical rows. No streamlit / apps imports here.
"""
from __future__ import annotations

from datetime import date

import pandas as pd

from libs.positions.conventions import POSITION_COLUMNS, Position, positions_frame
from libs.positions.store import load_book_physical, load_book_province, load_books, load_positions
from services.market_maker.refresh import (
    WIND_DISPATCH_PLANT, WIND_TRADES_ASSETS, contract_owners, horizon, wind_trades_frame,
)

HORIZON_MONTHS = 12


def books_with_province(engine) -> pd.DataFrame:
    """rm_books + asset attributes; load books get their position-derived province."""
    books = load_books(engine)
    for i, b in books.iterrows():
        if b["book_type"] == "load" and pd.isna(b["province"]):
            books.at[i, "province"] = load_book_province(engine, int(b["book_id"]))
    return books


def _wind_trade_rows(engine, book, months: list[date]) -> pd.DataFrame:
    """In-memory forward sells from wind_trades for a mapped wind asset book (never persisted)."""
    code = book["asset_code"] if isinstance(book["asset_code"], str) else None
    prov = book["province"]
    if code not in WIND_DISPATCH_PLANT or prov not in WIND_TRADES_ASSETS:
        return pd.DataFrame(columns=POSITION_COLUMNS)
    t = wind_trades_frame(engine, prov)
    rows = []
    for r in t.itertuples(index=False):
        m = pd.Timestamp(r.delivery_month).date().replace(day=1)
        if m not in months:
            continue
        rows.append(Position(book_id=int(book["book_id"]), province=prov, direction="sell",
                             volume_mwh=float(r.volume_mwh), delivery_month=m, tou_block="base",
                             price_cny_mwh=float(r.price_cny_mwh), source="bilateral",
                             instrument_type="forward", basis="wind_trades"))
    return positions_frame(rows)


def assemble_positions(engine, book_ids: list[int] | None, as_of: date) -> pd.DataFrame:
    """Full book: stored rows + in-memory rows (R13): load-book contract shorts (owner book only,
    R3) and wind_trades forward sells of mapped wind books. None = all books."""
    books = load_books(engine)
    ids = [int(b) for b in books["book_id"]] if book_ids is None else [int(b) for b in book_ids]
    frames = [load_positions(engine, ids)]
    months = horizon(as_of, HORIZON_MONTHS)
    for bid, prov in contract_owners(engine, books).items():
        if bid in ids:
            frames.append(load_book_physical(engine, bid, prov, months, as_of))
    for _, b in books[(books["book_type"] == "asset") & books["book_id"].isin(ids)].iterrows():
        frames.append(_wind_trade_rows(engine, b, months))
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=POSITION_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def month_start(as_of) -> date:
    ts = pd.Timestamp(as_of)
    return date(ts.year, ts.month, 1)


def first_forward_month(as_of) -> date:
    """First horizon month: next month unless as_of is the 1st. The current month is in delivery."""
    return horizon(pd.Timestamp(as_of).date(), 1)[0]


def coverage_note(as_of) -> str:
    return (f"VaR/delta cover months from {first_forward_month(as_of):%Y-%m}; "
            "current delivery month excluded")


def forward_only(exp: pd.DataFrame, as_of) -> pd.DataFrame:
    """R16 / I1: keep delivery months from the first horizon month on. Delivered and in-delivery
    (current-month) energy is excluded from delta / VaR / stress."""
    if exp.empty:
        return exp
    keep = pd.to_datetime(exp["delivery_month"]) >= pd.Timestamp(first_forward_month(as_of))
    return exp[keep].reset_index(drop=True)
