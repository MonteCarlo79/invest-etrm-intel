"""Uncached position assembly shared by the market-maker app and the Risk Agent tools.

R13: load-book physical rows are never stored, so the full book is stored rm_positions rows
plus in-memory load-book physical rows. No streamlit / apps imports here.
"""
from __future__ import annotations

from datetime import date

import pandas as pd

from libs.positions.conventions import POSITION_COLUMNS
from libs.positions.store import load_book_physical, load_book_province, load_books, load_positions
from services.market_maker.refresh import horizon

HORIZON_MONTHS = 12


def books_with_province(engine) -> pd.DataFrame:
    """rm_books + asset attributes; load books get their position-derived province."""
    books = load_books(engine)
    for i, b in books.iterrows():
        if b["book_type"] == "load" and pd.isna(b["province"]):
            books.at[i, "province"] = load_book_province(engine, int(b["book_id"]))
    return books


def assemble_positions(engine, book_ids: list[int] | None, as_of: date) -> pd.DataFrame:
    """Full book: stored rows + in-memory physical rows of load books (R13). None = all books."""
    books = load_books(engine)
    ids = [int(b) for b in books["book_id"]] if book_ids is None else [int(b) for b in book_ids]
    frames = [load_positions(engine, ids)]
    months = horizon(as_of, HORIZON_MONTHS)
    for bid in books.loc[(books["book_type"] == "load") & books["book_id"].isin(ids), "book_id"]:
        prov = load_book_province(engine, int(bid))
        if prov is None:
            continue
        frames.append(load_book_physical(engine, int(bid), prov, months, as_of))
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=POSITION_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def month_start(as_of) -> date:
    ts = pd.Timestamp(as_of)
    return date(ts.year, ts.month, 1)


def forward_only(exp: pd.DataFrame, as_of) -> pd.DataFrame:
    """R16: keep delivery months from the as-of month on; delivered energy has no forward
    price risk, so delta / VaR / stress must not include it."""
    if exp.empty:
        return exp
    keep = pd.to_datetime(exp["delivery_month"]) >= pd.Timestamp(month_start(as_of))
    return exp[keep].reset_index(drop=True)
