"""Shared data access + sidebar for the market-maker app.

R13: load-book physical rows are never stored, so every page builds the full
book through `book_positions` (stored rows + in-memory load-book physical rows).
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import streamlit as st

from libs.curves.store import load_curve, load_spot_hourly
from libs.positions.conventions import POSITION_COLUMNS
from libs.positions.store import load_book_physical, load_book_province, load_books, load_positions
from libs.risk.var import monthly_price_scenarios
from services.market_maker.refresh import horizon

HORIZON_MONTHS = 12
SCENARIO_HISTORY_DAYS = 400
DASH = "—"

# dataviz reference palette, categorical slots 1-3 (fixed order, light mode)
SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]
SOURCE_COLORS = {"manual": SERIES[0], "traded": SERIES[1], "spot_seasonal": SERIES[2]}
PHYS_FIN_COLORS = {"Physical": SERIES[0], "Financial": SERIES[1]}


@st.cache_data(ttl=600, show_spinner=False)
def get_books(_engine) -> pd.DataFrame:
    """rm_books + asset attributes; load books get their position-derived province."""
    books = load_books(_engine)
    for i, b in books.iterrows():
        if b["book_type"] == "load" and pd.isna(b["province"]):
            books.at[i, "province"] = load_book_province(_engine, int(b["book_id"]))
    return books


@st.cache_data(ttl=600, show_spinner="Loading positions…")
def book_positions(_engine, book_ids: tuple[int, ...], as_of: date) -> pd.DataFrame:
    """Full book: stored rm_positions rows + in-memory physical rows of load books (R13)."""
    ids = list(book_ids)
    frames = [load_positions(_engine, ids)]
    months = horizon(as_of, HORIZON_MONTHS)
    books = get_books(_engine)
    for bid in books.loc[(books["book_type"] == "load") & books["book_id"].isin(ids), "book_id"]:
        prov = load_book_province(_engine, int(bid))
        if prov is None:
            continue
        frames.append(load_book_physical(_engine, int(bid), prov, months, as_of))
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=POSITION_COLUMNS)
    return pd.concat(frames, ignore_index=True)


@st.cache_data(ttl=600, show_spinner=False)
def get_curve(_engine, as_of: date) -> pd.DataFrame:
    return load_curve(_engine, None, as_of)


@st.cache_data(ttl=3600, show_spinner=False)
def price_scenarios(_engine, province: str, as_of: date) -> pd.Series:
    hourly = load_spot_hourly(_engine, province, as_of - timedelta(days=SCENARIO_HISTORY_DAYS), as_of)
    return monthly_price_scenarios(hourly) if not hourly.empty else pd.Series(dtype=float)


def sidebar_selection(engine, books: pd.DataFrame, today: date) -> dict:
    sb = st.sidebar
    sb.header("Selection")
    as_of = sb.date_input("As of", value=today, key="mm_as_of")
    labels = {int(r.book_id): f"{r.book_id} · {r.book_name} ({r.book_type})"
              for r in books.itertuples(index=False)}
    ids = sb.multiselect("Books", list(labels), default=list(labels),
                         format_func=lambda b: labels[b], key="mm_books")
    provs = sorted({p for p in books["province"] if isinstance(p, str)})
    chosen = sb.multiselect("Provinces", provs, default=provs, key="mm_provinces")
    return {"as_of": as_of, "book_ids": tuple(sorted(ids)), "provinces": list(chosen),
            "books": books, "months": horizon(as_of, HORIZON_MONTHS)}


def selected_positions(engine, sel: dict) -> pd.DataFrame:
    pos = book_positions(engine, sel["book_ids"], sel["as_of"])
    return pos[pos["province"].isin(sel["provinces"])].reset_index(drop=True)


def book_name_map(sel: dict) -> dict[int, str]:
    return {int(b): n for b, n in zip(sel["books"]["book_id"], sel["books"]["book_name"])}


def month_start(as_of) -> date:
    ts = pd.Timestamp(as_of)
    return date(ts.year, ts.month, 1)


def is_delivered(delivery_month, as_of) -> bool:
    """Delivery month strictly before the as-of month (fully delivered)."""
    return pd.Timestamp(delivery_month) < pd.Timestamp(month_start(as_of))


def forward_only(exp: pd.DataFrame, as_of) -> pd.DataFrame:
    """R16: keep delivery months from the as-of month on; delivered energy has no forward
    price risk, so delta / VaR / stress must not include it."""
    if exp.empty:
        return exp
    keep = pd.to_datetime(exp["delivery_month"]) >= pd.Timestamp(month_start(as_of))
    return exp[keep].reset_index(drop=True)


def month_label(v) -> str:
    return pd.Timestamp(v).strftime("%Y-%m")


def fmt_num(v, digits: int = 0) -> str:
    """Never render missing as 0: NaN/None -> em dash."""
    if v is None or pd.isna(v):
        return DASH
    return f"{round(float(v), digits) + 0.0:,.{digits}f}"  # + 0.0: no "-0"


def is_admin() -> bool:
    try:
        from auth.rbac import get_role
        return get_role() == "Admin"
    except Exception:
        return False


def clear_caches() -> None:
    for f in (get_books, book_positions, get_curve, price_scenarios):
        f.clear()
