"""Positions store: the only DB-touching module in libs.positions."""
from __future__ import annotations

import calendar
import logging
from datetime import date

import pandas as pd
from sqlalchemy import text

from libs.positions.conventions import (
    POSITION_COLUMNS, PROVINCE_ALIASES, Position, canonical_province, positions_frame, signed_mwh,
)


log = logging.getLogger(__name__)


def _read_province(name, what: str, skipped: list) -> str | None:
    """Read paths (I6): an unmapped province skips the row/book with a warning instead of
    raising. Write paths (replace_physical, save_curve) still raise."""
    try:
        return canonical_province(name)
    except ValueError as e:
        log.warning("skipping %s: %s", what, e)
        skipped.append(f"{what}: {e}")
        return None


def _d(v) -> date:
    return v.date() if hasattr(v, "date") else date.fromisoformat(str(v)[:10])


def _month_pieces(start: date, end: date):
    """Yield (first_of_month, days_in_range_this_month) over [start, end] inclusive."""
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        first = date(y, m, 1)
        last = date(y, m, calendar.monthrange(y, m)[1])
        days = (min(end, last) - max(start, first)).days + 1
        yield first, days
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def load_positions(engine, book_ids: list[int] | None = None, status: str = "open") -> pd.DataFrame:
    sql = ("SELECT book_id, instrument_type, province, channel, direction, volume_mwh, price_cny_mwh,"
           " start_date, end_date, source, tou_block, basis FROM marketdata.rm_positions"
           " WHERE status = :status")
    params: dict = {"status": status}
    if book_ids is not None:
        if not book_ids:
            return positions_frame([])
        names = [f":b{i}" for i in range(len(book_ids))]
        sql += f" AND book_id IN ({','.join(names)})"
        params.update({f"b{i}": b for i, b in enumerate(book_ids)})
    with engine.connect() as conn:
        raw = conn.execute(text(sql), params).mappings().all()
    out: list[Position] = []
    skipped: list[str] = []
    for r in raw:
        prov = _read_province(r["province"], f"rm_positions row (book {r['book_id']})", skipped)
        if prov is None:
            continue
        start, end = _d(r["start_date"]), _d(r["end_date"])
        pieces = list(_month_pieces(start, end))
        total_days = sum(d for _, d in pieces)
        vol = float(r["volume_mwh"])
        allocated = 0.0
        for i, (first, days) in enumerate(pieces):
            piece = vol - allocated if i == len(pieces) - 1 else vol * days / total_days
            allocated += piece
            out.append(Position(
                book_id=int(r["book_id"]), province=prov, direction=r["direction"],
                volume_mwh=piece, delivery_month=first,
                tou_block=r["tou_block"] or "base",
                price_cny_mwh=None if r["price_cny_mwh"] is None else float(r["price_cny_mwh"]),
                source=r["source"] or "bilateral",
                instrument_type=r["instrument_type"], channel=r["channel"],
                basis=r["basis"] or "",
            ))
    df = positions_frame(out)
    df.attrs["skipped"] = skipped
    return df


def replace_physical(engine, book_id: int, rows: list[Position]) -> int:
    """Atomically replace this book's source='physical' rows; other sources untouched."""
    for r in rows:
        canonical_province(r.province)  # raises naming the unknown province
    ins = text(
        "INSERT INTO marketdata.rm_positions (book_id, instrument_type, province, channel, direction,"
        " volume_mwh, price_cny_mwh, start_date, end_date, status, source, tou_block, basis)"
        " VALUES (:book_id, 'profile', :province, 'annual', :direction, :volume_mwh, :price,"
        " :start_date, :end_date, 'open', 'physical', :tou_block, :basis)")
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM marketdata.rm_positions WHERE book_id = :b AND source = 'physical'"),
                     {"b": book_id})
        for r in rows:
            first = r.delivery_month
            last = date(first.year, first.month, calendar.monthrange(first.year, first.month)[1])
            conn.execute(ins, {
                "book_id": book_id, "province": r.province, "direction": r.direction,
                "volume_mwh": float(r.volume_mwh), "price": r.price_cny_mwh,
                "start_date": first, "end_date": last, "tou_block": r.tou_block, "basis": r.basis,
            })
    return len(rows)


def load_books(engine) -> pd.DataFrame:
    cols = ["book_id", "book_name", "book_type", "asset_id", "asset_type", "asset_code",
            "province", "capacity_mw", "bess_duration_h"]
    with engine.connect() as conn:
        raw = conn.execute(text(
            "SELECT b.id AS book_id, b.name AS book_name, b.book_type, b.asset_id, a.asset_type,"
            " a.asset_code, a.province, a.capacity_mw, a.bess_duration_h"
            " FROM marketdata.rm_books b LEFT JOIN marketdata.rm_assets a ON a.id = b.asset_id"
            " ORDER BY b.id")).mappings().all()
    df = pd.DataFrame([dict(r) for r in raw], columns=cols)
    skipped: list[str] = []
    provs, keep = [], []
    for bid, aid, p in zip(df["book_id"], df["asset_id"], df["province"]):
        if aid is None or pd.isna(aid) or p is None or pd.isna(p):
            provs.append(None)
            keep.append(True)
            continue
        prov = _read_province(p, f"book {bid}", skipped)
        provs.append(prov)
        keep.append(prov is not None)
    df["province"] = provs
    df = df[pd.Series(keep, index=df.index, dtype=bool)].reset_index(drop=True)
    df.attrs["skipped"] = skipped
    return df


def load_book_province(engine, book_id: int) -> str | None:
    """Most frequent canonical province among the book's rm_positions rows (ties: name order)."""
    with engine.connect() as conn:
        raw = conn.execute(text("SELECT province FROM marketdata.rm_positions WHERE book_id = :b"),
                           {"b": book_id}).scalars().all()
    skipped: list[str] = []
    mapped = [_read_province(p, f"rm_positions province (book {book_id})", skipped)
              for p in raw if p is not None]
    provs = pd.Series([p for p in mapped if p is not None], dtype=object)
    if provs.empty:
        return None
    counts = provs.value_counts()
    return sorted(counts[counts == counts.max()].index)[0]


def load_book_physical(engine, book_id: int, province: str, months: list[date],
                       as_of: date) -> pd.DataFrame:
    """In-memory physical sell rows for a load book from the province's active retail
    contracts. No DB writes: load books are owned by the retail-risk loader."""
    from libs.positions.physical import load_positions as contract_positions

    prov = canonical_province(province)
    aliases = list(PROVINCE_ALIASES[prov])
    names = [f":p{i}" for i in range(len(aliases))]
    sql = ("SELECT cc.contract_type, cc.price_cny_mwh, cc.start_date, cc.end_date,"
           " cc.annual_forecast_mwh, cc.monthly_forecast"
           " FROM marketdata.rm_customer_contracts cc"
           " JOIN marketdata.rm_customers cu ON cu.id = cc.customer_id"
           " WHERE cc.contract_status = 'active' AND cc.end_date >= :as_of"
           f" AND cu.province IN ({','.join(names)})")
    params: dict = {"as_of": as_of, **{f"p{i}": a for i, a in enumerate(aliases)}}
    with engine.connect() as conn:
        contracts = pd.read_sql(text(sql), conn, params=params)
    return positions_frame(contract_positions(book_id, prov, contracts, months, as_of))
