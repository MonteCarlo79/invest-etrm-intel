"""Positions store: the only DB-touching module in libs.positions."""
from __future__ import annotations

import calendar
from datetime import date

import pandas as pd
from sqlalchemy import text

from libs.positions.conventions import (
    POSITION_COLUMNS, Position, canonical_province, positions_frame, signed_mwh,
)


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
    for r in raw:
        prov = canonical_province(r["province"])
        start, end = _d(r["start_date"]), _d(r["end_date"])
        pieces = list(_month_pieces(start, end))
        total_days = sum(d for _, d in pieces)
        vol = float(r["volume_mwh"])
        for first, days in pieces:
            out.append(Position(
                book_id=int(r["book_id"]), province=prov, direction=r["direction"],
                volume_mwh=vol * days / total_days, delivery_month=first,
                tou_block=r["tou_block"] or "base",
                price_cny_mwh=None if r["price_cny_mwh"] is None else float(r["price_cny_mwh"]),
                source=r["source"] or "bilateral",
                instrument_type=r["instrument_type"], channel=r["channel"],
                basis=r["basis"] or "",
            ))
    return positions_frame(out)


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
    df["province"] = [None if (b is None or pd.isna(p)) else canonical_province(p)
                      for b, p in zip(df["asset_id"], df["province"])]
    return df
