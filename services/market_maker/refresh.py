"""Market-maker refresh job: regenerate physical positions and reference forward curves.

Asset books get source='physical' rm_positions rows (replace_physical). Load books are
NEVER written (R13: retail-risk owns every rm_positions row of a load book); their
contract-derived physical rows are computed in memory via load_book_physical.

    python -m services.market_maker.refresh --as-of 2026-11-15          # summary only
    python -m services.market_maker.refresh --as-of 2026-11-15 --apply  # write
"""
from __future__ import annotations

import argparse
import json
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import bindparam, text

from libs.asset_risk.chain import load_chain
from libs.curves.curve import reference_curve, spot_seasonal, traded_points
from libs.curves.inputs import trades_from_position_volumes, trades_from_wind_trades
from libs.curves.store import load_curve, load_spot_hourly, save_curve
from libs.positions.physical import bess_positions, generation_positions
from libs.positions.store import (
    load_book_physical, load_book_province, load_books, replace_physical,
)
from libs.trading_book.registry import get_station

WIND_DISPATCH_PLANT = {"lingt46": "悦盛昌渠风光储电站"}
HISTORY_DAYS = 400
_PV_COLS = ("delivery_date, hour, annual_price_cny_mwh, annual_volume_mwh,"
            " monthly_auction_price_cny_mwh, monthly_auction_volume_mwh,"
            " monthly_listed_price_cny_mwh, monthly_listed_volume_mwh,"
            " intramonth_match_price_cny_mwh, intramonth_match_volume_mwh")


def horizon(as_of: date, n: int) -> list[date]:
    """The next `n` first-of-months on or after as_of."""
    y, m = as_of.year, as_of.month
    if as_of.day != 1:
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    out = []
    for _ in range(n):
        out.append(date(y, m, 1))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def _wind_daily(engine, plant: str, start: date, end: date) -> pd.Series:
    with engine.connect() as c:
        df = pd.read_sql(text("SELECT datetime, gen_mwh FROM marketdata.wind_dispatch_15min"
                              " WHERE plant_name = :p AND datetime >= :s AND datetime < :e"),
                         c, params={"p": plant, "s": start, "e": end})
    if df.empty:
        return pd.Series(dtype=float)
    day = pd.to_datetime(df["datetime"]).dt.normalize()
    return pd.to_numeric(df["gen_mwh"], errors="coerce").groupby(day).sum(min_count=1).dropna()


def _bess_daily(engine, asset_id: int, start: date, end: date) -> pd.Series:
    ch = load_chain(engine, [asset_id], start.isoformat(), end.isoformat(), check_sign=False)
    if ch is None or ch.empty:
        return pd.Series(dtype=float)
    mwh = pd.to_numeric(ch["actual_mw"], errors="coerce") * 0.25
    day = pd.to_datetime(ch["ts"]).dt.normalize()
    return mwh.groupby(day).sum(min_count=1).dropna()


def _asset_rows(engine, b, months, start, end):
    """-> (rows, None) or (None, skip_reason)."""
    if b.province is None:
        return None, "no province"
    code = b.asset_code if isinstance(b.asset_code, str) else None
    if b.asset_type in ("wind", "solar"):
        plant = WIND_DISPATCH_PLANT.get(code)
        if plant is None:
            return None, "no dispatch source"
        rows = generation_positions(int(b.book_id), b.province,
                                    _wind_daily(engine, plant, start, end), months)
    elif b.asset_type == "bess":
        try:
            full = code is not None and get_station(code).data_path == "full"
        except KeyError:
            full = False
        if not full:
            return None, "no dispatch chain"
        rows = bess_positions(int(b.book_id), b.province,
                              _bess_daily(engine, int(b.asset_id), start, end), months)
    else:
        return None, "no dispatch source"
    return (rows, None) if rows else (None, "no data")


def _read(engine, sql: str, params: dict, expand: str | None = None) -> pd.DataFrame:
    stmt = text(sql)
    if expand:
        stmt = stmt.bindparams(bindparam(expand, expanding=True))
    with engine.connect() as c:
        return pd.read_sql(stmt, c, params=params)


def _province_curve(engine, prov, load_book_ids, months, as_of) -> pd.DataFrame:
    manual = load_curve(engine, prov, as_of)
    manual = manual[manual["source"] == "manual"]
    trades = []
    if load_book_ids:
        pv = _read(engine, f"SELECT {_PV_COLS} FROM marketdata.rm_position_volumes"
                           " WHERE book_id IN :b", {"b": list(load_book_ids)}, "b")
        trades.append(trades_from_position_volumes(pv, prov))
    if prov == "蒙西":
        wt = _read(engine, "SELECT delivery_month, channel, energy_kind, volume_mwh, energy_price"
                           " FROM marketdata.wind_trades", {})
        for col in ("volume_mwh", "energy_price"):
            wt[col] = pd.to_numeric(wt[col], errors="coerce")
        trades.append(trades_from_wind_trades(wt, prov))
    trades = [t for t in trades if not t.empty]
    traded = traded_points(pd.concat(trades, ignore_index=True)) if trades else None
    hourly = load_spot_hourly(engine, prov, as_of - timedelta(days=730), as_of)
    spot = spot_seasonal(hourly, prov, months)
    return reference_curve(manual, traded, spot)


def refresh(engine, as_of: date, horizon_months: int = 12, dry_run: bool = False) -> dict:
    months = horizon(as_of, horizon_months)
    start, end = as_of - timedelta(days=HISTORY_DAYS), as_of
    books = load_books(engine)
    out: dict = {"books": {}, "curves": {}, "skipped": []}

    def skip(bid, reason):
        out["books"][bid] = reason
        out["skipped"].append((bid, reason))

    curve_provs: dict[str, list[int]] = {}
    for b in books[books["book_type"] == "asset"].itertuples(index=False):
        bid = int(b.book_id)
        rows, reason = _asset_rows(engine, b, months, start, end)
        if reason:
            skip(bid, reason)
            continue
        out["books"][bid] = len(rows) if dry_run else replace_physical(engine, bid, rows)
        curve_provs.setdefault(b.province, [])

    contract_owner: dict[str, int] = {}
    for b in books[books["book_type"] == "load"].sort_values("book_id").itertuples(index=False):
        bid = int(b.book_id)
        prov = load_book_province(engine, bid)
        if prov is None:
            skip(bid, "no province")
            continue
        curve_provs.setdefault(prov, []).append(bid)
        if prov in contract_owner:
            out["books"][bid] = f"contracts assigned to book {contract_owner[prov]}"
            continue
        contract_owner[prov] = bid
        load_book_physical(engine, bid, prov, months, as_of)  # validate; never persisted
        out["books"][bid] = "in-memory"

    for prov in sorted(curve_provs):
        curve = _province_curve(engine, prov, curve_provs[prov], months, as_of)
        out["curves"][prov] = len(curve) if dry_run else save_curve(engine, curve, curve_date=as_of)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--as-of", type=date.fromisoformat, default=date.today())
    ap.add_argument("--horizon-months", type=int, default=12)
    ap.add_argument("--apply", action="store_true", help="write to the DB (default: summary only)")
    a = ap.parse_args(argv)
    from services.retail_risk.loader import get_engine
    res = refresh(get_engine(), a.as_of, a.horizon_months, dry_run=not a.apply)
    print(("APPLIED" if a.apply else "DRY RUN (pass --apply to write)"))
    print(json.dumps(res, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
