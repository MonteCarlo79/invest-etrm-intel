"""Physical position generators: realised history / retail contracts -> Position rows. Pure."""
from __future__ import annotations

import calendar
import json
from datetime import date

import pandas as pd

from libs.positions.conventions import Position

MIN_HISTORY_DAYS = 30
_PRICED_TYPES = ("fixed", "peak_offpeak")


def expected_monthly_mwh(daily_mwh: pd.Series, months: list[date]) -> pd.DataFrame:
    cols = ["delivery_month", "volume_mwh", "basis"]
    s = daily_mwh.dropna() if daily_mwh is not None else pd.Series(dtype=float)
    if s.empty:
        return pd.DataFrame(columns=cols)
    s = s.copy()
    s.index = pd.to_datetime(s.index)
    s = s.sort_index()
    last = s.index.max()
    trailing = s[s.index > last - pd.Timedelta(days=90)]
    out = []
    for m in months:
        days = calendar.monthrange(m.year, m.month)[1]
        start = pd.Timestamp(m)
        if len(s) < MIN_HISTORY_DAYS:
            mean, basis = s.mean(), "insufficient_history"
        else:
            same = s[(s.index.month == m.month) & (s.index < start)]
            if not same.empty:
                latest = same.index.max()
                same = same[(same.index.year == latest.year)]
                mean, basis = same.mean(), "same_month"
            else:
                mean, basis = trailing.mean(), "trailing_90d"
        out.append((m, float(mean) * days, basis))
    return pd.DataFrame(out, columns=cols)


def _rows(book_id, province, daily, months) -> list[Position]:
    df = expected_monthly_mwh(daily, months)
    return [
        Position(book_id=book_id, province=province, direction="buy",
                 volume_mwh=r.volume_mwh, delivery_month=r.delivery_month,
                 source="physical", basis=r.basis)
        for r in df.itertuples(index=False)
    ]


def generation_positions(book_id, province, daily_gen_mwh, months) -> list[Position]:
    return _rows(book_id, province, daily_gen_mwh, months)


def bess_positions(book_id, province, daily_net_discharge_mwh, months) -> list[Position]:
    return _rows(book_id, province, daily_net_discharge_mwh, months)


def _forecast(v) -> dict:
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except ValueError:
            return {}
    return v if isinstance(v, dict) else {}


def _num(v):
    return None if v is None or pd.isna(v) else float(v)


def _d(v) -> date:
    return pd.Timestamp(v).date()


def load_positions(book_id, province, contracts: pd.DataFrame, months, as_of: date) -> list[Position]:
    rows: list[Position] = []
    for m in months:
        mend = date(m.year, m.month, calendar.monthrange(m.year, m.month)[1])
        vol = {True: 0.0, False: 0.0}
        notional = 0.0
        for c in contracts.itertuples(index=False):
            if _d(c.start_date) > mend or _d(c.end_date) < m:
                continue
            v = _num(_forecast(c.monthly_forecast).get(str(m.month)))
            if v is None:
                a = _num(c.annual_forecast_mwh)
                if a is None:
                    continue
                v = a / 12
            if v <= 0:
                continue
            priced = c.contract_type in _PRICED_TYPES and _num(c.price_cny_mwh) is not None
            vol[priced] += v
            if priced:
                notional += v * float(c.price_cny_mwh)
        for priced in (True, False):
            if vol[priced] > 0:
                rows.append(Position(
                    book_id=book_id, province=province, direction="sell",
                    volume_mwh=vol[priced], delivery_month=m, source="physical",
                    price_cny_mwh=notional / vol[priced] if priced else None,
                    basis="contract_forecast"))
    return rows
