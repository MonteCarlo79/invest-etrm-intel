import datetime as dt
import math

import pandas as pd

from libs.curves.curve import CURVE_COLUMNS
from libs.positions.conventions import POSITION_COLUMNS
from libs.risk.exposure import exposure, portfolio_exposure

M = dt.date(2026, 12, 1)


def _pos(rows):
    out = []
    for r in rows:
        d = dict(book_id=1, province="蒙西", direction="buy", volume_mwh=0.0,
                 delivery_month=M, tou_block="base", price_cny_mwh=None,
                 source="physical", instrument_type="x", channel="c", basis="b")
        d.update(r)
        out.append(d)
    return pd.DataFrame(out, columns=POSITION_COLUMNS)


def _curve(price=330.0, month=M):
    return pd.DataFrame([dict(province="蒙西", delivery_month=month, tou_block="base",
                              price_cny_mwh=price, source="t")], columns=CURVE_COLUMNS)


def test_wind_book_hedge_and_mtm():
    p = _pos([dict(volume_mwh=1000),
              dict(direction="sell", volume_mwh=600, price_cny_mwh=350.0, source="bilateral")])
    r = exposure(p, _curve()).iloc[0]
    assert r.physical_mwh == 1000 and r.financial_mwh == -600 and r.net_mwh == 400
    assert math.isclose(r.hedge_ratio, 0.6)
    assert math.isclose(r.mtm_cny, 12000.0)
    assert r.mark_cny_mwh == 330.0


def test_missing_curve_gives_nan_mark_and_mtm():
    p = _pos([dict(volume_mwh=1000),
              dict(direction="sell", volume_mwh=600, price_cny_mwh=350.0, source="bilateral")])
    r = exposure(p, _curve(month=dt.date(2027, 1, 1))).iloc[0]
    assert r.net_mwh == 400
    assert math.isnan(r.mark_cny_mwh) and math.isnan(r.mtm_cny)


def test_unpriced_physical_sell_zero_mtm():
    p = _pos([dict(book_id=2, direction="sell", volume_mwh=50),
              dict(book_id=2, direction="sell", volume_mwh=20)])
    r = exposure(p, _curve()).iloc[0]
    assert r.net_mwh == -70 and r.mtm_cny == 0.0 and r.mark_cny_mwh == 330.0
    assert math.isnan(r.hedge_ratio) or r.physical_mwh != 0


def test_priced_physical_sell_mtm():
    p = _pos([dict(direction="sell", volume_mwh=10, price_cny_mwh=400.0)])
    assert math.isclose(exposure(p, _curve()).iloc[0].mtm_cny, 700.0)


def test_portfolio_sums_books():
    p = _pos([dict(book_id=1, volume_mwh=100), dict(book_id=2, direction="sell", volume_mwh=30,
              price_cny_mwh=350.0, source="bilateral")])
    pe = portfolio_exposure(exposure(p, _curve()))
    assert len(pe) == 1 and pe.iloc[0].net_mwh == 70
    assert math.isclose(pe.iloc[0].mtm_cny, 600.0)
