import json
from datetime import date

import pandas as pd

from libs.positions.physical import (
    bess_positions, expected_monthly_mwh, generation_positions, load_positions,
)


def _series(start, end, val):
    idx = pd.date_range(start, end, freq="D")
    return pd.Series(val, index=idx)


def test_same_month_basis():
    s = pd.concat([_series("2026-03-01", "2026-03-31", 10.0),
                   _series("2026-04-01", "2026-12-31", 20.0)])
    df = expected_monthly_mwh(s, [date(2027, 3, 1)])
    r = df.iloc[0]
    assert r.basis == "same_month" and r.volume_mwh == 310.0


def test_trailing_90d_basis():
    s = _series("2026-06-01", "2026-12-31", 5.0)
    s.loc["2026-12-01":] = 8.0
    df = expected_monthly_mwh(s, [date(2027, 3, 1)])
    r = df.iloc[0]
    assert r.basis == "trailing_90d"
    last90 = s.iloc[-90:].mean()
    assert abs(r.volume_mwh - last90 * 31) < 1e-9


def test_insufficient_history():
    s = _series("2026-03-01", "2026-03-12", 10.0)
    df = expected_monthly_mwh(s, [date(2027, 3, 1)])
    r = df.iloc[0]
    assert r.basis == "insufficient_history" and r.volume_mwh == 310.0


def test_empty():
    assert expected_monthly_mwh(pd.Series(dtype=float), [date(2027, 1, 1)]).empty
    assert generation_positions(1, "蒙西", pd.Series(dtype=float), [date(2027, 1, 1)]) == []


def test_generation_and_bess_rows():
    s = _series("2026-01-01", "2026-12-31", 1.0)
    for fn in (generation_positions, bess_positions):
        rows = fn(3, "蒙西", s, [date(2027, 2, 1)])
        assert len(rows) == 1
        p = rows[0]
        assert (p.direction, p.source, p.tou_block, p.volume_mwh) == ("buy", "physical", "base", 28.0)
        assert p.basis == "same_month" and p.book_id == 3 and p.province == "蒙西"


def _contracts():
    return pd.DataFrame([
        dict(contract_type="fixed", price_cny_mwh=400.0, monthly_forecast=None,
             annual_forecast_mwh=120.0, start_date=date(2027, 1, 1), end_date=date(2027, 12, 31)),
        dict(contract_type="indexed", price_cny_mwh=None,
             monthly_forecast=json.dumps({str(m): 20 for m in range(1, 13)}),
             annual_forecast_mwh=None, start_date=date(2027, 1, 1), end_date=date(2027, 12, 31)),
    ])


def test_load_positions_priced_and_unpriced():
    months = [date(2027, m, 1) for m in (1, 2)]
    rows = load_positions(1, "浙江", _contracts(), months, date(2026, 11, 15))
    assert len(rows) == 4
    for m in months:
        priced = [r for r in rows if r.delivery_month == m and r.price_cny_mwh is not None]
        unpriced = [r for r in rows if r.delivery_month == m and r.price_cny_mwh is None]
        assert len(priced) == 1 and priced[0].volume_mwh == 10 and priced[0].price_cny_mwh == 400
        assert len(unpriced) == 1 and unpriced[0].volume_mwh == 20
        assert priced[0].direction == "sell" and priced[0].source == "physical"


def test_load_weighted_price_and_dict_forecast():
    df = pd.DataFrame([
        dict(contract_type="fixed", price_cny_mwh=300.0, monthly_forecast={"1": 10},
             annual_forecast_mwh=None, start_date=date(2027, 1, 1), end_date=date(2027, 12, 31)),
        dict(contract_type="peak_offpeak", price_cny_mwh=500.0, monthly_forecast={"1": 30},
             annual_forecast_mwh=None, start_date=date(2027, 1, 1), end_date=date(2027, 12, 31)),
    ])
    rows = load_positions(1, "浙江", df, [date(2027, 1, 1)], date(2026, 11, 15))
    assert len(rows) == 1 and rows[0].volume_mwh == 40 and rows[0].price_cny_mwh == 450


def test_contract_ending_mid_year_stops():
    df = _contracts()
    df.loc[0, "end_date"] = date(2027, 6, 15)
    months = [date(2027, m, 1) for m in (6, 7)]
    rows = load_positions(1, "浙江", df, months, date(2026, 11, 15))
    assert [r for r in rows if r.price_cny_mwh is not None and r.delivery_month == date(2027, 6, 1)]
    assert not [r for r in rows if r.price_cny_mwh is not None and r.delivery_month == date(2027, 7, 1)]


def test_bess_negative_net_is_sell():
    s = _series("2026-01-01", "2026-12-31", -2.0)
    rows = bess_positions(3, "蒙西", s, [date(2027, 4, 1)])
    assert len(rows) == 1
    assert (rows[0].direction, rows[0].volume_mwh, rows[0].source) == ("sell", 60.0, "physical")


def test_bess_positive_net_stays_buy():
    s = _series("2026-01-01", "2026-12-31", 2.0)
    rows = bess_positions(3, "蒙西", s, [date(2027, 4, 1)])
    assert (rows[0].direction, rows[0].volume_mwh) == ("buy", 60.0)
