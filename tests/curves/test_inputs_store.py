from datetime import date

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from libs.curves.curve import CURVE_COLUMNS
from libs.curves.inputs import trades_from_position_volumes, trades_from_wind_trades
from libs.curves.store import load_curve, load_spot_hourly, save_curve


@pytest.fixture()
def eng():
    e = create_engine("sqlite:///:memory:", poolclass=StaticPool,
                      connect_args={"check_same_thread": False})
    with e.begin() as c:
        c.execute(text("ATTACH DATABASE ':memory:' AS marketdata"))
        c.execute(text("CREATE TABLE marketdata.rm_forward_curves (id INTEGER PRIMARY KEY, "
                       "province TEXT, product TEXT, curve_date DATE, delivery_date DATE, "
                       "delivery_hour INTEGER, price_cny_kwh REAL, source TEXT, "
                       "uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, tou_block TEXT)"))
        c.execute(text("CREATE TABLE marketdata.spot_prices_hourly (province TEXT, "
                       "datetime TIMESTAMP, rt_price REAL, da_price REAL)"))
    return e


def _wt(**kw):
    base = dict(asset_name="a", delivery_month=date(2026, 5, 1), channel="省内",
                energy_kind="正常", volume_mwh=100.0, energy_price=300.0)
    base.update(kw)
    return base


def test_wind_trades_filters():
    wt = pd.DataFrame([_wt(), _wt(energy_kind="汇总"), _wt(energy_kind="发电置换"),
                       _wt(channel="跨省"), _wt(volume_mwh=0.0),
                       _wt(delivery_month=None)])
    out = trades_from_wind_trades(wt)
    assert len(out) == 1
    r = out.iloc[0]
    assert (r.province, r.tou_block, r.price_cny_mwh, r.volume_mwh) == ("蒙西", "base", 300.0, 100.0)


def test_position_volumes_peak_and_base():
    pv = pd.DataFrame([dict(delivery_date=date(2026, 5, 3), hour=18,
                            annual_price_cny_mwh=350.0, annual_volume_mwh=10.0,
                            monthly_auction_price_cny_mwh=None, monthly_auction_volume_mwh=None,
                            monthly_listed_price_cny_mwh=None, monthly_listed_volume_mwh=None,
                            intramonth_match_price_cny_mwh=None, intramonth_match_volume_mwh=None)])
    out = trades_from_position_volumes(pv, "河北南网")
    assert sorted(out.tou_block) == ["base", "peak"]
    assert (out.province == "冀南").all() and (out.price_cny_mwh == 350.0).all()
    assert (out.volume_mwh == 10.0).all()


def test_save_load_roundtrip_idempotent_and_scoped(eng):
    with eng.begin() as c:
        c.execute(text("INSERT INTO marketdata.rm_forward_curves (province, product, curve_date, "
                       "delivery_date, price_cny_kwh, source) VALUES "
                       "('蒙西','scenario','2026-11-01','2026-05-01',0.1,'x')"))
    curve = pd.DataFrame([["河北南网", date(2026, 5, 1), "base", 351.25, "traded"],
                          ["蒙西", date(2026, 5, 1), "peak", 400.0, "manual"]],
                         columns=CURVE_COLUMNS)
    assert save_curve(eng, curve, date(2026, 11, 15)) == 2
    assert save_curve(eng, curve, date(2026, 11, 15)) == 2
    with eng.connect() as c:
        n = c.execute(text("SELECT count(*) FROM marketdata.rm_forward_curves "
                           "WHERE product='reference'")).scalar()
        assert n == 2
        assert sorted(x[0] for x in c.execute(text(
            "SELECT province FROM marketdata.rm_forward_curves "
            "WHERE product='reference'"))) == ["冀南", "蒙西"]
        assert c.execute(text("SELECT count(*) FROM marketdata.rm_forward_curves "
                              "WHERE product='scenario'")).scalar() == 1
    got = load_curve(eng).sort_values("province").reset_index(drop=True)
    assert list(got.columns) == CURVE_COLUMNS
    assert got.province.tolist() == ["冀南", "蒙西"]
    assert got.price_cny_mwh.tolist() == [351.25, 400.0]
    assert got.delivery_month.iloc[0] == date(2026, 5, 1)
    assert len(load_curve(eng, province="冀南")) == 1
    assert load_curve(eng, as_of=date(2026, 11, 1)).empty


def test_load_spot_hourly_aliases(eng):
    with eng.begin() as c:
        c.execute(text("INSERT INTO marketdata.spot_prices_hourly VALUES "
                       "('河北南网','2026-05-01 10:00:00',200,150),"
                       "('冀南','2026-05-02 10:00:00',210,150),"
                       "('山东','2026-05-01 10:00:00',999,1)"))
    out = load_spot_hourly(eng, "冀南", date(2026, 5, 1), date(2026, 5, 31))
    assert sorted(out.price) == [200.0, 210.0]
    assert {"datetime", "price"} <= set(out.columns)


def test_save_alias_then_canonical_no_duplicates(eng):
    def mk(name):
        return pd.DataFrame([[name, date(2026, 5, 1), "base", 300.0, "traded"]],
                            columns=CURVE_COLUMNS)
    save_curve(eng, mk("河北南网"), date(2026, 11, 15))
    save_curve(eng, mk("冀南"), date(2026, 11, 15))
    got = load_curve(eng)
    assert len(got) == 1 and got.province.iloc[0] == "冀南"
