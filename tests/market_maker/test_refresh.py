from datetime import date, datetime, timedelta

import pandas as pd
import pytest
from sqlalchemy import create_engine, event, text

from libs.positions.conventions import POSITION_COLUMNS
from libs.positions.store import load_book_physical, load_book_province
from services.market_maker import refresh as rf

AS_OF = date(2026, 11, 15)
MONTHS = [date(2026, 12, 1), date(2027, 1, 1)]

_DDL = [
    "CREATE TABLE marketdata.rm_assets (id INTEGER PRIMARY KEY, name TEXT, asset_type TEXT,"
    " province TEXT, capacity_mw REAL, bess_duration_h REAL, asset_code TEXT)",
    "CREATE TABLE marketdata.rm_books (id INTEGER PRIMARY KEY, name TEXT, book_type TEXT, asset_id INTEGER)",
    "CREATE TABLE marketdata.rm_positions (id INTEGER PRIMARY KEY AUTOINCREMENT, book_id INTEGER,"
    " instrument_type TEXT, province TEXT, channel TEXT, direction TEXT, volume_mwh REAL,"
    " price_cny_mwh REAL, start_date DATE, end_date DATE, counterparty TEXT, status TEXT,"
    " uploaded_at TEXT, upload_batch_id TEXT, source TEXT, tou_block TEXT, basis TEXT)",
    "CREATE TABLE marketdata.rm_customers (id INTEGER PRIMARY KEY, name TEXT, province TEXT, status TEXT)",
    "CREATE TABLE marketdata.rm_customer_contracts (id INTEGER PRIMARY KEY, customer_id INTEGER,"
    " contract_type TEXT, price_cny_mwh REAL, start_date DATE, end_date DATE,"
    " annual_forecast_mwh REAL, monthly_forecast TEXT, contract_status TEXT)",
    "CREATE TABLE marketdata.rm_position_volumes (id INTEGER PRIMARY KEY, book_id INTEGER,"
    " delivery_date DATE, hour INTEGER, annual_price_cny_mwh REAL, annual_volume_mwh REAL,"
    " monthly_auction_price_cny_mwh REAL, monthly_auction_volume_mwh REAL,"
    " monthly_listed_price_cny_mwh REAL, monthly_listed_volume_mwh REAL,"
    " intramonth_match_price_cny_mwh REAL, intramonth_match_volume_mwh REAL)",
    "CREATE TABLE marketdata.wind_dispatch_15min (plant_name TEXT, node_name TEXT, datetime TIMESTAMP,"
    " gen_mw REAL, gen_mwh REAL, rt_price REAL, data_date DATE)",
    "CREATE TABLE marketdata.wind_trades (id INTEGER PRIMARY KEY, asset_name TEXT, delivery_month DATE,"
    " channel TEXT, energy_kind TEXT, volume_mwh REAL, energy_price REAL)",
    "CREATE TABLE marketdata.spot_prices_hourly (province TEXT, datetime TIMESTAMP, rt_price REAL, da_price REAL)",
    "CREATE TABLE marketdata.rm_forward_curves (id INTEGER PRIMARY KEY, province TEXT, product TEXT,"
    " curve_date DATE, delivery_date DATE, delivery_hour INTEGER, price_cny_kwh REAL, source TEXT,"
    " uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, tou_block TEXT)",
]


def _seed(c):
    c.execute(text("INSERT INTO marketdata.rm_assets VALUES "
                   "(1,'零碳46','wind','inner_mongolia_mengxi',405,NULL,'lingt46'),"
                   "(2,'武川','bess','蒙西',1000,4,'wuchuan'),"
                   "(3,'乌海','bess','蒙西',100,2,'wuhai'),"
                   "(4,'二连','wind','蒙西',200,NULL,'erlian')"))
    c.execute(text("INSERT INTO marketdata.rm_books VALUES (6,'wind','asset',1),(7,'bess','asset',2),"
                   "(8,'wuhai','asset',3),(9,'erlian','asset',4),"
                   "(21,'load 冀南','load',NULL),(22,'load 冀南 b','load',NULL),(23,'empty','load',NULL)"))
    for b, prov in ((21, "河北南网"), (21, "河北南网"), (21, "浙江"), (22, "冀南")):
        c.execute(text("INSERT INTO marketdata.rm_positions (book_id, instrument_type, province, channel,"
                       " direction, volume_mwh, price_cny_mwh, start_date, end_date, status, source)"
                       " VALUES (:b,'bilateral',:p,'annual','buy',100,380,'2026-12-01','2026-12-31','open',NULL)"),
                  {"b": b, "p": prov})
    c.execute(text("INSERT INTO marketdata.rm_customers VALUES (1,'A','河北南网','active'),"
                   "(2,'B','冀南','active'),(3,'C','浙江','active')"))
    c.execute(text("INSERT INTO marketdata.rm_customer_contracts VALUES "
                   "(1,1,'fixed',400,'2026-01-01','2027-12-31',1200,NULL,'active'),"
                   "(2,2,'indexed',NULL,'2026-01-01','2027-12-31',600,'{\"12\": 50}','active'),"
                   "(3,1,'fixed',999,'2026-01-01','2027-12-31',1200,NULL,'expired'),"
                   "(4,1,'fixed',999,'2025-01-01','2026-10-31',1200,NULL,'active'),"
                   "(5,3,'fixed',999,'2026-01-01','2027-12-31',1200,NULL,'active')"))
    # wind: 45 days x 96 slots x 1 MWh
    start = datetime(2026, 9, 30)
    rows = [{"t": start + timedelta(minutes=15 * i)} for i in range(45 * 96)]
    c.execute(text("INSERT INTO marketdata.wind_dispatch_15min (plant_name, node_name, datetime, gen_mw,"
                   " gen_mwh) VALUES ('悦盛昌渠风光储电站','n',:t,4,1.0)"), rows)
    c.execute(text("INSERT INTO marketdata.wind_dispatch_15min (plant_name, node_name, datetime, gen_mw,"
                   " gen_mwh) VALUES ('other plant','n','2026-10-01 00:00:00',4,999.0)"))
    c.execute(text("INSERT INTO marketdata.rm_position_volumes (book_id, delivery_date, hour,"
                   " annual_price_cny_mwh, annual_volume_mwh) VALUES (21,'2026-12-03',18,350,10),"
                   "(22,'2026-12-04',3,330,10)"))
    c.execute(text("INSERT INTO marketdata.wind_trades (asset_name, delivery_month, channel, energy_kind,"
                   " volume_mwh, energy_price) VALUES ('新_悦盛昌渠#1期','2027-01-01','省内','正常',100,300)"))
    spot = [{"p": p, "t": datetime(2025, 12, 1) + timedelta(hours=h)}
            for p in ("河北南网", "蒙西") for h in range(72)]
    c.execute(text("INSERT INTO marketdata.spot_prices_hourly VALUES (:p,:t,400,NULL)"), spot)
    c.execute(text("INSERT INTO marketdata.rm_forward_curves (province, product, curve_date, delivery_date,"
                   " price_cny_kwh, source, tou_block) VALUES ('蒙西','reference','2026-11-01','2027-02-01',"
                   "0.5,'manual','base')"))


@pytest.fixture
def engine(tmp_path, monkeypatch):
    md = tmp_path / "marketdata.db"
    eng = create_engine(f"sqlite:///{tmp_path / 'main.db'}")

    @event.listens_for(eng, "connect")
    def _attach(conn, _):
        conn.execute(f"ATTACH DATABASE '{md}' AS marketdata")

    with eng.begin() as c:
        for s in _DDL:
            c.execute(text(s))
        _seed(c)

    calls = []

    def fake_chain(engine, asset_ids, start, end, check_sign=True, **kw):
        calls.append((tuple(asset_ids), check_sign))
        assert asset_ids == [2]
        rows = []
        for d in range(45):
            day = datetime(2026, 9, 30) + timedelta(days=d)
            for s in range(96):
                mw = 100.0 if s < 8 else (-100.0 if s < 17 else None)
                rows.append({"asset_id": 2, "ts": day + timedelta(minutes=15 * s), "actual_mw": mw})
        return pd.DataFrame(rows)

    monkeypatch.setattr(rf, "load_chain", fake_chain)
    eng.chain_calls = calls
    return eng


def _counts(eng):
    with eng.connect() as c:
        return (c.execute(text("SELECT count(*) FROM marketdata.rm_positions")).scalar(),
                c.execute(text("SELECT count(*) FROM marketdata.rm_forward_curves")).scalar())


def test_load_book_province_and_physical(engine):
    assert load_book_province(engine, 21) == "冀南"
    assert load_book_province(engine, 23) is None
    df = load_book_physical(engine, 21, "冀南", MONTHS, AS_OF)
    assert list(df.columns) == POSITION_COLUMNS
    assert set(df.direction) == {"sell"} and set(df.book_id) == {21}
    dec = df[df.delivery_month == date(2026, 12, 1)].sort_values("volume_mwh")
    assert dec.volume_mwh.tolist() == [50.0, 100.0]
    assert dec.price_cny_mwh.isna().tolist() == [True, False]
    assert dec.price_cny_mwh.iloc[1] == 400.0
    jan = df[df.delivery_month == date(2027, 1, 1)]
    assert sorted(jan.volume_mwh) == [50.0, 100.0]
    assert _counts(engine)[0] == 4  # no writes


def test_refresh_writes_asset_books_only_and_curves(engine):
    before = _counts(engine)
    out = rf.refresh(engine, AS_OF, horizon_months=12)
    assert out["books"] == {6: 12, 7: 12, 8: "no dispatch chain", 9: "no dispatch source",
                            21: "in-memory", 22: "contracts assigned to book 21", 23: "no province"}
    assert engine.chain_calls == [((2,), False)]
    with engine.connect() as c:
        phys = pd.read_sql(text("SELECT book_id, direction, volume_mwh, start_date, province FROM "
                                "marketdata.rm_positions WHERE source='physical'"), c)
        load_rows = c.execute(text("SELECT count(*) FROM marketdata.rm_positions WHERE book_id IN "
                                   "(21,22,23)")).scalar()
    assert set(phys.book_id) == {6, 7}  # R13: never in load books
    assert load_rows == 4
    w = phys[phys.book_id == 6]
    assert len(w) == 12 and set(w.direction) == {"buy"} and set(w.province) == {"蒙西"}
    assert w[w.start_date == "2026-12-01"].volume_mwh.iloc[0] == pytest.approx(96 * 31)
    b = phys[phys.book_id == 7]
    assert set(b.direction) == {"sell"}  # net -25 MWh/day
    assert b[b.start_date == "2026-12-01"].volume_mwh.iloc[0] == pytest.approx(25 * 31)

    assert out["curves"] == {"冀南": 3, "蒙西": 5}
    with engine.connect() as c:
        cur = pd.read_sql(text("SELECT province, delivery_date, tou_block, price_cny_kwh, source FROM "
                               "marketdata.rm_forward_curves WHERE curve_date='2026-11-15'"), c)
    j = cur[cur.province == "冀南"].set_index("tou_block")
    assert j.loc["base", "source"] == "traded" and j.loc["base", "price_cny_kwh"] == pytest.approx(0.34)
    assert j.loc["offpeak", "source"] == "spot_seasonal"
    m = cur[cur.province == "蒙西"]
    assert m[m.delivery_date == "2027-01-01"].source.tolist() == ["traded"]
    assert m[m.delivery_date == "2027-02-01"].source.tolist() == ["manual"]

    after = _counts(engine)
    out2 = rf.refresh(engine, AS_OF, horizon_months=12)
    assert out2["books"] == out["books"] and out2["curves"] == out["curves"]
    assert _counts(engine) == after
    assert after != before


def test_dry_run_writes_nothing(engine):
    before = _counts(engine)
    dry = rf.refresh(engine, AS_OF, horizon_months=12, dry_run=True)
    assert _counts(engine) == before
    real = rf.refresh(engine, AS_OF, horizon_months=12)
    assert dry["books"] == real["books"] and dry["curves"] == real["curves"]
    assert dry["skipped"] == real["skipped"]
