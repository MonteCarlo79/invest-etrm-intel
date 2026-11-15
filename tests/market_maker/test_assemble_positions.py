"""assemble_positions: wind_trades forward sells (I2) and R3 contract ownership (I4)."""
from datetime import date

import pandas as pd
from sqlalchemy import text

from libs.curves.curve import CURVE_COLUMNS
from libs.risk.exposure import exposure
from services.market_maker import refresh as rf
from services.market_maker.positions import assemble_positions
from tests.market_maker.test_refresh import AS_OF, engine  # noqa: F401  (fixture reuse)

JAN = date(2027, 1, 1)


def _n_positions(eng):
    with eng.connect() as c:
        return c.execute(text("SELECT count(*) FROM marketdata.rm_positions")).scalar()


def test_wind_book_gets_wind_trades_sell_in_memory(engine):
    rf.refresh(engine, AS_OF, horizon_months=12)
    before = _n_positions(engine)
    pos = assemble_positions(engine, [6], AS_OF)
    sells = pos[pos["basis"] == "wind_trades"]
    assert len(sells) == 1
    s = sells.iloc[0]
    assert (s.book_id, s.province, s.direction, s.source, s.instrument_type, s.tou_block) == \
        (6, "蒙西", "sell", "bilateral", "forward", "base")
    assert s.delivery_month == JAN and s.volume_mwh == 100.0 and s.price_cny_mwh == 300.0
    assert set(pos.loc[pos["source"] == "physical", "direction"]) == {"buy"}
    exp = exposure(pos, pd.DataFrame(columns=CURVE_COLUMNS))
    jan = exp[exp["delivery_month"] == JAN].iloc[0]
    assert jan.physical_mwh > 0 and jan.financial_mwh == -100.0 and jan.hedge_ratio > 0
    assert _n_positions(engine) == before  # never persisted (R13 pattern)


def test_wind_trades_only_for_mapped_book(engine):
    pos = assemble_positions(engine, [7, 8, 9, 21], AS_OF)
    assert not (pos["basis"] == "wind_trades").any()


def test_province_contract_short_counted_once(engine):
    # books 21 and 22 are both 冀南 load books: only the lowest book_id owns the contracts (R3)
    pos = assemble_positions(engine, None, AS_OF)
    contract = pos[pos["basis"] == "contract_forecast"]
    assert set(contract["book_id"]) == {21}
    dec = contract[contract["delivery_month"] == date(2026, 12, 1)]
    assert sorted(dec["volume_mwh"]) == [50.0, 100.0]
    only22 = assemble_positions(engine, [22], AS_OF)
    assert not (only22["basis"] == "contract_forecast").any()
    assert rf.contract_owners(engine, rf.load_books(engine)) == {21: "冀南"}


def _add_unmapped(eng):
    with eng.begin() as c:
        c.execute(text("INSERT INTO marketdata.rm_assets VALUES (9,'湖北站','wind','hubei',50,NULL,'hb')"))
        c.execute(text("INSERT INTO marketdata.rm_books VALUES (30,'hubei wind','asset',9)"))
        c.execute(text("INSERT INTO marketdata.rm_positions (book_id, instrument_type, province, channel,"
                       " direction, volume_mwh, price_cny_mwh, start_date, end_date, status, source)"
                       " VALUES (21,'bilateral','火星','annual','buy',100,380,'2026-12-01','2026-12-31',"
                       "'open','bilateral')"))


def test_skipped_notes_survive_assembly(engine):
    from services.market_maker.positions import books_with_province, skipped_notes
    _add_unmapped(engine)
    books = books_with_province(engine)
    assert 30 not in set(books["book_id"])
    assert any("book 30" in n and "hubei" in n for n in skipped_notes(books))
    pos = assemble_positions(engine, None, AS_OF)
    notes = skipped_notes(pos)
    assert any("book 30" in n for n in notes) and any("火星" in n for n in notes)
    assert not (pos["province"] == "火星").any()
    clean = assemble_positions(engine, [7], AS_OF)
    assert any("book 30" in n for n in skipped_notes(clean))  # book-level skip still reported
    assert skipped_notes(pd.DataFrame()) == []


def test_contract_owners_reuses_books_province(engine, monkeypatch):
    from services.market_maker.positions import books_with_province
    books = books_with_province(engine)

    def no_query(*a, **k):
        raise AssertionError("contract_owners must not query when province is known")

    monkeypatch.setattr(rf, "load_book_province", no_query)
    known = books[books["province"].notna() | (books["book_type"] != "load")]
    assert rf.contract_owners(engine, known) == {21: "冀南"}
