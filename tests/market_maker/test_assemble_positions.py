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
