# tests/services/retail_risk/test_run_backfill.py
import datetime
import pandas as pd
import pytest
from services.retail_risk import schemas
from services.retail_risk.run_backfill import trades_to_volumes


def test_trades_to_volumes_vwap_merges_green():
    trades = pd.DataFrame([
        [datetime.date(2026, 3, 1), 8, "annual", "bilateral", "buy", 10.0, 350.0, None, "年度双边", "f"],
        [datetime.date(2026, 3, 1), 8, "annual", "forward", "buy", 2.0, 400.0, "绿电", "绿电", "f"],
    ], columns=schemas.TRADES_COLS)
    out = trades_to_volumes(trades)
    assert len(out) == 1                       # green folded into annual, VWAP-merged
    row = out.iloc[0]
    assert row.volume_mwh == 12.0
    assert row.vwap_cny_mwh == pytest.approx((10 * 350 + 2 * 400) / 12)


def test_trades_to_volumes_attaches_load():
    trades = pd.DataFrame([
        [datetime.date(2026, 4, 1), 0, "annual", "bilateral", "buy", 5.0, 344.0, None, "yr_sb", "f"],
    ], columns=schemas.TRADES_COLS)
    load = pd.DataFrame([{"delivery_date": datetime.date(2026, 4, 1), "hour": 0,
                          "nominated_mwh": 42.2, "settled_mwh": 38.7}])
    out = trades_to_volumes(trades, load)
    assert out.iloc[0].nominated_mwh == 42.2 and out.iloc[0].settled_mwh == 38.7


def test_trades_to_volumes_propagates_estimated():
    trades = pd.DataFrame([
        [datetime.date(2026, 3, 1), 8, "monthly_auction", "forward", "buy",
         10.0, 400.0, None, "月度竞价 (est)", "f"],
    ], columns=schemas.TRADES_COLS)
    out = trades_to_volumes(trades)
    assert out.iloc[0].estimated == True  # noqa: E712
