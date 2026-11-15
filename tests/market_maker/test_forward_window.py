"""I1: the current delivery month is excluded from delta / VaR (forward-only from the first horizon month)."""
from datetime import date

import pandas as pd

from libs.positions.conventions import POSITION_COLUMNS
from services.market_maker import agent_tools as at
from services.market_maker.positions import coverage_note, first_forward_month, forward_only
from libs.risk.exposure import exposure

AS_OF = date(2026, 11, 16)
NOV, DEC = date(2026, 11, 1), date(2026, 12, 1)


def _load_book_positions():
    base = dict(book_id=21, province="冀南", tou_block="base", instrument_type="bilateral",
                channel="annual", basis="")
    rows = [dict(base, direction="buy", volume_mwh=500.0, delivery_month=NOV, price_cny_mwh=380.0,
                 source="bilateral"),
            dict(base, direction="sell", volume_mwh=800.0, delivery_month=NOV, price_cny_mwh=None,
                 source="physical"),
            dict(base, direction="buy", volume_mwh=300.0, delivery_month=DEC, price_cny_mwh=380.0,
                 source="bilateral"),
            dict(base, direction="sell", volume_mwh=1000.0, delivery_month=DEC, price_cny_mwh=None,
                 source="physical")]
    return pd.DataFrame(rows, columns=POSITION_COLUMNS)


def test_first_forward_month_and_note():
    assert first_forward_month(AS_OF) == DEC
    assert first_forward_month(date(2026, 11, 1)) == NOV
    assert coverage_note(AS_OF) == "VaR/delta cover months from 2026-12; current delivery month excluded"


def test_current_month_purchases_excluded_december_nets():
    exp = forward_only(exposure(_load_book_positions(), pd.DataFrame(
        columns=["province", "delivery_month", "tou_block", "price_cny_mwh", "source"])), AS_OF)
    assert [pd.Timestamp(m).date() for m in exp["delivery_month"]] == [DEC]
    assert exp["net_mwh"].tolist() == [-700.0]


def test_get_var_excludes_current_month(monkeypatch):
    monkeypatch.setattr(at, "assemble_positions", lambda e, ids, a: _load_book_positions())
    monkeypatch.setattr(at, "load_curve", lambda e, p, a: pd.DataFrame(
        columns=["province", "delivery_month", "tou_block", "price_cny_mwh", "source"]))
    monkeypatch.setattr(at, "_price_scenarios", lambda e, p, a: pd.Series(dtype=float))
    monkeypatch.setattr(at, "_today", lambda: AS_OF)
    monkeypatch.setattr(at, "books_with_province", lambda e: pd.DataFrame(
        columns=["book_id", "asset_type", "province", "capacity_mw", "bess_duration_h"]))
    r = at.execute_tool("get_var", {}, object())
    assert r["net_delta_mwh_by_province"] == {"冀南": -700.0}
    assert r["coverage"] == "VaR/delta cover months from 2026-12; current delivery month excluded"
    g = at.execute_tool("get_greeks", {}, object())
    assert [x["delivery_month"] for x in g["delta_by_month"]] == ["2026-12-01"]
