"""DB-free tests for services/arb_match/compute.py — md row → dispatch slot frame.

Owner decision (2026-11-15, libs/asset_risk/chain.py::MD_ROW_TO_SLOT): an md
price row stamped D prices the 15-min dispatch slot starting at D + 15 min.
"""
import pandas as pd

from services.arb_match.compute import _load_prices, compute_day


class _FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchall(self):
        return self.rows


def test_price_row_stamped_0045_prices_slot_0100():
    cur = _FakeCursor([(pd.Timestamp("2026-10-15 00:45"), 400.0),
                       (pd.Timestamp("2026-10-15 01:00"), 999.0)])
    prices = _load_prices(cur, "cleared", "X储能电站", "2026-10-15", "2026-10-16")
    assert prices[pd.Timestamp("2026-10-15 01:00")] == 400.0
    dispatch = pd.DataFrame({"ts": [pd.Timestamp("2026-10-15 01:00")],
                             "actual_mw": [100.0]})
    m = compute_day(dispatch, prices)
    assert m["modeled_discharge_rev_cny"] == 100.0 * 0.25 * 400.0
    assert m["intervals_priced"] == 1


def test_first_slot_keeps_price_load_window_starts_15min_early():
    for kind in ("cleared", "nodal"):
        cur = _FakeCursor([(pd.Timestamp("2026-10-14 23:45"), 250.0)])
        prices = _load_prices(cur, kind, "k", "2026-10-15", "2026-10-16")
        _, params = cur.calls[0]
        assert params[1] == "2026-10-14 23:45:00"
        assert prices[pd.Timestamp("2026-10-15 00:00")] == 250.0
