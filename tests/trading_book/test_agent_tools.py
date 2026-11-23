"""Trader-agent payloads over the book (libs.trading_book.agent_tools)."""
from datetime import date

import numpy as np
import pandas as pd

from libs.trading_book import agent_tools as t

DAY = date(2026, 11, 18)


def _fake(sql, engine, params=None):
    q = str(sql)
    if "md_id_cleared_energy" in q:
        ts = pd.date_range(f"{DAY} 00:00", periods=48, freq="15min")   # half day
        return pd.DataFrame({"datetime": ts, "cleared_price": np.full(48, 300.0),
                             "cleared_energy_mwh": np.full(48, -50.0)})
    if "bess_nomination_96" in q:
        return pd.DataFrame({"data_date": [DAY] * 96, "slot": range(1, 97),
                             "nominated_mw": np.full(96, -60.0)})
    if "bess_actual_96" in q:
        return pd.DataFrame(columns=["data_date", "slot", "actual_mw"])
    if "ops_bess_dispatch_15min" in q:
        return pd.DataFrame()
    raise AssertionError(q[:100])


def test_dispatch_payload_reads_the_book_not_the_dead_ops_table(monkeypatch):
    monkeypatch.setattr(pd, "read_sql", _fake)
    out = t.dispatch_day_payload(None, "suyou", DAY)
    assert out["coverage_slots"] == {"nominated_mw": 96, "actual_mw": 0,
                                     "cleared_mw": 48, "cleared_price_cny_mwh": 48}
    assert len(out["rows"]) == 96 and out["rows"][0]["time"] == "00:00"
    first, second, last = out["rows"][0], out["rows"][1], out["rows"][95]
    assert first["nominated_mw"] == -60.0
    # md row stamped T is slot T+15min (book frame, 24e00aa7): the 00:00 row
    # lands in the 00:15 slot, so slot 00:00 has no cleared value.
    assert first["cleared_mw"] is None and second["cleared_mw"] == -50.0
    assert second["cleared_price_cny_mwh"] == 300.0
    assert first["actual_mw"] is None              # missing layer is null, not 0
    assert last["time"] == "23:45" and last["cleared_mw"] is None


def test_unknown_and_non_full_codes_return_error_with_valid_list():
    out = t.dispatch_day_payload(None, "nope", DAY)
    assert "unknown asset_code" in out["error"] and "suyou" in out["valid_asset_codes"]
    out = t.dispatch_day_payload(None, "wuhai", DAY)     # settlement_only
    assert "no dispatch chain" in out["error"]


def test_attribution_code_alias():
    assert t.attribution_code("bameng") == "bameng"   # 'hetao' alias retired
    assert t.attribution_code("suyou") == "suyou"
    assert t.attribution_code("mystery") == "mystery"


def test_station_codes_are_the_full_bess_books():
    codes = t.station_codes()
    assert {"suyou", "hangjinqi", "siziwangqi", "gushanliang", "bameng", "wulate"} <= set(codes)
    assert "wuhai" not in codes and "lingt46" not in codes
