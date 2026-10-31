"""DB-free unit tests for AssetBook readers — SQL text + window/alignment only.

No RDS in unit tests: the readers themselves are exercised against the live
DB by callers, not here.
"""
from datetime import date

import pandas as pd

from libs.trading_book.asset_book import (
    _ACTUAL_SQL, _CLEARED_SQL, _STRATEGY_SQL, _cst_naive_window, _to_96,
)


def test_window_is_naive_cst():
    s, e = _cst_naive_window(date(2026, 11, 1))
    assert s == "2026-11-01 00:00:00" and e == "2026-11-02 00:00:00"
    assert "+08" not in s and "+08" not in e  # POSIX-inversion guard


def test_sql_targets_right_tables():
    assert "md_id_cleared_energy" in _CLEARED_SQL
    assert "ops_bess_dispatch_15min" in _ACTUAL_SQL
    assert "Asia/Shanghai" not in _CLEARED_SQL  # columns already CST-naive; no rendering needed


def test_strategy_sql_reads_nodal_agent_family():
    # nodal_strategy_daily has no scope column — the nodal-agent scope is the
    # model_version family ('nodal_agent_v1', …), latest created_at wins.
    assert "nodal_strategy_daily" in _STRATEGY_SQL
    assert "nodal_agent" in _STRATEGY_SQL
    assert "target_date" in _STRATEGY_SQL


def test_to_96_aligns_quarter_hours():
    df = pd.DataFrame({
        "ts": ["2026-11-01 00:00:00", "2026-11-01 00:15:00", "2026-11-01 23:45:00"],
        "v": [1.0, 2.0, 96.0],
    })
    out = _to_96(df, "ts", "v")
    assert len(out) == 96
    assert out[0] == 1.0 and out[1] == 2.0 and out[95] == 96.0
    assert out[2] != out[2]  # NaN for slots with no source row
    empty = _to_96(df.iloc[0:0], "ts", "v")
    assert len(empty) == 96 and all(x != x for x in empty)  # all NaN
