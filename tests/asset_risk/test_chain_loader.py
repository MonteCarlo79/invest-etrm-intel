"""Task 11: _load_chain over legacy chain + WeCom nominations + multi-account
exchange-cleared energy + WeCom actuals (bess_actual_96 → ops_bess_dispatch_15min),
with the actual/nominated sign guard.

sqlite-ATTACH harness; timestamps stored as Beijing-naive TEXT.
"""
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from libs.asset_risk.chain import _load_chain, load_chain


def _base_engine(with_code: bool = True):
    eng = create_engine("sqlite:///:memory:")
    code_col = ", asset_code TEXT" if with_code else ""
    with eng.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS marketdata"))
        conn.execute(text("CREATE TABLE marketdata.rm_assets "
                          "(id INTEGER PRIMARY KEY, name TEXT, asset_type TEXT, province TEXT, "
                          f"capacity_mw REAL, bess_duration_h REAL{code_col})"))
        conn.execute(text("CREATE TABLE marketdata.rm_dispatch_chain "
                          "(id INTEGER PRIMARY KEY, asset_id INTEGER, interval_start TEXT, "
                          "soc_pct REAL, nominated_mw REAL, da_cleared_mw REAL, "
                          "rt_cleared_mw REAL, actual_mw REAL, restriction TEXT, "
                          "source_file TEXT, upload_batch_id TEXT)"))
        conn.execute(text("CREATE TABLE marketdata.md_id_cleared_energy "
                          "(plant_name TEXT, datetime TEXT, data_date TEXT, "
                          "cleared_price REAL, cleared_energy_mwh REAL)"))
        conn.execute(text("CREATE TABLE marketdata.bess_nomination_96 "
                          "(asset_code TEXT, data_date TEXT, slot INTEGER, nominated_mw REAL, "
                          "source_file TEXT, created_at TEXT, "
                          "PRIMARY KEY (asset_code, data_date, slot))"))
        conn.execute(text("CREATE TABLE marketdata.bess_actual_96 "
                          "(asset_code TEXT, data_date TEXT, slot INTEGER, actual_mw REAL, "
                          "source_file TEXT, created_at TEXT, "
                          "PRIMARY KEY (asset_code, data_date, slot))"))
        conn.execute(text("CREATE TABLE marketdata.ops_bess_dispatch_15min "
                          "(asset_code TEXT, interval_start TEXT, "
                          "nominated_dispatch_mw REAL, actual_dispatch_mw REAL)"))
    return eng


def _chain(conn, aid, ts, nom, act, rt=None):
    conn.execute(text("INSERT INTO marketdata.rm_dispatch_chain "
                      "(asset_id, interval_start, nominated_mw, rt_cleared_mw, actual_mw, "
                      "source_file) VALUES (:a, :ts, :n, :r, :ac, 'xlsx')"),
                 {"a": aid, "ts": ts, "n": nom, "r": rt, "ac": act})


def _cleared(conn, plant, ts, mw):
    conn.execute(text("INSERT INTO marketdata.md_id_cleared_energy "
                      "(plant_name, datetime, data_date, cleared_price, cleared_energy_mwh) "
                      "VALUES (:p, :ts, :d, 300.0, :mw)"),
                 {"p": plant, "ts": ts, "d": ts[:10], "mw": mw})


@pytest.fixture()
def sqlite_engine():
    eng = _base_engine()
    with eng.begin() as conn:
        conn.execute(text("INSERT INTO marketdata.rm_assets VALUES "
                          "(1, '裕昭沙子坝', 'bess', 'test', 100, 4.0, 'gushanliang'), "
                          "(3, '武川', 'bess', 'test', 100, 4.0, 'wuchuan'), "
                          "(9, 'X9', 'bess', 'test', 50, 2.0, NULL)"))
        # asset 1: legacy actual at 00:00 wins over bess_actual_96 (99) and ops
        _chain(conn, 1, "2026-09-01 00:00:00", 10, 5.0)
        for slot in (1, 2, 3):
            conn.execute(text("INSERT INTO marketdata.bess_nomination_96 "
                              "(asset_code, data_date, slot, nominated_mw) "
                              "VALUES ('gushanliang', '2026-09-01', :s, 10)"), {"s": slot})
        conn.execute(text("INSERT INTO marketdata.bess_actual_96 (asset_code, data_date, slot, "
                          "actual_mw) VALUES ('gushanliang', '2026-09-01', 1, 99), "
                          "('gushanliang', '2026-09-01', 2, 12.0)"))
        # ops: 00:15 loses to bess_actual_96; 00:30 is the only source
        conn.execute(text("INSERT INTO marketdata.ops_bess_dispatch_15min VALUES "
                          "('gushanliang', '2026-09-01 00:15:00', 10, 88), "
                          "('gushanliang', '2026-09-01 00:30:00', 10, 7.5)"))
        _cleared(conn, "裕昭沙子坝储能电站", "2026-09-01 02:00:00", 3.0)
        # out-of-range day must not leak in
        _cleared(conn, "裕昭沙子坝储能电站", "2026-09-05 02:00:00", 1.0)
        # asset 3 武川: two exchange accounts
        _cleared(conn, "旭纯哈乐储能电站", "2026-09-01 01:00:00", 10.0)
        _cleared(conn, "旭纯哈乐储能电站二", "2026-09-01 01:00:00", 15.0)
        _cleared(conn, "旭纯哈乐储能电站二", "2026-09-01 01:15:00", -4.0)
        # asset 9: no asset_code → legacy rows only
        _chain(conn, 9, "2026-09-01 00:00:00", 5, 4)
        _chain(conn, 9, "2026-09-01 00:15:00", -5, -4)
    yield eng
    eng.dispose()


@pytest.fixture()
def sqlite_engine_bad_sign():
    eng = _base_engine()
    with eng.begin() as conn:
        conn.execute(text("INSERT INTO marketdata.rm_assets VALUES "
                          "(1, '裕昭沙子坝', 'bess', 'test', 100, 4.0, 'gushanliang')"))
        _chain(conn, 1, "2026-09-01 00:00:00", 10, -10)
        _chain(conn, 1, "2026-09-01 00:15:00", -10, 10)
    yield eng
    eng.dispose()


def test_actuals_fill_from_bess_actual_96_then_ops(sqlite_engine):
    df = _load_chain(sqlite_engine, [1], "2026-09-01", "2026-09-02")
    # slot 00:00 has legacy actual → kept; 00:15 only in bess_actual_96 → 12.0; 00:30 only in ops → 7.5
    assert df.set_index("ts").loc["2026-09-01 00:00:00", "actual_mw"] == 5.0
    assert df.set_index("ts").loc["2026-09-01 00:15:00", "actual_mw"] == 12.0
    assert df.set_index("ts").loc["2026-09-01 00:30:00", "actual_mw"] == 7.5


def test_multi_account_cleared_sums_present_rows(sqlite_engine):
    # Review Focus 4: 武川 two accounts; slot A has both (10 + 15), slot B has one (−4)
    df = _load_chain(sqlite_engine, [3], "2026-09-01", "2026-09-02").set_index("ts")
    assert df.loc["2026-09-01 01:00:00", "rt_cleared_mw"] == 25.0
    assert df.loc["2026-09-01 01:15:00", "rt_cleared_mw"] == -4.0


def test_asset_without_code_returns_legacy_only(sqlite_engine):
    # Review Focus 3
    df = _load_chain(sqlite_engine, [9], None, None)
    assert len(df) == 2 and df["nominated_mw"].notna().all()


def test_sign_disagreement_raises(sqlite_engine_bad_sign):
    with pytest.raises(ValueError, match="sign disagreement"):
        _load_chain(sqlite_engine_bad_sign, [1], None, None)


def test_no_times_four_on_cleared(sqlite_engine):
    df = _load_chain(sqlite_engine, [1], "2026-09-01", "2026-09-02").set_index("ts")
    assert df.loc["2026-09-01 02:00:00", "rt_cleared_mw"] == 3.0   # stored 3.0 → 3.0


# --- additional coverage beyond the brief ---

def test_range_bounds_exclude_other_days(sqlite_engine):
    df = _load_chain(sqlite_engine, [1], "2026-09-01", "2026-09-02")
    assert df["ts"].max() < pd.Timestamp("2026-09-02")


def test_identity_columns_filled_and_shape(sqlite_engine):
    df = _load_chain(sqlite_engine, [1, 3], "2026-09-01", "2026-09-02")
    assert list(df.columns) == ["asset_id", "asset", "capacity_mw", "ts", "nominated_mw",
                                "da_cleared_mw", "rt_cleared_mw", "actual_mw",
                                "restriction", "soc_pct"]
    assert df["asset"].notna().all() and df["capacity_mw"].notna().all()
    assert set(df.loc[df["asset_id"] == 3, "asset"]) == {"武川"}


def test_month_without_overlap_does_not_raise(sqlite_engine):
    # 武川 has cleared rows only — no actual×nominated overlap → no guard trip
    assert not _load_chain(sqlite_engine, [3], None, None).empty


def test_pre_migration_uses_literal_code_for_actuals():
    eng = _base_engine(with_code=False)
    with eng.begin() as conn:
        conn.execute(text("INSERT INTO marketdata.rm_assets VALUES "
                          "(1, '裕昭沙子坝', 'bess', 'test', 100, 4.0)"))
        _chain(conn, 1, "2026-09-01 00:00:00", 10, None)
        conn.execute(text("INSERT INTO marketdata.bess_actual_96 (asset_code, data_date, slot, "
                          "actual_mw) VALUES ('gushanliang', '2026-09-01', 1, 6.0)"))
    df = _load_chain(eng, [1], None, None)
    assert df.iloc[0]["actual_mw"] == 6.0


def test_public_alias():
    assert load_chain is _load_chain
