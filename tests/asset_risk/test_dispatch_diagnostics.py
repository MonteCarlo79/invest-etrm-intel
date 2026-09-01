"""Tests for tab_diagnostics helpers (exec gap, bid fail, restrictions, defects).

sqlite-ATTACH harness (same shape as test_agent_tools.py): schema-prefixed SQL
runs unmodified; interval_start/datetime stored as Beijing-naive TEXT.
"""
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from apps.asset_risk.tab_diagnostics import (
    PLANT_MAP,
    _attach_prices,
    _load_chain,
    _load_prices,
    _month_asset_matrix,
    bid_fail_monthly,
    capacity_loss_monthly,
    exec_gap_monthly,
    find_defect_events,
    restriction_monthly,
)


@pytest.fixture()
def sqlite_engine():
    eng = create_engine("sqlite:///:memory:")
    with eng.connect() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS marketdata"))
        conn.execute(text("CREATE TABLE marketdata.rm_assets "
                          "(id INTEGER PRIMARY KEY, name TEXT, asset_type TEXT, province TEXT, "
                          "capacity_mw REAL, bess_duration_h REAL)"))
        conn.execute(text("CREATE TABLE marketdata.rm_dispatch_chain "
                          "(id INTEGER PRIMARY KEY, asset_id INTEGER, interval_start TEXT, "
                          "soc_pct REAL, nominated_mw REAL, da_cleared_mw REAL, "
                          "rt_cleared_mw REAL, actual_mw REAL, restriction TEXT, "
                          "source_file TEXT, upload_batch_id TEXT)"))
        conn.execute(text("CREATE TABLE marketdata.md_id_cleared_energy "
                          "(plant_name TEXT, datetime TEXT, cleared_price REAL, "
                          "cleared_energy_mwh REAL)"))
        conn.execute(text("INSERT INTO marketdata.rm_assets VALUES "
                          "(1, 'A1', 'bess', 'test', 100, 4.0), (2, 'A2', 'bess', 'test', 200, 4.0)"))

        def chain(aid, ts, nom, da, rt, act, res=None, src="xlsx"):
            conn.execute(text("INSERT INTO marketdata.rm_dispatch_chain "
                              "(asset_id, interval_start, nominated_mw, da_cleared_mw, "
                              "rt_cleared_mw, actual_mw, restriction, source_file) "
                              "VALUES (:a, :ts, :n, :d, :r, :ac, :res, :src)"),
                         {"a": aid, "ts": ts, "n": nom, "d": da, "r": rt, "ac": act,
                          "res": res, "src": src})

        def quarter(n, base="2026-06-01"):
            h, m = divmod(n * 15, 60)
            return f"{base} {h:02d}:{m:02d}:00"

        # Asset 1: discharge exec gap — 4 intervals rt=10, actual=7 (3 MWh, ¥900 @300)
        for i in range(4):
            chain(1, quarter(i), 10, 10, 10, 7)
        # below-threshold row (rt=0.4) — excluded from exec-gap masks
        chain(1, quarter(4), 0.4, 0.4, 0.4, 0.3)
        # charge exec gap — 4 intervals rt=-10, actual=-12 (-2 MWh, ¥-600)
        for i in range(5, 9):
            chain(1, quarter(i), -10, -10, -10, -12)
        # NULL actual (backfill-style) — excluded from exec-gap num AND denom
        chain(1, quarter(9), 10, 10, 10, None, src="backfill")
        # bid fail dis — 4 intervals nom=10, da=7 (3 MWh, ¥900)
        for i in range(10, 14):
            chain(1, quarter(i), 10, 7, 7, 7)
        # bid fail chg — 4 intervals nom=-10, da=-6 (-4 MWh, ¥-1200)
        for i in range(14, 18):
            chain(1, quarter(i), -10, -6, -6, -6)
        # restrictions (rows 18-19 carry rt values to exercise in-window gap fields)
        chain(1, quarter(18), 0, 0, 10, 7, res="charge_only")
        chain(1, quarter(19), 0, 0, -10, -12, res="charge_only")
        chain(1, quarter(20), 0, 0, 0, 0, res="discharge_only")
        # defect run: 5 consecutive actual=0, rt=30 (>25% of 100MW) — 37.5 MWh, ¥11250
        for i in range(21, 26):
            chain(1, quarter(i), 30, 30, 30, 0)
        # non-flagged separator (actual ≠ 0) — breaks the run
        chain(1, quarter(26), 30, 30, 30, 30)
        # short run (3, actual=0, rt=30) — rejected by min_run
        for i in range(27, 30):
            chain(1, quarter(i), 30, 30, 30, 0)
        # below-threshold run (rt=20 < 25) — rejected
        for i in range(30, 35):
            chain(1, quarter(i), 20, 20, 20, 0)
        # gap-split run: 2 flags, one 30-min time gap, 2 flags — two short runs, rejected
        chain(1, quarter(36), 30, 30, 30, 0)
        chain(1, quarter(37), 30, 30, 30, 0)
        chain(1, quarter(39), 30, 30, 30, 0)  # skips interval 38
        chain(1, quarter(40), 30, 30, 30, 0)
        # Asset 2 July rows (month grouping in matrix)
        chain(2, "2026-07-01 00:00:00", 10, 10, 10, 5)
        # Asset 3 锡西二 — capacity rate 280 (named-asset test)
        conn.execute(text("INSERT INTO marketdata.rm_assets VALUES "
                          "(3, '锡西二', 'bess', 'test', 100, 4.0)"))
        chain(3, "2026-06-01 00:00:00", 10, 10, 10, 5)

        # Prices for P1: 300 at every ts+15min except interval 0's target (00:15),
        # so interval 0 stays unpriced (MWh counted, ¥ skipped)
        def price(ts):
            conn.execute(text("INSERT INTO marketdata.md_id_cleared_energy "
                              "(plant_name, datetime, cleared_price, cleared_energy_mwh) "
                              "VALUES ('P1', :ts, 300.0, 0)"), {"ts": ts})

        for n in range(96):
            if n == 1:
                continue  # interval 0's price target — deliberately missing
            price(quarter(n))
        price("2026-07-01 00:15:00")  # asset 2's interval (P2 uses same value via map)
        conn.execute(text("UPDATE marketdata.md_id_cleared_energy SET plant_name='P2' "
                          "WHERE datetime='2026-07-01 00:15:00'"))
        conn.commit()
    yield eng
    eng.dispose()


# --- loaders (dialect passthrough, expanding-IN, range filter) ---

def test_load_chain_sqlite(sqlite_engine):
    df = _load_chain(sqlite_engine, [1], None, None)
    assert len(df) == 39
    assert str(df["ts"].iloc[0]) == "2026-06-01 00:00:00"
    assert set(df["asset"]) == {"A1"}


def test_load_chain_range_filter(sqlite_engine):
    df = _load_chain(sqlite_engine, [1, 2], "2026-07-01", None)
    assert set(df["asset"]) == {"A2"}
    assert len(df) == 1


def test_load_prices(sqlite_engine):
    df = _load_prices(sqlite_engine, ["P1"], None, "2026-06-02")
    assert len(df) == 95  # 96 minus the deliberately missing first
    assert set(df["plant_name"]) == {"P1"}


def test_attach_prices_period_end(sqlite_engine):
    chain = _load_chain(sqlite_engine, [1], None, None)
    prices = _load_prices(sqlite_engine, ["P1"], None, "2026-06-02")
    df = _attach_prices(chain, prices, {"A1": "P1"})
    first = df[df["ts"] == "2026-06-01 00:00:00"].iloc[0]
    second = df[df["ts"] == "2026-06-01 00:15:00"].iloc[0]
    assert pd.isna(first["price_cny_mwh"])          # unpriced interval stays NaN
    assert second["price_cny_mwh"] == 300.0         # priced at ts+15min


# --- metric helpers ---

def test_exec_gap_monthly_values(sqlite_engine):
    chain = _load_chain(sqlite_engine, [1], None, None)
    prices = _load_prices(sqlite_engine, ["P1"], None, "2026-06-02")
    df = _attach_prices(chain, prices, {"A1": "P1"})
    df["month"] = df["ts"].dt.strftime("%Y-%m")
    m = exec_gap_monthly(df)
    row = m[(m["asset"] == "A1") & (m["month"] == "2026-06")].iloc[0]
    # dis mask catches: 4 gap rows (3) + bid rows (0) + restriction row 18 (0.75)
    # + all defect-family rows (37.5+7.5sep0+22.5+25+30 = 122.5... see below) → gap 118.75
    assert row["dis_gap_mwh"] == pytest.approx(118.75)
    # ¥ = 118.75 x 300 minus the unpriced first interval (0.75 MWh x 300 = 225)
    assert row["dis_gap_cny"] == pytest.approx(35400.0)
    # denominator: 142 MWh (10 + 7 + 2.5 + 37.5 + 7.5 + 22.5 + 25 + 30);
    # backfill NULL-actual row (2.5) excluded from num AND denom
    assert row["dis_cleared_mwh"] == pytest.approx(142.0)
    assert row["dis_gap_pct"] == pytest.approx(118.75 / 142.0 * 100, abs=0.01)
    assert row["chg_gap_mwh"] == pytest.approx(-2.5)     # 4x(-12+10)x0.25 + row19 (-0.5)
    assert row["chg_gap_cny"] == pytest.approx(-750.0)
    assert row["chg_cleared_mwh"] == pytest.approx(18.5)  # 10 + 6 + 2.5
    assert row["chg_gap_pct"] == pytest.approx(-2.5 / 18.5 * 100, abs=0.01)


def test_bid_fail_monthly_values(sqlite_engine):
    chain = _load_chain(sqlite_engine, [1], None, None)
    prices = _load_prices(sqlite_engine, ["P1"], None, "2026-06-02")
    df = _attach_prices(chain, prices, {"A1": "P1"})
    df["month"] = df["ts"].dt.strftime("%Y-%m")
    m = bid_fail_monthly(df)
    row = m[(m["asset"] == "A1") & (m["month"] == "2026-06")].iloc[0]
    assert row["dis_fail_mwh"] == pytest.approx(3.0)     # 4 x (10-7) x 0.25
    assert row["dis_fail_cny"] == pytest.approx(900.0)
    assert row["chg_fail_mwh"] == pytest.approx(-4.0)    # 4 x (-6+10) x -0.25
    assert row["chg_fail_cny"] == pytest.approx(-1200.0)


def test_restriction_monthly(sqlite_engine):
    chain = _load_chain(sqlite_engine, [1], None, None)
    prices = _load_prices(sqlite_engine, ["P1"], None, "2026-06-02")
    df = _attach_prices(chain, prices, {"A1": "P1"})
    df["month"] = df["ts"].dt.strftime("%Y-%m")
    m = restriction_monthly(df)
    row = m[(m["asset"] == "A1") & (m["month"] == "2026-06")].iloc[0]
    assert row["charge_only_intervals"] == 2
    assert row["discharge_only_intervals"] == 1
    assert row["total_intervals"] == 39
    assert row["restricted_share"] == pytest.approx(3 / 39)
    assert row["moved_mwh"] == pytest.approx(5.0)        # (10 + 10 + 0) x 0.25
    # in-window exec gap: row18 (10-7)x0.25 = 0.75; row19 (-12+10)x0.25 = -0.5
    assert row["gap_dis_mwh"] == pytest.approx(0.75)
    assert row["gap_chg_mwh"] == pytest.approx(-0.5)
    assert row["gap_cny"] == pytest.approx(75.0)         # 0.75x300 + (-0.5)x300


def test_capacity_loss_exec_gap(sqlite_engine):
    chain = _load_chain(sqlite_engine, [1, 3], None, None)
    prices = _load_prices(sqlite_engine, ["P1"], None, "2026-06-02")
    df = _attach_prices(chain, prices, {"A1": "P1"})
    df["month"] = df["ts"].dt.strftime("%Y-%m")
    m = capacity_loss_monthly(df, kind="exec_gap")
    a1 = m[(m["asset"] == "A1") & (m["month"] == "2026-06")].iloc[0]
    assert a1["dis_shortfall_mwh"] == pytest.approx(118.75)
    assert a1["capcomp_rate"] == 350.0                    # default rate
    assert a1["capacity_loss_cny"] == pytest.approx(118.75 * 350.0)
    xxe = m[m["asset"] == "锡西二"].iloc[0]
    assert xxe["capcomp_rate"] == 280.0                   # named-asset rate
    assert xxe["capacity_loss_cny"] == pytest.approx(1.25 * 280.0)


def test_capacity_loss_bid_fail_and_override(sqlite_engine):
    chain = _load_chain(sqlite_engine, [1], None, None)
    prices = _load_prices(sqlite_engine, ["P1"], None, "2026-06-02")
    df = _attach_prices(chain, prices, {"A1": "P1"})
    df["month"] = df["ts"].dt.strftime("%Y-%m")
    m = capacity_loss_monthly(df, kind="bid_fail")
    row = m[(m["asset"] == "A1") & (m["month"] == "2026-06")].iloc[0]
    assert row["dis_shortfall_mwh"] == pytest.approx(3.0)
    assert row["capacity_loss_cny"] == pytest.approx(3.0 * 350.0)
    m2 = capacity_loss_monthly(df, kind="exec_gap", rate_map={"A1": 280.0})
    row2 = m2[(m2["asset"] == "A1") & (m2["month"] == "2026-06")].iloc[0]
    assert row2["capacity_loss_cny"] == pytest.approx(118.75 * 280.0)


# --- defect events ---

def test_find_defect_events(sqlite_engine):
    chain = _load_chain(sqlite_engine, [1], None, None)
    prices = _load_prices(sqlite_engine, ["P1"], None, "2026-06-02")
    df = _attach_prices(chain, prices, {"A1": "P1"})
    events = find_defect_events(df)
    assert len(events) == 1
    e = events.iloc[0]
    assert e["asset"] == "A1"
    assert e["intervals"] == 5
    assert e["lost_mwh"] == pytest.approx(37.5)          # 5 x 30 x 0.25
    assert e["lost_cny"] == pytest.approx(11250.0)       # 37.5 x 300


# --- matrix ---

def test_month_asset_matrix_totals(sqlite_engine):
    chain = _load_chain(sqlite_engine, [1, 2], None, None)
    p1 = _load_prices(sqlite_engine, ["P1"], None, "2026-06-02")
    p2 = _load_prices(sqlite_engine, ["P2"], None, "2026-07-02")
    df = _attach_prices(chain, pd.concat([p1, p2]), {"A1": "P1", "A2": "P2"})
    df["month"] = df["ts"].dt.strftime("%Y-%m")
    m = exec_gap_monthly(df)
    mat = _month_asset_matrix(m, "dis_gap_cny")
    assert "组合合计" in mat.columns
    assert "资产合计" in mat.index
    assert mat.loc["2026-06", "A1"] == pytest.approx(35400.0)
    assert mat.loc["2026-07", "A2"] == pytest.approx(375.0)   # (10-5) x 0.25 x 300
    assert mat.loc["资产合计", "组合合计"] == pytest.approx(mat.drop(index="资产合计")["组合合计"].sum())


def test_plant_map_covers_six_assets():
    assert len(PLANT_MAP) == 6
