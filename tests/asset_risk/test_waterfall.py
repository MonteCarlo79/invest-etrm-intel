"""Tests for tab_waterfall helpers (4-leg P&L waterfall + investment-standard params).

sqlite-ATTACH harness (same shape as test_dispatch_diagnostics.py).
"""
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from apps.asset_risk.standard_params import (
    STANDARD_DOD,
    STANDARD_RTE,
    STANDARD_SOH,
    age_years,
    params_for_asset,
)
from apps.asset_risk.tab_waterfall import (
    _load_settlement_actuals,
    build_waterfall,
    stage_arbitrage,
    stage_capacity,
)


# ---------- standard params (pure, no fixture needed) ----------

def test_age_years_mapping():
    assert age_years("2024-01-15", "2026-06-01") == 2
    assert age_years(None, "2026-06-01") == 0
    assert age_years("2010-01-01", "2026-06-01") == 15  # clipped
    assert age_years("2026-05-01", "2026-06-01") == 0


def test_params_for_asset():
    p0 = params_for_asset(100.0, 4.0, "2026-01-01", "2026-06-01")
    assert p0["age"] == 0
    assert p0["roundtrip_eff"] == pytest.approx(STANDARD_RTE[0])
    assert p0["energy_cap_mwh"] == pytest.approx(100 * 4 * STANDARD_DOD * STANDARD_SOH[0])
    p2 = params_for_asset(100.0, 4.0, "2024-01-15", "2026-06-01")
    assert p2["age"] == 2
    assert p2["roundtrip_eff"] == pytest.approx(STANDARD_RTE[2])
    assert p2["soh"] == pytest.approx(STANDARD_SOH[2])
    assert p2["energy_cap_mwh"] == pytest.approx(100 * 4 * STANDARD_DOD * STANDARD_SOH[2])


# ---------- DB fixture ----------

@pytest.fixture()
def sqlite_engine():
    eng = create_engine("sqlite:///:memory:")
    with eng.connect() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS marketdata"))
        conn.execute(text("CREATE TABLE marketdata.rm_assets "
                          "(id INTEGER PRIMARY KEY, name TEXT, asset_type TEXT, province TEXT, "
                          "capacity_mw REAL, bess_duration_h REAL, commission_date TEXT)"))
        conn.execute(text("CREATE TABLE marketdata.rm_books "
                          "(id INTEGER PRIMARY KEY, name TEXT, book_type TEXT, asset_id INTEGER)"))
        conn.execute(text("CREATE TABLE marketdata.rm_settlements "
                          "(id INTEGER PRIMARY KEY, book_id INTEGER, settlement_month TEXT)"))
        conn.execute(text("CREATE TABLE marketdata.rm_settlement_items "
                          "(id INTEGER PRIMARY KEY, settlement_id INTEGER, category TEXT, "
                          "peak_period TEXT, delivery_date TEXT, volume_mwh REAL, "
                          "price_cny_kwh REAL, amount_cny REAL, amount_receivable_cny REAL, "
                          "amount_settled_cny REAL, amount_diff_cny REAL, "
                          "counterparty TEXT, notes TEXT)"))
        conn.execute(text("CREATE TABLE marketdata.rm_dispatch_chain "
                          "(id INTEGER PRIMARY KEY, asset_id INTEGER, interval_start TEXT, "
                          "soc_pct REAL, nominated_mw REAL, da_cleared_mw REAL, "
                          "rt_cleared_mw REAL, actual_mw REAL, restriction TEXT)"))
        conn.execute(text("CREATE TABLE marketdata.md_id_cleared_energy "
                          "(plant_name TEXT, datetime TEXT, cleared_price REAL, "
                          "cleared_energy_mwh REAL)"))

        conn.execute(text("INSERT INTO marketdata.rm_assets VALUES "
                          "(1, 'A1', 'bess', 'test', 100, 4.0, '2024-01-15')"))
        conn.execute(text("INSERT INTO marketdata.rm_books VALUES (10, 'B1', 'asset', 1)"))
        conn.execute(text("INSERT INTO marketdata.rm_settlements VALUES "
                          "(100, 10, '2026-06-01'), (101, 10, '2026-07-01')"))
        # June bill: discharge +3,696,278; charge -2,828,643; capcomp +5,228,990;
        # coal_capacity_charge -749,172; system_operation -89,596; transmission -20,000
        items = [
            (100, "discharge_energy", 14996.29, 3696278.0),
            (100, "charge_energy", 16911.34, -2828643.0),
            (100, "capacity_compensation", 0.0, 5228990.0),
            (100, "coal_capacity_charge", None, -749172.0),
            (100, "system_operation", None, -89596.0),
            (100, "transmission", None, -20000.0),
            (101, "discharge_energy", 1000.0, 250000.0),
            (101, "charge_energy", 1200.0, -200000.0),
        ]
        for sid, cat, vol, amt in items:
            conn.execute(text("INSERT INTO marketdata.rm_settlement_items "
                              "(settlement_id, category, volume_mwh, amount_cny) "
                              "VALUES (:sid, :cat, :vol, :amt)"),
                         {"sid": sid, "cat": cat, "vol": vol, "amt": amt})

        def chain(aid, ts, nom, rt):
            conn.execute(text("INSERT INTO marketdata.rm_dispatch_chain "
                              "(asset_id, interval_start, nominated_mw, rt_cleared_mw) "
                              "VALUES (:a, :ts, :n, :r)"),
                         {"a": aid, "ts": ts, "n": nom, "r": rt})

        chain(1, "2026-06-01 00:00:00", 10, 8)     # discharge nomination/cleared
        chain(1, "2026-06-01 00:15:00", -12, -10)  # charge
        chain(1, "2026-06-01 00:30:00", 10, 8)
        # Prices: 300 for the first two intervals (at ts+15min), missing third
        conn.execute(text("INSERT INTO marketdata.md_id_cleared_energy VALUES "
                          "('P1', '2026-06-01 00:15:00', 300.0, 0)"))
        conn.execute(text("INSERT INTO marketdata.md_id_cleared_energy VALUES "
                          "('P1', '2026-06-01 00:30:00', 300.0, 0)"))
        conn.commit()
    yield eng
    eng.dispose()


# ---------- stage math ----------

def test_stage_arbitrage_values(sqlite_engine):
    df = pd.DataFrame({
        "asset": ["A1"] * 3,
        "month": ["2026-06"] * 3,
        "mw": [10.0, -12.0, 10.0],
        "price_cny_mwh": [300.0, 300.0, None],
    })
    m = stage_arbitrage(df.rename(columns={"mw": "nominated_mw"}), "nominated_mw")
    row = m[m["asset"] == "A1"].iloc[0]
    assert row["dis_mwh"] == pytest.approx(5.0)       # (10 + 10) x 0.25
    assert row["chg_mwh"] == pytest.approx(3.0)       # 12 x 0.25
    # arb = 10x0.25x300 + (-12)x0.25x300 + (NaN price skipped) = 750 - 900 = -150
    assert row["arb_cny"] == pytest.approx(-150.0)


def test_stage_capacity_rates():
    dis = pd.Series([100.0, 50.0], index=[0, 1])
    assets = pd.Series(["悦杭独贵", "锡西二"], index=[0, 1])
    cap = stage_capacity(dis, assets)
    assert cap.iloc[0] == pytest.approx(100.0 * 350.0)
    assert cap.iloc[1] == pytest.approx(50.0 * 280.0)


def test_load_settlement_actuals(sqlite_engine):
    df = _load_settlement_actuals(sqlite_engine, [1], None, None)
    june = df[df["month"] == "2026-06"].iloc[0]
    assert june["arb_cny"] == pytest.approx(3696278.0 - 2828643.0)
    assert june["cap_cny"] == pytest.approx(5228990.0)
    assert june["fee_cny"] == pytest.approx(-749172.0 - 89596.0)   # 系统运行费+线损 (signed)
    assert june["other_cny"] == pytest.approx(-20000.0)            # remaining categories
    july = df[df["month"] == "2026-07"].iloc[0]
    assert july["arb_cny"] == pytest.approx(50000.0)


def test_build_waterfall_bridge_identity(sqlite_engine):
    bench = pd.DataFrame({"asset": ["A1"], "month": ["2026-06"],
                          "arb_cny": [1000.0], "dis_mwh": [10.0], "cap_cny": [3500.0]})
    nominated = pd.DataFrame({"asset": ["A1"], "month": ["2026-06"],
                              "arb_cny": [900.0], "dis_mwh": [9.0], "cap_cny": [3150.0]})
    cleared = pd.DataFrame({"asset": ["A1"], "month": ["2026-06"],
                            "arb_cny": [800.0], "dis_mwh": [8.0], "cap_cny": [2800.0]})
    actual = pd.DataFrame({"asset": ["A1"], "month": ["2026-06"],
                           "arb_cny": [700.0], "cap_cny": [2500.0],
                           "fee_cny": [-100.0], "other_cny": [-50.0]})
    wf = build_waterfall(bench, nominated, cleared, actual)
    for comp in ["arb_cny", "cap_cny"]:
        std = wf.loc[(wf["stage"] == "投资标准") & (wf["component"] == comp), "cny"].iloc[0]
        d1 = wf.loc[(wf["stage"] == "Δ策略与预测") & (wf["component"] == comp), "cny"].iloc[0]
        nom = wf.loc[(wf["stage"] == "申报") & (wf["component"] == comp), "cny"].iloc[0]
        d2 = wf.loc[(wf["stage"] == "Δ出清校核") & (wf["component"] == comp), "cny"].iloc[0]
        clr = wf.loc[(wf["stage"] == "出清") & (wf["component"] == comp), "cny"].iloc[0]
        d3 = wf.loc[(wf["stage"] == "Δ执行与费用") & (wf["component"] == comp), "cny"].iloc[0]
        act = wf.loc[(wf["stage"] == "实际") & (wf["component"] == comp), "cny"].iloc[0]
        assert std + d1 == pytest.approx(nom)
        assert nom + d2 == pytest.approx(clr)
        assert clr + d3 == pytest.approx(act)
