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
    attribution_rows,
    build_waterfall,
    guard_cleared_leg,
    stage_arbitrage,
    stage_capacity,
)


# ---------- 出清腿降级守卫 (cleared-vs-billed volume, >30% downgrade) ----------

def _cleared_frame(dis_mwh, month="2026-09", asset="景怡查干哈达"):
    return pd.DataFrame({"asset": [asset], "month": [month], "dis_mwh": [dis_mwh],
                         "chg_mwh": [dis_mwh * 1.06], "arb_cny": [dis_mwh * 400.0],
                         "cap_cny": [dis_mwh * 350.0]})


def _actual_frame(dis_mwh, month="2026-09", asset="景怡查干哈达"):
    return pd.DataFrame({"asset": [asset], "month": [month], "arb_cny": [12301035.0],
                         "cap_cny": [37305119.0], "fee_cny": [-5113078.0],
                         "other_cny": [-522071.0], "dis_mwh": [dis_mwh],
                         "chg_mwh": [117940.6]})


def test_guard_cleared_leg_triggers_on_176pct_divergence():
    # 查干哈达 2026-09 real figures: cleared 294,457 vs billed 106,589 → 176%
    cleared, untrusted = guard_cleared_leg(_cleared_frame(294456.669), _actual_frame(106589.03))
    assert untrusted == [("景怡查干哈达", "2026-09")]
    row = cleared.iloc[0]
    assert row["dis_mwh"] == pytest.approx(106589.03)
    assert row["arb_cny"] == pytest.approx(12301035.0)
    assert row["cap_cny"] == pytest.approx(37305119.0)


def test_guard_cleared_leg_healthy_month_untouched():
    cleared, untrusted = guard_cleared_leg(_cleared_frame(100000.0), _actual_frame(106589.03))
    assert untrusted == []
    assert cleared.iloc[0]["dis_mwh"] == pytest.approx(100000.0)


def test_guard_cleared_leg_no_ground_truth_leaves_as_is():
    # billed discharge 0/absent → cannot verify → keep cleared as-is
    cleared, untrusted = guard_cleared_leg(_cleared_frame(294456.669), _actual_frame(0.0))
    assert untrusted == []
    assert cleared.iloc[0]["dis_mwh"] == pytest.approx(294456.669)


def test_guard_cleared_leg_empty_actual_noop():
    cleared = _cleared_frame(50000.0)
    out, untrusted = guard_cleared_leg(cleared, pd.DataFrame())
    assert untrusted == [] and out.equals(cleared)


def test_load_settlement_actuals_volumes(sqlite_engine):
    df = _load_settlement_actuals(sqlite_engine, [1], None, None)
    june = df[df["month"] == "2026-06"].iloc[0]
    assert "dis_mwh" in df.columns and "chg_mwh" in df.columns
    assert june["dis_mwh"] > 0 and june["chg_mwh"] > 0


# ---------- standard params (pure, no fixture needed) ----------

def test_age_years_mapping():
    assert age_years("2024-01-15", "2026-06-01") == 2
    assert age_years(None, "2026-06-01") == 0
    assert age_years("2010-01-01", "2026-06-01") == 15  # clipped
    assert age_years("2026-05-01", "2026-06-01") == 0


def test_age_years_timestamp_inputs():
    """pd.Timestamp inputs (from benchmark_leg's month_start) — prod bug 2026-09-02:
    Timestamp − datetime.date TypeError."""
    import pandas as pd
    assert age_years("2024-01-15", pd.Timestamp("2026-06-01")) == 2
    assert age_years(pd.Timestamp("2024-01-15"), "2026-06-01") == 2
    assert age_years(pd.Timestamp("2024-01-15"), pd.Timestamp("2026-06-01")) == 2


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


def test_load_settlement_actuals_month_token(sqlite_engine):
    """'YYYY-MM' start tokens are normalized to valid dates (prod bug 2026-09-02:
    '2026-08' was passed straight into a PG date comparison and crashed)."""
    df = _load_settlement_actuals(sqlite_engine, [1], "2026-07", None)
    assert set(df["month"]) == {"2026-07"}
    df2 = _load_settlement_actuals(sqlite_engine, [1], "2026-06", None)
    assert set(df2["month"]) == {"2026-06", "2026-07"}


def test_attribution_decomposition_identities():
    """Each attribution block sums exactly to its waterfall deviation column
    (waterfall sign convention: + gain, − loss)."""
    df = pd.DataFrame({
        "asset": ["A1"] * 6,
        "month": ["2026-06"] * 6,
        "ts": pd.date_range("2026-06-01", periods=6, freq="15min"),
        "nominated_mw": [10.0, 10.0, -10.0, -10.0, 10.0, 30.0],
        "da_cleared_mw": [8.0, 8.0, -8.0, -8.0, 8.0, 30.0],
        "rt_cleared_mw": [7.0, 7.0, -7.0, -7.0, 7.0, 30.0],
        "actual_mw": [7.0, 5.0, -7.0, -7.0, 0.0, 0.0],
        "restriction": [None, None, None, None, None, "charge_only"],
        "soc_pct": [50.0, 50.0, 96.0, 50.0, 50.0, 50.0],
        "capacity_mw": [100.0] * 6,
        "price_cny_mwh": [300.0] * 6,
    })
    bench = pd.DataFrame({"asset": ["A1"], "month": ["2026-06"],
                          "arb_cny": [1000.0], "dis_mwh": [10.0], "cap_cny": [3500.0]})
    nominated = pd.DataFrame({"asset": ["A1"], "month": ["2026-06"],
                              "arb_cny": [3000.0], "dis_mwh": [20.0], "cap_cny": [7000.0]})
    cleared = pd.DataFrame({"asset": ["A1"], "month": ["2026-06"],
                            "arb_cny": [2500.0], "dis_mwh": [18.0], "cap_cny": [6300.0]})
    actual = pd.DataFrame({"asset": ["A1"], "month": ["2026-06"],
                           "arb_cny": [2000.0], "cap_cny": [5600.0],
                           "fee_cny": [-100.0], "other_cny": [-50.0],
                           "dis_mwh": [16.0], "chg_mwh": [5.0]})
    wf = build_waterfall(bench, nominated, cleared, actual)
    fc_rows = [{"arb_cny": 800.0}]
    attr = attribution_rows(df, wf, bench, nominated, cleared, actual, fc_rows)
    _by = {(r["偏差区间"], r["成因"]): r["cny"] for _, r in attr.iterrows()}
    by_cause = _by

    # key-shape regression: merged benchmark rows carry BOTH arb_cny (benchmark)
    # and fc_arb_cny (forecast) — forecast must read fc_arb_cny, not arb_cny
    attr_m = attribution_rows(df, wf, bench, nominated, cleared, actual,
                              [{"arb_cny": 1000.0, "fc_arb_cny": 800.0}])
    assert dict(zip(attr_m["成因"], attr_m["cny"]))[
        "预测误差（预测最优 − 投资标准）"] == pytest.approx(-200.0)

    # Δ策略与预测 rows: −200 + 2200 = +2000 = wf arb delta (3000−1000)
    assert by_cause[("Δ策略与预测", "预测误差（预测最优 − 投资标准）")] == pytest.approx(-200.0)
    assert by_cause[("Δ策略与预测", "申报策略选择（申报 − 预测最优）")] == pytest.approx(2200.0)
    seg1 = (by_cause[("Δ策略与预测", "预测误差（预测最优 − 投资标准）")]
            + by_cause[("Δ策略与预测", "申报策略选择（申报 − 预测最优）")])
    assert seg1 == pytest.approx(2000.0)
    assert by_cause[("Δ策略与预测", "容量补偿（Δ放电量 × 补偿单价）")] == pytest.approx(7000.0 - 3500.0)

    # Δ出清校核: 日前 −150 + 实时 −75 = −225; 残差 = −500 − (−225) = −275
    assert by_cause[("Δ出清校核", "日前未中标（日前出清 − 申报）")] == pytest.approx(-150.0)
    assert by_cause[("Δ出清校核", "实时校核（实时出清 − 日前出清）")] == pytest.approx(-75.0)
    assert by_cause[("Δ出清校核", "残差（价格基差/出清降级调整）")] == pytest.approx(-275.0)
    assert by_cause[("Δ出清校核", "　其中：SOC校核（soc≥95% 仍出清充电）")] == pytest.approx(-525.0)

    # Δ执行与费用 rows tie to wf segment total −1350
    assert by_cause[("Δ执行与费用", "红绿灯限制窗口（仅充电/仅放电时段）")] == pytest.approx(-2250.0)
    assert by_cause[("Δ执行与费用", "设备非响应事件（出清≠0 实际≈0）")] == pytest.approx(0.0)
    assert by_cause[("Δ执行与费用", "阻塞/调度核减/涉网试验（调度残差）")] == pytest.approx(-675.0)
    assert by_cause[("Δ执行与费用", "系统运行费+线损（仅存在于结算单）")] == pytest.approx(-100.0)
    assert by_cause[("Δ执行与费用", "其他费用（仅存在于结算单）")] == pytest.approx(-50.0)
    assert by_cause[("Δ执行与费用", "结算口径/价格基差（残差）")] == pytest.approx(2425.0)

    # exact ties per segment
    for seg_name, seg_total in [("Δ策略与预测", 2000.0 + 3500.0),
                                ("Δ出清校核", -500.0 - 700.0),
                                ("Δ执行与费用", -500.0 - 700.0 - 100.0 - 50.0)]:
        rows = attr[attr["偏差区间"] == seg_name]
        rows = rows[~rows["成因"].str.startswith("　其中")]  # informational subset
        assert rows["cny"].sum() == pytest.approx(seg_total), seg_name

    # bridge identity still holds on the wf used above
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


def test_attribution_nan_chain_shows_no_data():
    """Auto-nomination era carries rt_cleared only — causes with all-NaN gating
    columns must show 无数据, never a fake ¥0."""
    df = pd.DataFrame({
        "asset": ["A1"] * 4,
        "month": ["2026-10"] * 4,
        "ts": pd.date_range("2026-10-01", periods=4, freq="15min"),
        "nominated_mw": [float("nan")] * 4,
        "da_cleared_mw": [float("nan")] * 4,
        "rt_cleared_mw": [5.0, 5.0, -5.0, -5.0],
        "actual_mw": [float("nan")] * 4,
        "restriction": [None] * 4,
        "soc_pct": [float("nan")] * 4,
        "capacity_mw": [100.0] * 4,
        "price_cny_mwh": [300.0] * 4,
    })
    bench = pd.DataFrame({"asset": ["A1"], "month": ["2026-10"],
                          "arb_cny": [1000.0], "dis_mwh": [10.0], "cap_cny": [3500.0]})
    nominated = pd.DataFrame({"asset": ["A1"], "month": ["2026-10"],
                              "arb_cny": [900.0], "dis_mwh": [9.0], "cap_cny": [3150.0]})
    cleared = pd.DataFrame({"asset": ["A1"], "month": ["2026-10"],
                            "arb_cny": [800.0], "dis_mwh": [8.0], "cap_cny": [2800.0]})
    actual = pd.DataFrame({"asset": ["A1"], "month": ["2026-10"],
                           "arb_cny": [700.0], "cap_cny": [2500.0],
                           "fee_cny": [-100.0], "other_cny": [-50.0],
                           "dis_mwh": [7.0], "chg_mwh": [3.0]})
    wf = build_waterfall(bench, nominated, cleared, actual)
    attr = attribution_rows(df, wf, bench, nominated, cleared, actual, [{"arb_cny": 800.0}])
    by_cause = {(r["偏差区间"], r["成因"]): r["cny"] for _, r in attr.iterrows()}
    import math
    assert math.isnan(by_cause[("Δ出清校核", "日前未中标（日前出清 − 申报）")])
    assert math.isnan(by_cause[("Δ出清校核", "实时校核（实时出清 − 日前出清）")])
    assert math.isnan(by_cause[("Δ执行与费用", "设备非响应事件（出清≠0 实际≈0）")])
    assert math.isnan(by_cause[("Δ执行与费用", "红绿灯限制窗口（仅充电/仅放电时段）")])
    fmt = dict(zip(attr["成因"], attr["金额_fmt"]))
    assert fmt["日前未中标（日前出清 − 申报）"] == "无数据"
    # wf-sourced rows still have values
    assert by_cause[("Δ策略与预测", "容量补偿（Δ放电量 × 补偿单价）")] == pytest.approx(3150.0 - 3500.0)


# ---------- benchmark LP price fallback (2026-10 cleared-price gap) ----------

@pytest.fixture()
def lp_solver(monkeypatch):
    """HiGHS instead of pulp's bundled x86_64 CBC (cannot run on this arm64
    Mac; production Fargate keeps CBC). Same workaround as test_market_impact."""
    import pulp
    monkeypatch.setattr(
        pulp, "PULP_CBC_CMD",
        lambda **kw: pulp.HiGHS(msg=False, timeLimit=kw.get("timeLimit")))


def test_benchmark_leg_falls_back_to_filled_nodal(sqlite_engine, lp_solver):
    """When the cleared-price feed has no complete 96-slot day in the month,
    benchmark_leg falls back to md_mengxi_nodal_price_96_filled (the
    station's own registry node) and flags note='节点填充电价'."""
    from apps.asset_risk.tab_waterfall import benchmark_leg

    with sqlite_engine.connect() as conn:
        conn.execute(text("INSERT INTO marketdata.rm_assets VALUES "
                          "(7, '裕昭沙子坝', 'bess', 'test', 500, 4.0, '2025-12-25')"))
        conn.execute(text("CREATE TABLE marketdata.md_mengxi_nodal_price_96_filled "
                          "(node_name TEXT, metric_time TEXT, time_order_96 INTEGER, "
                          "avg_node_price REAL)"))
        # cleared feed: rows exist but price is NULL (the 2026-10 gap shape)
        conn.execute(text("INSERT INTO marketdata.md_id_cleared_energy "
                          "(plant_name, datetime, cleared_price, cleared_energy_mwh) "
                          "VALUES ('裕昭沙子坝储能电站', '2026-10-01 00:15:00', NULL, 1.0)"))
        for slot in range(1, 97):
            ts = pd.Timestamp("2026-10-01") + pd.Timedelta(minutes=15 * (slot - 1))
            px = 10.0 if slot <= 48 else 100.0
            conn.execute(text("INSERT INTO marketdata.md_mengxi_nodal_price_96_filled "
                              "VALUES ('内蒙.裕昭沙子坝储能电站/220kV.1M', :t, :s, :p)"),
                         {"t": str(ts), "s": slot, "p": px})
        conn.commit()

    res = benchmark_leg(sqlite_engine, 7, "2026-10", 500.0, 4.0, "2025-12-25",
                        350.0, "裕昭沙子坝储能电站", asset_name="裕昭沙子坝")
    assert res["days_solved"] == 1
    assert res["arb_cny"] > 0
    assert res["note"] == "节点填充电价"


def test_benchmark_leg_no_fallback_when_cleared_complete(sqlite_engine, lp_solver):
    """Complete cleared-price days keep the original source (note stays '')."""
    from apps.asset_risk.tab_waterfall import benchmark_leg

    with sqlite_engine.connect() as conn:
        conn.execute(text("INSERT INTO marketdata.rm_assets VALUES "
                          "(8, '裕昭沙子坝', 'bess', 'test', 500, 4.0, '2025-12-25')"))
        for n in range(96):
            ts = pd.Timestamp("2026-06-02") + pd.Timedelta(minutes=15 * (n + 1))
            px = 10.0 if n < 48 else 100.0
            conn.execute(text("INSERT INTO marketdata.md_id_cleared_energy "
                              "(plant_name, datetime, cleared_price, cleared_energy_mwh) "
                              "VALUES ('裕昭沙子坝储能电站', :t, :p, 1.0)"),
                         {"t": str(ts), "p": px})
        conn.commit()

    res = benchmark_leg(sqlite_engine, 8, "2026-06", 500.0, 4.0, "2025-12-25",
                        350.0, "裕昭沙子坝储能电站", asset_name="裕昭沙子坝")
    assert res["days_solved"] == 1
    assert res["arb_cny"] > 0
    assert res["note"] == ""
