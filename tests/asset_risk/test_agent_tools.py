"""Tests for tab_agent tools (get_settlement_summary, get_deviation_analysis)
and platform-pattern invariants (grounding rule, tool inventory).

Uses in-memory sqlite with `marketdata` attached so schema-prefixed SQL runs
unmodified (same harness as tests/asset_risk/test_pnl_filters.py).
"""
import json

import pytest
from sqlalchemy import create_engine, text

from apps.asset_risk.tab_agent import tools, _execute_tool, _AGENT_BASE_SYSTEM


@pytest.fixture()
def sqlite_engine():
    eng = create_engine("sqlite:///:memory:")
    with eng.connect() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS marketdata"))
        conn.execute(text("CREATE TABLE marketdata.rm_assets "
                          "(id INTEGER PRIMARY KEY, name TEXT, asset_type TEXT, province TEXT, "
                          "capacity_mw REAL)"))
        conn.execute(text("CREATE TABLE marketdata.rm_books "
                          "(id INTEGER PRIMARY KEY, name TEXT, book_type TEXT, asset_id INTEGER)"))
        conn.execute(text("CREATE TABLE marketdata.rm_settlements "
                          "(id INTEGER PRIMARY KEY, book_id INTEGER, settlement_month TEXT)"))
        conn.execute(text("CREATE TABLE marketdata.rm_settlement_items "
                          "(id INTEGER PRIMARY KEY, settlement_id INTEGER, category TEXT, "
                          "amount_cny REAL, volume_mwh REAL, notes TEXT)"))
        conn.execute(text("CREATE TABLE marketdata.rm_dispatch_chain "
                          "(id INTEGER PRIMARY KEY, asset_id INTEGER, interval_start TEXT, "
                          "soc_pct REAL, nominated_mw REAL, da_cleared_mw REAL, "
                          "rt_cleared_mw REAL, actual_mw REAL, restriction TEXT)"))

        conn.execute(text("INSERT INTO marketdata.rm_assets VALUES (1, 'A1', 'bess', '内蒙古', 100)"))
        conn.execute(text("INSERT INTO marketdata.rm_books VALUES (7, 'B7', 'asset', 1)"))
        conn.execute(text("INSERT INTO marketdata.rm_books VALUES (8, 'B8', 'asset', NULL)"))
        conn.execute(text("INSERT INTO marketdata.rm_assets VALUES (2, 'A2', 'bess', '内蒙古', 100)"))
        conn.execute(text("INSERT INTO marketdata.rm_books VALUES (9, 'B9', 'asset', 2)"))
        # one 15-min interval at 100 MW on 2026-04-01 → 25 MWh (R8 unit pin)
        conn.execute(text("INSERT INTO marketdata.rm_dispatch_chain "
                          "(asset_id, interval_start, nominated_mw, da_cleared_mw, "
                          "rt_cleared_mw, actual_mw) "
                          "VALUES (2, '2026-04-01 10:00:00', 100, 100, 100, 100)"))
        conn.execute(text("INSERT INTO marketdata.rm_settlements VALUES (1, 7, '2026-02-01')"))
        conn.execute(text("INSERT INTO marketdata.rm_settlements VALUES (2, 7, '2026-03-01')"))
        items = [
            (1, "discharge_energy", 10000.0, 100.0),
            (1, "charge_energy", -4000.0, 120.0),
            (1, "capacity_compensation", 1500.0, 0.0),
            (2, "discharge_energy", 20000.0, 200.0),
        ]
        for sid, cat, amt, vol in items:
            conn.execute(text("INSERT INTO marketdata.rm_settlement_items "
                              "(settlement_id, category, amount_cny, volume_mwh) "
                              "VALUES (:sid, :cat, :amt, :vol)"),
                         {"sid": sid, "cat": cat, "amt": amt, "vol": vol})

        # 12 fifteen-minute-slot rows on 2026-02-01: 10 MW nominated -> 9 -> 8 -> 7 actual
        for i in range(12):
            conn.execute(text(
                "INSERT INTO marketdata.rm_dispatch_chain "
                "(asset_id, interval_start, nominated_mw, da_cleared_mw, rt_cleared_mw, "
                "actual_mw, restriction) "
                "VALUES (1, :ts, 10, 9, 8, 7, :res)"),
                {"ts": f"2026-02-01 00:{i*5:02d}:00",
                 "res": "charge_only" if i == 0 else None})
        conn.commit()
    yield eng
    eng.dispose()


# --- get_settlement_summary ---

def test_settlement_summary_single_month(sqlite_engine):
    r = _execute_tool("get_settlement_summary", {"book_id": 7, "month": "2026-02"}, sqlite_engine)
    assert r["net_pnl_cny"] == pytest.approx(7500.0)          # 10000 - 4000 + 1500
    assert r["arb_income_cny"] == pytest.approx(6000.0)       # discharge + charge
    assert r["discharge_mwh"] == pytest.approx(100.0)
    assert r["charge_mwh"] == pytest.approx(120.0)
    assert r["arb_spread_cny_mwh"] == pytest.approx(60.0)     # 6000 / 100
    assert len(r["by_category"]) == 3


def test_settlement_summary_empty_month(sqlite_engine):
    r = _execute_tool("get_settlement_summary", {"book_id": 7, "month": "2026-06"}, sqlite_engine)
    assert "No settlement data" in r["message"]


# --- get_deviation_analysis ---

def test_deviation_analysis_chain(sqlite_engine):
    r = _execute_tool("get_deviation_analysis",
                      {"book_id": 7, "start_date": "2026-02-01", "end_date": "2026-02-01"},
                      sqlite_engine)
    # R8: MW × 0.25 h per interval (was /12, a 5-minute assumption) → ×3
    assert r["intervals"] == 12
    assert r["nominated_mwh"] == pytest.approx(30.0)
    assert r["da_cleared_mwh"] == pytest.approx(27.0)
    assert r["rt_cleared_mwh"] == pytest.approx(24.0)
    assert r["actual_mwh"] == pytest.approx(21.0)
    assert r["da_vs_nominated_mwh"] == pytest.approx(-3.0)
    assert r["rt_vs_da_mwh"] == pytest.approx(-3.0)
    assert r["actual_vs_rt_mwh"] == pytest.approx(-3.0)
    assert r["restricted_intervals"] == 1


def test_deviation_analysis_one_interval_100mw_is_25mwh(sqlite_engine):
    r = _execute_tool("get_deviation_analysis",
                      {"book_id": 9, "start_date": "2026-04-01", "end_date": "2026-04-01"},
                      sqlite_engine)
    assert r["intervals"] == 1
    assert r["nominated_mwh"] == pytest.approx(25.0)
    assert r["actual_mwh"] == pytest.approx(25.0)


def test_deviation_analysis_no_linked_asset(sqlite_engine):
    r = _execute_tool("get_deviation_analysis",
                      {"book_id": 8, "start_date": "2026-02-01", "end_date": "2026-02-01"},
                      sqlite_engine)
    assert "no linked asset" in r["message"].lower()


def test_deviation_analysis_no_data_in_range(sqlite_engine):
    r = _execute_tool("get_deviation_analysis",
                      {"book_id": 7, "start_date": "2026-05-01", "end_date": "2026-05-02"},
                      sqlite_engine)
    assert "No dispatch-chain data" in r["message"]


# --- Platform-pattern invariants ---

def test_tool_inventory_exact():
    names = [t["name"] for t in tools]
    assert sorted(names) == sorted([
        "get_asset_list", "get_book_pnl", "get_settlement_summary",
        "get_deviation_analysis", "get_waterfall", "get_diagnostics",
        "get_merchant_share"])


def test_removed_tools_unknown(sqlite_engine):
    for name in ("get_position_mtm", "get_var"):
        assert "error" in _execute_tool(name, {"book_id": 7}, sqlite_engine)


def test_system_prompt_lists_new_tools_not_removed():
    for t in ("get_waterfall", "get_diagnostics", "get_merchant_share"):
        assert t in _AGENT_BASE_SYSTEM
    for t in ("get_position_mtm", "get_var"):
        assert t not in _AGENT_BASE_SYSTEM


def test_system_prompt_has_grounding_rule():
    flat = " ".join(_AGENT_BASE_SYSTEM.split())
    assert "never from general training data" in flat
    assert "tool call in this conversation" in flat


# --- get_merchant_share ---

def test_get_merchant_share_tool(sqlite_engine):
    with sqlite_engine.begin() as conn:
        # March: canonical discharge 20000 + capacity 5000, plus an exchange
        # 售电侧合计 twin that prefer_canonical_settlements must drop.
        conn.execute(text("INSERT INTO marketdata.rm_settlement_items "
                          "(settlement_id, category, amount_cny, volume_mwh, notes) VALUES "
                          "(2, 'capacity_compensation', 5000, 0, NULL), "
                          "(2, 'discharge_energy', 99999, 0, '放电结算: 交易中心售电侧合计')"))
    r = _execute_tool("get_merchant_share", {"book_id": 7}, sqlite_engine)
    assert [m["month"] for m in r["monthly"]] == ["2026-02", "2026-03"]
    feb = r["monthly"][0]
    assert feb["merchant_share"] == pytest.approx(10000 / 11500)
    assert r["latest"]["month"] == "2026-03"
    assert r["latest"]["merchant_share"] == pytest.approx(20000 / 25000)
    json.dumps(r)  # JSON-serialisable


def test_get_merchant_share_no_data(sqlite_engine):
    r = _execute_tool("get_merchant_share", {"book_id": 8}, sqlite_engine)
    assert r["monthly"] == [] and r["latest"] is None


# --- get_diagnostics / get_waterfall (pre-migration literal maps, 悦杭独贵) ---

_PLANT = "悦杭独贵储能电站"


@pytest.fixture()
def wf_engine():
    eng = create_engine("sqlite:///:memory:")
    with eng.begin() as conn:
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
                          "amount_cny REAL, volume_mwh REAL, notes TEXT)"))
        conn.execute(text("CREATE TABLE marketdata.rm_dispatch_chain "
                          "(id INTEGER PRIMARY KEY, asset_id INTEGER, interval_start TEXT, "
                          "soc_pct REAL, nominated_mw REAL, da_cleared_mw REAL, "
                          "rt_cleared_mw REAL, actual_mw REAL, restriction TEXT)"))
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
        conn.execute(text("INSERT INTO marketdata.rm_assets VALUES "
                          "(1, '悦杭独贵', 'bess', '内蒙古', 100, 4.0, '2024-06-01')"))
        conn.execute(text("INSERT INTO marketdata.rm_books VALUES (1, 'B1', 'asset', 1)"))
        conn.execute(text("INSERT INTO marketdata.rm_settlements VALUES (1, 1, '2026-06-01')"))
        conn.execute(text("INSERT INTO marketdata.rm_settlement_items "
                          "(settlement_id, category, amount_cny, volume_mwh) VALUES "
                          "(1, 'discharge_energy', 3000, 10), (1, 'charge_energy', -1000, -10), "
                          "(1, 'capacity_compensation', 3500, 0)"))
        # 4 discharge slots (10 MW, price 300) then 4 charge slots (−10 MW, price 100);
        # slot 0 actual 7 → exec gap 0.75 MWh.
        for i in range(8):
            h, m = divmod(i * 15, 60)
            ts = f"2026-06-01 {h:02d}:{m:02d}:00"
            mw = 10.0 if i < 4 else -10.0
            act = 7.0 if i == 0 else mw
            conn.execute(text(
                "INSERT INTO marketdata.rm_dispatch_chain "
                "(asset_id, interval_start, nominated_mw, da_cleared_mw, rt_cleared_mw, "
                "actual_mw) VALUES (1, :ts, :mw, :mw, :mw, :act)"),
                {"ts": ts, "mw": mw, "act": act})
            h2, m2 = divmod((i + 1) * 15, 60)
            conn.execute(text(
                "INSERT INTO marketdata.md_id_cleared_energy VALUES "
                "(:p, :ts, '2026-06-01', :price, :mw)"),
                {"p": _PLANT, "ts": f"2026-06-01 {h2:02d}:{m2:02d}:00",
                 "price": 300.0 if i < 4 else 100.0, "mw": mw})
    yield eng
    eng.dispose()


def test_get_diagnostics_tool_shape(wf_engine):
    r = _execute_tool("get_diagnostics", {"asset": "悦杭独贵", "start_date": "2026-06-01",
                                          "end_date": "2026-06-30"}, wf_engine)
    assert set(r) >= {"exec_gap", "bid_fail", "restriction", "defect_events"}
    for k in ("exec_gap", "bid_fail", "restriction", "defect_events"):
        assert isinstance(r[k], list)
    gap = r["exec_gap"][0]
    assert gap["asset"] == "悦杭独贵" and gap["month"] == "2026-06"
    assert gap["dis_gap_mwh"] == pytest.approx(0.75)
    json.dumps(r)


def test_get_diagnostics_unknown_asset(wf_engine):
    r = _execute_tool("get_diagnostics", {"asset": "nope", "start_date": "2026-06-01",
                                          "end_date": "2026-06-30"}, wf_engine)
    assert "error" in r


@pytest.fixture()
def _fake_legs(monkeypatch):
    import libs.asset_risk.waterfall as wf
    monkeypatch.setattr(wf, "benchmark_leg", lambda *a, **k: {
        "arb_cny": 4000.0, "dis_mwh": 12.0, "chg_mwh": 13.0, "cap_cny": 4200.0,
        "days_solved": 1, "days_skipped": 29, "age": 2, "note": ""})
    monkeypatch.setattr(wf, "forecast_optimal_leg",
                        lambda *a, **k: {"arb_cny": 2500.0, "note": ""})


def test_get_waterfall_tool(wf_engine, _fake_legs):
    r = _execute_tool("get_waterfall", {"asset": "悦杭独贵", "months": ["2026-06"]}, wf_engine)
    st = r["stage_totals"]
    assert st["投资标准"] == pytest.approx(8200.0)        # 4000 + 4200
    # 申报: arb 4×10×0.25×300 − 4×10×0.25×100 = 2000; cap 10 MWh × 350 = 3500
    assert st["申报"] == pytest.approx(5500.0)
    assert st["出清"] == pytest.approx(5500.0)
    assert st["实际"] == pytest.approx(5500.0)            # bills 3000 − 1000 + 3500
    assert st["Δ策略与预测"] == pytest.approx(-2700.0)
    assert r["attribution"] and all("_fmt" not in k for row in r["attribution"] for k in row)
    assert {"偏差区间", "成因", "mwh", "cny"} <= set(r["attribution"][0])
    json.dumps(r)


def test_get_waterfall_unit_ratio_error(wf_engine, _fake_legs):
    with wf_engine.begin() as conn:
        conn.execute(text("UPDATE marketdata.rm_settlement_items SET volume_mwh = 3 "
                          "WHERE category = 'discharge_energy'"))
    r = _execute_tool("get_waterfall", {"asset": "悦杭独贵", "months": ["2026-06"]}, wf_engine)
    assert r["error"].startswith("cleared/billed volume ratio > 3 for:")
    assert "悦杭独贵 2026-06" in r["error"]


# --- headless agent shares the grounding prompt ---

def test_headless_agent_uses_shared_system_prompt():
    from unittest.mock import MagicMock, patch
    from services.asset_risk.headless_agent import run_asset_risk_query
    resp = MagicMock(stop_reason="end_turn", content=[MagicMock(text="ok", type="text")])
    client = MagicMock()
    client.messages.create.return_value = resp
    with patch("services.asset_risk.headless_agent._make_client", return_value=client), \
         patch("services.asset_risk.headless_agent._make_engine", return_value=MagicMock()):
        run_asset_risk_query("q", api_key="k", pg_url="postgresql://x")
    system = client.messages.create.call_args.kwargs["system"]
    assert system.startswith("今天是 ")
    assert _AGENT_BASE_SYSTEM in system
    tool_names = {t["name"] for t in client.messages.create.call_args.kwargs["tools"]}
    assert "get_waterfall" in tool_names and "get_var" not in tool_names
