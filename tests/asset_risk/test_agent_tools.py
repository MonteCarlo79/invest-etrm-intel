"""Tests for tab_agent tools (get_settlement_summary, get_deviation_analysis)
and platform-pattern invariants (grounding rule, tool inventory).

Uses in-memory sqlite with `marketdata` attached so schema-prefixed SQL runs
unmodified (same harness as tests/asset_risk/test_pnl_filters.py).
"""
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
                          "amount_cny REAL, volume_mwh REAL)"))
        conn.execute(text("CREATE TABLE marketdata.rm_dispatch_chain "
                          "(id INTEGER PRIMARY KEY, asset_id INTEGER, interval_start TEXT, "
                          "soc_pct REAL, nominated_mw REAL, da_cleared_mw REAL, "
                          "rt_cleared_mw REAL, actual_mw REAL, restriction TEXT)"))

        conn.execute(text("INSERT INTO marketdata.rm_assets VALUES (1, 'A1', 'bess', '内蒙古', 100)"))
        conn.execute(text("INSERT INTO marketdata.rm_books VALUES (7, 'B7', 'asset', 1)"))
        conn.execute(text("INSERT INTO marketdata.rm_books VALUES (8, 'B8', 'asset', NULL)"))
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

        # 12 five-minute intervals on 2026-02-01: 10 MW nominated -> 9 -> 8 -> 7 actual
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
    assert r["intervals"] == 12
    assert r["nominated_mwh"] == pytest.approx(10.0)
    assert r["da_cleared_mwh"] == pytest.approx(9.0)
    assert r["rt_cleared_mwh"] == pytest.approx(8.0)
    assert r["actual_mwh"] == pytest.approx(7.0)
    assert r["da_vs_nominated_mwh"] == pytest.approx(-1.0)
    assert r["rt_vs_da_mwh"] == pytest.approx(-1.0)
    assert r["actual_vs_rt_mwh"] == pytest.approx(-1.0)
    assert r["restricted_intervals"] == 1


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

def test_tool_inventory_includes_spec_tools():
    names = {t["name"] for t in tools}
    # spec'd set (handoff item #2): original 4 + the 2 that were missing
    assert {"get_book_pnl", "get_position_mtm", "get_var", "get_asset_list",
            "get_settlement_summary", "get_deviation_analysis"} <= names


def test_system_prompt_has_grounding_rule():
    flat = " ".join(_AGENT_BASE_SYSTEM.split())
    assert "never from general training data" in flat
    assert "tool call in this conversation" in flat
