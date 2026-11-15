"""Risk Agent tools: no DB, no network. Data access is monkeypatched."""
import json
import subprocess
import sys
from datetime import date
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from libs.curves.curve import CURVE_COLUMNS
from libs.positions.conventions import POSITION_COLUMNS
from services.market_maker import agent_tools as at

AS_OF = date(2026, 11, 15)
M1, M2 = date(2026, 12, 1), date(2027, 1, 1)
DELIVERED = date(2026, 10, 1)
ROOT = str(__import__("pathlib").Path(__file__).resolve().parents[2])


def _pos(rows):
    out = []
    for r in rows:
        d = dict(book_id=1, province="蒙西", direction="buy", volume_mwh=0.0, delivery_month=M1,
                 tou_block="base", price_cny_mwh=None, source="physical", instrument_type="x",
                 channel="c", basis="b")
        d.update(r)
        out.append(d)
    return pd.DataFrame(out, columns=POSITION_COLUMNS)


def _curve(*months, price=330.0):
    return pd.DataFrame([dict(province="蒙西", delivery_month=m, tou_block="base",
                              price_cny_mwh=price, source="t") for m in months], columns=CURVE_COLUMNS)


@pytest.fixture
def wired(monkeypatch):
    pos = _pos([dict(volume_mwh=1000),
                dict(direction="sell", volume_mwh=600, price_cny_mwh=350.0, source="bilateral"),
                dict(delivery_month=M2, volume_mwh=100),          # no mark for M2
                dict(delivery_month=DELIVERED, volume_mwh=50)])   # delivered, no mark
    books = pd.DataFrame([dict(book_id=1, book_name="wind", book_type="asset", asset_id=1,
                               asset_type="wind", asset_code="w", province="蒙西",
                               capacity_mw=None, bess_duration_h=None)])
    monkeypatch.setattr(at, "assemble_positions", lambda e, ids, a: pos)
    monkeypatch.setattr(at, "books_with_province", lambda e: books)
    monkeypatch.setattr(at, "load_curve", lambda e, p, a: _curve(M1))
    monkeypatch.setattr(at, "_today", lambda: AS_OF)
    return pos


def test_tool_inventory():
    assert [t["name"] for t in at.tools] == [
        "get_book_list", "get_exposure", "get_mtm", "get_var", "get_greeks", "get_curve"]


def test_grounding_rule_opens_prompt():
    assert at.AGENT_BASE_SYSTEM.startswith(
        "Your knowledge comes exclusively from the data tools below")
    assert "marks_missing" in at.AGENT_BASE_SYSTEM and "provinces_missing" in at.AGENT_BASE_SYSTEM


def test_exposure_net_and_hedge(wired):
    r = at.execute_tool("get_exposure", {"book_id": 1}, object())
    row = next(x for x in r["records"] if x["delivery_month"] == "2026-12-01")
    assert row["net_mwh"] == 400 and row["hedge_ratio"] == pytest.approx(0.6)
    assert row["delivered"] is False
    assert next(x for x in r["records"] if x["delivery_month"] == "2026-10-01")["delivered"] is True
    json.dumps(r)  # serialisable, no NaN objects


def test_missing_mark_flagged_not_zero(wired):
    r = at.execute_tool("get_mtm", {}, object())
    assert "2027-01-01" in r["marks_missing"]
    m2 = [x for x in r["records"] if x["delivery_month"] == "2027-01-01"]
    assert m2 and all(x["mtm_cny"] is None and x["mark_cny_mwh"] is None for x in m2)
    m1 = next(x for x in r["records"] if x["delivery_month"] == "2026-12-01")
    assert m1["mtm_cny"] == pytest.approx(12000.0)
    assert r["total_mtm_cny"] == pytest.approx(12000.0)
    assert "NaN" not in json.dumps(r)


def test_var_forward_only_and_missing(wired, monkeypatch):
    seen = {}
    def scen(e, prov, a):
        seen["called"] = prov
        return pd.Series(dtype=float)
    monkeypatch.setattr(at, "_price_scenarios", scen)
    r = at.execute_tool("get_var", {"confidence": 0.95}, object())
    assert r["provinces_missing"] == ["蒙西"]
    assert r["delta_missing_mwh"]["蒙西"] == 500  # 400 (Dec) + 100 (Jan); delivered Oct excluded
    assert r["historical"] is None


def test_var_value(wired, monkeypatch):
    idx = pd.date_range("2026-01-01", periods=60)
    monkeypatch.setattr(at, "_price_scenarios",
                        lambda e, p, a: pd.Series(np.sin(np.arange(60)) * 10, index=idx))
    r = at.execute_tool("get_var", {"confidence": 0.99}, object())
    assert r["historical"] > 0 and r["provinces_missing"] == [] and r["confidence"] == 0.99


def test_greeks_delta_forward_only(wired):
    r = at.execute_tool("get_greeks", {}, object())
    months = [x["delivery_month"] for x in r["delta_by_month"]]
    assert "2026-10-01" not in months and "2026-12-01" in months
    assert r["bess_strips"] == []


def test_book_list_and_curve(wired, monkeypatch):
    bl = at.execute_tool("get_book_list", {}, object())
    assert bl["records"][0]["book_id"] == 1 and bl["records"][0]["capacity_mw"] is None
    c = at.execute_tool("get_curve", {"province": "蒙西"}, object())
    assert c["records"][0]["price_cny_mwh"] == 330.0


def test_unknown_tool_and_exceptions_are_data(monkeypatch):
    assert "error" in at.execute_tool("nope", {}, object())
    def boom(*a, **k):
        raise RuntimeError("db down")
    monkeypatch.setattr(at, "books_with_province", boom)
    assert at.execute_tool("get_book_list", {}, object()) == {"error": "db down"}


def test_headless_loop_executes_tool_and_returns_text(monkeypatch):
    from services.market_maker import headless_agent as ha
    calls = []
    def create(**kw):
        calls.append(kw)
        if len(calls) == 1:
            blk = SimpleNamespace(type="tool_use", name="get_book_list", input={}, id="t1")
            return SimpleNamespace(stop_reason="tool_use", content=[blk])
        return SimpleNamespace(stop_reason="end_turn",
                               content=[SimpleNamespace(type="text", text="done")])
    client = SimpleNamespace(messages=SimpleNamespace(create=create))
    monkeypatch.setattr(ha, "_make_client", lambda k: client)
    monkeypatch.setattr(ha, "_make_engine", lambda u: object())
    monkeypatch.setattr(ha, "execute_tool", lambda n, i, e: {"error": "x"})
    assert ha.run_market_maker_query("q", "key", "url") == "done"
    assert calls[0]["system"].startswith("Your knowledge comes exclusively")
    assert "2026" in calls[0]["system"].split("Your knowledge", 1)[1][-300:] or "今天是" in calls[0]["system"]
    assert calls[1]["messages"][-1]["content"][0]["tool_use_id"] == "t1"


@pytest.mark.parametrize("mod", ["services.market_maker.agent_tools",
                                 "services.market_maker.headless_agent"])
def test_import_is_streamlit_and_apps_free(mod):
    code = ("import sys, importlib; importlib.import_module(%r);"
            "bad=[m for m in sys.modules if m=='streamlit' or m.startswith('streamlit.')"
            " or m=='apps' or m.startswith('apps.')];"
            "print(bad); sys.exit(1 if bad else 0)" % mod)
    r = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr


def test_skipped_notes_in_var_and_exposure(monkeypatch):
    pos = _pos([dict(volume_mwh=1000)])
    pos.attrs["skipped"] = ["book 30: unknown province: 'hubei'"]
    monkeypatch.setattr(at, "assemble_positions", lambda e, ids, a: pos)
    monkeypatch.setattr(at, "load_curve", lambda e, p, a: _curve(M1))
    monkeypatch.setattr(at, "_price_scenarios", lambda e, p, a: pd.Series(dtype=float))
    monkeypatch.setattr(at, "_today", lambda: AS_OF)
    for tool in ("get_var", "get_exposure"):
        r = at.execute_tool(tool, {}, object())
        assert r["skipped"] == ["book 30: unknown province: 'hubei'"], tool
        json.dumps(r)


def test_skipped_empty_when_clean(wired, monkeypatch):
    monkeypatch.setattr(at, "_price_scenarios", lambda e, p, a: pd.Series(dtype=float))
    assert at.execute_tool("get_var", {}, object())["skipped"] == []
    assert at.execute_tool("get_exposure", {}, object())["skipped"] == []


def test_prompt_mentions_skipped():
    assert ("If a tool result has a non-empty 'skipped' list, state that these books/rows are "
            "excluded and the totals are incomplete.") in at.AGENT_BASE_SYSTEM
