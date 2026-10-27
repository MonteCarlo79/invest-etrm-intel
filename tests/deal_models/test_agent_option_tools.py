# tests/deal_models/test_agent_option_tools.py
import json
from libs.deal_models.adapters import agent_tools

def test_dispatch_get_option_value(monkeypatch):
    monkeypatch.setattr(agent_tools._snapshot, "read_latest",
                        lambda eng, p, a, d: {"province": p, "total_yuan_kw": 314.75, "asof_date": "2026-10-25"})
    out = agent_tools.dispatch_tool("get_option_value", {"province": "山东", "asset_type": "bess", "duration_h": 2.0})
    assert "314.75" in out and "山东" in out

def test_dispatch_get_option_value_no_row(monkeypatch):
    monkeypatch.setattr(agent_tools._snapshot, "read_latest", lambda *a: None)
    out = agent_tools.dispatch_tool("get_option_value", {"province": "福建", "asset_type": "bess", "duration_h": 2.0})
    assert "no snapshot" in out

def test_schemas_registered():
    names = {t["name"] for t in agent_tools.AGENT_TOOLS}
    assert {"get_option_value", "screen_option_values", "get_policy_levers",
            "list_opportunity_cards"} <= names

def test_dispatch_list_opportunity_cards(monkeypatch):
    rows = [{"id": 11, "kind": "permit", "province": "山西", "asset_type": "bess",
             "validation": "confirmed", "scan_date": "2026-10-26",
             "thesis": "期" * 200}]
    monkeypatch.setattr(agent_tools._scout_store, "list_cards",
                        lambda eng, status=None, kind=None: rows)
    payload = json.loads(agent_tools.dispatch_tool("list_opportunity_cards", {}))
    row = payload["rows"][0]
    assert row["id"] == 11 and row["kind"] == "permit" and row["province"] == "山西"
    assert row["asset_type"] == "bess" and row["validation"] == "confirmed"
    assert row["scan_date"] == "2026-10-26"
    assert len(row["thesis"]) == 120          # thesis truncated to first 120 chars

def test_dispatch_list_opportunity_cards_empty(monkeypatch):
    monkeypatch.setattr(agent_tools._scout_store, "list_cards", lambda *a, **k: [])
    out = agent_tools.dispatch_tool("list_opportunity_cards", {"status": "new"})
    assert "no cards" in out

def test_dispatch_list_opportunity_cards_db_error(monkeypatch):
    def boom(*a, **k): raise RuntimeError("conn refused")
    monkeypatch.setattr(agent_tools._scout_store, "list_cards", boom)
    payload = json.loads(agent_tools.dispatch_tool("list_opportunity_cards", {}))
    assert "conn refused" in payload["error"]
