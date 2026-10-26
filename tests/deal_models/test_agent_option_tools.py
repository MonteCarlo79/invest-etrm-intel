# tests/deal_models/test_agent_option_tools.py
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
    assert {"get_option_value", "screen_option_values", "get_policy_levers"} <= names
