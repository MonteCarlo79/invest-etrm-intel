"""Vault-context injection tests for the bess_map and mengxi headless agents.

Proves the vault block returned by vault_reader.retrieve_vault_context reaches
the `system` kwarg of the anthropic messages.create call in both agents.

Deviation from task brief (hermeticity): in addition to the brief's patches we
stub the other knowledge/DB/LLM touchpoints (advanced_retrieval.retrieve_for_agent,
expert_memory read/write helpers) so the tests make no real DB or Anthropic
network calls. All stubs patch module attributes; the headless agents resolve
these names via call-time `from ... import ...`, so the patches hold.
"""
from unittest.mock import MagicMock

import services.knowledge_pool.vault_reader as vr
import services.knowledge_pool.expert_memory as em
import services.knowledge_pool.advanced_retrieval as ar
import shared.anthropic_client as ac


def _fake_llm_client():
    resp = MagicMock()
    resp.stop_reason = "end_turn"
    block = MagicMock()
    block.text = "答案"
    resp.content = [block]
    client = MagicMock()
    client.messages.create.return_value = resp
    return client


def _stub_expert_memory(monkeypatch):
    monkeypatch.setattr(em, "get_relevant_insights", lambda *a, **kw: [])
    monkeypatch.setattr(em, "inject_expert_memory", lambda *a, **kw: "")
    monkeypatch.setattr(em, "extract_spot_insights", lambda **kw: 0)


def test_bess_map_system_contains_vault_block(monkeypatch):
    import services.bess_map.headless_agent as ba
    monkeypatch.setattr(vr, "retrieve_vault_context", lambda q: "## Vault knowledge\nMARKER_BESS")
    monkeypatch.setattr(ba, "_make_engine", lambda pg_url="": MagicMock(dispose=lambda: None))
    monkeypatch.setattr(ar, "retrieve_for_agent", lambda **kw: "")
    _stub_expert_memory(monkeypatch)
    fake = _fake_llm_client()
    monkeypatch.setattr(ac, "make_client", lambda api_key: fake)
    ba.run_bess_map_query("山东价格", api_key="k", pg_url="")
    system = fake.messages.create.call_args.kwargs["system"]
    assert "MARKER_BESS" in system


def test_mengxi_system_contains_vault_block(monkeypatch):
    import services.mengxi_trading.headless_agent as ma
    monkeypatch.setattr(vr, "retrieve_vault_context", lambda q: "## Vault knowledge\nMARKER_MX")
    monkeypatch.setattr(ma, "_make_engine", lambda pg_url="": MagicMock())
    _stub_expert_memory(monkeypatch)
    fake = _fake_llm_client()
    monkeypatch.setattr(ac, "make_client", lambda api_key: fake)
    ma.run_mengxi_query("蒙西收益", api_key="k", pg_url="")
    system = fake.messages.create.call_args.kwargs["system"]
    assert "MARKER_MX" in system
