from unittest.mock import MagicMock, patch

import services.knowledge_pool.expert_memory as em


def test_stored_insights_mirrored_to_vault(monkeypatch):
    # fake LLM extraction returning one insight
    fake_resp = MagicMock()
    fake_resp.content = [MagicMock(text='{"insights": [{"insight": "山东午后光伏压价", "type": "market_view", "province": "山东", "confidence": "high"}]}')]
    fake_client = MagicMock()
    fake_client.messages.create.return_value = fake_resp
    monkeypatch.setattr(em, "_make_anthropic_client", lambda api_key: fake_client)

    # fake DB connection
    cur = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.__enter__.return_value = conn
    monkeypatch.setattr(em, "get_conn", lambda: conn)

    calls = []
    import services.knowledge_pool.vault_writer as vw
    monkeypatch.setattr(vw, "write_insight_note", lambda **kw: calls.append(kw) or "ok")

    stored = em.extract_spot_insights(user_msg="q", agent_reply="a", api_key="k")
    assert stored == 1
    assert calls == [{
        "category": "market_view",
        "content": "山东午后光伏压价",
        "source_app": "spot_market",
        "province": "山东",
        "confidence": "high",
    }]


def test_source_app_param_flows_to_note(monkeypatch):
    fake_resp = MagicMock()
    fake_resp.content = [MagicMock(text='{"insights": [{"insight": "蒙西调度偏差大", "type": "ops_note", "confidence": "medium"}]}')]
    fake_client = MagicMock()
    fake_client.messages.create.return_value = fake_resp
    monkeypatch.setattr(em, "_make_anthropic_client", lambda api_key: fake_client)
    cur = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.__enter__.return_value = conn
    monkeypatch.setattr(em, "get_conn", lambda: conn)
    calls = []
    import services.knowledge_pool.vault_writer as vw
    monkeypatch.setattr(vw, "write_insight_note", lambda **kw: calls.append(kw) or "ok")

    em.extract_spot_insights(user_msg="q", agent_reply="a", api_key="k", source_app="mengxi_trader")
    assert calls[0]["source_app"] == "mengxi_trader"
