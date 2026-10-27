# tests/option_engine/test_scout_thesis.py
from unittest.mock import MagicMock
from services.option_engine.scout import thesis

def test_delta_gate_suppresses_unchanged(monkeypatch):
    prior = {"id": 9, "evidence_json": {"z": 2.4, "basis_30d": 52.0}}
    monkeypatch.setattr(thesis.store, "recent_same_signature", lambda eng, sig, within_days=40: prior)
    needs, card = thesis.needs_draft(None, {"signature": "s", "evidence": {"z": 2.45, "basis_30d": 52.5}})
    assert needs is False and card["id"] == 9

def test_delta_gate_passes_on_drift():
    class E:  # engine stub: recent_same_signature returns None via real SQL path being absent
        def connect(self): raise RuntimeError("no db")
    needs, card = thesis.needs_draft(E(), {"signature": "s2", "evidence": {"z": 3.9}}, )
    assert needs is True and card is None                  # lookup failure → draft (never silently suppress)

def test_draft_thesis_grounded_prompt_and_parse():
    client = MagicMock()
    client.messages.create.return_value.content = [MagicMock(text="机会：山东 DA-RT 基差走阔…")]
    out = thesis.draft_thesis(client, {"kind": "trading", "province": "山东", "evidence": {"z": 2.4}})
    assert "山东" in out
    kwargs = client.messages.create.call_args.kwargs
    assert kwargs["model"] == "claude-sonnet-4-6" and kwargs["max_tokens"] == 600
    assert "2.4" in kwargs["messages"][0]["content"]       # evidence pack in the prompt
