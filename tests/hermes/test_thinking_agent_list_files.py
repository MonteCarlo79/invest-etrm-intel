"""list_app_files must not let the LLM conclude an app has no code when the
app simply isn't shipped in the hermes image (2026-11-09 design review)."""
from services.hermes.thinking_agent import ThinkingAgent


def test_missing_app_reported_as_unavailable_not_absent():
    out = ThinkingAgent._tool_list_app_files(object(), "definitely-not-an-app-xyz")
    assert out.startswith("UNAVAILABLE") and "not shipped" in out


def test_health_defaults_to_haiku_design_to_sonnet(monkeypatch):
    monkeypatch.delenv("HERMES_THINK_HEALTH_MODEL", raising=False)
    monkeypatch.delenv("HERMES_THINK_DESIGN_MODEL", raising=False)
    assert ThinkingAgent._resolve_model(object(), "health") == "claude-haiku-4-5-20251001"
    assert ThinkingAgent._resolve_model(object(), "design") == "claude-sonnet-4-6"
    monkeypatch.setenv("HERMES_THINK_HEALTH_MODEL", "sonnet")
    assert ThinkingAgent._resolve_model(object(), "health") == "claude-sonnet-4-6"
