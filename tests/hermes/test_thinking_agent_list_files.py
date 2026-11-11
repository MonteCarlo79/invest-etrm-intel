"""list_app_files must not let the LLM conclude an app has no code when the
app simply isn't shipped in the hermes image (2026-11-09 design review)."""
from services.hermes.thinking_agent import ThinkingAgent


def test_missing_app_reported_as_unavailable_not_absent():
    out = ThinkingAgent._tool_list_app_files(object(), "definitely-not-an-app-xyz")
    assert out.startswith("UNAVAILABLE") and "not shipped" in out
