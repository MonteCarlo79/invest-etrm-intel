"""Round-cap tests for the headless agent loops (no DB, no API calls).

Each agent loop must withdraw tools after _MAX_TOOL_ROUNDS tool batches so an
uncooperative model can't loop forever (2026-09-06: mengxi agent spin-looped
get_dispatch_data 15+ times inside a committee section).
"""
from unittest.mock import patch

import services.asset_risk.headless_agent as asset_risk
import services.hermes.market_agent_bridge as bridge
import services.mengxi_trading.headless_agent as mengxi


class _Block:
    def __init__(self, type_, **kw):
        self.type = type_
        for k, v in kw.items():
            setattr(self, k, v)


class _Resp:
    def __init__(self, stop_reason, content):
        self.stop_reason = stop_reason
        self.content = content


class LoopingClient:
    """Always requests a tool while tools are offered; answers when they are not."""

    def __init__(self, tool_name):
        self.calls = []
        self._tool_name = tool_name

    @property
    def messages(self):
        return self

    def create(self, **kw):
        self.calls.append(kw)
        if kw.get("tools"):
            return _Resp("tool_use", [
                _Block("tool_use", name=self._tool_name,
                       id=f"t{len(self.calls)}", input={}),
            ])
        return _Resp("end_turn", [_Block("text", text="best-effort answer")])


def _assert_capped(client, out, max_rounds):
    assert out == "best-effort answer"
    tool_calls = [c for c in client.calls if c.get("tools")]
    bare_calls = [c for c in client.calls if not c.get("tools")]
    assert len(tool_calls) == max_rounds
    assert len(bare_calls) == 1


def _patched_memory():
    return [
        patch("services.knowledge_pool.expert_memory.get_relevant_insights",
              return_value=[]),
        patch("services.knowledge_pool.expert_memory.inject_expert_memory",
              return_value=""),
        patch("services.knowledge_pool.expert_memory.extract_spot_insights",
              return_value=None),
    ]


def test_spot_agent_caps_tool_rounds():
    client = LoopingClient("get_market_fundamentals")
    patches = _patched_memory() + [
        patch("shared.anthropic_client.make_client", return_value=client),
        patch("services.spot_mcp.tools.get_spot_prices", return_value={}),
        patch("services.spot_mcp.tools.get_interprov_flow", return_value={}),
        patch("services.spot_mcp.tools.get_market_summaries", return_value={}),
        patch("services.spot_mcp.tools.get_market_fundamentals", return_value={"ok": True}),
    ]
    for p in patches:
        p.start()
    try:
        out = bridge._run_spot_query("测试问题", api_key="k")
    finally:
        for p in patches:
            p.stop()
    _assert_capped(client, out, bridge._MAX_TOOL_ROUNDS)


def test_mengxi_agent_caps_tool_rounds():
    client = LoopingClient("get_dispatch_data")
    patches = _patched_memory() + [
        patch("shared.anthropic_client.make_client", return_value=client),
        patch.object(mengxi, "_dispatch", return_value='{"ok": true}'),
    ]
    # vault_reader is optional in the agent; neutralise if importable
    try:
        import services.knowledge_pool.vault_reader  # noqa: F401
        patches.append(patch(
            "services.knowledge_pool.vault_reader.retrieve_vault_context",
            return_value=""))
    except ImportError:
        pass
    for p in patches:
        p.start()
    try:
        out = mengxi.run_mengxi_query("测试问题", api_key="k", pg_url="sqlite://")
    finally:
        for p in patches:
            p.stop()
    _assert_capped(client, out, mengxi._MAX_TOOL_ROUNDS)


def test_asset_risk_agent_caps_tool_rounds():
    client = LoopingClient("get_book_pnl")
    patches = _patched_memory() + [
        patch("shared.anthropic_client.make_client", return_value=client),
        patch("apps.asset_risk.tab_agent._execute_tool", return_value={"ok": True}),
    ]
    for p in patches:
        p.start()
    try:
        out = asset_risk.run_asset_risk_query("测试问题", api_key="k", pg_url="sqlite://")
    finally:
        for p in patches:
            p.stop()
    _assert_capped(client, out, asset_risk._MAX_TOOL_ROUNDS)
