"""Strategist agent — Claude tool-use chat with session persistence."""
from __future__ import annotations
import os
import streamlit as st
from shared.anthropic_client import make_client as _make_anthropic_client, is_llm_available
from libs.deal_models.adapters.agent_tools import AGENT_TOOLS, dispatch_tool

_SYSTEM = """You are a quantitative deal-structuring advisor for renewable energy assets in China's spot markets.

You have access to the following tools:
- run_price_simulation: simulate forward price paths (OU or PCA)
- run_dispatch_valuation: estimate BESS/wind annual revenue from price paths
- run_project_cashflow: compute project IRR, DSCR, NPV
- run_monte_carlo: full probabilistic analysis (price→dispatch→cashflow)
- price_deal_structure: price a floor/cap/collar/swap/tolling/PPA against MC revenue paths

Guidelines:
- Always run run_monte_carlo before price_deal_structure
- Cite P10/P50/P90 statistics when answering revenue questions
- Express premiums and revenues in ¥M/year for readability
- When asked "what floor guarantees X% IRR at P90?", iterate: try floor=P10 revenue, compute cashflow at P10, adjust
"""

_TOOL_ICONS = {
    "run_price_simulation": "📈",
    "run_dispatch_valuation": "⚡",
    "run_project_cashflow": "💰",
    "run_monte_carlo": "🎲",
    "price_deal_structure": "🤝",
}


def _run_agent_turn(messages: list, text_ph) -> tuple[str, list]:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not is_llm_available(api_key):
        return "No LLM configured (set ANTHROPIC_API_KEY or BEDROCK_REGION).", messages

    client = _make_anthropic_client(api_key)
    status_ph = st.empty()

    while True:
        streamed = ""
        status_ph.caption("⏳ Thinking…")
        with client.messages.stream(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=_SYSTEM,
            tools=AGENT_TOOLS,
            messages=messages,
        ) as stream:
            for chunk in stream.text_stream:
                streamed += chunk
                status_ph.empty()
                text_ph.markdown(streamed + "▌")
            final = stream.get_final_message()

        messages = messages + [{"role": "assistant", "content": final.content}]

        if final.stop_reason == "end_turn":
            status_ph.empty()
            text_ph.markdown(streamed)
            return streamed, messages

        if final.stop_reason != "tool_use":
            status_ph.empty()
            return f"Unexpected stop: {final.stop_reason}", messages

        tool_results = []
        for block in final.content:
            if block.type == "tool_use":
                icon = _TOOL_ICONS.get(block.name, "⚙️")
                status_ph.caption(f"{icon} Calling `{block.name}`…")
                result_str = dispatch_tool(block.name, block.input)
                tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": result_str})

        status_ph.empty()
        messages = messages + [{"role": "user", "content": tool_results}]


def render() -> None:
    st.header("💬 Strategist")
    st.caption("Ask quantitative questions about deal structure, pricing, and project returns.")

    if "agent_messages" not in st.session_state:
        st.session_state["agent_messages"] = []
    if "agent_display" not in st.session_state:
        st.session_state["agent_display"] = [{"role": "assistant", "content": (
            "Hello! I can help you structure and price renewable energy deals. Try asking:\n"
            "- *What floor revenue guarantees 8% equity IRR at P90?*\n"
            "- *Price a revenue floor at ¥15M/year for this BESS project*\n"
            "- *How sensitive is IRR to capex vs average price?*"
        )}]

    if st.button("🗑 Clear Chat", key="strat_clear"):
        st.session_state["agent_messages"] = []
        st.session_state["agent_display"] = []
        st.rerun()

    for msg in st.session_state["agent_display"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    user_input = st.chat_input("Ask the Strategist…")
    if user_input:
        st.session_state["agent_display"].append({"role": "user", "content": user_input})
        st.session_state["agent_messages"].append({"role": "user", "content": user_input})
        with st.chat_message("user"):
            st.markdown(user_input)

        with st.chat_message("assistant"):
            text_ph = st.empty()
            reply, new_msgs = _run_agent_turn(st.session_state["agent_messages"], text_ph)
            st.session_state["agent_messages"] = new_msgs
            st.session_state["agent_display"].append({"role": "assistant", "content": reply})
