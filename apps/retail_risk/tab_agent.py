"""Tab 6 — Agent: Claude-powered retail risk assistant."""
from __future__ import annotations

import json
import os

import pandas as pd
import streamlit as st
from sqlalchemy import text

from services.retail_risk.agent_tools import tools, execute_tool as _execute_tool


def render_agent(engine):
    """Render agent chat tab."""
    st.subheader("Retail Risk Agent")

    from shared.anthropic_client import make_client as _make_anthropic_client, is_llm_available

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not is_llm_available(api_key):
        st.warning("No LLM configured (set ANTHROPIC_API_KEY or BEDROCK_REGION). Agent unavailable.")
        return

    if "retail_agent_messages" not in st.session_state:
        st.session_state.retail_agent_messages = []

    for msg in st.session_state.retail_agent_messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if prompt := st.chat_input("Ask about retail margins, coverage, customers..."):
        st.session_state.retail_agent_messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                response = _call_agent(prompt, engine, api_key)
                st.markdown(response)
        st.session_state.retail_agent_messages.append({"role": "assistant", "content": response})


def _call_agent(user_message: str, engine, api_key: str) -> str:
    """Call Claude with retail risk management tools."""
    from shared.anthropic_client import make_client as _make_anthropic_client

    client = _make_anthropic_client(api_key)

    system_prompt = (
        "You are a retail electricity risk management assistant for a Chinese energy trading company. "
        "You have access to tools that query the retail risk management database. "
        "Answer questions about customer margins, procurement coverage, P&L rankings, and contract pipelines. "
        "Use CNY for monetary values and MWh for energy volumes. Respond concisely in English or Chinese as appropriate."
    )

    messages = [{"role": "user", "content": user_message}]
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=system_prompt,
        tools=tools,
        messages=messages,
    )

    if response.stop_reason == "tool_use":
        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                result = _execute_tool(block.name, block.input, engine)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result, default=str),
                })

        messages.append({"role": "assistant", "content": response.content})
        messages.append({"role": "user", "content": tool_results})

        final = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=system_prompt,
            tools=tools,
            messages=messages,
        )
        return _extract_text(final)

    return _extract_text(response)


def _extract_text(response) -> str:
    for block in response.content:
        if hasattr(block, "text"):
            return block.text
    return "No response generated."
