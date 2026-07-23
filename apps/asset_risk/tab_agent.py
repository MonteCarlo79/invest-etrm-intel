"""Tab 6 — Agent: Claude-powered risk assistant."""
from __future__ import annotations

import os
import json
import pandas as pd
import streamlit as st
from sqlalchemy import text


def render_agent(engine):
    """Render agent chat tab."""
    st.subheader("Risk Agent")

    from shared.anthropic_client import is_llm_available
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not is_llm_available(api_key):
        st.warning("No LLM configured (set ANTHROPIC_API_KEY or BEDROCK_REGION). Agent unavailable.")
        return

    if "agent_messages" not in st.session_state:
        st.session_state.agent_messages = []

    for msg in st.session_state.agent_messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if prompt := st.chat_input("Ask about your asset risk positions..."):
        st.session_state.agent_messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                response = _call_agent(prompt, engine, api_key)
                st.markdown(response)
        st.session_state.agent_messages.append({"role": "assistant", "content": response})


def _call_agent(user_message: str, engine, api_key: str) -> str:
    """Call Claude with risk management tools."""
    from shared.anthropic_client import make_client as _make_anthropic_client

    client = _make_anthropic_client(api_key)

    tools = [
        {
            "name": "get_book_pnl",
            "description": "Get P&L breakdown by category for a book.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "book_id": {"type": "integer"},
                },
                "required": ["book_id"],
            },
        },
        {
            "name": "get_position_mtm",
            "description": "Get current MtM summary with unrealised P&L for a book.",
            "input_schema": {
                "type": "object",
                "properties": {"book_id": {"type": "integer"}},
                "required": ["book_id"],
            },
        },
        {
            "name": "get_var",
            "description": "Get current VaR figures for a book.",
            "input_schema": {
                "type": "object",
                "properties": {"book_id": {"type": "integer"}},
                "required": ["book_id"],
            },
        },
        {
            "name": "get_asset_list",
            "description": "Get list of registered assets and their books.",
            "input_schema": {"type": "object", "properties": {}},
        },
    ]

    system_prompt = (
        "You are an asset risk management assistant for a Chinese electricity trading company. "
        "You have access to tools that query the risk management database. "
        "Answer questions about P&L, positions, VaR, and assets. "
        "Use CNY for all monetary values. Respond concisely."
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


def _execute_tool(name: str, inputs: dict, engine) -> dict:
    with engine.connect() as conn:
        if name == "get_asset_list":
            df = pd.read_sql(text("""
                SELECT a.id, a.name, a.asset_type, a.province, a.capacity_mw,
                       b.id as book_id, b.name as book_name
                FROM marketdata.rm_assets a
                LEFT JOIN marketdata.rm_books b ON b.asset_id = a.id ORDER BY a.name
            """), conn)
            return {"assets": df.to_dict("records")}

        elif name == "get_book_pnl":
            df = pd.read_sql(text("""
                SELECT si.category, SUM(si.amount_cny) as total_cny, SUM(si.volume_mwh) as total_mwh
                FROM marketdata.rm_settlement_items si
                JOIN marketdata.rm_settlements s ON s.id = si.settlement_id
                WHERE s.book_id = :bid GROUP BY si.category ORDER BY total_cny DESC
            """), conn, params={"bid": inputs["book_id"]})
            return {"pnl_by_category": df.to_dict("records"), "net_pnl": float(df["total_cny"].sum())}

        elif name == "get_position_mtm":
            df = pd.read_sql(text("""
                SELECT direction, volume_mwh, price_cny_mwh, channel, province
                FROM marketdata.rm_positions WHERE book_id = :bid AND status = 'open'
            """), conn, params={"bid": inputs["book_id"]})
            if df.empty:
                return {"positions": [], "total_unrealized": 0}
            from libs.risk.mtm import compute_mtm
            fwd = pd.read_sql(text("""
                SELECT DISTINCT ON (province) province, price_cny_kwh * 1000 as price
                FROM marketdata.rm_forward_curves ORDER BY province, curve_date DESC
            """), conn)
            fwd_prices = dict(zip(fwd["province"], fwd["price"])) if not fwd.empty else {}
            results = compute_mtm(df.to_dict("records"), fwd_prices)
            return {"positions": results, "total_unrealized": sum(r["unrealized_pnl_cny"] for r in results)}

        elif name == "get_var":
            df = pd.read_sql(text("""
                SELECT * FROM marketdata.rm_var_snapshots
                WHERE book_id = :bid ORDER BY snapshot_date DESC LIMIT 1
            """), conn, params={"bid": inputs["book_id"]})
            return df.to_dict("records")[0] if not df.empty else {"message": "No VaR data"}

    return {"error": f"Unknown tool: {name}"}
