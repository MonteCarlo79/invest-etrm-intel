"""Tab 6 — Agent: Claude-powered risk assistant (platform pattern, v21+).

Platform requirements (CLAUDE.md): domain grounding rule, agent_memory
read/write scoped to app key `asset_risk`, Haiku auto-extract with auto-save,
multi-turn conversation, tool-use loop.

`tools` and `_execute_tool` are imported by services/asset_risk/headless_agent.py
— keep both names and signatures stable.
"""
from __future__ import annotations

import json
import os

import pandas as pd
import streamlit as st
from sqlalchemy import text

_ASSET_RISK_APP_NAME = "asset_risk"
_ASSET_RISK_MEM_KEY = "asset_risk_v1"
_HISTORY_TURNS = 20
_MAX_TOOL_ROUNDS = 6

from libs.asset_risk.agent_tools import (  # noqa: F401
    AGENT_BASE_SYSTEM as _AGENT_BASE_SYSTEM,
    tools,
    execute_tool as _execute_tool,
)



# ── Agent memory (platform pattern: agent_memory table, app-scoped) ───────────

@st.cache_resource
def _ensure_memory_table(_engine):
    with _engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS marketdata.agent_memory (
                id SERIAL PRIMARY KEY,
                app TEXT NOT NULL DEFAULT 'asset_risk',
                category TEXT NOT NULL,
                subject TEXT NOT NULL,
                content TEXT NOT NULL,
                source TEXT DEFAULT 'manual',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                active BOOLEAN DEFAULT TRUE
            )
        """))
        conn.execute(text("""
            ALTER TABLE marketdata.agent_memory
            ADD COLUMN IF NOT EXISTS app TEXT NOT NULL DEFAULT 'asset_risk'
        """))
    return True


@st.cache_data(ttl=60)
def _load_memories(_engine, _key) -> pd.DataFrame:
    try:
        with _engine.connect() as conn:
            return pd.read_sql(text(
                "SELECT id, category, subject, content, source "
                "FROM marketdata.agent_memory WHERE active AND app = :app ORDER BY id"
            ), conn, params={"app": _ASSET_RISK_APP_NAME})
    except Exception:
        return pd.DataFrame(columns=["id", "category", "subject", "content", "source"])


def _save_memory(engine, category: str, subject: str, content: str, source: str = "manual"):
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO marketdata.agent_memory (app, category, subject, content, source) "
            "VALUES (:app, :cat, :sub, :con, :src)"
        ), {"app": _ASSET_RISK_APP_NAME, "cat": category, "sub": subject,
            "con": content, "src": source})
    _load_memories.clear()


def _delete_memory(engine, memory_id: int):
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE marketdata.agent_memory SET active = FALSE WHERE id = :mid AND app = :app"
        ), {"mid": memory_id, "app": _ASSET_RISK_APP_NAME})
    _load_memories.clear()


def _build_system(engine) -> str:
    base = _AGENT_BASE_SYSTEM
    mems = _load_memories(engine, _ASSET_RISK_MEM_KEY)
    if not mems.empty:
        mem_lines = "\n".join(
            f"- [{r.category}] {r.subject}: {r.content}" for r in mems.itertuples()
        )
        base += f"\n\n## Analyst preferences & domain knowledge\n{mem_lines}"
    return base


def _extract_memories(user_msg: str, agent_reply: str, api_key: str) -> list[dict]:
    """Haiku extraction of memorable facts/preferences from a turn (v21+ pattern)."""
    from shared.anthropic_client import make_client as _make_client
    try:
        _sys = (
            "Extract memorable analyst preferences or domain facts from the conversation. "
            "Return a JSON array of objects with keys: category (string, e.g. 'preference', "
            "'market_view', 'methodology', 'asset_note', 'red_flag'), "
            "subject (short title ≤8 words), content (one sentence). "
            "Only extract genuinely reusable insights — not one-off data points. "
            "Return [] if nothing is worth remembering."
        )
        resp = _make_client(api_key).messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            system=_sys,
            messages=[{"role": "user", "content": f"User: {user_msg}\n\nAgent: {agent_reply}"}],
        )
        raw = next((b.text for b in resp.content if hasattr(b, "text")), "[]")
        start, end = raw.find("["), raw.rfind("]")
        if start == -1:
            return []
        return json.loads(raw[start:end + 1])
    except Exception:
        return []


# ── Chat rendering ─────────────────────────────────────────────────────────────

def render_agent(engine):
    """Render agent chat tab."""
    st.subheader("Risk Agent")

    from shared.anthropic_client import is_llm_available
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not is_llm_available(api_key):
        st.warning("No LLM configured (set ANTHROPIC_API_KEY or BEDROCK_REGION). Agent unavailable.")
        return

    try:
        _ensure_memory_table(engine)
    except Exception:
        pass  # non-fatal: agent works without memory

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
                history = st.session_state.agent_messages[:-1]
                response = _call_agent(prompt, engine, api_key, history)
                st.markdown(response)
        st.session_state.agent_messages.append({"role": "assistant", "content": response})

        # Auto-extract + auto-save memories (v21+ pattern, no confirmation panel)
        saved = 0
        for item in _extract_memories(prompt, response, api_key):
            if all(k in item for k in ("category", "subject", "content")):
                try:
                    _save_memory(engine, item["category"], item["subject"],
                                 item["content"], source="auto")
                    saved += 1
                except Exception:
                    pass
        if saved:
            st.toast(f"🧠 Saved {saved} new memor{'y' if saved == 1 else 'ies'} to agent memory")

    with st.expander("🧠 Agent memory (manage)"):
        mems = _load_memories(engine, _ASSET_RISK_MEM_KEY)
        if mems.empty:
            st.caption("No memories saved yet.")
        for r in mems.itertuples():
            c1, c2 = st.columns([6, 1])
            c1.markdown(f"**[{r.category}] {r.subject}** — {r.content}")
            if c2.button("Delete", key=f"del_mem_{r.id}"):
                _delete_memory(engine, r.id)
                st.rerun()


def _call_agent(user_message: str, engine, api_key: str, history: list[dict]) -> str:
    """Multi-turn agent call with a tool-use loop."""
    from shared.anthropic_client import make_client as _make_client

    client = _make_client(api_key)
    system = _build_system(engine)

    messages = [{"role": m["role"], "content": m["content"]}
                for m in history[-_HISTORY_TURNS:]]
    messages.append({"role": "user", "content": user_message})

    for _ in range(_MAX_TOOL_ROUNDS):
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=system,
            tools=tools,
            messages=messages,
        )
        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            return _extract_text(response)

        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                result = _execute_tool(block.name, block.input, engine)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result, default=str),
                })
        if not tool_results:
            return _extract_text(response)
        messages.append({"role": "user", "content": tool_results})

    return "Stopped after reaching the tool-use round limit — please rephrase or narrow the question."


def _extract_text(response) -> str:
    for block in response.content:
        if hasattr(block, "text"):
            return block.text
    return "No response generated."


