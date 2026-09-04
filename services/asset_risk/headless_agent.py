"""Headless Asset Risk agent.

Routes asset risk questions to DB query tools:
get_book_pnl, get_position_mtm, get_var, get_asset_list.

Usage:
    from services.asset_risk.headless_agent import run_asset_risk_query
    answer = run_asset_risk_query("Book 1 P&L?", api_key, pg_url)
"""
from __future__ import annotations

import json
import logging
import os

logger = logging.getLogger(__name__)

_SYSTEM = """\
You are an asset risk management analyst for a Chinese electricity trading company. \
You track book P&L, position mark-to-market, value at risk (VaR), \
and portfolio exposure across the asset book.

## Rules
1. Always call get_asset_list first if you don't know the book_id.
2. Always call a tool before stating any P&L, VaR, or MTM figure.
3. Quote all monetary values in CNY (元); MWh for energy volumes.
4. Respond concisely with actionable risk insights.
5. Respond in the same language as the question (Chinese or English).
"""


def _make_client(api_key: str):
    from shared.anthropic_client import make_client
    return make_client(api_key)


def _make_engine(pg_url: str):
    url = pg_url or os.environ.get("PGURL") or os.environ.get("DATABASE_URL", "")
    if not url:
        raise ValueError("No database URL configured (set pg_url, PGURL, or DATABASE_URL)")
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if url.startswith("postgresql://") and not url.startswith("postgresql+psycopg2://"):
        url = "postgresql+psycopg2://" + url[len("postgresql://"):]
    from sqlalchemy import create_engine
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 10})


def run_asset_risk_query(question: str, api_key: str, pg_url: str = "") -> str:
    """Run the asset risk headless agent and return its answer."""
    from apps.asset_risk.tab_agent import tools as _tools, _execute_tool

    client = _make_client(api_key)
    engine = _make_engine(pg_url)

    system = _SYSTEM
    try:
        from services.knowledge_pool.expert_memory import get_relevant_insights, inject_expert_memory
        insights = get_relevant_insights(question, limit=4)
        mem_block = inject_expert_memory(insights)
        if mem_block:
            system += f"\n\n{mem_block}"
    except Exception:
        pass

    messages = [{"role": "user", "content": question}]
    while True:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=system,
            tools=_tools,
            messages=messages,
        )
        messages = messages + [{"role": "assistant", "content": resp.content}]
        if resp.stop_reason == "end_turn":
            answer = next((b.text for b in resp.content if hasattr(b, "text")), "")
            try:
                from services.knowledge_pool.expert_memory import extract_spot_insights
                extract_spot_insights(user_msg=question, agent_reply=answer, api_key=api_key)
            except Exception:
                pass
            return answer
        tool_results = []
        for block in resp.content:
            if block.type == "tool_use":
                result = _execute_tool(block.name, block.input, engine)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result, default=str),
                })
        if not tool_results:
            return next((b.text for b in resp.content if hasattr(b, "text")), "")
        messages = messages + [{"role": "user", "content": tool_results}]
