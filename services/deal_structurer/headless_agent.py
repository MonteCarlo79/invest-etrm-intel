"""Headless Deal Structurer agent.

Routes deal structuring questions to the deal model tools:
run_price_simulation, run_dispatch_valuation, run_project_cashflow,
run_monte_carlo, price_deal_structure.

Usage:
    from services.deal_structurer.headless_agent import run_deal_query
    answer = run_deal_query("蒙西100MWh BESS的IRR?", api_key, pg_url)
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_SYSTEM = """\
You are a deal structuring analyst for power assets. \
You build financial models to evaluate BESS and wind projects: \
PPA structures, project IRR, equity IRR, DSCR, NPV, capacity/energy revenue splits, \
dispatch valuations, and deal structure pricing.

## Workflow
1. Use run_price_simulation to simulate price paths for the target province.
2. Use run_dispatch_valuation to estimate annual revenue from the price paths.
3. Use run_project_cashflow to compute IRR/NPV for given capex/opex assumptions.
4. Use run_monte_carlo for a full probabilistic analysis in one step.
5. Use price_deal_structure to price a revenue floor/cap/collar/swap/tolling/PPA.

## Rules
- Always call a tool before stating any financial figure (IRR, NPV, revenue).
- Quote all monetary values in CNY (元); MWh for energy.
- State all model assumptions explicitly (kappa, mu, sigma, capex, debt ratio, etc.).
- Respond concisely with actionable insights.
- Respond in the same language as the question (Chinese or English).
"""


def _make_client(api_key: str):
    from shared.anthropic_client import make_client
    return make_client(api_key)


def run_deal_query(question: str, api_key: str, pg_url: str = "") -> str:
    """Run the deal structurer headless agent and return its answer.

    pg_url is accepted for interface consistency with other headless agents
    but not used — deal model tools run local Python models, not DB queries.
    """
    from libs.deal_models.adapters.agent_tools import AGENT_TOOLS, dispatch_tool

    client = _make_client(api_key)

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
            tools=AGENT_TOOLS,
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
                result_str = dispatch_tool(block.name, block.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result_str,
                })
        if not tool_results:
            return next((b.text for b in resp.content if hasattr(b, "text")), "")
        messages = messages + [{"role": "user", "content": tool_results}]
