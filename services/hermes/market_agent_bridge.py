"""Market agent bridge — routes Hermes MARKET_AGENT requests to the correct headless agent.

Market keys:
    gb, au, ercot, caiso, pjm, ph, po, bess-map, spot
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

# Mapping from market key → module + function to lazy-import
_INTL_MARKETS = {"au", "ercot", "caiso", "pjm", "ph", "po"}

_MARKET_CONFIGS = {
    "au":    "services.au_knowledge.config",
    "ercot": "services.ercot_knowledge.config",
    "caiso": "services.caiso_knowledge.config",
    "pjm":   "services.pjm_knowledge.config",
    "ph":    "services.ph_knowledge.config",
    "po":    "services.po_knowledge.config",
}


def run_market_query(market: str, question: str, api_key: str, pg_url: str = "") -> str:
    """Route a question to the right market headless agent and return the answer."""
    pg_url = pg_url or os.environ.get("PGURL") or os.environ.get("DATABASE_URL", "")

    market = market.lower().strip()

    if market == "gb":
        from services.gb_knowledge.headless_agent import run_gb_query
        return run_gb_query(question=question, api_key=api_key, pg_url=pg_url)

    if market in _INTL_MARKETS:
        import importlib
        cfg_module = _MARKET_CONFIGS[market]
        try:
            mod = importlib.import_module(cfg_module)
            cfg = mod.MARKET_CONFIG
        except Exception as e:
            return f"Could not load config for market '{market}': {e}"
        from services.intl_market_common.headless_agent import run_market_query as _run_intl
        return _run_intl(cfg=cfg, question=question, api_key=api_key, pg_url=pg_url)

    if market == "bess-map":
        from services.bess_map.headless_agent import run_bess_map_query
        return run_bess_map_query(question=question, api_key=api_key, pg_url=pg_url)

    if market == "spot":
        return _run_spot_query(question=question, api_key=api_key)

    if market == "internet":
        from services.hermes.internet_agent import run_internet_query
        return run_internet_query(question=question, api_key=api_key)

    return f"Unknown market '{market}'. Available: gb, au, ercot, caiso, pjm, ph, po, bess-map, spot, internet"


def _run_spot_query(question: str, api_key: str) -> str:
    """Simple spot market query using MCP tools directly."""
    import anthropic
    import json
    from datetime import date, timedelta

    client = anthropic.Anthropic(api_key=api_key)

    # Import spot tools
    try:
        from services.spot_mcp.tools import get_spot_prices, get_market_summaries, search_reference_docs
    except ImportError as e:
        return f"Spot market tools unavailable: {e}"

    tools = [
        {"name": "get_spot_prices",
         "description": "China spot electricity market prices by province and date range.",
         "input_schema": {"type": "object",
                          "properties": {"start_date": {"type": "string"}, "end_date": {"type": "string"},
                                         "province": {"type": "string"}},
                          "required": ["start_date", "end_date"]}},
        {"name": "get_market_summaries",
         "description": "Daily spot market summary statistics.",
         "input_schema": {"type": "object",
                          "properties": {"start_date": {"type": "string"}, "end_date": {"type": "string"}},
                          "required": ["start_date", "end_date"]}},
        {"name": "search_reference_docs",
         "description": "Search spot market regulatory documents and reference materials.",
         "input_schema": {"type": "object",
                          "properties": {"query": {"type": "string"}, "limit": {"type": "integer"}},
                          "required": ["query"]}},
    ]

    def dispatch(name: str, inputs: dict) -> str:
        try:
            if name == "get_spot_prices":
                result = get_spot_prices(**inputs)
            elif name == "get_market_summaries":
                result = get_market_summaries(**inputs)
            elif name == "search_reference_docs":
                result = search_reference_docs(**inputs)
            else:
                return "Unknown tool"
            return json.dumps(result, ensure_ascii=False, default=str)
        except Exception as e:
            return f"Error: {e}"

    system = (
        "You are the China Spot Electricity Market analyst. "
        "Answer questions using data from your tools only. "
        "Quote numbers with full units (¥/MWh). "
        "State the date range used. "
        "If no data is available, say so clearly."
    )

    messages = [{"role": "user", "content": question}]
    while True:
        resp = client.messages.create(
            model="claude-sonnet-4-6", max_tokens=2048,
            system=system, tools=tools, messages=messages,
        )
        messages = messages + [{"role": "assistant", "content": resp.content}]
        if resp.stop_reason == "end_turn":
            return next((b.text for b in resp.content if hasattr(b, "text")), "")
        tool_results = []
        for block in resp.content:
            if block.type == "tool_use":
                result_str = dispatch(block.name, block.input)
                tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": result_str})
        if not tool_results:
            return next((b.text for b in resp.content if hasattr(b, "text")), "")
        messages = messages + [{"role": "user", "content": tool_results}]
