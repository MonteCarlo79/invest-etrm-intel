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

    if market in ("mengxi", "im", "inner-mongolia"):
        from services.mengxi_trading.headless_agent import run_mengxi_query
        return run_mengxi_query(question=question, api_key=api_key, pg_url=pg_url)

    if market == "internet":
        from services.hermes.internet_agent import run_internet_query
        return run_internet_query(question=question, api_key=api_key)

    return f"Unknown market '{market}'. Available: gb, au, ercot, caiso, pjm, ph, po, bess-map, spot, mengxi, internet"


def _run_spot_query(question: str, api_key: str) -> str:
    """Full Strategist-parity spot market agent with 7 data tools."""
    import anthropic
    from shared.anthropic_client import make_client as _make_anthropic_client
    import json

    client = _make_anthropic_client(api_key)

    try:
        from services.spot_mcp.tools import (
            get_spot_prices,
            get_interprov_flow,
            get_market_summaries,
            get_market_fundamentals,
        )
        from services.knowledge_pool.knowledge_docs import search_reference_docs as _srd
    except ImportError as e:
        return f"Spot market tools unavailable: {e}"

    # bess_mcp is only present in the bess-mcp container, not in hermes — make optional
    try:
        from services.bess_mcp.tools import bess_get_portfolio_pnl as _bess_pnl
        _bess_pnl_available = True
    except ImportError:
        _bess_pnl_available = False
        _bess_pnl = None

    tools = [
        {
            "name": "get_spot_prices",
            "description": (
                "Fetch day-ahead (DA) and real-time (RT) spot electricity clearing prices "
                "from public.spot_daily. Prices in ¥/kWh. Covers all Chinese provinces "
                "participating in spot markets."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string", "description": "ISO date e.g. '2026-01-01'"},
                    "end_date":   {"type": "string", "description": "ISO date e.g. '2026-06-30'"},
                    "provinces":  {"type": "array", "items": {"type": "string"},
                                   "description": "Optional list of province_en names"},
                },
                "required": ["start_date", "end_date"],
            },
        },
        {
            "name": "get_interprov_flow",
            "description": (
                "Fetch inter-provincial spot trading data (省间现货交易情况). "
                "Returns daily peak/floor average prices and volumes for exporting "
                "(送端) and importing (受端) provinces."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string"},
                    "end_date":   {"type": "string"},
                },
                "required": ["start_date", "end_date"],
            },
        },
        {
            "name": "get_market_summaries",
            "description": (
                "Fetch AI-generated daily market narrative summaries covering price levels, "
                "key drivers, inter-provincial flows, and notable events."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string"},
                    "end_date":   {"type": "string"},
                },
                "required": ["start_date", "end_date"],
            },
        },
        {
            "name": "get_market_fundamentals",
            "description": (
                "Fetch market fundamentals for Chinese electricity provinces: "
                "installed capacity by fuel type (万kW), generation mix (亿kWh), "
                "and seasonal peak loads (MW). Data covers 2024 and 2025."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "provinces": {"type": "array", "items": {"type": "string"},
                                  "description": "Optional list of province_en names. Omit for all."},
                    "year":      {"type": "integer", "description": "2024 or 2025"},
                },
                "required": [],
            },
        },
        *([{
            "name": "get_bess_pnl",
            "description": (
                "Fetch daily P&L and dispatch metrics for Inner Mongolia BESS assets "
                "across all strategy scenarios. Assets: suyou, hangjinqi, siziwangqi, gushanliang. "
                "Scenarios: perfect_foresight_hourly, forecast_ols_rt_time_v1, nominated_dispatch, "
                "cleared_actual, trading_cleared."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "start_date":   {"type": "string"},
                    "end_date":     {"type": "string"},
                    "asset_codes":  {"type": "array", "items": {"type": "string"},
                                     "description": "Optional. Omit for all 4 IM assets."},
                },
                "required": ["start_date", "end_date"],
            },
        }] if _bess_pnl_available else []),
        {
            "name": "search_reference_docs",
            "description": (
                "Search the spot market knowledge base: market rules, annual reports, "
                "policy documents, regulatory filings, technical specs, research reports, "
                "Excel spreadsheets with trading volumes / network losses / contract data, "
                "and all market-fundamentals files (2.8M chunks). "
                "Supports Chinese and English queries."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "query":    {"type": "string", "description": "Search query in Chinese or English"},
                    "category": {"type": "string",
                                 "description": "Optional: market_rules | annual_report | policy_doc | "
                                                "technical_spec | research_report | other"},
                    "limit":    {"type": "integer", "description": "Max chunks (default 5, max 10)"},
                },
                "required": ["query"],
            },
        },
    ]

    def dispatch(name: str, inputs: dict) -> str:
        try:
            if name == "get_spot_prices":
                result = get_spot_prices(**inputs)
            elif name == "get_interprov_flow":
                result = get_interprov_flow(**inputs)
            elif name == "get_market_summaries":
                result = get_market_summaries(**inputs)
            elif name == "get_market_fundamentals":
                result = get_market_fundamentals(**inputs)
            elif name == "get_bess_pnl":
                if not _bess_pnl_available:
                    return json.dumps({"error": "BESS P&L tool not available in this environment"})
                result = _bess_pnl(
                    asset_codes=inputs.get("asset_codes"),
                    start_date=inputs["start_date"],
                    end_date=inputs["end_date"],
                )
            elif name == "search_reference_docs":
                rows = _srd(
                    query=inputs["query"],
                    category=inputs.get("category"),
                    app="strategist",
                    limit=min(int(inputs.get("limit", 5)), 10),
                )
                result = {"count": len(rows), "chunks": rows}
            else:
                return json.dumps({"error": f"Unknown tool: {name}"})
            return json.dumps(result, ensure_ascii=False, default=str)
        except Exception as e:
            logger.error("spot tool %s error: %s", name, e)
            return json.dumps({"error": str(e)})

    system = """\
You are a specialist analyst for China's spot electricity market, \
answering via the Hermes assistant in Feishu. \
Your knowledge comes exclusively from the data tools below. \
Never state any price, trend, or market event unless it was returned by a tool call.

## Domain definitions
- DA price: Day-Ahead clearing price (¥/kWh). RT price: Real-Time price (¥/kWh).
- Spread: DA − RT; positive = DA premium (normal); negative = RT spike.
- 送端: Exporting province. 受端: Importing province.
- Province names in DB: Shandong, Guangdong, Mengxi, Shanxi, Gansu, Sichuan, Yunnan, \
Guizhou, Guangxi, Hunan, Hubei, Anhui, Zhejiang, Jiangsu, Fujian, Henan, Shaanxi, \
Ningxia, Xinjiang, Liaoning, Jilin, Heilongjiang, Mengdong, Hebei, Hebei-North, \
Hebei-South, Qinghai, Jiangxi, Hainan, Chongqing, Shanghai, Beijing, Tianjin.

## Rules
1. Call a tool before stating any price, spread, volume, or trend.
2. Use markdown tables for multi-province or multi-period comparisons.
3. Cite the date range of the data used in every response.
4. For structural questions (fuel mix, capacity, renewables), call get_market_fundamentals.
5. For BESS asset performance in Inner Mongolia, call get_bess_pnl.
6. For market rules, policy docs, or knowledge base questions, call search_reference_docs.
7. Keep responses concise — this is a chat interface, not a full report.
8. Respond in the same language as the question (Chinese or English).\
"""

    # Inject accumulated expert insights (READ path — same pattern as GB Strategist)
    try:
        from services.knowledge_pool.expert_memory import get_relevant_insights, inject_expert_memory
        insights = get_relevant_insights(question, limit=5)
        mem_block = inject_expert_memory(insights)
        if mem_block:
            system += f"\n\n{mem_block}"
    except Exception:
        pass

    messages = [{"role": "user", "content": question}]
    while True:
        resp = client.messages.create(
            model="claude-sonnet-4-6", max_tokens=2048,  # tool-use; global.anthropic.claude-sonnet-4-6 is the only confirmed-working model
            system=system, tools=tools, messages=messages,
        )
        messages = messages + [{"role": "assistant", "content": resp.content}]
        if resp.stop_reason == "end_turn":
            answer = next((b.text for b in resp.content if hasattr(b, "text")), "")
            # Extract and store new insights from this exchange (WRITE path)
            try:
                from services.knowledge_pool.expert_memory import extract_spot_insights
                extract_spot_insights(user_msg=question, agent_reply=answer, api_key=api_key)
            except Exception:
                pass
            return answer
        tool_results = []
        for block in resp.content:
            if block.type == "tool_use":
                result_str = dispatch(block.name, block.input)
                tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": result_str})
        if not tool_results:
            return next((b.text for b in resp.content if hasattr(b, "text")), "")
        messages = messages + [{"role": "user", "content": tool_results}]
