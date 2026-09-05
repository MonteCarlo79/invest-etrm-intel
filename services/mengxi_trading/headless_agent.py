"""Headless Mengxi BESS Trading Operations agent.

Covers Inner Mongolia (Mengxi) BESS daily P&L attribution, 15-min dispatch
data, RT clearing prices, and knowledge base search for market rules.

4 operating assets:
    suyou       → 景蓝乌尔图
    hangjinqi   → 悦杭独贵
    siziwangqi  → 景通四益堂储
    gushanliang → 裕昭沙子坝

Usage:
    from services.mengxi_trading.headless_agent import run_mengxi_query
    answer = run_mengxi_query("四个资产上周P&L？", api_key, pg_url)
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date

import pandas as pd
from sqlalchemy import create_engine, text as sql_text

logger = logging.getLogger(__name__)


def _make_engine(pg_url: str):
    url = pg_url or os.environ.get("PGURL") or os.environ.get("DATABASE_URL", "")
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if url.startswith("postgresql://") and not url.startswith("postgresql+psycopg2://"):
        url = "postgresql+psycopg2://" + url[len("postgresql://"):]
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 10})


_TOOLS = [
    {
        "name": "get_asset_pnl",
        "description": (
            "Get daily P&L attribution for one or all Mengxi BESS assets over a date range. "
            "Returns: trade_date, asset_code, pf_unrestricted_pnl, pf_grid_feasible_pnl, "
            "tt_forecast_optimal_pnl, tt_strategy_pnl, nominated_pnl, cleared_actual_pnl (CNY). "
            "Loss waterfall: PF Unrestricted → PF Grid-Feasible → Forecast Optimal → Strategy "
            "→ Nominated → Cleared Actual. Use to analyse revenue performance and loss breakdown."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "YYYY-MM-DD"},
                "end_date":   {"type": "string", "description": "YYYY-MM-DD"},
                "asset_code": {
                    "type": "string",
                    "description": "suyou / hangjinqi / siziwangqi / gushanliang. Omit for all assets.",
                },
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "get_dispatch_data",
        "description": (
            "Get 15-min dispatch data for a Mengxi BESS asset on a specific date. "
            "Returns: interval_start, nominated_dispatch_mw (申报曲线, MW), "
            "actual_dispatch_mw (实际充放曲线, MW), nodal_price_excel (CNY/MWh). "
            "Positive = discharge, negative = charge. "
            "Use to analyse execution gaps, nomination vs actual, and intraday dispatch patterns."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_code": {
                    "type": "string",
                    "description": "suyou / hangjinqi / siziwangqi / gushanliang",
                },
                "date": {"type": "string", "description": "YYYY-MM-DD"},
            },
            "required": ["asset_code", "date"],
        },
    },
    {
        "name": "get_rt_prices",
        "description": (
            "Get hourly average Mengxi province RT clearing prices (CNY/MWh) for a date range. "
            "Use to contextualise market conditions and explain P&L drivers."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "YYYY-MM-DD"},
                "end_date":   {"type": "string", "description": "YYYY-MM-DD"},
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "get_strategy_comparison",
        "description": (
            "Compare multiple dispatch strategy P&Ls for an asset over a date range. "
            "Returns daily totals for perfect_foresight_hourly, forecast_ols_rt_time_v1, "
            "nominated_dispatch, cleared_actual, trading_cleared strategies. "
            "Use to benchmark actual vs theoretical performance."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date":  {"type": "string", "description": "YYYY-MM-DD"},
                "end_date":    {"type": "string", "description": "YYYY-MM-DD"},
                "asset_codes": {
                    "type": "array", "items": {"type": "string"},
                    "description": "Optional list of asset codes. Omit for all 4.",
                },
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "search_knowledge_base",
        "description": (
            "Search the company knowledge base for trading policies, market rules, "
            "settlement procedures, ancillary service rules, and grid codes for Mengxi. "
            "Use when asked about regulations, bidding rules, or settlement logic."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query":    {"type": "string", "description": "Search terms in Chinese or English"},
                "category": {
                    "type": "string",
                    "description": "Optional: market_rules | policy_doc | technical_spec | other",
                },
            },
            "required": ["query"],
        },
    },
]

_SYSTEM = """\
You are the Mengxi BESS Trading Analyst — a specialist in Inner Mongolia (蒙西) \
BESS dispatch performance, P&L attribution, and market trading analysis via the Hermes assistant.

## Assets under management (4 operating BESS plants)
| Code         | Chinese name   |
|--------------|----------------|
| suyou        | 景蓝乌尔图     |
| hangjinqi    | 悦杭独贵       |
| siziwangqi   | 景通四益堂储   |
| gushanliang  | 裕昭沙子坝     |

## P&L waterfall (loss attribution chain)
PF Unrestricted → PF Grid-Feasible → Forecast Optimal → Strategy → Nominated → Cleared Actual

## Rules
1. Always call get_asset_pnl before making any financial claims.
2. Use get_dispatch_data to analyse specific dispatch days or execution gaps.
3. Use get_rt_prices to contextualise market price conditions.
4. Use get_strategy_comparison for benchmarking actual vs theoretical performance.
5. Use search_knowledge_base for questions about trading rules, settlement, or market policy.
6. Respond concisely with actionable insights for the trading team.
7. Respond in the same language as the question (Chinese or English).
8. Quote all monetary values in CNY (元); clearly state the date range used.
"""


def _dispatch(name: str, inputs: dict, engine) -> str:
    try:
        if name == "get_asset_pnl":
            where = ["trade_date >= :start", "trade_date <= :end"]
            params: dict = {"start": inputs["start_date"], "end": inputs["end_date"]}
            if inputs.get("asset_code"):
                where.append("asset_code = :asset")
                params["asset"] = inputs["asset_code"]
            df = pd.read_sql(
                sql_text(
                    f"SELECT trade_date, asset_code, "
                    f"pf_unrestricted_pnl, pf_grid_feasible_pnl, "
                    f"tt_forecast_optimal_pnl, tt_strategy_pnl, "
                    f"nominated_pnl, cleared_actual_pnl "
                    f"FROM reports.bess_asset_daily_attribution "
                    f"WHERE {' AND '.join(where)} "
                    f"ORDER BY trade_date, asset_code LIMIT 200"
                ),
                engine, params=params,
            )
            return df.to_json(orient="records", default_handler=str)

        if name == "get_dispatch_data":
            df = pd.read_sql(
                sql_text(
                    "SELECT interval_start, asset_code, "
                    "nominated_dispatch_mw, actual_dispatch_mw, nodal_price_excel "
                    "FROM marketdata.ops_bess_dispatch_15min "
                    "WHERE asset_code = :asset AND data_date = :dt "
                    "ORDER BY interval_start"
                ),
                engine,
                params={"asset": inputs["asset_code"], "dt": inputs["date"]},
            )
            return df.to_json(orient="records", default_handler=str)

        if name == "get_rt_prices":
            df = pd.read_sql(
                sql_text(
                    "SELECT date_trunc('hour', time) AS hour, "
                    "AVG(price) AS avg_rt_price_cny_mwh "
                    "FROM public.hist_mengxi_provincerealtimeclearprice_15min "
                    "WHERE time::date BETWEEN :start AND :end "
                    "GROUP BY 1 ORDER BY 1 LIMIT 500"
                ),
                engine,
                params={"start": inputs["start_date"], "end": inputs["end_date"]},
            )
            return df.to_json(orient="records", default_handler=str)

        if name == "get_strategy_comparison":
            where = ["trade_date >= :start", "trade_date <= :end"]
            params = {"start": inputs["start_date"], "end": inputs["end_date"]}
            if inputs.get("asset_codes"):
                placeholders = ", ".join(f":a{i}" for i, _ in enumerate(inputs["asset_codes"]))
                where.append(f"asset_code IN ({placeholders})")
                for i, code in enumerate(inputs["asset_codes"]):
                    params[f"a{i}"] = code
            df = pd.read_sql(
                sql_text(
                    f"SELECT trade_date, asset_code, scenario_name AS strategy, total_pnl "
                    f"FROM reports.bess_asset_daily_scenario_pnl "
                    f"WHERE {' AND '.join(where)} "
                    f"ORDER BY trade_date, asset_code, scenario_name LIMIT 500"
                ),
                engine, params=params,
            )
            return df.to_json(orient="records", default_handler=str)

        if name == "search_knowledge_base":
            from services.knowledge_pool.knowledge_docs import search_reference_docs
            rows = search_reference_docs(
                query=inputs["query"],
                category=inputs.get("category"),
                app="trader",
                limit=5,
            )
            if not rows:
                return "No matching documents found."
            return json.dumps(rows, ensure_ascii=False, default=str)

        return json.dumps({"error": f"Unknown tool: {name}"})

    except Exception as exc:
        logger.error("mengxi tool %s error: %s", name, exc)
        return json.dumps({"error": str(exc)})


def run_mengxi_query(question: str, api_key: str, pg_url: str = "") -> str:
    """Run the Mengxi trading headless agent and return its answer."""
    import anthropic
    from shared.anthropic_client import make_client as _make_anthropic_client

    client = _make_anthropic_client(api_key)
    engine = _make_engine(pg_url)

    # Build system prompt with expert insights injected (READ path)
    system = _SYSTEM
    try:
        from services.knowledge_pool.expert_memory import get_relevant_insights, inject_expert_memory
        insights = get_relevant_insights(question, limit=4)
        mem_block = inject_expert_memory(insights)
        if mem_block:
            system += f"\n\n{mem_block}"
    except Exception:
        pass
    try:
        from services.knowledge_pool import vault_reader
        vault_ctx = vault_reader.retrieve_vault_context(question)
        if vault_ctx:
            system += f"\n\n{vault_ctx}"
    except Exception:
        pass

    messages = [{"role": "user", "content": question}]
    while True:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=system,
            tools=_TOOLS,
            messages=messages,
        )
        messages = messages + [{"role": "assistant", "content": resp.content}]
        if resp.stop_reason == "end_turn":
            answer = next((b.text for b in resp.content if hasattr(b, "text")), "")
            # Extract and store new insights (WRITE path)
            try:
                from services.knowledge_pool.expert_memory import extract_spot_insights
                extract_spot_insights(user_msg=question, agent_reply=answer, api_key=api_key, source_app="mengxi_trader")
            except Exception:
                pass
            return answer
        tool_results = []
        for block in resp.content:
            if block.type == "tool_use":
                result_str = _dispatch(block.name, block.input, engine)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result_str,
                })
        if not tool_results:
            return next((b.text for b in resp.content if hasattr(b, "text")), "")
        messages = messages + [{"role": "user", "content": tool_results}]
