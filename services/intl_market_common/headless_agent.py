"""Headless (no-Streamlit) BESS Market Strategist agent for intl markets.

Covers: AU, ERCOT, CAISO, PJM, PH, PO.

Usage:
    from services.au_knowledge.config import MARKET_CONFIG
    from services.intl_market_common.headless_agent import run_market_query
    answer = run_market_query(MARKET_CONFIG, "What were AU BESS revenues in April?", api_key, pg_url)
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date

import pandas as pd
import psycopg2

from services.intl_market_common.market_config import MarketConfig

logger = logging.getLogger(__name__)


def _open_conn(pg_url: str):
    conn = psycopg2.connect(pg_url, connect_timeout=10)
    conn.autocommit = True
    return conn


def _query(conn, sql: str, params=None) -> pd.DataFrame:
    return pd.read_sql(sql, conn, params=params)


# ── SQL helpers ───────────────────────────────────────────────────────────────

def _get_spot_price(conn, prefix: str, start: str, end: str) -> pd.DataFrame:
    try:
        return _query(
            conn,
            f"SELECT settlement_date, region, AVG(spot_price) AS avg_spot_price "
            f"FROM intl_market.{prefix}spot_price "
            f"WHERE settlement_date BETWEEN %s AND %s "
            f"GROUP BY settlement_date, region ORDER BY settlement_date",
            (start, end),
        )
    except Exception:
        return pd.DataFrame()


def _get_ancillary(conn, prefix: str, start: str, end: str) -> pd.DataFrame:
    try:
        return _query(
            conn,
            f"SELECT settlement_date, service, clearing_price, volume_mw "
            f"FROM intl_market.{prefix}ancillary_results "
            f"WHERE settlement_date BETWEEN %s AND %s "
            f"ORDER BY settlement_date, service",
            (start, end),
        )
    except Exception:
        return pd.DataFrame()


def _get_daily_index(conn, prefix: str, start: str, end: str) -> pd.DataFrame:
    try:
        return _query(
            conn,
            f"SELECT settlement_date, market, revenue_permw, revenue_permwh "
            f"FROM intl_market.{prefix}bess_daily_index "
            f"WHERE settlement_date BETWEEN %s AND %s AND duration = '*' "
            f"ORDER BY settlement_date, market",
            (start, end),
        )
    except Exception:
        return pd.DataFrame()


def _get_monthly_index(conn, prefix: str, start: str, end: str) -> pd.DataFrame:
    try:
        return _query(
            conn,
            f"SELECT year_month AS month, market, revenue_permw "
            f"FROM intl_market.{prefix}bess_monthly_index "
            f"WHERE year_month BETWEEN %s AND %s AND duration = '*' "
            f"ORDER BY year_month, market",
            (start[:7], end[:7]),
        )
    except Exception:
        return pd.DataFrame()


def _get_leaderboard(conn, prefix: str, start: str, end: str, top_n: int = 20) -> pd.DataFrame:
    try:
        return _query(
            conn,
            f"WITH lb AS ( "
            f"  SELECT asset, SUM(revenue) AS total_revenue, "
            f"    AVG(rated_power) AS rated_power_mw "
            f"  FROM intl_market.{prefix}bess_leaderboard "
            f"  WHERE settlement_date BETWEEN %s AND %s "
            f"  GROUP BY asset ORDER BY total_revenue DESC LIMIT %s "
            f") SELECT lb.asset, lb.total_revenue, lb.rated_power_mw FROM lb",
            (start, end, top_n),
        )
    except Exception:
        return pd.DataFrame()


def _get_assets(conn, prefix: str) -> pd.DataFrame:
    try:
        return _query(
            conn,
            f"WITH rp AS (SELECT DISTINCT ON (asset) asset, CAST(value AS NUMERIC) AS rated_power_mw "
            f"            FROM intl_market.{prefix}bess_assets WHERE history_table='rated_power' "
            f"            ORDER BY asset, date_from DESC NULLS LAST) "
            f"SELECT rp.asset, rp.rated_power_mw FROM rp",
        )
    except Exception:
        return pd.DataFrame()


def _search_knowledge(conn, prefix: str, query: str, limit: int = 6) -> pd.DataFrame:
    try:
        return _query(
            conn,
            f"SELECT source, doc_type, title, url, published_date, left(content, 1500) AS snippet "
            f"FROM intl_market.{prefix}knowledge_docs "
            f"WHERE title ILIKE %s "
            f"ORDER BY published_date DESC NULLS LAST LIMIT %s",
            ("%" + query.replace("%", "").replace("_", "") + "%", limit),
        )
    except Exception:
        return pd.DataFrame()


def _load_memories(conn, app_key: str) -> pd.DataFrame:
    try:
        return _query(
            conn,
            "SELECT id, category, subject, content, source, created_at "
            "FROM marketdata.agent_memory WHERE app = %s AND active = TRUE "
            "ORDER BY created_at DESC",
            (app_key,),
        )
    except Exception:
        return pd.DataFrame()


# ── Agent ─────────────────────────────────────────────────────────────────────

def run_market_query(cfg: MarketConfig, question: str, api_key: str, pg_url: str) -> str:
    """Run the BESS Market Strategist agent and return its text answer."""
    import anthropic
    from shared.anthropic_client import make_client as _make_anthropic_client
    client = _make_anthropic_client(api_key)

    pg_url = pg_url or os.environ.get("PGURL") or os.environ.get("DATABASE_URL", "")
    conn = _open_conn(pg_url)
    prefix = cfg.table_prefix

    # Build system prompt
    mems = _load_memories(conn, cfg.app_key)
    system = (
        f"You are the {cfg.name} BESS Market Strategist.\n\n"
        f"GROUNDING RULE: Answer only from data returned by your tools. "
        f"Never state specific prices or market events from training data.\n\n"
        f"MARKET CONTEXT:\n"
        f"- Market: {cfg.name} | System operator: {cfg.system_operator}\n"
        f"- Currency: {cfg.currency_sym} ({cfg.currency_code})\n"
        f"- Ancillary services: {cfg.ancillary_label}\n"
        f"- Wholesale market: {cfg.wholesale_label}\n"
        f"- Intervals per day: {cfg.intervals_per_day}\n\n"
        f"ANALYTICAL FRAMEWORK:\n"
        f"- For spot price questions → call get_spot_price\n"
        f"- For ancillary market questions → call get_ancillary_results\n"
        f"- For BESS leaderboard / asset performance → call get_bess_leaderboard\n"
        f"- For BESS revenue index → call get_bess_revenue_index\n"
        f"- For BESS asset data → call get_bess_assets\n"
        f"- For market context, regulation, research → call search_knowledge_base\n"
    )

    # Try advanced retrieval for context
    try:
        from services.intl_market_common.advanced_retrieval_base import retrieve_for_agent
        kb_ctx = retrieve_for_agent(question, api_key, cfg, top_k=4)
        if kb_ctx and "No relevant" not in kb_ctx:
            system += f"\n\nKNOWLEDGE BASE CONTEXT:\n{kb_ctx}"
    except Exception:
        pass

    if not mems.empty:
        mem_lines = "\n".join(f"- [{r.category}] {r.subject}: {r.content}" for r in mems.itertuples())
        system += f"\n\nAnalyst notes from prior sessions:\n{mem_lines}"

    tools = [
        {"name": "get_spot_price",
         "description": f"Daily average spot price ({cfg.currency_code}/MWh) for {cfg.name}.",
         "input_schema": {"type": "object",
                          "properties": {"start_date": {"type": "string"}, "end_date": {"type": "string"}},
                          "required": ["start_date", "end_date"]}},
        {"name": "get_ancillary_results",
         "description": f"{cfg.ancillary_label} ancillary service clearing prices and volumes.",
         "input_schema": {"type": "object",
                          "properties": {"start_date": {"type": "string"}, "end_date": {"type": "string"}},
                          "required": ["start_date", "end_date"]}},
        {"name": "get_bess_leaderboard",
         "description": "Top BESS assets by total revenue for a date range.",
         "input_schema": {"type": "object",
                          "properties": {"start_date": {"type": "string"}, "end_date": {"type": "string"},
                                         "top_n": {"type": "integer"}},
                          "required": ["start_date", "end_date"]}},
        {"name": "get_bess_revenue_index",
         "description": "BESS fleet revenue index (daily or monthly).",
         "input_schema": {"type": "object",
                          "properties": {"start_date": {"type": "string"}, "end_date": {"type": "string"},
                                         "granularity": {"type": "string", "enum": ["daily", "monthly"]}},
                          "required": ["start_date", "end_date"]}},
        {"name": "get_bess_assets",
         "description": "Database of registered BESS assets with capacity.",
         "input_schema": {"type": "object",
                          "properties": {"min_power_mw": {"type": "number"}},
                          "required": []}},
        {"name": "search_knowledge_base",
         "description": "Search market reports, regulation docs, and research.",
         "input_schema": {"type": "object",
                          "properties": {"query": {"type": "string"}},
                          "required": ["query"]}},
    ]

    def dispatch(name: str, inputs: dict) -> str:
        try:
            if name == "search_knowledge_base":
                try:
                    from services.intl_market_common.advanced_retrieval_base import retrieve_for_agent
                    return retrieve_for_agent(inputs["query"], api_key, cfg, top_k=6)
                except Exception:
                    pass
                results = _search_knowledge(conn, prefix, inputs["query"])
                if results.empty:
                    return "No matching knowledge documents found."
                return "\n\n---\n\n".join(
                    f"[{r['source']}] {r['title']} ({r['published_date']})\n{r['snippet']}"
                    for _, r in results.iterrows()
                )
            elif name == "get_spot_price":
                df = _get_spot_price(conn, prefix, inputs["start_date"], inputs["end_date"])
                return df.round(2).to_json(orient="records", date_format="iso") if not df.empty else "No data."
            elif name == "get_ancillary_results":
                df = _get_ancillary(conn, prefix, inputs["start_date"], inputs["end_date"])
                return df.round(2).to_json(orient="records") if not df.empty else "No ancillary data."
            elif name == "get_bess_leaderboard":
                df = _get_leaderboard(conn, prefix, inputs["start_date"], inputs["end_date"], inputs.get("top_n", 20))
                return df.round(2).to_json(orient="records") if not df.empty else "No leaderboard data."
            elif name == "get_bess_revenue_index":
                if inputs.get("granularity", "monthly") == "daily":
                    df = _get_daily_index(conn, prefix, inputs["start_date"], inputs["end_date"])
                else:
                    df = _get_monthly_index(conn, prefix, inputs["start_date"], inputs["end_date"])
                return df.round(2).to_json(orient="records", date_format="iso") if not df.empty else "No index data."
            elif name == "get_bess_assets":
                df = _get_assets(conn, prefix)
                return f"Total: {len(df)} assets\n" + df.to_json(orient="records") if not df.empty else "No assets."
        except Exception as e:
            return f"Error: {e}"
        return "Unknown tool"

    messages = [{"role": "user", "content": question}]
    while True:
        resp = client.messages.create(
            model="claude-sonnet-4-6", max_tokens=4096,
            system=system, tools=tools, messages=messages,
        )
        messages = messages + [{"role": "assistant", "content": resp.content}]
        if resp.stop_reason == "end_turn":
            conn.close()
            return next((b.text for b in resp.content if hasattr(b, "text")), "")
        tool_results = []
        for block in resp.content:
            if block.type == "tool_use":
                result_str = dispatch(block.name, block.input)
                tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": result_str})
        if not tool_results:
            conn.close()
            return next((b.text for b in resp.content if hasattr(b, "text")), "")
        messages = messages + [{"role": "user", "content": tool_results}]
