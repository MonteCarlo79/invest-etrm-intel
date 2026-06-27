"""Headless (no-Streamlit) GB Market Strategist agent.

Extracts the Strategist agent from apps/gb-market/app.py without any Streamlit dependency.

Usage:
    from services.gb_knowledge.headless_agent import run_gb_query
    answer = run_gb_query("What were GB BESS revenues in May?", api_key, pg_url)
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta

import pandas as pd
import psycopg2

logger = logging.getLogger(__name__)

_APP_KEY = "gb_market"


def _open_conn(pg_url: str):
    conn = psycopg2.connect(pg_url, connect_timeout=10)
    conn.autocommit = True
    return conn


def _query(conn, sql: str, params=None) -> pd.DataFrame:
    return pd.read_sql(sql, conn, params=params)


def _load_memories(conn) -> pd.DataFrame:
    try:
        return _query(
            conn,
            "SELECT id, category, subject, content, source, created_at "
            "FROM marketdata.agent_memory WHERE app = %s AND active = TRUE "
            "ORDER BY created_at DESC",
            (_APP_KEY,),
        )
    except Exception:
        return pd.DataFrame()


def _search_knowledge(conn, query: str, sources=None, limit: int = 6) -> pd.DataFrame:
    try:
        fts_params = [query, query]
        src_clause = "AND source = ANY(%s)" if sources else ""
        if sources:
            fts_params.append(sources)
        fts_params.append(limit)
        return _query(
            conn,
            f"SELECT source, doc_type, title, url, published_date, left(content, 1500) AS snippet "
            f"FROM gb_knowledge.knowledge_docs "
            f"WHERE search_vector @@ to_tsquery('english', "
            f"  regexp_replace(plainto_tsquery('english', %s)::text, ' & ', ' | ', 'g')"
            f") {src_clause} "
            f"ORDER BY ts_rank(search_vector, plainto_tsquery('english', %s)) DESC LIMIT %s",
            fts_params,
        )
    except Exception:
        try:
            params2 = ["%" + query.strip().replace("%", "").replace("_", "") + "%", limit]
            return _query(
                conn,
                "SELECT source, doc_type, title, url, published_date, left(content, 1500) AS snippet "
                "FROM gb_knowledge.knowledge_docs "
                "WHERE title ILIKE %s "
                "ORDER BY published_date DESC NULLS LAST LIMIT %s",
                params2,
            )
        except Exception:
            return pd.DataFrame()


def _get_elexon_sp_daily(conn, start: str, end: str) -> pd.DataFrame:
    try:
        return _query(
            conn,
            "SELECT date, AVG(system_sell_price) AS avg_ssp, AVG(system_buy_price) AS avg_sbp, "
            "AVG(net_imbalance_volume) AS avg_niv "
            "FROM intl_market.gb_elexon_sp "
            "WHERE date BETWEEN %s AND %s GROUP BY date ORDER BY date",
            (start, end),
        )
    except Exception:
        return pd.DataFrame()


def _get_wind_forecast(conn, start: str, end: str) -> pd.DataFrame:
    try:
        return _query(
            conn,
            "SELECT start_time, generation_mw FROM intl_market.gb_wind_forecast "
            "WHERE start_time BETWEEN %s AND %s ORDER BY start_time LIMIT 96",
            (start, end),
        )
    except Exception:
        return pd.DataFrame()


_STRATEGIST_TOOLS = [
    {"name": "get_system_price",
     "description": "Half-hourly GB system price (£/MWh) and NIV (MW) for a date range.",
     "input_schema": {"type": "object",
                      "properties": {"start_date": {"type": "string"}, "end_date": {"type": "string"}},
                      "required": ["start_date", "end_date"]}},
    {"name": "get_epex_prices",
     "description": "EPEX day-ahead half-hourly prices (GBP/MWh) including daily baseload/peak/offpeak.",
     "input_schema": {"type": "object",
                      "properties": {"start_date": {"type": "string"}, "end_date": {"type": "string"}},
                      "required": ["start_date", "end_date"]}},
    {"name": "get_ancillary_results",
     "description": "DX ancillary service clearing prices (GBP/MW/h) and cleared volumes (MW).",
     "input_schema": {"type": "object",
                      "properties": {"start_date": {"type": "string"}, "end_date": {"type": "string"},
                                     "services": {"type": "array", "items": {"type": "string"}}},
                      "required": ["start_date", "end_date"]}},
    {"name": "get_market_summary",
     "description": "Daily summary: avg system price, EPEX baseload, spread, avg NIV.",
     "input_schema": {"type": "object",
                      "properties": {"start_date": {"type": "string"}, "end_date": {"type": "string"}},
                      "required": ["start_date", "end_date"]}},
    {"name": "get_bess_leaderboard",
     "description": "Asset-level GB BESS revenue leaderboard (£/MW/day) for a date range.",
     "input_schema": {"type": "object",
                      "properties": {"start_date": {"type": "string"}, "end_date": {"type": "string"},
                                     "market": {"type": "string"}, "top_n": {"type": "integer"}},
                      "required": ["start_date", "end_date"]}},
    {"name": "get_bess_revenue_index",
     "description": "GB BESS industry-average revenue index (£/MW/day or £/MW/month).",
     "input_schema": {"type": "object",
                      "properties": {"start_date": {"type": "string"}, "end_date": {"type": "string"},
                                     "granularity": {"type": "string", "enum": ["daily", "monthly"]}},
                      "required": ["start_date", "end_date"]}},
    {"name": "get_bess_assets",
     "description": "GB BESS asset register (power, capacity, owner, operator, location).",
     "input_schema": {"type": "object",
                      "properties": {"min_power_mw": {"type": "number"},
                                     "owner": {"type": "string"}, "operator": {"type": "string"}},
                      "required": []}},
    {"name": "get_elexon_ops",
     "description": "Elexon-official GB settlement prices (SSP/SBP, NIV) and wind forecast.",
     "input_schema": {"type": "object",
                      "properties": {"start_date": {"type": "string"}, "end_date": {"type": "string"}},
                      "required": ["start_date", "end_date"]}},
    {"name": "search_knowledge_base",
     "description": "Search GB energy market knowledge: articles, reports, regulatory changes.",
     "input_schema": {"type": "object",
                      "properties": {"query": {"type": "string"},
                                     "sources": {"type": "array", "items": {"type": "string"}}},
                      "required": ["query"]}},
]


def _compute_irr(cashflows: list[float]) -> float:
    rate = 0.1
    for _ in range(100):
        npv = sum(cf / (1 + rate) ** t for t, cf in enumerate(cashflows))
        dnpv = sum(-t * cf / (1 + rate) ** (t + 1) for t, cf in enumerate(cashflows))
        if abs(dnpv) < 1e-10:
            break
        rate -= npv / dnpv
        if rate <= -1:
            rate = -0.999
    return rate


def run_gb_query(question: str, api_key: str, pg_url: str) -> str:
    """Run the GB BESS Market Strategist agent and return its text answer."""
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    pg_url = pg_url or os.environ.get("PGURL") or os.environ.get("DATABASE_URL", "")
    conn = _open_conn(pg_url)

    # Build system prompt
    system = (
        "You are the GB BESS Market Strategist.\n\n"
        "GROUNDING RULE: Answer only from data returned by your tools. "
        "Never state specific prices or market events from training data.\n\n"
        "MARKET CONTEXT:\n"
        "- Market: Great Britain | System operator: National Grid ESO\n"
        "- Currency: GBP (£) — prices in £/MWh or £/MW/day\n"
        "- Revenue streams: BM, CM, frequency_response (DCL/DCH/DRL/DRH/DML/DMH), wholesale (EPEX)\n"
        "- Intervals: 30-minute settlement periods\n\n"
        "ANALYTICAL FRAMEWORK:\n"
        "- System price & NIV → call get_system_price or get_elexon_ops\n"
        "- EPEX day-ahead → call get_epex_prices\n"
        "- Ancillary (DX) → call get_ancillary_results\n"
        "- Market overview → call get_market_summary\n"
        "- BESS leaderboard → call get_bess_leaderboard\n"
        "- BESS revenue trend → call get_bess_revenue_index\n"
        "- Asset data → call get_bess_assets\n"
        "- Market context/research → call search_knowledge_base\n"
    )

    # Try advanced retrieval
    try:
        from services.gb_knowledge.advanced_retrieval import retrieve_for_gb_agent
        kb_ctx = retrieve_for_gb_agent(query=question, api_key=api_key, top_k=4)
        if kb_ctx and "No relevant" not in kb_ctx:
            system += f"\n\nKNOWLEDGE BASE CONTEXT:\n{kb_ctx}"
    except Exception:
        pass

    mems = _load_memories(conn)
    if not mems.empty:
        mem_lines = "\n".join(f"- [{r.category}] {r.subject}: {r.content}" for r in mems.itertuples())
        system += f"\n\nAnalyst notes from prior sessions:\n{mem_lines}"

    def dispatch(name: str, inputs: dict) -> str:
        try:
            if name == "search_knowledge_base":
                try:
                    from services.gb_knowledge.advanced_retrieval import retrieve_for_gb_agent
                    return retrieve_for_gb_agent(
                        query=inputs["query"], api_key=api_key,
                        sources=inputs.get("sources") or None, top_k=6,
                    )
                except Exception:
                    pass
                results = _search_knowledge(conn, inputs["query"], sources=inputs.get("sources") or None)
                if results.empty:
                    return "No matching knowledge documents found."
                return "\n\n---\n\n".join(
                    f"[{r['source']}] {r['title']} ({r['published_date']})\n{r['snippet']}"
                    for _, r in results.iterrows()
                )

            elif name == "get_system_price":
                df = _query(
                    conn,
                    "SELECT sp.date, sp.settlement_period, sp.system_price, n.niv "
                    "FROM intl_market.gb_system_price sp "
                    "LEFT JOIN intl_market.gb_niv n "
                    "  ON sp.date = n.date AND sp.settlement_period = n.settlement_period "
                    "WHERE sp.date BETWEEN %s AND %s ORDER BY sp.date, sp.settlement_period",
                    (inputs["start_date"], inputs["end_date"]),
                )
                if df.empty:
                    return "No system price data for the requested period."
                summary = df.groupby("date").agg(
                    avg_system_price=("system_price", "mean"),
                    avg_niv=("niv", "mean"),
                ).round(2).reset_index()
                return summary.to_json(orient="records", date_format="iso")

            elif name == "get_epex_prices":
                df = _query(
                    conn,
                    "SELECT delivery_date, daily_baseload, daily_peakload, daily_offpeak, AVG(price) AS avg_price "
                    "FROM intl_market.gb_epex_da_hh "
                    "WHERE delivery_date BETWEEN %s AND %s "
                    "GROUP BY delivery_date, daily_baseload, daily_peakload, daily_offpeak "
                    "ORDER BY delivery_date",
                    (inputs["start_date"], inputs["end_date"]),
                )
                return df.round(2).to_json(orient="records", date_format="iso") if not df.empty else "No EPEX data."

            elif name == "get_ancillary_results":
                services = inputs.get("services") or []
                if services:
                    placeholders = ",".join(["%s"] * len(services))
                    df = _query(
                        conn,
                        f"SELECT efa_date, service, AVG(clearing_price) AS avg_price, AVG(cleared_volume) AS avg_vol "
                        f"FROM intl_market.gb_dx_results "
                        f"WHERE efa_date BETWEEN %s AND %s AND service IN ({placeholders}) "
                        f"GROUP BY efa_date, service ORDER BY efa_date, service",
                        (inputs["start_date"], inputs["end_date"], *services),
                    )
                else:
                    df = _query(
                        conn,
                        "SELECT service, AVG(clearing_price) AS avg_price, AVG(cleared_volume) AS avg_vol "
                        "FROM intl_market.gb_dx_results "
                        "WHERE efa_date BETWEEN %s AND %s GROUP BY service",
                        (inputs["start_date"], inputs["end_date"]),
                    )
                return df.round(2).to_json(orient="records") if not df.empty else "No ancillary data."

            elif name == "get_market_summary":
                sp = _query(
                    conn,
                    "SELECT date, AVG(system_price) AS avg_system_price "
                    "FROM intl_market.gb_system_price "
                    "WHERE date BETWEEN %s AND %s GROUP BY date ORDER BY date",
                    (inputs["start_date"], inputs["end_date"]),
                )
                epex = _query(
                    conn,
                    "SELECT delivery_date AS date, MAX(daily_baseload) AS epex_baseload "
                    "FROM intl_market.gb_epex_da_hh "
                    "WHERE delivery_date BETWEEN %s AND %s GROUP BY delivery_date ORDER BY delivery_date",
                    (inputs["start_date"], inputs["end_date"]),
                )
                merged = sp.merge(epex, on="date", how="outer")
                merged["spread_sys_epex"] = (merged["avg_system_price"] - merged["epex_baseload"]).round(2)
                return merged.round(2).to_json(orient="records", date_format="iso") if not merged.empty else "No data."

            elif name == "get_bess_leaderboard":
                market = inputs.get("market")
                top_n = inputs.get("top_n", 20)
                sql_params = [inputs["start_date"], inputs["end_date"]]
                market_clause = "AND market = %s" if market else ""
                if market:
                    sql_params.append(market)
                sql_params.append(top_n)
                df = _query(
                    conn,
                    f"SELECT asset, SUM(revenue) AS total_revenue, AVG(revspermw)*48 AS avg_rev_per_mw_day "
                    f"FROM intl_market.gb_bess_leaderboard "
                    f"WHERE settlement_date BETWEEN %s AND %s {market_clause} "
                    f"GROUP BY asset ORDER BY total_revenue DESC LIMIT %s",
                    sql_params,
                )
                return df.round(2).to_json(orient="records") if not df.empty else "No leaderboard data."

            elif name == "get_bess_revenue_index":
                granularity = inputs.get("granularity", "monthly")
                if granularity == "daily":
                    df = _query(
                        conn,
                        "SELECT settlement_date, market, revenue_permw, revenue_permwh "
                        "FROM intl_market.gb_bess_daily_index "
                        "WHERE settlement_date BETWEEN %s AND %s AND duration='*' "
                        "ORDER BY settlement_date, market",
                        (inputs["start_date"], inputs["end_date"]),
                    )
                else:
                    df = _query(
                        conn,
                        "SELECT month, market, revenue_permw, revenue_permwh "
                        "FROM intl_market.gb_bess_monthly_index "
                        "WHERE month BETWEEN %s AND %s AND duration='*' "
                        "ORDER BY month, market",
                        (inputs["start_date"][:7] + "-01", inputs["end_date"][:7] + "-01"),
                    )
                return df.round(2).to_json(orient="records", date_format="iso") if not df.empty else "No index data."

            elif name == "get_bess_assets":
                conditions = ["history_table = 'rated_power'"]
                params: list = []
                if inputs.get("min_power_mw"):
                    conditions.append("CAST(value AS NUMERIC) >= %s")
                    params.append(inputs["min_power_mw"])
                where = " AND ".join(conditions)
                df = _query(
                    conn,
                    f"WITH rp AS (SELECT DISTINCT ON (asset) asset, CAST(value AS NUMERIC) AS rated_power_mw, "
                    f"  gsp, developer, commissioning_date "
                    f"  FROM intl_market.gb_bess_assets WHERE {where} ORDER BY asset, valid_from DESC), "
                    f"ec AS (SELECT DISTINCT ON (asset) asset, CAST(value AS NUMERIC) AS energy_capacity_mwh "
                    f"       FROM intl_market.gb_bess_assets WHERE history_table='energy_capacity' "
                    f"       ORDER BY asset, valid_from DESC), "
                    f"ow AS (SELECT DISTINCT ON (asset) asset, value AS owner "
                    f"       FROM intl_market.gb_bess_assets WHERE history_table='owner' "
                    f"       ORDER BY asset, valid_from DESC) "
                    f"SELECT rp.asset, ow.owner, rp.developer, rp.rated_power_mw, ec.energy_capacity_mwh, "
                    f"  rp.commissioning_date, rp.gsp "
                    f"FROM rp LEFT JOIN ec ON ec.asset=rp.asset LEFT JOIN ow ON ow.asset=rp.asset",
                    params or None,
                )
                if inputs.get("owner") and not df.empty:
                    df = df[df["owner"].str.contains(inputs["owner"], case=False, na=False)]
                if inputs.get("operator") and not df.empty:
                    df = df[df.get("operator", pd.Series()).str.contains(inputs["operator"], case=False, na=False)]
                if df.empty:
                    return "No assets found."
                return f"Total: {len(df)} assets, {df['rated_power_mw'].sum():.0f} MW\n" + df.to_json(orient="records", date_format="iso")

            elif name == "get_elexon_ops":
                sp_df = _get_elexon_sp_daily(conn, inputs["start_date"], inputs["end_date"])
                if sp_df.empty:
                    return "No Elexon system price data for this range."
                now_utc = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                future_utc = (datetime.utcnow() + timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M:%SZ")
                wf_df = _get_wind_forecast(conn, now_utc, future_utc)
                wf_summary = wf_df.to_json(orient="records", date_format="iso") if not wf_df.empty else "[]"
                return (
                    f"Settlement system prices (daily avg, £/MWh):\n"
                    + sp_df.round(2).to_json(orient="records", date_format="iso")
                    + f"\n\nWind generation forecast (next 48h):\n{wf_summary}"
                )

        except Exception as e:
            return f"Error: {e}"
        return "Unknown tool"

    messages = [{"role": "user", "content": question}]
    while True:
        resp = client.messages.create(
            model="claude-sonnet-4-6", max_tokens=4096,
            system=system, tools=_STRATEGIST_TOOLS, messages=messages,
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
