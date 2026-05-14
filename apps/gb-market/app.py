"""GB Market Intelligence — Streamlit app.

Two Claude agents:
  - Strategist: GB market fundamentals (system price, EPEX, NIV, ancillary)
  - Quant: BESS investment economics (benchmarking index, leaderboard, IRR)

Port 8508 | ALB slug: gb-market | Memory app key: gb_market
"""
import json
import os
import sys
from datetime import date, datetime, timedelta

import anthropic
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", "config", ".env"))

# Ensure repo root is on sys.path so all services.* packages are importable
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="GB Market Intelligence",
    page_icon="🇬🇧",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------

@st.cache_resource
def _get_conn():
    url = (
        os.environ.get("PGURL")
        or os.environ.get("DATABASE_URL")
        or "postgresql://postgres:root@127.0.0.1:5433/marketdata"
    )
    return psycopg2.connect(url, connect_timeout=10, keepalives=1,
                            keepalives_idle=60, keepalives_interval=10,
                            keepalives_count=5)


def _conn():
    conn = _get_conn()
    try:
        conn.cursor().execute("SELECT 1")
    except Exception:
        _get_conn.clear()
        conn = _get_conn()
    return conn


def _query(sql: str, params=None) -> pd.DataFrame:
    return pd.read_sql(sql, _conn(), params=params)


# ---------------------------------------------------------------------------
# Agent memory helpers
# ---------------------------------------------------------------------------

_APP_KEY = "gb_market"
_ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
_client = anthropic.Anthropic(api_key=_ANTHROPIC_KEY)


def _ensure_memory_table():
    cur = _conn().cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS marketdata.agent_memory (
            id SERIAL PRIMARY KEY,
            app TEXT NOT NULL DEFAULT 'gb_market',
            category TEXT NOT NULL,
            subject TEXT NOT NULL,
            content TEXT NOT NULL,
            source TEXT DEFAULT 'manual',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            active BOOLEAN DEFAULT TRUE
        )
    """)
    cur.execute("ALTER TABLE marketdata.agent_memory ADD COLUMN IF NOT EXISTS app TEXT DEFAULT 'gb_market'")
    _conn().commit()


def _ensure_ingestion_log_table():
    cur = _conn().cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS intl_market.gb_ingestion_log (
            id SERIAL PRIMARY KEY,
            run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            trigger TEXT NOT NULL,
            date_from DATE,
            date_to DATE,
            status TEXT NOT NULL,
            rows_ingested JSONB,
            error_msg TEXT,
            duration_seconds NUMERIC
        )
    """)
    _conn().commit()


def _ensure_knowledge_table():
    cur = _conn().cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS intl_market.gb_knowledge_docs (
            id              SERIAL PRIMARY KEY,
            source          TEXT NOT NULL,
            doc_type        TEXT NOT NULL,
            title           TEXT,
            url             TEXT UNIQUE,
            published_date  DATE,
            content         TEXT NOT NULL,
            fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            search_vector   TSVECTOR GENERATED ALWAYS AS (
                                to_tsvector('english',
                                    coalesce(title, '') || ' ' || left(content, 100000))
                            ) STORED
        )
    """)
    cur.execute(
        "CREATE INDEX IF NOT EXISTS gb_knowledge_docs_fts "
        "ON intl_market.gb_knowledge_docs USING GIN(search_vector)"
    )
    _conn().commit()


@st.cache_data(ttl=60)
def _search_knowledge(query: str, sources: list | None = None, limit: int = 8) -> pd.DataFrame:
    try:
        source_filter = ""
        params: list = [query, query]
        if sources:
            source_filter = "AND source = ANY(%s)"
            params.append(sources)
        params.append(limit)
        return _query(
            "SELECT source, doc_type, title, url, published_date, "
            "left(content, 1500) AS snippet, "
            "ts_rank(search_vector, plainto_tsquery('english', %s)) AS rank "
            "FROM intl_market.gb_knowledge_docs "
            f"WHERE search_vector @@ plainto_tsquery('english', %s) {source_filter} "
            "ORDER BY rank DESC LIMIT %s",
            params,
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def _knowledge_doc_counts() -> pd.DataFrame:
    try:
        return _query(
            "SELECT source, doc_type, COUNT(*) AS docs, MAX(fetched_at) AS last_fetch "
            "FROM intl_market.gb_knowledge_docs "
            "GROUP BY source, doc_type ORDER BY source, doc_type"
        )
    except Exception:
        return pd.DataFrame()


def _log_ingestion_run(trigger: str, date_from, date_to, status: str,
                        rows: dict | None, error_msg: str | None, duration: float):
    try:
        cur = _conn().cursor()
        cur.execute(
            "INSERT INTO intl_market.gb_ingestion_log "
            "(trigger, date_from, date_to, status, rows_ingested, error_msg, duration_seconds) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (trigger, date_from, date_to, status,
             json.dumps(rows) if rows else None, error_msg, round(duration, 1)),
        )
        _conn().commit()
    except Exception:
        pass  # never crash the scheduler thread due to logging failure


@st.cache_data(ttl=30)
def _get_ingestion_logs(limit: int = 20) -> pd.DataFrame:
    try:
        return _query(
            "SELECT id, run_at AT TIME ZONE 'Asia/Singapore' AS run_at_sgt, "
            "trigger, date_from, date_to, status, rows_ingested, error_msg, duration_seconds "
            "FROM intl_market.gb_ingestion_log "
            "ORDER BY run_at DESC LIMIT %s",
            (limit,),
        )
    except Exception:
        return pd.DataFrame()


def _run_ingestion_job(date_from, date_to, trigger: str = "manual") -> dict:
    """Run full GB ingestion; return result dict. Called by scheduler and UI button."""
    import io, time
    from contextlib import redirect_stdout

    from services.modo_energy.gb_ingestion import run_gb_backfill

    t0 = time.time()
    buf = io.StringIO()
    try:
        with redirect_stdout(buf):
            run_gb_backfill(date_from, date_to)
        duration = time.time() - t0
        _log_ingestion_run(trigger, date_from, date_to, "success", None, None, duration)
        return {"status": "success", "log": buf.getvalue(), "duration": duration}
    except Exception as exc:
        duration = time.time() - t0
        _log_ingestion_run(trigger, date_from, date_to, "error", None, str(exc), duration)
        return {"status": "error", "error": str(exc), "log": buf.getvalue(), "duration": duration}


def _run_knowledge_ingest_job(only: list[str] | None = None, trigger: str = "manual") -> dict:
    """Run knowledge base ingestion; return result dict."""
    import time
    t0 = time.time()
    try:
        from services.gb_knowledge.ingest import run_knowledge_ingest
        results = run_knowledge_ingest(only=only, verbose=False)
        duration = time.time() - t0
        total = sum(results.values())
        return {"status": "success", "results": results, "total": total, "duration": duration}
    except Exception as exc:
        duration = time.time() - t0
        return {"status": "error", "error": str(exc), "duration": duration}


@st.cache_resource
def _start_scheduler():
    """Start APScheduler background scheduler (runs once per process via cache_resource)."""
    from apscheduler.schedulers.background import BackgroundScheduler

    def _daily_market_job():
        yesterday = date.today() - timedelta(days=1)
        _run_ingestion_job(yesterday, yesterday, trigger="scheduled")
        _table_counts.clear()
        _get_ingestion_logs.clear()

    def _daily_knowledge_job():
        _run_knowledge_ingest_job(trigger="scheduled")

    scheduler = BackgroundScheduler(timezone="Asia/Singapore")
    scheduler.add_job(_daily_market_job, "cron", hour=3, minute=0,
                      id="gb_daily_market", misfire_grace_time=3600)
    scheduler.add_job(_daily_knowledge_job, "cron", hour=3, minute=30,
                      id="gb_daily_knowledge", misfire_grace_time=3600)
    scheduler.start()
    return scheduler


@st.cache_data(ttl=60)
def _load_memories(app_key: str) -> pd.DataFrame:
    try:
        return _query(
            "SELECT id, category, subject, content, source, created_at "
            "FROM marketdata.agent_memory WHERE app = %s AND active = TRUE "
            "ORDER BY created_at DESC",
            (app_key,),
        )
    except Exception:
        return pd.DataFrame()


def _save_memory(category: str, subject: str, content: str, source: str = "manual"):
    cur = _conn().cursor()
    cur.execute(
        "INSERT INTO marketdata.agent_memory (app, category, subject, content, source) "
        "VALUES (%s, %s, %s, %s, %s)",
        (_APP_KEY, category, subject, content, source),
    )
    _conn().commit()
    _load_memories.clear()


def _delete_memory(mem_id: int):
    cur = _conn().cursor()
    cur.execute("UPDATE marketdata.agent_memory SET active = FALSE WHERE id = %s", (mem_id,))
    _conn().commit()
    _load_memories.clear()


def _extract_memories(user_msg: str, agent_reply: str) -> list[dict]:
    resp = _client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=512,
        system=(
            "Extract memorable analyst preferences or domain facts from this GB power market "
            "conversation. Return a JSON array of objects with keys: "
            "category (one of: market_view, methodology, asset_note, investment_thesis, red_flag), "
            "subject (short title ≤8 words), content (one sentence). "
            "Only extract genuinely reusable insights — not ephemeral data points. "
            "Return [] if nothing is worth remembering."
        ),
        messages=[{"role": "user", "content": f"User: {user_msg}\n\nAgent: {agent_reply[:1500]}"}],
    )
    raw = next((b.text for b in resp.content if hasattr(b, "text")), "[]")
    start, end = raw.find("["), raw.rfind("]")
    if start == -1:
        return []
    try:
        return json.loads(raw[start:end + 1])
    except json.JSONDecodeError:
        return []


# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------

_GB_STRATEGIST_BASE_SYSTEM = """You are the GB Market Strategist, an expert in Great Britain electricity markets.

GROUNDING RULE: Answer only from data returned by your tools in this conversation. Never state specific prices, volumes, system conditions, or market events from your training data. If you have not called a tool yet, call one before answering factual questions.

DOMAIN CONTEXT:
- Settlement periods: 30-min half-hourly blocks, SP1–SP48 each day
- System price: the Balancing Mechanism clearing price (can be negative)
- NIV (Net Imbalance Volume): positive = system long (oversupplied), negative = system short
- EPEX DA: day-ahead auction clearing prices in GBP/MWh
- Ancillary services: Dynamic Containment (DC), Dynamic Moderation (DM), Dynamic Regulation (DR)
  - Each service has Low (L) and High (H) directions, with EFA block auctions (EFA 1–6)
  - Clearing prices in GBP/MW/h (capacity payment)
- Key BESS revenue streams: BM (balancing mechanism), CM (capacity market), frequency_response (DC/DM/DR)

ANALYTICAL FRAMEWORK:
- For price/market questions → call get_system_price or get_epex_prices
- For market tightness/balance → call get_system_price (NIV direction indicates system balance)
- For ancillary market questions → call get_ancillary_results
- For combined market overview → call get_market_summary
"""


def _build_strategist_system() -> str:
    mems = _load_memories(_APP_KEY)
    if mems.empty:
        return _GB_STRATEGIST_BASE_SYSTEM
    mem_lines = "\n".join(
        f"- [{r.category}] {r.subject}: {r.content}" for r in mems.itertuples()
    )
    return _GB_STRATEGIST_BASE_SYSTEM + f"\n\n## Analyst memory from prior sessions:\n{mem_lines}"


_GB_QUANT_BASE_SYSTEM = """You are the GB Quant, an expert in BESS investment economics for the GB market.

GROUNDING RULE: Answer only from data returned by your tools in this conversation. Never state specific revenue figures, IRR estimates, or market benchmarks from training data. Always fetch data before answering.

DOMAIN CONTEXT:
- GB BESS revenues are expressed in £/MW/day or £/MW/month (capacity-normalised)
- Revenue streams: BM (balancing mechanism), CM (capacity market), frequency_response, wholesale (EPEX arbitrage)
- Modo Energy BESS index = industry-average revenue across all GB BESS assets
- Leaderboard = per-asset, per-settlement-period, per-market revenue breakdown
- Duration matters: longer duration (2h+) captures more BM/wholesale; shorter (0.5h) is better for DC
- IRR methodology: unlevered, 15-year life, O&M 2% of capex/yr, degradation 2%/yr revenue reduction

ANALYTICAL FRAMEWORK:
- For revenue trend questions → call get_bess_daily_index or get_bess_monthly_index
- For asset comparison → call get_leaderboard
- For market landscape → call get_asset_database
- For investment return → call estimate_irr (note this is a parametric estimate, not a full LP model)
"""


def _build_quant_system() -> str:
    mems = _load_memories(_APP_KEY)
    if mems.empty:
        return _GB_QUANT_BASE_SYSTEM
    mem_lines = "\n".join(
        f"- [{r.category}] {r.subject}: {r.content}" for r in mems.itertuples()
    )
    return _GB_QUANT_BASE_SYSTEM + f"\n\n## Analyst memory from prior sessions:\n{mem_lines}"


# ---------------------------------------------------------------------------
# Strategist tools
# ---------------------------------------------------------------------------

_STRATEGIST_TOOLS = [
    {
        "name": "get_system_price",
        "description": "Half-hourly GB system price (£/MWh) and NIV (MW) for a date range.",
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "ISO date e.g. '2026-05-01'"},
                "end_date":   {"type": "string", "description": "ISO date e.g. '2026-05-10'"},
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "get_epex_prices",
        "description": "EPEX day-ahead half-hourly prices (GBP/MWh) including daily baseload/peak/offpeak.",
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
        "name": "get_ancillary_results",
        "description": "DX ancillary service clearing prices (GBP/MW/h) and cleared volumes (MW) by service and EFA block.",
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string"},
                "end_date":   {"type": "string"},
                "services": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Service codes e.g. ['DCL','DRL','DCH','DRH','DMH','DML']. Leave empty for all.",
                },
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "get_market_summary",
        "description": "Daily summary combining avg system price, EPEX baseload, spread (system-EPEX), avg NIV.",
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
        "name": "search_knowledge_base",
        "description": (
            "Semantic full-text search over GB energy market knowledge: articles, reports, "
            "market notices, regulatory changes, and commentary from Elexon, ENTSO-E, "
            "Timera Energy, Modo Energy, and Meteologica. "
            "Use this to answer questions about market context, policy changes, or research."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Keywords or natural language question to search for.",
                },
                "sources": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Filter by source. Options: elexon, entso_e, timera, modo, meteologica. "
                        "Leave empty to search all sources."
                    ),
                },
            },
            "required": ["query"],
        },
    },
]


def _dispatch_strategist(name: str, inputs: dict) -> str:
    try:
        if name == "search_knowledge_base":
            results = _search_knowledge(
                inputs["query"],
                sources=inputs.get("sources") or None,
                limit=6,
            )
            if results.empty:
                return "No matching knowledge documents found."
            out = []
            for _, row in results.iterrows():
                out.append(
                    f"[{row['source']} / {row['doc_type']}] {row['title']} "
                    f"({row['published_date']})\n{row['snippet']}"
                )
            return "\n\n---\n\n".join(out)

        elif name == "get_system_price":
            df = _query(
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
                min_system_price=("system_price", "min"),
                max_system_price=("system_price", "max"),
                avg_niv=("niv", "mean"),
            ).round(2).reset_index()
            return summary.to_json(orient="records", date_format="iso")

        elif name == "get_epex_prices":
            df = _query(
                "SELECT delivery_date, settlement_period, price, volume, "
                "daily_baseload, daily_peakload, daily_offpeak "
                "FROM intl_market.gb_epex_da_hh "
                "WHERE delivery_date BETWEEN %s AND %s ORDER BY delivery_date, settlement_period",
                (inputs["start_date"], inputs["end_date"]),
            )
            if df.empty:
                return "No EPEX DA data for the requested period."
            daily = df.groupby("delivery_date").agg(
                daily_baseload=("daily_baseload", "first"),
                daily_peakload=("daily_peakload", "first"),
                daily_offpeak=("daily_offpeak", "first"),
                avg_price=("price", "mean"),
            ).round(2).reset_index()
            return daily.to_json(orient="records", date_format="iso")

        elif name == "get_ancillary_results":
            services = inputs.get("services") or []
            if services:
                placeholders = ",".join(["%s"] * len(services))
                df = _query(
                    f"SELECT efa_date, efa, service, clearing_price, cleared_volume, service_type "
                    f"FROM intl_market.gb_dx_results "
                    f"WHERE efa_date BETWEEN %s AND %s AND service IN ({placeholders}) "
                    f"ORDER BY efa_date, efa, service",
                    (inputs["start_date"], inputs["end_date"], *services),
                )
            else:
                df = _query(
                    "SELECT efa_date, efa, service, clearing_price, cleared_volume, service_type "
                    "FROM intl_market.gb_dx_results "
                    "WHERE efa_date BETWEEN %s AND %s ORDER BY efa_date, efa, service",
                    (inputs["start_date"], inputs["end_date"]),
                )
            if df.empty:
                return "No DX ancillary data for the requested period."
            summary = df.groupby(["service"]).agg(
                avg_clearing_price=("clearing_price", "mean"),
                avg_cleared_volume=("cleared_volume", "mean"),
                min_price=("clearing_price", "min"),
                max_price=("clearing_price", "max"),
            ).round(2).reset_index()
            return summary.to_json(orient="records")

        elif name == "get_market_summary":
            sp = _query(
                "SELECT date, AVG(system_price) AS avg_system_price "
                "FROM intl_market.gb_system_price "
                "WHERE date BETWEEN %s AND %s GROUP BY date ORDER BY date",
                (inputs["start_date"], inputs["end_date"]),
            )
            epex = _query(
                "SELECT delivery_date AS date, MAX(daily_baseload) AS epex_baseload "
                "FROM intl_market.gb_epex_da_hh "
                "WHERE delivery_date BETWEEN %s AND %s GROUP BY delivery_date ORDER BY delivery_date",
                (inputs["start_date"], inputs["end_date"]),
            )
            niv = _query(
                "SELECT date, AVG(niv) AS avg_niv "
                "FROM intl_market.gb_niv "
                "WHERE date BETWEEN %s AND %s GROUP BY date ORDER BY date",
                (inputs["start_date"], inputs["end_date"]),
            )
            merged = sp.merge(epex, on="date", how="outer").merge(niv, on="date", how="outer")
            merged["spread_sys_epex"] = (merged["avg_system_price"] - merged["epex_baseload"]).round(2)
            merged = merged.round(2)
            if merged.empty:
                return "No data available for the requested period."
            return merged.to_json(orient="records", date_format="iso")

    except Exception as e:
        return f"Error: {e}"
    return "Unknown tool"


# ---------------------------------------------------------------------------
# Quant tools
# ---------------------------------------------------------------------------

_QUANT_TOOLS = [
    {
        "name": "get_bess_daily_index",
        "description": "Daily GB BESS industry-average revenue (£/MW/day, £/MWh/day) by market stream.",
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string"},
                "end_date":   {"type": "string"},
                "markets": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Filter by market e.g. ['bm','cm','frequency_response']. Leave empty for all.",
                },
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "get_bess_monthly_index",
        "description": "Monthly GB BESS industry-average revenue (£/MW/month) by market stream.",
        "input_schema": {
            "type": "object",
            "properties": {
                "month_from": {"type": "string", "description": "YYYY-MM e.g. '2024-01'"},
                "month_to":   {"type": "string", "description": "YYYY-MM e.g. '2026-05'"},
                "markets": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Filter by market. Leave empty for all.",
                },
            },
            "required": ["month_from", "month_to"],
        },
    },
    {
        "name": "get_leaderboard",
        "description": "Asset-level BESS performance leaderboard (revenue and £/MW) for a date range.",
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string"},
                "end_date":   {"type": "string"},
                "market": {
                    "type": "string",
                    "description": "Filter by specific market e.g. 'dml','dcl','bm'. Leave empty for all.",
                },
                "top_n": {"type": "integer", "description": "Number of top assets to return (default 20)."},
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "get_asset_database",
        "description": "GB BESS asset registry: power, capacity, location, developer, GSP.",
        "input_schema": {
            "type": "object",
            "properties": {
                "min_power_mw": {"type": "number", "description": "Minimum rated power filter."},
                "developer":    {"type": "string", "description": "Filter by developer name (partial match)."},
            },
            "required": [],
        },
    },
    {
        "name": "estimate_irr",
        "description": (
            "Parametric unlevered IRR for a GB BESS project. "
            "Uses the Modo monthly index (last 12 available months) as the revenue proxy. "
            "Returns IRR + sensitivity table across capex and revenue scenarios."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "power_mw":         {"type": "number", "description": "BESS rated power in MW."},
                "duration_h":       {"type": "number", "description": "Storage duration in hours."},
                "capex_gbp_per_kw": {"type": "number", "description": "Total capex in £/kW."},
                "opex_pct_capex":   {"type": "number", "description": "Annual O&M as % of capex (default 2.0)."},
                "project_life_yrs": {"type": "integer", "description": "Project life in years (default 15)."},
            },
            "required": ["power_mw", "duration_h", "capex_gbp_per_kw"],
        },
    },
]


def _compute_irr(cashflows: list[float]) -> float:
    """Newton-Raphson IRR from a list of cashflows starting with capex (negative)."""
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


def _dispatch_quant(name: str, inputs: dict) -> str:
    try:
        if name == "get_bess_daily_index":
            markets = inputs.get("markets") or []
            if markets:
                ph = ",".join(["%s"] * len(markets))
                df = _query(
                    f"SELECT settlement_date, market, revenue_permw, revenue_permwh "
                    f"FROM intl_market.gb_bess_daily_index "
                    f"WHERE settlement_date BETWEEN %s AND %s AND market IN ({ph}) "
                    f"ORDER BY settlement_date, market",
                    (inputs["start_date"], inputs["end_date"], *markets),
                )
            else:
                df = _query(
                    "SELECT settlement_date, market, revenue_permw, revenue_permwh "
                    "FROM intl_market.gb_bess_daily_index "
                    "WHERE settlement_date BETWEEN %s AND %s AND duration = '*' "
                    "ORDER BY settlement_date, market",
                    (inputs["start_date"], inputs["end_date"]),
                )
            if df.empty:
                return "No daily index data for the requested period."
            return df.to_json(orient="records", date_format="iso")

        elif name == "get_bess_monthly_index":
            month_from = inputs["month_from"] + "-01"
            month_to   = inputs["month_to"]   + "-01"
            markets = inputs.get("markets") or []
            if markets:
                ph = ",".join(["%s"] * len(markets))
                df = _query(
                    f"SELECT month, market, revenue_permw, revenue_permwh "
                    f"FROM intl_market.gb_bess_monthly_index "
                    f"WHERE month BETWEEN %s AND %s AND market IN ({ph}) "
                    f"ORDER BY month, market",
                    (month_from, month_to, *markets),
                )
            else:
                df = _query(
                    "SELECT month, market, revenue_permw, revenue_permwh "
                    "FROM intl_market.gb_bess_monthly_index "
                    "WHERE month BETWEEN %s AND %s AND duration = '*' "
                    "ORDER BY month, market",
                    (month_from, month_to),
                )
            if df.empty:
                return "No monthly index data for the requested period."
            return df.to_json(orient="records", date_format="iso")

        elif name == "get_leaderboard":
            market = inputs.get("market")
            top_n  = inputs.get("top_n", 20)
            if market:
                df = _query(
                    "SELECT asset, market, SUM(revenue) AS total_revenue, "
                    "AVG(revspermw) AS avg_revspermw, AVG(rated_power) AS rated_power_mw "
                    "FROM intl_market.gb_bess_leaderboard "
                    "WHERE settlement_date BETWEEN %s AND %s AND market = %s "
                    "GROUP BY asset, market ORDER BY total_revenue DESC LIMIT %s",
                    (inputs["start_date"], inputs["end_date"], market, top_n),
                )
            else:
                df = _query(
                    "SELECT asset, SUM(revenue) AS total_revenue, "
                    "AVG(revspermw) AS avg_revspermw, AVG(rated_power) AS rated_power_mw "
                    "FROM intl_market.gb_bess_leaderboard "
                    "WHERE settlement_date BETWEEN %s AND %s "
                    "GROUP BY asset ORDER BY total_revenue DESC LIMIT %s",
                    (inputs["start_date"], inputs["end_date"], top_n),
                )
            if df.empty:
                return "No leaderboard data for the requested period."
            return df.round(2).to_json(orient="records")

        elif name == "get_asset_database":
            # Get unique assets with their rated power (history_table = 'rated_power')
            conditions = ["history_table = 'rated_power'"]
            params: list = []
            min_mw = inputs.get("min_power_mw")
            developer = inputs.get("developer")
            if min_mw is not None:
                conditions.append("CAST(value AS NUMERIC) >= %s")
                params.append(min_mw)
            if developer:
                conditions.append("developer ILIKE %s")
                params.append(f"%{developer}%")
            where = " AND ".join(conditions)
            df = _query(
                f"SELECT DISTINCT ON (asset) asset, "
                f"CAST(value AS NUMERIC) AS rated_power_mw, latitude, longitude, "
                f"gsp, developer, manufacturer, commissioning_date, dno, is_co_located "
                f"FROM intl_market.gb_bess_assets WHERE {where} "
                f"ORDER BY asset, valid_from DESC",
                params or None,
            )
            if df.empty:
                return "No assets found matching the criteria."
            return (
                f"Total: {len(df)} assets, "
                f"Total rated power: {df['rated_power_mw'].sum():.0f} MW\n"
                + df.head(50).to_json(orient="records", date_format="iso")
            )

        elif name == "estimate_irr":
            power_mw   = float(inputs["power_mw"])
            duration_h = float(inputs["duration_h"])
            capex_per_kw = float(inputs["capex_gbp_per_kw"])
            opex_pct   = float(inputs.get("opex_pct_capex", 2.0)) / 100
            life_yrs   = int(inputs.get("project_life_yrs", 15))

            # Fetch last 12 months of monthly index, all markets combined
            df = _query(
                "SELECT month, SUM(revenue_permw) AS total_revpermw "
                "FROM intl_market.gb_bess_monthly_index "
                "WHERE duration = '*' "
                "GROUP BY month ORDER BY month DESC LIMIT 12"
            )
            if df.empty:
                return "No monthly index data available to estimate IRR."

            avg_monthly_rev_per_mw = df["total_revpermw"].mean()
            annual_rev_per_mw = avg_monthly_rev_per_mw * 12

            capex_total = power_mw * capex_per_kw * 1000  # £
            opex_annual = capex_total * opex_pct

            cashflows = [-capex_total]
            for yr in range(1, life_yrs + 1):
                degrad = (1 - 0.02) ** (yr - 1)
                rev = power_mw * annual_rev_per_mw * degrad
                cashflows.append(rev - opex_annual)

            irr = _compute_irr(cashflows)
            npv_10 = sum(cf / 1.10 ** t for t, cf in enumerate(cashflows))

            # Sensitivity: ±20% capex, ±20% revenue
            sens = []
            for capex_mult in [0.8, 1.0, 1.2]:
                for rev_mult in [0.8, 1.0, 1.2]:
                    c0 = -capex_total * capex_mult
                    cfs = [c0] + [
                        power_mw * annual_rev_per_mw * rev_mult * (1 - 0.02) ** (yr - 1) - opex_annual
                        for yr in range(1, life_yrs + 1)
                    ]
                    sens.append({
                        "capex_factor": f"{capex_mult:.0%}",
                        "revenue_factor": f"{rev_mult:.0%}",
                        "irr": f"{_compute_irr(cfs) * 100:.1f}%",
                    })

            return json.dumps({
                "inputs": {
                    "power_mw": power_mw, "duration_h": duration_h,
                    "capex_gbp_per_kw": capex_per_kw,
                    "avg_monthly_rev_per_mw_gbp": round(avg_monthly_rev_per_mw, 0),
                    "annual_rev_per_mw_gbp": round(annual_rev_per_mw, 0),
                    "capex_total_gbp": round(capex_total, 0),
                    "opex_annual_gbp": round(opex_annual, 0),
                    "project_life_yrs": life_yrs,
                },
                "result": {
                    "unlevered_irr": f"{irr * 100:.1f}%",
                    "npv_at_10pct_gbp": round(npv_10, 0),
                    "revenue_source": f"Modo monthly index avg (last {len(df)} months)",
                },
                "sensitivity": sens,
            }, indent=2)

    except Exception as e:
        return f"Error: {e}"
    return "Unknown tool"


# ---------------------------------------------------------------------------
# Agent turn loop
# ---------------------------------------------------------------------------

def _run_agent_turn(messages: list, system: str, tools: list, dispatch_fn) -> tuple[str, list, list]:
    tool_events: list[dict] = []
    while True:
        resp = _client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=system,
            tools=tools,
            messages=messages,
        )
        messages = messages + [{"role": "assistant", "content": resp.content}]

        if resp.stop_reason == "end_turn":
            text = next((b.text for b in resp.content if hasattr(b, "text")), "")
            return text, messages, tool_events

        tool_results = []
        for block in resp.content:
            if block.type == "tool_use":
                result_str = dispatch_fn(block.name, block.input)
                tool_events.append({"tool": block.name, "result": result_str[:200]})
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result_str,
                })
        messages = messages + [{"role": "user", "content": tool_results}]


# ---------------------------------------------------------------------------
# Data query helpers for visualisation tabs
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def _get_system_price_range(start: str, end: str) -> pd.DataFrame:
    return _query(
        "SELECT sp.date, sp.settlement_period, sp.system_price, n.niv "
        "FROM intl_market.gb_system_price sp "
        "LEFT JOIN intl_market.gb_niv n "
        "  ON sp.date = n.date AND sp.settlement_period = n.settlement_period "
        "WHERE sp.date BETWEEN %s AND %s ORDER BY sp.date, sp.settlement_period",
        (start, end),
    )


@st.cache_data(ttl=300)
def _get_epex_range(start: str, end: str) -> pd.DataFrame:
    return _query(
        "SELECT delivery_date, settlement_period, price, daily_baseload, daily_peakload, daily_offpeak "
        "FROM intl_market.gb_epex_da_hh WHERE delivery_date BETWEEN %s AND %s "
        "ORDER BY delivery_date, settlement_period",
        (start, end),
    )


@st.cache_data(ttl=300)
def _get_dx_range(start: str, end: str) -> pd.DataFrame:
    return _query(
        "SELECT efa_date, efa, service, clearing_price, cleared_volume "
        "FROM intl_market.gb_dx_results WHERE efa_date BETWEEN %s AND %s "
        "ORDER BY efa_date, efa, service",
        (start, end),
    )


@st.cache_data(ttl=300)
def _get_daily_index_range(start: str, end: str) -> pd.DataFrame:
    return _query(
        "SELECT settlement_date, market, revenue_permw "
        "FROM intl_market.gb_bess_daily_index WHERE settlement_date BETWEEN %s AND %s AND duration = '*' "
        "ORDER BY settlement_date, market",
        (start, end),
    )


@st.cache_data(ttl=300)
def _get_monthly_index_range(start: str, end: str) -> pd.DataFrame:
    return _query(
        "SELECT month, market, revenue_permw "
        "FROM intl_market.gb_bess_monthly_index WHERE month BETWEEN %s AND %s AND duration = '*' "
        "ORDER BY month, market",
        (start, end),
    )


@st.cache_data(ttl=300)
def _get_asset_revenue_map(start: str, end: str, market: str) -> pd.DataFrame:
    """Per-asset revenue per MW for the map colour scale."""
    return _query(
        "SELECT asset, "
        "  SUM(revenue) / NULLIF(SUM(rated_power), 0) AS rev_per_mw, "
        "  SUM(revenue) AS total_revenue "
        "FROM intl_market.gb_bess_leaderboard "
        "WHERE settlement_date BETWEEN %s AND %s AND market = %s "
        "GROUP BY asset",
        (start, end, market),
    )


@st.cache_data(ttl=300)
def _get_leaderboard_range(start: str, end: str, top_n: int = 20) -> pd.DataFrame:
    return _query(
        "WITH lb AS ( "
        "  SELECT asset, "
        "    SUM(CASE WHEN market='total' THEN revenue ELSE 0 END) AS total_revenue, "
        "    SUM(CASE WHEN market='wholesale' THEN revenue ELSE 0 END) AS wholesale, "
        "    SUM(CASE WHEN market='frequency_response' THEN revenue ELSE 0 END) AS freq_response, "
        "    SUM(CASE WHEN market='bm' THEN revenue ELSE 0 END) AS bm, "
        "    SUM(CASE WHEN market='imbalance' THEN revenue ELSE 0 END) AS imbalance, "
        "    SUM(CASE WHEN market='reserve' THEN revenue ELSE 0 END) AS reserve, "
        "    AVG(CASE WHEN market='total' THEN revspermw END) AS avg_revspermw, "
        "    AVG(CASE WHEN market='total' THEN revspermwh END) AS avg_revspermwh, "
        "    AVG(CASE WHEN market='total' THEN rated_power END) AS rated_power_mw "
        "  FROM intl_market.gb_bess_leaderboard "
        "  WHERE settlement_date BETWEEN %s AND %s "
        "  GROUP BY asset ORDER BY total_revenue DESC LIMIT %s "
        "), "
        "assets AS ( "
        "  SELECT DISTINCT ON (asset) asset, developer, integrator "
        "  FROM intl_market.gb_bess_assets "
        "  WHERE history_table = 'rated_power' "
        "  ORDER BY asset, valid_from DESC "
        ") "
        "SELECT lb.asset, a.developer, a.integrator, "
        "  lb.total_revenue, lb.wholesale, lb.freq_response, lb.bm, lb.imbalance, lb.reserve, "
        "  lb.avg_revspermw, lb.avg_revspermwh, lb.rated_power_mw "
        "FROM lb LEFT JOIN assets a ON a.asset = lb.asset",
        (start, end, top_n),
    )


@st.cache_data(ttl=3600)
def _get_assets() -> pd.DataFrame:
    return _query(
        "SELECT DISTINCT ON (asset) asset, "
        "CAST(value AS NUMERIC) AS rated_power_mw, latitude, longitude, "
        "gsp, developer, integrator, manufacturer, commissioning_date, dno, "
        "is_co_located, co_located_type "
        "FROM intl_market.gb_bess_assets WHERE history_table = 'rated_power' "
        "ORDER BY asset, valid_from DESC"
    )


@st.cache_data(ttl=300)
def _table_counts() -> dict:
    tables = [
        "gb_bess_assets", "gb_bess_daily_index", "gb_bess_monthly_index",
        "gb_bess_leaderboard", "gb_system_price", "gb_niv",
        "gb_epex_da_hh", "gb_dx_results",
    ]
    out = {}
    for t in tables:
        try:
            df = _query(f"SELECT COUNT(*) AS n FROM intl_market.{t}")
            out[t] = int(df["n"].iloc[0])
        except Exception:
            out[t] = "error"
    return out


# ---------------------------------------------------------------------------
# App initialisation (runs once per process via cache_resource / explicit call)
# ---------------------------------------------------------------------------

_ensure_memory_table()
_ensure_ingestion_log_table()
_ensure_knowledge_table()
_start_scheduler()  # no-op after first call (cache_resource)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("🇬🇧 GB Market")
    st.caption("Powered by Modo Energy API")
    today = date.today()
    default_start = today - timedelta(days=30)

    st.subheader("Date Range")
    d_start = st.date_input("From", value=default_start, key="d_start")
    d_end   = st.date_input("To",   value=today,         key="d_end")
    date_start = d_start.isoformat()
    date_end   = d_end.isoformat()

    st.divider()
    st.caption("v1 · ap-southeast-1")


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_overview, tab_ancillary, tab_bess, tab_map, tab_strategist, tab_quant, tab_knowledge, tab_mgmt = st.tabs([
    "Market Overview", "Ancillary Markets", "BESS Benchmarking",
    "Asset Map", "Strategist", "Quant", "Knowledge Base", "Data Management",
])

# ---- Market Overview -------------------------------------------------------
with tab_overview:
    st.header("GB Market Overview")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("System Price (£/MWh)")
        sp_df = _get_system_price_range(date_start, date_end)
        if sp_df.empty:
            st.info("No system price data. Run a backfill in Data Management.")
        else:
            sp_df["datetime"] = pd.to_datetime(sp_df["date"].astype(str)) + pd.to_timedelta(
                (sp_df["settlement_period"] - 1) * 30, unit="min"
            )
            fig = px.line(sp_df, x="datetime", y="system_price",
                          labels={"system_price": "£/MWh", "datetime": ""},
                          color_discrete_sequence=["#1f77b4"])
            fig.add_hline(y=0, line_dash="dash", line_color="gray", line_width=1)
            fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=300)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Net Imbalance Volume (MW)")
        if sp_df.empty:
            st.info("No NIV data.")
        else:
            niv_df = sp_df.dropna(subset=["niv"])
            if not niv_df.empty:
                niv_df["colour"] = niv_df["niv"].apply(lambda x: "short" if x < 0 else "long")
                fig2 = px.bar(niv_df, x="datetime", y="niv", color="colour",
                              color_discrete_map={"short": "#d62728", "long": "#2ca02c"},
                              labels={"niv": "MW", "datetime": ""})
                fig2.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=300, showlegend=False)
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.info("No NIV data in range.")

    st.subheader("EPEX Day-Ahead Prices — Heatmap (£/MWh)")
    epex_df = _get_epex_range(date_start, date_end)
    if epex_df.empty:
        st.info("No EPEX DA data.")
    else:
        pivot = epex_df.pivot_table(index="settlement_period", columns="delivery_date",
                                    values="price", aggfunc="mean")
        fig3 = go.Figure(go.Heatmap(
            z=pivot.values,
            x=[str(c) for c in pivot.columns],
            y=pivot.index.tolist(),
            colorscale="RdYlGn_r",
            colorbar=dict(title="£/MWh"),
        ))
        fig3.update_layout(
            height=400,
            xaxis_title="Delivery date",
            yaxis_title="Settlement period",
            margin=dict(l=40, r=0, t=0, b=40),
        )
        st.plotly_chart(fig3, use_container_width=True)

        # Daily summary table
        daily_epex = epex_df.groupby("delivery_date").agg(
            Baseload=("daily_baseload", "first"),
            Peak=("daily_peakload", "first"),
            Offpeak=("daily_offpeak", "first"),
        ).round(2).reset_index()
        daily_epex.columns = ["Date", "Baseload (£/MWh)", "Peak (£/MWh)", "Offpeak (£/MWh)"]
        st.dataframe(daily_epex.tail(14).sort_values("Date", ascending=False),
                     use_container_width=True, hide_index=True)

# ---- Ancillary Markets -----------------------------------------------------
with tab_ancillary:
    st.header("Ancillary Markets — Dynamic Services (DX)")

    dx_df = _get_dx_range(date_start, date_end)
    if dx_df.empty:
        st.info("No DX results data in range.")
    else:
        services_available = sorted(dx_df["service"].unique().tolist())
        selected_services = st.multiselect(
            "Services", services_available,
            default=services_available[:6] if len(services_available) >= 6 else services_available,
        )
        if selected_services:
            filtered = dx_df[dx_df["service"].isin(selected_services)]

            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Clearing Price (£/MW/h)")
                fig = px.line(
                    filtered.groupby(["efa_date", "service"])["clearing_price"].mean().reset_index(),
                    x="efa_date", y="clearing_price", color="service",
                    labels={"clearing_price": "£/MW/h", "efa_date": ""},
                )
                fig.update_layout(height=300, margin=dict(l=0, r=0, t=0, b=0))
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.subheader("Cleared Volume (MW)")
                fig2 = px.line(
                    filtered.groupby(["efa_date", "service"])["cleared_volume"].sum().reset_index(),
                    x="efa_date", y="cleared_volume", color="service",
                    labels={"cleared_volume": "MW", "efa_date": ""},
                )
                fig2.update_layout(height=300, margin=dict(l=0, r=0, t=0, b=0))
                st.plotly_chart(fig2, use_container_width=True)

            st.subheader("Summary Statistics by Service")
            summary = filtered.groupby("service").agg(
                avg_price=("clearing_price", "mean"),
                max_price=("clearing_price", "max"),
                min_price=("clearing_price", "min"),
                avg_volume=("cleared_volume", "mean"),
            ).round(2).reset_index()
            summary.columns = ["Service", "Avg Price (£/MW/h)", "Max", "Min", "Avg Volume (MW)"]
            st.dataframe(summary, use_container_width=True, hide_index=True)

# ---- BESS Benchmarking -----------------------------------------------------
with tab_bess:
    st.header("BESS Benchmarking Index")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Daily Revenue Index (£/MW/day) by Market")
        daily_idx = _get_daily_index_range(date_start, date_end)
        if daily_idx.empty:
            st.info("No daily index data.")
        else:
            _daily_components = daily_idx[daily_idx["market"] != "total"]
            _daily_total = daily_idx[daily_idx["market"] == "total"].sort_values("settlement_date")
            fig = px.bar(
                _daily_components,
                x="settlement_date", y="revenue_permw", color="market",
                labels={"revenue_permw": "£/MW/day", "settlement_date": ""},
                barmode="relative",
            )
            if not _daily_total.empty:
                fig.add_scatter(
                    x=_daily_total["settlement_date"], y=_daily_total["revenue_permw"],
                    mode="lines", name="total",
                    line=dict(color="black", width=2, dash="dash"),
                )
            fig.update_layout(height=350, margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Market Avg (£/MW/day)")
        if not daily_idx.empty:
            mkt_avg = daily_idx[daily_idx["market"] != "total"].groupby("market")["revenue_permw"].mean().round(2).reset_index()
            mkt_avg.columns = ["Market", "Avg £/MW/day"]
            mkt_avg = mkt_avg.sort_values("Avg £/MW/day", ascending=False)
            st.dataframe(mkt_avg, use_container_width=True, hide_index=True)

    st.subheader("Monthly Revenue Index (£/MW/month)")
    month_start = (d_start.replace(day=1) - timedelta(days=365)).isoformat()
    monthly_idx = _get_monthly_index_range(month_start, date_end)
    if monthly_idx.empty:
        st.info("No monthly index data.")
    else:
        fig2 = px.bar(
            monthly_idx[monthly_idx["market"] != "total"],
            x="month", y="revenue_permw", color="market",
            labels={"revenue_permw": "£/MW/month", "month": ""},
            barmode="stack",
        )
        fig2.update_layout(height=320, margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader(f"Asset Leaderboard — Top 20 by Revenue")
    leader_df = _get_leaderboard_range(date_start, date_end)
    if leader_df.empty:
        st.info("No leaderboard data.")
    else:
        for col in ["total_revenue", "wholesale", "freq_response", "bm", "imbalance", "reserve",
                    "avg_revspermw", "avg_revspermwh", "rated_power_mw"]:
            if col in leader_df.columns:
                leader_df[col] = pd.to_numeric(leader_df[col], errors="coerce")
        for col in ["total_revenue", "wholesale", "freq_response", "bm", "imbalance", "reserve"]:
            if col in leader_df.columns:
                leader_df[col] = leader_df[col].round(0)
        leader_df["avg_revspermw"] = leader_df["avg_revspermw"].round(2)
        leader_df["avg_revspermwh"] = leader_df["avg_revspermwh"].round(2)
        leader_df["rated_power_mw"] = leader_df["rated_power_mw"].round(1)
        # Reorder: asset, owner, operator first, then financials
        cols_order = ["asset", "developer", "integrator",
                      "total_revenue", "wholesale", "freq_response", "bm", "imbalance", "reserve",
                      "avg_revspermw", "avg_revspermwh", "rated_power_mw"]
        leader_df = leader_df[[c for c in cols_order if c in leader_df.columns]]
        leader_df.columns = [
            {"asset": "Asset", "developer": "Owner/Developer", "integrator": "Operator",
             "total_revenue": "Total (£)", "wholesale": "Wholesale (£)",
             "freq_response": "Freq Response (£)", "bm": "BM (£)",
             "imbalance": "Imbalance (£)", "reserve": "Reserve (£)",
             "avg_revspermw": "£/MW/SP", "avg_revspermwh": "£/MWh/SP",
             "rated_power_mw": "Rated Power (MW)"}.get(c, c)
            for c in leader_df.columns
        ]
        st.dataframe(leader_df, use_container_width=True, hide_index=True)

# ---- Asset Map -------------------------------------------------------------
_REV_COLOR_SCALE = [[0, "#d73027"], [0.5, "#fee08b"], [1, "#1a9850"]]  # red → yellow → green
_MAP_MARKETS = ["total", "wholesale", "frequency_response", "bm", "imbalance", "reserve"]

with tab_map:
    st.header("GB BESS Asset Map")

    assets_df = _get_assets()
    if assets_df.empty:
        st.info("No asset data. Run a backfill.")
    else:
        # KPIs
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Assets", len(assets_df))
        col2.metric("Total Capacity", f"{assets_df['rated_power_mw'].sum():.0f} MW")
        col3.metric("Transmission-connected",
                    int(assets_df["transmission_connected"].sum()) if "transmission_connected" in assets_df.columns else 0)

        # Revenue colour controls
        st.markdown("---")
        mc1, mc2, mc3 = st.columns([2, 2, 2])
        map_from = mc1.date_input("From", value=date.today() - timedelta(days=90), key="map_from")
        map_to   = mc2.date_input("To",   value=date.today() - timedelta(days=1),  key="map_to")
        map_mkt  = mc3.selectbox("Colour by revenue source", _MAP_MARKETS, key="map_mkt")

        rev_df = _get_asset_revenue_map(map_from.isoformat(), map_to.isoformat(), map_mkt)
        map_df = assets_df.dropna(subset=["latitude", "longitude", "rated_power_mw"])

        if not rev_df.empty:
            map_df = map_df.merge(rev_df[["asset", "rev_per_mw"]], on="asset", how="left")
        else:
            map_df["rev_per_mw"] = float("nan")

        has_rev = "rev_per_mw" in map_df.columns and map_df["rev_per_mw"].notna().any()

        col_map, col_rank = st.columns([3, 1])

        with col_map:
            _hover_common = {
                "rated_power_mw": ":.0f", "gsp": True,
                "developer": True, "integrator": True,
                "commissioning_date": True,
                "latitude": False, "longitude": False,
            }
            if has_rev:
                fig_map = px.scatter_mapbox(
                    map_df,
                    lat="latitude", lon="longitude",
                    size="rated_power_mw",
                    color="rev_per_mw",
                    color_continuous_scale=_REV_COLOR_SCALE,
                    hover_name="asset",
                    hover_data={**_hover_common, "rev_per_mw": ":.2f"},
                    labels={"rev_per_mw": f"£/MW ({map_mkt})", "rated_power_mw": "MW",
                            "developer": "Owner", "integrator": "Operator"},
                    zoom=5, center={"lat": 53.5, "lon": -1.5},
                    mapbox_style="open-street-map", height=580,
                )
            else:
                fig_map = px.scatter_mapbox(
                    map_df,
                    lat="latitude", lon="longitude",
                    size="rated_power_mw",
                    color="developer",
                    hover_name="asset",
                    hover_data=_hover_common,
                    labels={"developer": "Owner", "integrator": "Operator",
                            "rated_power_mw": "MW"},
                    zoom=5, center={"lat": 53.5, "lon": -1.5},
                    mapbox_style="open-street-map", height=580,
                )
            fig_map.update_layout(margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig_map, use_container_width=True)

        with col_rank:
            if has_rev:
                rank_df = (
                    map_df[["asset", "rev_per_mw"]].dropna()
                    .sort_values("rev_per_mw", ascending=True)
                    .tail(30)
                )
                fig_rank = px.bar(
                    rank_df,
                    x="rev_per_mw", y="asset",
                    orientation="h",
                    color="rev_per_mw",
                    color_continuous_scale=_REV_COLOR_SCALE,
                    labels={"rev_per_mw": f"£/MW", "asset": ""},
                    title=f"Top assets — {map_mkt}",
                )
                fig_rank.update_layout(
                    coloraxis_showscale=False,
                    margin=dict(l=0, r=10, t=30, b=0),
                    height=580,
                    yaxis=dict(tickfont=dict(size=9)),
                )
                st.plotly_chart(fig_rank, use_container_width=True)
            else:
                st.info("No revenue data for the selected period/market.")

        # Table
        with st.expander("Asset details"):
            detail_cols = ["asset", "rated_power_mw", "developer", "integrator",
                           "gsp", "manufacturer", "commissioning_date", "dno"]
            show_df = assets_df[[c for c in detail_cols if c in assets_df.columns]].copy()
            show_df.rename(columns={
                "asset": "Asset", "rated_power_mw": "Power (MW)",
                "developer": "Owner/Developer", "integrator": "Operator",
                "gsp": "GSP", "manufacturer": "Manufacturer",
                "commissioning_date": "Commissioned", "dno": "DNO",
            }, inplace=True)
            st.dataframe(show_df, use_container_width=True, hide_index=True)

# ---- Knowledge Base --------------------------------------------------------
with tab_knowledge:
    st.header("GB Market Knowledge Base")
    st.caption("Articles, reports, and market commentary from Elexon, ENTSO-E, Timera, Modo, Meteologica")

    # Coverage table
    kc1, kc2 = st.columns([2, 1])
    with kc1:
        kb_counts = _knowledge_doc_counts()
        if kb_counts.empty:
            st.info("Knowledge base is empty. Run ingestion from Data Management tab.")
        else:
            st.dataframe(kb_counts, use_container_width=True, hide_index=True)

    with kc2:
        if st.button("Refresh knowledge stats"):
            _knowledge_doc_counts.clear()
            st.rerun()
        if st.button("Run knowledge ingest now", type="primary"):
            with st.spinner("Fetching from all knowledge sources…"):
                result = _run_knowledge_ingest_job(trigger="manual")
            if result["status"] == "success":
                st.success(f"Done — {result['total']} new docs in {result['duration']:.1f}s")
                if result.get("results"):
                    st.json(result["results"])
            else:
                st.error(f"Failed: {result['error']}")
            _knowledge_doc_counts.clear()
            st.rerun()

    st.divider()
    st.subheader("Search Knowledge Base")
    kb_query = st.text_input("Search query", placeholder="e.g. BESS frequency response market trends 2025")
    kb_sources = st.multiselect(
        "Filter by source (all if empty)",
        ["elexon", "entso_e", "timera", "modo", "meteologica"],
    )
    if kb_query:
        _search_knowledge.clear()
        results_df = _search_knowledge(kb_query, sources=kb_sources or None, limit=10)
        if results_df.empty:
            st.info("No results found.")
        else:
            for _, row in results_df.iterrows():
                with st.expander(
                    f"[{row['source']}] {row['title'] or 'Untitled'}  —  {row['published_date']}",
                    expanded=False,
                ):
                    if row.get("url"):
                        st.markdown(f"[View source]({row['url']})")
                    st.text(row["snippet"])


# ---- Strategist Agent ------------------------------------------------------
with tab_strategist:
    st.header("Strategist — GB Market Analysis")
    st.caption("Grounded on DB data only · Memory persists across sessions")

    if "strat_history" not in st.session_state:
        st.session_state["strat_history"] = []

    for msg in st.session_state["strat_history"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    user_input = st.chat_input("Ask about GB market fundamentals, system price, ancillary services…")
    if user_input:
        st.session_state["strat_history"].append({"role": "user", "content": user_input})
        with st.chat_message("user"):
            st.markdown(user_input)

        with st.chat_message("assistant"):
            with st.spinner("Analysing…"):
                api_messages = [
                    {"role": m["role"], "content": m["content"]}
                    for m in st.session_state["strat_history"]
                ]
                try:
                    reply, updated, tool_events = _run_agent_turn(
                        api_messages, _build_strategist_system(),
                        _STRATEGIST_TOOLS, _dispatch_strategist,
                    )
                except Exception as _agent_err:
                    reply = f"⚠️ API error: {_agent_err}. Please try again."
                    tool_events = []

            st.markdown(reply)

            if tool_events:
                with st.expander(f"Tools used ({len(tool_events)})", expanded=False):
                    for ev in tool_events:
                        st.caption(f"**{ev['tool']}** → {ev['result'][:120]}…")

        st.session_state["strat_history"].append({"role": "assistant", "content": reply})

        # Auto-extract memories
        suggestions = _extract_memories(user_input, reply)
        for sug in suggestions:
            _save_memory(sug["category"], sug["subject"], sug["content"], source="auto")
        if suggestions:
            st.toast(f"Saved {len(suggestions)} memory item(s)")

    if st.session_state["strat_history"] and st.button("Clear chat", key="clear_strat"):
        st.session_state["strat_history"] = []
        st.rerun()

# ---- Quant Agent -----------------------------------------------------------
with tab_quant:
    st.header("Quant — BESS Investment Economics")
    st.caption("Grounded on Modo index data · Parametric IRR model")

    if "quant_history" not in st.session_state:
        st.session_state["quant_history"] = []

    for msg in st.session_state["quant_history"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    user_input_q = st.chat_input("Ask about BESS revenues, leaderboard, or IRR estimates…")
    if user_input_q:
        st.session_state["quant_history"].append({"role": "user", "content": user_input_q})
        with st.chat_message("user"):
            st.markdown(user_input_q)

        with st.chat_message("assistant"):
            with st.spinner("Calculating…"):
                api_messages_q = [
                    {"role": m["role"], "content": m["content"]}
                    for m in st.session_state["quant_history"]
                ]
                try:
                    reply_q, _, tool_events_q = _run_agent_turn(
                        api_messages_q, _build_quant_system(),
                        _QUANT_TOOLS, _dispatch_quant,
                    )
                except Exception as _agent_err:
                    reply_q = f"⚠️ API error: {_agent_err}. Please try again."
                    tool_events_q = []

            st.markdown(reply_q)

            if tool_events_q:
                with st.expander(f"Tools used ({len(tool_events_q)})", expanded=False):
                    for ev in tool_events_q:
                        st.caption(f"**{ev['tool']}** → {ev['result'][:120]}…")

        st.session_state["quant_history"].append({"role": "assistant", "content": reply_q})

        suggestions_q = _extract_memories(user_input_q, reply_q)
        for sug in suggestions_q:
            _save_memory(sug["category"], sug["subject"], sug["content"], source="auto")
        if suggestions_q:
            st.toast(f"Saved {len(suggestions_q)} memory item(s)")

    if st.session_state["quant_history"] and st.button("Clear chat", key="clear_quant"):
        st.session_state["quant_history"] = []
        st.rerun()

# ---- Data Management -------------------------------------------------------
with tab_mgmt:
    st.header("Data Management")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Table Row Counts")
        if st.button("Refresh counts"):
            _table_counts.clear()
        counts = _table_counts()
        counts_df = pd.DataFrame([
            {"Table": t, "Rows": n} for t, n in counts.items()
        ])
        st.dataframe(counts_df, use_container_width=True, hide_index=True)

    with col2:
        st.subheader("Run Backfill")
        st.caption("Fetches Modo data and upserts into DB.")
        bf_start = st.date_input("Backfill from", value=date.today() - timedelta(days=30), key="bf_start")
        bf_end   = st.date_input("Backfill to",   value=date.today(), key="bf_end")
        if st.button("Run Backfill", type="primary"):
            with st.spinner("Fetching from Modo Energy API…"):
                result = _run_ingestion_job(bf_start, bf_end, trigger="manual")
            if result["status"] == "success":
                st.success(f"Backfill complete in {result['duration']:.1f}s")
            else:
                st.error(f"Backfill failed: {result['error']}")
            if result.get("log"):
                st.code(result["log"])
            _table_counts.clear()
            _get_ingestion_logs.clear()
            st.rerun()

    st.divider()
    st.subheader("Knowledge Base Ingest")
    kb_col1, kb_col2 = st.columns(2)
    with kb_col1:
        kb_only = st.multiselect(
            "Sources (all if empty)",
            ["elexon", "entso_e", "timera", "modo", "meteologica"],
            key="kb_only",
        )
    with kb_col2:
        if st.button("Run Knowledge Ingest", type="secondary"):
            with st.spinner("Fetching knowledge from all sources…"):
                kr = _run_knowledge_ingest_job(
                    only=kb_only or None, trigger="manual"
                )
            if kr["status"] == "success":
                st.success(f"{kr['total']} new docs in {kr['duration']:.1f}s")
                if kr.get("results"):
                    st.json(kr["results"])
            else:
                st.error(f"Knowledge ingest failed: {kr['error']}")
            _knowledge_doc_counts.clear()

    st.divider()
    st.subheader("Scheduled Downloads")

    # Scheduler status
    try:
        sched = _start_scheduler()
        jobs = sched.get_jobs()
        if jobs:
            job = jobs[0]
            next_run = job.next_run_time
            next_str = next_run.strftime("%Y-%m-%d %H:%M SGT") if next_run else "—"
            st.success(f"Scheduler running · Next run: **{next_str}** (daily 03:00 SGT)")
        else:
            st.warning("Scheduler has no active jobs.")
    except Exception as e:
        st.error(f"Scheduler error: {e}")

    st.caption("Recent ingestion runs (auto-refreshes every 30s)")
    if st.button("Refresh logs"):
        _get_ingestion_logs.clear()
    logs_df = _get_ingestion_logs(limit=20)
    if logs_df.empty:
        st.info("No ingestion runs recorded yet.")
    else:
        for _, row in logs_df.iterrows():
            status_icon = "✅" if row["status"] == "success" else "❌"
            with st.expander(
                f"{status_icon} {row['run_at_sgt']}  [{row['trigger']}]  "
                f"{row['date_from']} → {row['date_to']}  ({row['duration_seconds']}s)",
                expanded=(row["status"] == "error"),
            ):
                if row["status"] == "error" and row["error_msg"]:
                    st.error(row["error_msg"])
                if row["rows_ingested"]:
                    st.json(row["rows_ingested"])

    st.divider()
    st.subheader("Agent Memory")
    mems = _load_memories(_APP_KEY)
    if mems.empty:
        st.info("No memories saved yet.")
    else:
        for _, row in mems.iterrows():
            c1, c2 = st.columns([10, 1])
            with c1:
                st.markdown(f"**[{row['category']}]** {row['subject']}: {row['content']}")
                st.caption(f"{row['source']} · {row['created_at']}")
            with c2:
                if st.button("🗑", key=f"del_{row['id']}"):
                    _delete_memory(row["id"])
                    st.rerun()

    st.divider()
    st.subheader("Add Memory Manually")
    with st.form("add_mem"):
        cat = st.selectbox("Category",
                           ["market_view", "methodology", "asset_note", "investment_thesis", "red_flag"])
        subj = st.text_input("Subject (≤8 words)")
        cont = st.text_area("Content (one sentence)")
        if st.form_submit_button("Save"):
            _save_memory(cat, subj, cont, source="manual")
            st.success("Saved")
            st.rerun()
