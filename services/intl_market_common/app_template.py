"""Shared market intelligence app template — runs for AU, ERCOT, PJM, CAISO.

GB keeps its own apps/gb-market/app.py (unchanged).
Each new market app.py calls: run_market_app(MARKET_CONFIG, _app_file=__file__)

Design rules:
- All @st.cache_data functions are at MODULE LEVEL with `prefix: str` param for cache isolation.
- @st.cache_resource functions are at module level.
- st.set_page_config() must be called BEFORE run_market_app() in each market's app.py.
"""
import importlib
import json
import logging
import os
import pathlib
import sys
import uuid
from datetime import date, timedelta

import anthropic
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st

from services.intl_market_common.market_config import MarketConfig

logger = logging.getLogger(__name__)

_ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
_client = anthropic.Anthropic(api_key=_ANTHROPIC_KEY)
_REV_COLOR_SCALE = [[0, "#d73027"], [0.5, "#fee08b"], [1, "#1a9850"]]

# ---------------------------------------------------------------------------
# DB helpers (module-level, shared across all markets in same process)
# ---------------------------------------------------------------------------

@st.cache_resource(ttl=3600)
def _get_conn():
    url = (
        os.environ.get("PGURL")
        or os.environ.get("DATABASE_URL")
        or "postgresql://postgres:root@127.0.0.1:5433/marketdata"
    )
    conn = psycopg2.connect(url, connect_timeout=10)
    conn.autocommit = True
    return conn


def _conn():
    conn = _get_conn()
    if conn.closed:
        _get_conn.clear()
        conn = _get_conn()
    return conn


def _query(sql: str, params=None) -> pd.DataFrame:
    return pd.read_sql(sql, _conn(), params=params)


# ---------------------------------------------------------------------------
# Daily report settings helpers
# ---------------------------------------------------------------------------

def _raw_db_conn():
    """Open a fresh psycopg2 connection (safe to call from background threads)."""
    url = (
        os.environ.get("PGURL")
        or os.environ.get("DATABASE_URL")
        or "postgresql://postgres:root@127.0.0.1:5433/marketdata"
    )
    conn = psycopg2.connect(url, connect_timeout=5)
    conn.autocommit = True
    return conn


def _is_report_enabled(market_code: str) -> bool:
    """Return True if daily report sending is enabled for this market (default: True)."""
    try:
        conn = _raw_db_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM intl_market.platform_settings "
                "WHERE market_code = %s AND key = 'daily_report_enabled'",
                (market_code,),
            )
            row = cur.fetchone()
        conn.close()
        if row is None:
            return True
        return row[0].lower() in ("true", "1", "yes")
    except Exception:
        return True  # on DB error, default to enabled


def _set_report_enabled(market_code: str, enabled: bool) -> None:
    """Persist daily report enabled state for this market to DB."""
    conn = _raw_db_conn()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS intl_market.platform_settings (
                market_code TEXT NOT NULL,
                key         TEXT NOT NULL,
                value       TEXT,
                updated_at  TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (market_code, key)
            )
        """)
        cur.execute(
            """
            INSERT INTO intl_market.platform_settings (market_code, key, value, updated_at)
            VALUES (%s, 'daily_report_enabled', %s, NOW())
            ON CONFLICT (market_code, key) DO UPDATE
              SET value = EXCLUDED.value, updated_at = NOW()
            """,
            (market_code, "true" if enabled else "false"),
        )
    conn.close()


def _run_connector_to_db(connector, conn, prefix: str) -> int:
    """Insert docs yielded by connector.fetch() into {prefix}knowledge_docs. Returns count."""
    n = 0
    for doc in connector.fetch():
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO intl_market.{prefix}knowledge_docs "
                "(source, doc_type, title, url, published_date, content) "
                "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (url) DO NOTHING",
                (
                    getattr(connector, "source", "modo_ai"),
                    doc["doc_type"], doc.get("title", ""),
                    doc.get("url"), doc.get("published_date"), doc["content"],
                ),
            )
            if cur.rowcount > 0:
                n += 1
    conn.commit()
    return n


# ---------------------------------------------------------------------------
# Module-level cached data functions — all accept `prefix: str`
# ---------------------------------------------------------------------------

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


@st.cache_data(ttl=60)
def _search_knowledge(prefix: str, query: str, sources=None, limit: int = 8) -> pd.DataFrame:
    try:
        src_clause = "AND source = ANY(%s)" if sources else ""
        fts_params = [query, query]
        if sources:
            fts_params.append(sources)
        fts_params.append(limit)
        df = _query(
            f"SELECT source, doc_type, title, url, published_date, "
            f"left(content, 1500) AS snippet, "
            f"ts_rank(search_vector, plainto_tsquery('english', %s)) AS rank "
            f"FROM intl_market.{prefix}knowledge_docs "
            f"WHERE search_vector @@ to_tsquery('english', "
            f"  regexp_replace(plainto_tsquery('english', %s)::text, ' & ', ' | ', 'g')"
            f") {src_clause} "
            f"ORDER BY rank DESC LIMIT %s",
            fts_params,
        )
        if not df.empty:
            return df
        like_q = "%" + query.strip().replace("%", "").replace("_", "") + "%"
        ilike_params = [like_q]
        if sources:
            ilike_params.append(sources)
        ilike_params.append(limit)
        return _query(
            f"SELECT source, doc_type, title, url, published_date, "
            f"left(content, 1500) AS snippet, 0.0::float AS rank "
            f"FROM intl_market.{prefix}knowledge_docs "
            f"WHERE title ILIKE %s {src_clause} "
            f"ORDER BY published_date DESC NULLS LAST LIMIT %s",
            ilike_params,
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def _knowledge_doc_counts(prefix: str) -> pd.DataFrame:
    try:
        return _query(
            f"SELECT source, doc_type, COUNT(*) AS docs, MAX(fetched_at) AS last_fetch "
            f"FROM intl_market.{prefix}knowledge_docs "
            f"GROUP BY source, doc_type ORDER BY source, doc_type"
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def _kb_stats_metrics(prefix: str) -> dict:
    """Return KB + insight statistics for the metrics dashboard."""
    from datetime import timezone as _tz
    _now = datetime.now(_tz.utc)

    def _ago(ts):
        if ts is None:
            return "never"
        _ts = ts if (hasattr(ts, "tzinfo") and ts.tzinfo) else ts.replace(tzinfo=_tz.utc)
        h = int((_now - _ts).total_seconds() / 3600)
        return f"{h}h ago" if h < 48 else f"{h // 24}d ago"

    docs_row = _query(
        f"SELECT COUNT(*)::int AS n, MAX(fetched_at) AS last_fetch "
        f"FROM intl_market.{prefix}knowledge_docs"
    )
    insights_row = _query(
        f"SELECT COUNT(*)::int AS n FROM intl_market.{prefix}expert_insights WHERE active = TRUE"
    )
    digest_row = _query(
        f"""SELECT COUNT(DISTINCT d.id)::int AS digested,
               (SELECT COUNT(*)::int FROM intl_market.{prefix}knowledge_docs) AS total
            FROM intl_market.{prefix}knowledge_docs d
            WHERE EXISTS (
                SELECT 1 FROM intl_market.{prefix}expert_insights i
                WHERE i.source_doc_url = d.url
            )"""
    )
    sched_row = _query(
        f"""SELECT COUNT(*) FILTER (WHERE status = 'success')::int AS successes,
               COUNT(*)::int AS total, MAX(run_at) AS last_run
            FROM intl_market.{prefix}ingestion_log
            WHERE run_at > NOW() - INTERVAL '30 days'"""
    )

    n_docs      = int(docs_row.iloc[0]["n"])       if not docs_row.empty      else 0
    n_insights  = int(insights_row.iloc[0]["n"])   if not insights_row.empty  else 0
    digested    = int(digest_row.iloc[0]["digested"]) if not digest_row.empty else 0
    total_d     = int(digest_row.iloc[0]["total"])    if not digest_row.empty else 0
    successes   = int(sched_row.iloc[0]["successes"]) if not sched_row.empty  else 0
    total_runs  = int(sched_row.iloc[0]["total"])     if not sched_row.empty  else 0
    last_run    = sched_row.iloc[0]["last_run"]        if not sched_row.empty  else None

    return {
        "n_docs":          n_docs,
        "n_insights":      n_insights,
        "digested":        digested,
        "total_digestible": total_d,
        "digest_pct":      digested / total_d   if total_d    > 0 else 0.0,
        "successes":       successes,
        "total_runs":      total_runs,
        "sched_pct":       successes / total_runs if total_runs > 0 else 0.0,
        "last_ingest_ago": _ago(last_run),
    }


@st.cache_data(ttl=300)
def _get_daily_index(prefix: str, start: str, end: str) -> pd.DataFrame:
    try:
        return _query(
            f"SELECT settlement_date, market, revenue_permw, revenue_permwh "
            f"FROM intl_market.{prefix}bess_daily_index "
            f"WHERE settlement_date BETWEEN %s AND %s AND duration = '*' "
            f"ORDER BY settlement_date, market",
            (start, end),
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def _get_monthly_index(prefix: str, start: str, end: str) -> pd.DataFrame:
    try:
        return _query(
            f"SELECT year_month AS month, market, revenue_permw "
            f"FROM intl_market.{prefix}bess_monthly_index "
            f"WHERE year_month BETWEEN %s AND %s AND duration = '*' "
            f"ORDER BY year_month, market",
            (start[:7], end[:7]),
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def _get_leaderboard(prefix: str, start: str, end: str, top_n: int = 20) -> pd.DataFrame:
    try:
        return _query(
            f"WITH lb AS ( "
            f"  SELECT asset, SUM(revenue) AS total_revenue, "
            f"    AVG(rated_power) AS rated_power_mw, "
            f"    AVG(energy_capacity) AS energy_capacity_mwh "
            f"  FROM intl_market.{prefix}bess_leaderboard "
            f"  WHERE settlement_date BETWEEN %s AND %s "
            f"  GROUP BY asset ORDER BY total_revenue DESC LIMIT %s "
            f"), "
            f"op AS (SELECT DISTINCT ON (asset) asset, value AS operator "
            f"       FROM intl_market.{prefix}bess_assets WHERE history_table='operator' "
            f"       ORDER BY asset, date_from DESC NULLS LAST), "
            f"ow AS (SELECT DISTINCT ON (asset) asset, value AS owner "
            f"       FROM intl_market.{prefix}bess_assets WHERE history_table='owner' "
            f"       ORDER BY asset, date_from DESC NULLS LAST) "
            f"SELECT lb.asset, ow.owner, op.operator, lb.total_revenue, "
            f"  lb.rated_power_mw, lb.energy_capacity_mwh "
            f"FROM lb LEFT JOIN op ON op.asset=lb.asset LEFT JOIN ow ON ow.asset=lb.asset",
            (start, end, top_n),
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def _get_assets(prefix: str) -> pd.DataFrame:
    try:
        return _query(
            f"WITH rp AS ( "
            f"  SELECT DISTINCT ON (asset) asset, "
            f"  CAST(value AS NUMERIC) AS rated_power_mw "
            f"  FROM intl_market.{prefix}bess_assets WHERE history_table = 'rated_power' "
            f"  ORDER BY asset, date_from DESC NULLS LAST "
            f"), "
            f"ec AS (SELECT DISTINCT ON (asset) asset, CAST(value AS NUMERIC) AS energy_capacity_mwh "
            f"       FROM intl_market.{prefix}bess_assets WHERE history_table = 'energy_capacity' "
            f"       ORDER BY asset, date_from DESC NULLS LAST), "
            f"op AS (SELECT DISTINCT ON (asset) asset, value AS operator "
            f"       FROM intl_market.{prefix}bess_assets WHERE history_table='operator' "
            f"       ORDER BY asset, date_from DESC NULLS LAST), "
            f"ow AS (SELECT DISTINCT ON (asset) asset, value AS owner "
            f"       FROM intl_market.{prefix}bess_assets WHERE history_table='owner' "
            f"       ORDER BY asset, date_from DESC NULLS LAST) "
            f"SELECT rp.asset, ow.owner, op.operator, rp.rated_power_mw, ec.energy_capacity_mwh "
            f"FROM rp "
            f"LEFT JOIN ec ON ec.asset=rp.asset "
            f"LEFT JOIN op ON op.asset=rp.asset "
            f"LEFT JOIN ow ON ow.asset=rp.asset",
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def _get_asset_revenue_map(prefix: str, start: str, end: str, market: str) -> pd.DataFrame:
    try:
        return _query(
            f"SELECT asset, "
            f"  SUM(revenue) / NULLIF(SUM(rated_power), 0) AS rev_per_mw "
            f"FROM intl_market.{prefix}bess_leaderboard "
            f"WHERE settlement_date BETWEEN %s AND %s AND market = %s "
            f"GROUP BY asset",
            (start, end, market),
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def _get_spot_price(prefix: str, start: str, end: str) -> pd.DataFrame:
    try:
        return _query(
            f"SELECT settlement_date, region, AVG(spot_price) AS avg_spot_price "
            f"FROM intl_market.{prefix}spot_price "
            f"WHERE settlement_date BETWEEN %s AND %s "
            f"GROUP BY settlement_date, region ORDER BY settlement_date",
            (start, end),
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def _get_ancillary(prefix: str, start: str, end: str) -> pd.DataFrame:
    try:
        return _query(
            f"SELECT settlement_date, service, clearing_price, volume_mw "
            f"FROM intl_market.{prefix}ancillary_results "
            f"WHERE settlement_date BETWEEN %s AND %s "
            f"ORDER BY settlement_date, service",
            (start, end),
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def _table_counts(prefix: str) -> pd.DataFrame:
    _COVERAGE = [
        (f"{prefix}spot_price",      "settlement_date"),
        (f"{prefix}ancillary_results","settlement_date"),
        (f"{prefix}bess_daily_index", "settlement_date"),
        (f"{prefix}bess_monthly_index","year_month"),
        (f"{prefix}bess_leaderboard", "settlement_date"),
        (f"{prefix}bess_assets",      None),
        (f"{prefix}knowledge_docs",   None),
    ]
    rows = []
    for table, date_col in _COVERAGE:
        try:
            if date_col:
                df = _query(
                    f"SELECT COUNT(*) AS n, MIN({date_col})::text AS min_d, "
                    f"MAX({date_col})::text AS max_d FROM intl_market.{table}"
                )
                row = df.iloc[0]
                rows.append({"Table": table, "Rows": int(row["n"]),
                             "From": row["min_d"], "To": row["max_d"]})
            else:
                df = _query(f"SELECT COUNT(*) AS n FROM intl_market.{table}")
                rows.append({"Table": table, "Rows": int(df["n"].iloc[0]),
                             "From": None, "To": None})
        except Exception:
            rows.append({"Table": table, "Rows": "error", "From": None, "To": None})
    return pd.DataFrame(rows)


@st.cache_data(ttl=30)
def _get_ingestion_logs(prefix: str, limit: int = 20) -> pd.DataFrame:
    try:
        return _query(
            f"SELECT id, run_at AT TIME ZONE 'Asia/Singapore' AS run_at_sgt, "
            f"trigger, date_from, date_to, status, rows_ingested, error_msg, duration_seconds "
            f"FROM intl_market.{prefix}ingestion_log "
            f"ORDER BY run_at DESC LIMIT %s",
            (limit,),
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=30)
def _list_recent_sessions(prefix: str, limit: int = 3) -> pd.DataFrame:
    try:
        return _query(
            f"SELECT session_id, jsonb_array_length(messages) AS msg_count, updated_at "
            f"FROM intl_market.{prefix}analyst_sessions "
            f"WHERE jsonb_array_length(messages) > 0 "
            f"ORDER BY updated_at DESC LIMIT %s",
            (limit,),
        )
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Scheduler (module-level, one per process per market)
# ---------------------------------------------------------------------------

_scheduler_instances: dict = {}
_scheduler_lock = __import__("threading").Lock()


def _start_scheduler(code: str, name: str, prefix: str, api_key: str, app_file: str):
    with _scheduler_lock:
        if code in _scheduler_instances:
            return _scheduler_instances[code]
    from apscheduler.schedulers.background import BackgroundScheduler

    def _daily_market_job():
        yesterday = date.today() - timedelta(days=1)
        try:
            mod = importlib.import_module(f"services.modo_energy.{code}_ingestion")
            mod.run_ingestion(yesterday, yesterday)
            _table_counts.clear()
            _get_ingestion_logs.clear()
        except Exception as exc:
            logger.error("Daily market job failed for %s: %s", code, exc)

    def _daily_knowledge_job():
        try:
            mod = importlib.import_module(f"services.{code}_knowledge.ingest")
            mod.run_knowledge_ingest(verbose=False)
        except Exception as exc:
            logger.error("Daily knowledge job failed for %s: %s", code, exc)

    def _modo_ai_job():
        try:
            from services.intl_market_common.modo_ai_base import ModoAIConnector
            cfg_mod = importlib.import_module(f"services.{code}_knowledge.config")
            cfg = cfg_mod.MARKET_CONFIG
            connector = ModoAIConnector(cfg)
            conn = _conn()
            n = _run_connector_to_db(connector, conn, prefix)
            logger.info("Modo AI distillation for %s: %d new docs", code, n)
        except Exception as exc:
            logger.error("Modo AI job failed for %s: %s", code, exc)

    def _kb_digest_job():
        try:
            from services.intl_market_common.expert_memory_base import digest_kb_docs
            n = digest_kb_docs(api_key, prefix, name, limit=100)
            logger.info("KB digest for %s: %d new insights", code, n)
        except Exception as exc:
            logger.error("KB digest failed for %s: %s", code, exc)

    def _daily_report_job():
        if not _is_report_enabled(code):
            logger.info("Daily report disabled for %s — skipping", code)
            return
        try:
            _rpt_path = pathlib.Path(app_file).with_name("daily_report.py")
            if not _rpt_path.exists():
                return
            import importlib.util
            spec = importlib.util.spec_from_file_location("daily_report", _rpt_path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            rpt_date = mod._get_latest_data_date(mod._get_conn())
            pdf_bytes, ai_commentary = mod.generate_report_pdf(rpt_date)
            try:
                mod.send_daily_report_email(pdf_bytes, rpt_date, ai_commentary=ai_commentary)
                logger.info("Daily report emailed for %s (%s)", code, rpt_date)
            except Exception as e:
                logger.error("Daily report email failed for %s: %s", code, e)
            wecom_url = os.environ.get("WECOM_WEBHOOK_URL", "")
            if wecom_url:
                try:
                    mod.send_daily_report_wecom(pdf_bytes, rpt_date,
                                                webhook_url=wecom_url,
                                                ai_commentary=ai_commentary)
                    logger.info("Daily report sent to WeCom for %s (%s)", code, rpt_date)
                except Exception as e:
                    logger.error("Daily report WeCom failed for %s: %s", code, e)
        except Exception as exc:
            logger.error("Daily report job failed for %s: %s", code, exc)

    scheduler = BackgroundScheduler(timezone="Asia/Singapore")
    scheduler.add_job(_daily_market_job,   "cron", hour=3,  minute=0,  id=f"{code}_daily_market",   misfire_grace_time=3600)
    scheduler.add_job(_daily_knowledge_job,"cron", hour=3,  minute=30, id=f"{code}_daily_knowledge", misfire_grace_time=3600)
    scheduler.add_job(_kb_digest_job,      "cron", hour=3,  minute=45, id=f"{code}_kb_digest",       misfire_grace_time=3600)
    scheduler.add_job(_modo_ai_job,        "cron", hour=4,  minute=0,  id=f"{code}_modo_ai",         misfire_grace_time=3600)
    scheduler.add_job(_daily_report_job,   "cron", hour=6,  minute=0,  id=f"{code}_daily_report",    misfire_grace_time=3600)
    scheduler.start()
    with _scheduler_lock:
        _scheduler_instances[code] = scheduler
    print(f"[SCHEDULER] {code} scheduler started", flush=True)
    return scheduler


# ---------------------------------------------------------------------------
# Main app function
# ---------------------------------------------------------------------------

def run_market_app(cfg: MarketConfig, _app_file: str | None = None) -> None:
    prefix   = cfg.table_prefix
    app_key  = cfg.app_key
    currency = cfg.currency_sym
    _app_dir = pathlib.Path(_app_file).parent if _app_file else pathlib.Path(__file__).parent

    # ── Ensure core tables exist ──────────────────────────────────────────────
    def _ensure_tables():
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
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS intl_market.{prefix}analyst_sessions (
                session_id TEXT PRIMARY KEY,
                messages   JSONB NOT NULL DEFAULT '[]',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS intl_market.{prefix}knowledge_docs (
                id              SERIAL PRIMARY KEY,
                source          TEXT NOT NULL,
                doc_type        TEXT NOT NULL,
                title           TEXT,
                url             TEXT UNIQUE,
                published_date  DATE,
                content         TEXT NOT NULL,
                fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                search_vector   TSVECTOR GENERATED ALWAYS AS (
                    to_tsvector('english', coalesce(title,'') || ' ' || left(content,100000))
                ) STORED
            )
        """)
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS {prefix}knowledge_docs_fts "
            f"ON intl_market.{prefix}knowledge_docs USING GIN(search_vector)"
        )
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS intl_market.{prefix}expert_insights (
                id SERIAL PRIMARY KEY,
                insight_text TEXT,
                insight_type TEXT,
                confidence TEXT,
                source_session TEXT,
                active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        _conn().commit()

    try:
        _ensure_tables()
    except Exception as _e:
        logger.warning("_ensure_tables failed: %s", _e)

    # ── Ingestion helpers ─────────────────────────────────────────────────────
    def _run_ingestion_job(date_from, date_to, trigger: str = "manual") -> dict:
        import io, time
        from contextlib import redirect_stdout
        t0 = time.time()
        buf = io.StringIO()
        try:
            mod = importlib.import_module(f"services.modo_energy.{cfg.code}_ingestion")
            with redirect_stdout(buf):
                mod.run_ingestion(date_from, date_to)
            duration = time.time() - t0
            _log_ingestion(trigger, date_from, date_to, "success", None, None, duration)
            return {"status": "success", "log": buf.getvalue(), "duration": duration}
        except Exception as exc:
            duration = time.time() - t0
            _log_ingestion(trigger, date_from, date_to, "error", None, str(exc), duration)
            return {"status": "error", "error": str(exc), "log": buf.getvalue(), "duration": duration}

    def _log_ingestion(trigger, date_from, date_to, status, rows, error_msg, duration):
        try:
            cur = _conn().cursor()
            cur.execute(
                f"INSERT INTO intl_market.{prefix}ingestion_log "
                f"(trigger, date_from, date_to, status, rows_ingested, error_msg, duration_seconds) "
                f"VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (trigger, date_from, date_to, status,
                 json.dumps(rows) if rows else None, error_msg, round(duration, 1)),
            )
            _conn().commit()
        except Exception:
            pass

    def _run_knowledge_ingest_job(only=None, trigger: str = "manual") -> dict:
        import time
        t0 = time.time()
        try:
            mod = importlib.import_module(f"services.{cfg.code}_knowledge.ingest")
            results = mod.run_knowledge_ingest(only=only, verbose=False)
            duration = time.time() - t0
            return {"status": "success", "results": results, "total": sum(results.values()), "duration": duration}
        except Exception as exc:
            return {"status": "error", "error": str(exc), "duration": time.time() - t0}

    # ── Session persistence ───────────────────────────────────────────────────
    def _save_session(session_id: str, messages: list):
        try:
            cur = _conn().cursor()
            cur.execute(
                f"INSERT INTO intl_market.{prefix}analyst_sessions (session_id, messages, updated_at) "
                f"VALUES (%s, %s::jsonb, NOW()) "
                f"ON CONFLICT (session_id) DO UPDATE SET messages=EXCLUDED.messages, updated_at=NOW()",
                (session_id, json.dumps(messages)),
            )
            _conn().commit()
        except Exception as exc:
            logger.debug("_save_session failed: %s", exc)

    def _load_session(session_id: str) -> list:
        try:
            cur = _conn().cursor()
            cur.execute(
                f"SELECT messages FROM intl_market.{prefix}analyst_sessions WHERE session_id=%s",
                (session_id,),
            )
            row = cur.fetchone()
            return row[0] if row else []
        except Exception:
            return []

    # ── Memory helpers ────────────────────────────────────────────────────────
    def _save_memory(category, subject, content, source="manual"):
        cur = _conn().cursor()
        cur.execute(
            "INSERT INTO marketdata.agent_memory (app, category, subject, content, source) "
            "VALUES (%s, %s, %s, %s, %s)",
            (app_key, category, subject, content, source),
        )
        _conn().commit()
        _load_memories.clear()

    def _delete_memory(mem_id: int):
        cur = _conn().cursor()
        cur.execute("UPDATE marketdata.agent_memory SET active=FALSE WHERE id=%s", (mem_id,))
        _conn().commit()

    # ── Expert memory helpers ─────────────────────────────────────────────────
    def _get_insight_count() -> int:
        try:
            df = _query(
                f"SELECT COUNT(*) AS n FROM intl_market.{prefix}expert_insights WHERE active=TRUE"
            )
            return int(df.iloc[0]["n"]) if not df.empty else 0
        except Exception:
            return 0

    def _generate_interview_questions() -> list[dict]:
        try:
            summary = _query(
                f"SELECT insight_type, confidence, COUNT(*) AS n "
                f"FROM intl_market.{prefix}expert_insights WHERE active=TRUE "
                f"GROUP BY insight_type, confidence ORDER BY n DESC"
            )
            sample = _query(
                f"SELECT insight_text, insight_type "
                f"FROM intl_market.{prefix}expert_insights WHERE active=TRUE "
                f"ORDER BY id DESC LIMIT 15"
            )
            ctx = ["Current insight pool:"]
            if not summary.empty:
                for _, r in summary.iterrows():
                    ctx.append(f"  {r['insight_type']} ({r['confidence']}): {int(r['n'])} insights")
            else:
                ctx.append("  (empty)")
            ctx.append("\nSample insights (do NOT duplicate):")
            if not sample.empty:
                for _, r in sample.iterrows():
                    ctx.append(f"  [{r['insight_type']}] {str(r['insight_text'])[:120]}")
            system = (
                f"You are the {cfg.name} BESS market strategist auditing your knowledge base. "
                f"Identify 5 knowledge gaps and generate one precise expert interview question per gap — "
                f"something only a practitioner with hands-on {cfg.name} BESS experience can answer. "
                f"Prioritise: operational strategies, counterintuitive market patterns, regulatory changes, "
                f"revenue stack differentiation, grid/locational patterns. "
                f"Respond ONLY with valid JSON: "
                f'{{"questions": [{{"question": "...", "topic": "market_structure|regulation|operations|bess_economics|grid_services", "why_asking": "one sentence"}}]}}'
            )
            resp = _client.messages.create(
                model="claude-haiku-4-5-20251001", max_tokens=900,
                system=system,
                messages=[{"role": "user", "content": "\n".join(ctx)}],
            )
            raw = resp.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.split("```", 2)[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            return json.loads(raw).get("questions", [])[:5]
        except Exception as exc:
            logger.warning("Gap analysis failed: %s", exc)
            return []

    def _store_interview_answer(question: str, answer: str, topic: str):
        insight_text = f"[Expert interview] Q: {question[:150]} | A: {answer}"
        cur = _conn().cursor()
        cur.execute(
            f"INSERT INTO intl_market.{prefix}expert_insights "
            f"(insight_text, insight_type, confidence, source_session) VALUES (%s, %s, 'high', %s)",
            (insight_text[:1000], topic, date.today().isoformat()),
        )
        _conn().commit()

    # ── Document upload/ingest helpers ────────────────────────────────────────
    def _ingest_url(url: str) -> dict:
        import requests
        from bs4 import BeautifulSoup
        try:
            resp = requests.get(url, timeout=30, headers={"User-Agent": "BESSPlatformBot/1.0"})
        except Exception as exc:
            return {"status": "error", "msg": f"Fetch failed: {exc}"}
        if resp.status_code != 200:
            return {"status": "error", "msg": f"HTTP {resp.status_code}"}
        soup = BeautifulSoup(resp.text, "html.parser")
        title_el = soup.find("h1") or soup.find("title")
        title = title_el.get_text(" ", strip=True) if title_el else url
        for tag in soup.find_all(["script", "style", "nav", "header", "footer"]):
            tag.decompose()
        content = "\n\n".join(
            el.get_text(" ", strip=True) for el in soup.find_all(["p", "h1", "h2", "h3", "li"])
            if el.get_text(" ", strip=True)
        ) or soup.get_text(" ", strip=True)
        if not content.strip():
            return {"status": "error", "msg": "No text extracted."}
        try:
            cur = _conn().cursor()
            cur.execute(
                f"INSERT INTO intl_market.{prefix}knowledge_docs "
                f"(source, doc_type, title, url, published_date, content) VALUES (%s,%s,%s,%s,%s,%s) "
                f"ON CONFLICT (url) DO UPDATE SET content=EXCLUDED.content, title=EXCLUDED.title, fetched_at=NOW()",
                ("upload", "article", title, url, date.today(), content),
            )
            return {"status": "success", "msg": f"Ingested '{title}' ({len(content):,} chars)"}
        except Exception as exc:
            return {"status": "error", "msg": f"DB insert failed: {exc}"}

    def _ingest_uploaded_file(filename: str, data: bytes) -> dict:
        import io
        ext = filename.rsplit(".", 1)[-1].lower()
        try:
            if ext == "txt":
                content = data.decode("utf-8", errors="replace")
            elif ext == "pdf":
                from pypdf import PdfReader
                reader = PdfReader(io.BytesIO(data))
                content = "\n\n".join(p.extract_text() for p in reader.pages if p.extract_text())
            elif ext in ("xlsx", "xls"):
                xl = pd.ExcelFile(io.BytesIO(data))
                content = "\n\n".join(
                    f"Sheet: {s}\n{xl.parse(s).to_string(index=False)}" for s in xl.sheet_names
                )
            elif ext in ("docx", "doc"):
                from docx import Document
                doc = Document(io.BytesIO(data))
                content = "\n".join(p.text for p in doc.paragraphs if p.text.strip())
            else:
                return {"status": "error", "msg": f"Unsupported type: .{ext}"}
        except Exception as exc:
            return {"status": "error", "msg": f"Text extraction failed: {exc}"}
        if not content.strip():
            return {"status": "error", "msg": "No text extracted."}
        url_key = f"upload://{filename}"
        doc_type = {"pdf": "pdf", "txt": "text", "xlsx": "excel", "xls": "excel",
                    "docx": "word", "doc": "word"}.get(ext, "document")
        try:
            cur = _conn().cursor()
            cur.execute(
                f"INSERT INTO intl_market.{prefix}knowledge_docs "
                f"(source, doc_type, title, url, published_date, content) VALUES (%s,%s,%s,%s,%s,%s) "
                f"ON CONFLICT (url) DO UPDATE SET content=EXCLUDED.content, fetched_at=NOW()",
                ("upload", doc_type, filename, url_key, date.today(), content),
            )
            return {"status": "success", "msg": f"Ingested '{filename}' ({len(content):,} chars)"}
        except Exception as exc:
            return {"status": "error", "msg": f"DB insert failed: {exc}"}

    # ── Strategist system prompt ──────────────────────────────────────────────
    def _build_strategist_system(query: str = "") -> str:
        base = (
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
        if query:
            try:
                from services.intl_market_common.expert_memory_base import get_insights, inject_memory
                insights = get_insights(query, prefix, limit=5)
                mem_block = inject_memory(insights, cfg.name)
                if mem_block:
                    base += f"\n\n{mem_block}"
            except Exception:
                pass
        mems = _load_memories(app_key)
        if not mems.empty:
            mem_lines = "\n".join(f"- [{r.category}] {r.subject}: {r.content}" for r in mems.itertuples())
            base += f"\n\n## Analyst notes from prior sessions:\n{mem_lines}"
        return base

    def _build_quant_system() -> str:
        base = (
            f"You are the {cfg.name} Quant, an expert in BESS investment economics.\n\n"
            f"GROUNDING RULE: Answer only from data returned by your tools.\n\n"
            f"DOMAIN CONTEXT:\n"
            f"- Market: {cfg.name} | Currency: {cfg.currency_sym}\n"
            f"- BESS revenues expressed in {cfg.currency_sym}/MW/day or {cfg.currency_sym}/MW/month\n"
            f"- IRR: unlevered, 15-year life, O&M 2% capex/yr, degradation 2%/yr\n\n"
            f"ANALYTICAL FRAMEWORK:\n"
            f"- Revenue trends → call get_bess_daily_index or get_bess_monthly_index\n"
            f"- Asset comparison → call get_leaderboard\n"
            f"- Market landscape → call get_asset_database\n"
            f"- Investment return → call estimate_irr\n"
        )
        mems = _load_memories(app_key)
        if not mems.empty:
            mem_lines = "\n".join(f"- [{r.category}] {r.subject}: {r.content}" for r in mems.itertuples())
            base += f"\n\n## Analyst memory:\n{mem_lines}"
        return base

    # ── Agent tools ───────────────────────────────────────────────────────────
    _STRATEGIST_TOOLS = [
        {"name": "get_spot_price",
         "description": f"Daily average spot price ({currency}/MWh) for {cfg.name} from {cfg.wholesale_label}.",
         "input_schema": {"type": "object",
                          "properties": {"start_date": {"type": "string"}, "end_date": {"type": "string"}},
                          "required": ["start_date", "end_date"]}},
        {"name": "get_ancillary_results",
         "description": f"{cfg.ancillary_label} ancillary service clearing prices and volumes.",
         "input_schema": {"type": "object",
                          "properties": {"start_date": {"type": "string"}, "end_date": {"type": "string"},
                                         "services": {"type": "array", "items": {"type": "string"}}},
                          "required": ["start_date", "end_date"]}},
        {"name": "get_bess_leaderboard",
         "description": f"Asset-level {cfg.name} BESS revenue leaderboard.",
         "input_schema": {"type": "object",
                          "properties": {"start_date": {"type": "string"}, "end_date": {"type": "string"},
                                         "market": {"type": "string"}, "top_n": {"type": "integer"}},
                          "required": ["start_date", "end_date"]}},
        {"name": "get_bess_revenue_index",
         "description": f"{cfg.name} BESS industry-average revenue index ({currency}/MW/day or /month).",
         "input_schema": {"type": "object",
                          "properties": {"start_date": {"type": "string"}, "end_date": {"type": "string"},
                                         "granularity": {"type": "string", "enum": ["daily", "monthly"]}},
                          "required": ["start_date", "end_date"]}},
        {"name": "get_bess_assets",
         "description": f"{cfg.name} BESS asset register (power, capacity, owner, operator).",
         "input_schema": {"type": "object",
                          "properties": {"min_power_mw": {"type": "number"}, "owner": {"type": "string"}},
                          "required": []}},
        {"name": "search_knowledge_base",
         "description": f"Semantic search over {cfg.name} BESS knowledge base.",
         "input_schema": {"type": "object",
                          "properties": {"query": {"type": "string"},
                                         "sources": {"type": "array", "items": {"type": "string"}}},
                          "required": ["query"]}},
    ]

    _QUANT_TOOLS = [
        {"name": "get_bess_daily_index",
         "description": f"Daily {cfg.name} BESS revenue index ({currency}/MW/day) by market stream.",
         "input_schema": {"type": "object",
                          "properties": {"start_date": {"type": "string"}, "end_date": {"type": "string"},
                                         "markets": {"type": "array", "items": {"type": "string"}}},
                          "required": ["start_date", "end_date"]}},
        {"name": "get_bess_monthly_index",
         "description": f"Monthly {cfg.name} BESS revenue index ({currency}/MW/month) by market stream.",
         "input_schema": {"type": "object",
                          "properties": {"month_from": {"type": "string"}, "month_to": {"type": "string"},
                                         "markets": {"type": "array", "items": {"type": "string"}}},
                          "required": ["month_from", "month_to"]}},
        {"name": "get_leaderboard",
         "description": f"{cfg.name} BESS leaderboard — per-asset revenue for a date range.",
         "input_schema": {"type": "object",
                          "properties": {"start_date": {"type": "string"}, "end_date": {"type": "string"},
                                         "market": {"type": "string"}, "top_n": {"type": "integer"}},
                          "required": ["start_date", "end_date"]}},
        {"name": "get_asset_database",
         "description": f"{cfg.name} BESS asset registry: power, capacity, owner, operator.",
         "input_schema": {"type": "object",
                          "properties": {"min_power_mw": {"type": "number"}},
                          "required": []}},
        {"name": "estimate_irr",
         "description": f"Parametric unlevered IRR for a {cfg.name} BESS project.",
         "input_schema": {"type": "object",
                          "properties": {"power_mw": {"type": "number"}, "duration_h": {"type": "number"},
                                         "capex_per_kw": {"type": "number"},
                                         "opex_pct_capex": {"type": "number"},
                                         "project_life_yrs": {"type": "integer"}},
                          "required": ["power_mw", "duration_h", "capex_per_kw"]}},
    ]

    def _dispatch_strategist(name: str, inputs: dict) -> str:
        try:
            if name == "search_knowledge_base":
                try:
                    from services.intl_market_common.advanced_retrieval_base import retrieve_for_agent
                    return retrieve_for_agent(inputs["query"], _ANTHROPIC_KEY, cfg,
                                             sources=inputs.get("sources") or None, top_k=6)
                except Exception:
                    pass
                results = _search_knowledge(prefix, inputs["query"], sources=inputs.get("sources") or None, limit=6)
                if results.empty:
                    return "No matching knowledge documents found."
                return "\n\n---\n\n".join(
                    f"[{r['source']}] {r['title']} ({r['published_date']})\n{r['snippet']}"
                    for _, r in results.iterrows()
                )
            elif name == "get_spot_price":
                df = _get_spot_price(prefix, inputs["start_date"], inputs["end_date"])
                if df.empty:
                    return f"No spot price data for the requested period. (Table: intl_market.{prefix}spot_price may be empty)"
                return df.round(2).to_json(orient="records", date_format="iso")
            elif name == "get_ancillary_results":
                df = _get_ancillary(prefix, inputs["start_date"], inputs["end_date"])
                if df.empty:
                    return f"No ancillary data. (Table: intl_market.{prefix}ancillary_results may be empty)"
                services = inputs.get("services") or []
                if services:
                    df = df[df["service"].isin(services)]
                summary = df.groupby("service").agg(
                    avg_clearing_price=("clearing_price", "mean"),
                    avg_volume_mw=("volume_mw", "mean"),
                ).round(2).reset_index()
                return summary.to_json(orient="records")
            elif name == "get_bess_leaderboard":
                top_n = inputs.get("top_n", 20)
                market = inputs.get("market")
                df = _get_leaderboard(prefix, inputs["start_date"], inputs["end_date"], top_n)
                if df.empty:
                    return "No leaderboard data."
                if market:
                    df2 = _query(
                        f"SELECT asset, SUM(revenue) AS total_revenue "
                        f"FROM intl_market.{prefix}bess_leaderboard "
                        f"WHERE settlement_date BETWEEN %s AND %s AND market=%s "
                        f"GROUP BY asset ORDER BY total_revenue DESC LIMIT %s",
                        (inputs["start_date"], inputs["end_date"], market, top_n),
                    )
                    return df2.round(2).to_json(orient="records") if not df2.empty else "No leaderboard data for that market."
                return df.round(2).to_json(orient="records")
            elif name == "get_bess_revenue_index":
                granularity = inputs.get("granularity", "monthly")
                if granularity == "daily":
                    df = _get_daily_index(prefix, inputs["start_date"], inputs["end_date"])
                else:
                    df = _get_monthly_index(prefix, inputs["start_date"], inputs["end_date"])
                if df.empty:
                    return "No BESS revenue index data."
                return df.round(2).to_json(orient="records", date_format="iso")
            elif name == "get_bess_assets":
                df = _get_assets(prefix)
                if df.empty:
                    return "No asset data."
                if inputs.get("min_power_mw"):
                    df = df[pd.to_numeric(df["rated_power_mw"], errors="coerce") >= inputs["min_power_mw"]]
                if inputs.get("owner") and not df.empty:
                    df = df[df["owner"].str.contains(inputs["owner"], case=False, na=False)]
                return f"Total: {len(df)} assets\n" + df.to_json(orient="records")
        except Exception as e:
            return f"Error: {e}"
        return "Unknown tool"

    def _compute_irr(cashflows):
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
                        f"FROM intl_market.{prefix}bess_daily_index "
                        f"WHERE settlement_date BETWEEN %s AND %s AND market IN ({ph}) "
                        f"ORDER BY settlement_date, market",
                        (inputs["start_date"], inputs["end_date"], *markets),
                    )
                else:
                    df = _get_daily_index(prefix, inputs["start_date"], inputs["end_date"])
                return df.to_json(orient="records", date_format="iso") if not df.empty else "No daily index data."
            elif name == "get_bess_monthly_index":
                df = _get_monthly_index(prefix, inputs["month_from"], inputs["month_to"])
                return df.to_json(orient="records", date_format="iso") if not df.empty else "No monthly index data."
            elif name == "get_leaderboard":
                df = _get_leaderboard(prefix, inputs["start_date"], inputs["end_date"], inputs.get("top_n", 20))
                return df.round(2).to_json(orient="records") if not df.empty else "No leaderboard data."
            elif name == "get_asset_database":
                df = _get_assets(prefix)
                if df.empty:
                    return "No assets found."
                if inputs.get("min_power_mw"):
                    df = df[pd.to_numeric(df["rated_power_mw"], errors="coerce") >= inputs["min_power_mw"]]
                return f"Total: {len(df)} assets\n" + df.head(50).to_json(orient="records")
            elif name == "estimate_irr":
                power_mw = float(inputs["power_mw"])
                duration_h = float(inputs["duration_h"])
                capex_per_kw = float(inputs["capex_per_kw"])
                opex_pct = float(inputs.get("opex_pct_capex", 2.0)) / 100
                life_yrs = int(inputs.get("project_life_yrs", 15))
                df = _query(
                    f"SELECT year_month AS month, SUM(revenue_permw) AS total_revpermw "
                    f"FROM intl_market.{prefix}bess_monthly_index "
                    f"WHERE duration='*' GROUP BY year_month ORDER BY year_month DESC LIMIT 12"
                )
                if df.empty:
                    return "No monthly index data available."
                avg_monthly = df["total_revpermw"].mean()
                annual_rev = avg_monthly * 12
                capex_total = power_mw * capex_per_kw * 1000
                opex_annual = capex_total * opex_pct
                cashflows = [-capex_total] + [
                    power_mw * annual_rev * (1 - 0.02) ** (yr - 1) - opex_annual
                    for yr in range(1, life_yrs + 1)
                ]
                irr = _compute_irr(cashflows)
                npv_10 = sum(cf / 1.10 ** t for t, cf in enumerate(cashflows))
                sens = []
                for cm in [0.8, 1.0, 1.2]:
                    for rm in [0.8, 1.0, 1.2]:
                        cfs = [-capex_total * cm] + [
                            power_mw * annual_rev * rm * (1 - 0.02) ** (yr - 1) - opex_annual
                            for yr in range(1, life_yrs + 1)
                        ]
                        sens.append({"capex": f"{cm:.0%}", "revenue": f"{rm:.0%}",
                                     "irr": f"{_compute_irr(cfs) * 100:.1f}%"})
                return json.dumps({
                    "inputs": {"power_mw": power_mw, "duration_h": duration_h, "capex_per_kw": capex_per_kw,
                               "avg_monthly_rev_per_mw": round(avg_monthly, 0), "annual_rev_per_mw": round(annual_rev, 0)},
                    "result": {"unlevered_irr": f"{irr * 100:.1f}%", "npv_at_10pct": round(npv_10, 0)},
                    "sensitivity": sens,
                }, indent=2)
        except Exception as e:
            return f"Error: {e}"
        return "Unknown tool"

    def _extract_memories(user_msg: str, agent_reply: str) -> list[dict]:
        resp = _client.messages.create(
            model="claude-haiku-4-5-20251001", max_tokens=512,
            system=(
                f"Extract memorable analyst preferences or domain facts from this {cfg.name} "
                f"power market conversation. Return a JSON array with keys: "
                f"category (market_view|methodology|asset_note|investment_thesis|red_flag), "
                f"subject (≤8 words), content (one sentence). Return [] if nothing reusable."
            ),
            messages=[{"role": "user", "content": f"User: {user_msg}\n\nAgent: {agent_reply[:1500]}"}],
        )
        raw = next((b.text for b in resp.content if hasattr(b, "text")), "[]")
        s, e = raw.find("["), raw.rfind("]")
        if s == -1:
            return []
        try:
            return json.loads(raw[s:e + 1])
        except Exception:
            return []

    def _run_agent_turn(messages, system, tools, dispatch_fn):
        tool_events = []
        while True:
            resp = _client.messages.create(
                model="claude-sonnet-4-6", max_tokens=4096,
                system=system, tools=tools, messages=messages,
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
                    tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": result_str})
            messages = messages + [{"role": "user", "content": tool_results}]

    # ── Start scheduler ───────────────────────────────────────────────────────
    _start_scheduler(cfg.code, cfg.name, prefix, _ANTHROPIC_KEY, str(_app_dir / "daily_report.py"))

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.title(f"{cfg.flag_emoji} {cfg.name}")
        st.caption("Powered by Modo Energy API")
        today = date.today()
        default_start = today - timedelta(days=30)
        st.subheader("Date Range")
        d_start = st.date_input("From", value=default_start, key=f"{cfg.code}_d_start")
        d_end   = st.date_input("To",   value=today,         key=f"{cfg.code}_d_end")
        date_start = d_start.isoformat()
        date_end   = d_end.isoformat()

        st.divider()
        st.subheader("Daily Report")
        _rpt_cur = _is_report_enabled(cfg.code)
        _rpt_toggle = st.toggle("Send daily report", value=_rpt_cur,
                                key=f"{cfg.code}_rpt_enabled",
                                help="Enable or disable the 6 AM daily email + WeCom report")
        if _rpt_toggle != _rpt_cur:
            _set_report_enabled(cfg.code, _rpt_toggle)
            st.success("Saved" if _rpt_toggle else "Daily report disabled")

        st.divider()
        st.caption(f"Port {cfg.port} · ap-southeast-1")

    # ── Tabs ──────────────────────────────────────────────────────────────────
    (tab_overview, tab_ancillary, tab_bess, tab_map,
     tab_knowledge, tab_strategist, tab_quant, tab_library, tab_mgmt) = st.tabs([
        "Market Overview", "Ancillary Markets", "BESS Benchmarking", "Asset Map",
        "Knowledge Base", "Strategist", "Quant", "Library", "Data Management",
    ])

    # ── Market Overview ───────────────────────────────────────────────────────
    with tab_overview:
        st.header(f"{cfg.name} — Market Overview")
        spot_df = _get_spot_price(prefix, date_start, date_end)
        if spot_df.empty:
            st.info(
                f"No spot price data for {cfg.name}. "
                f"Run a backfill in Data Management once Modo API exposes {cfg.code} endpoints."
            )
        else:
            regions = spot_df["region"].unique().tolist() if "region" in spot_df.columns else []
            fig = go.Figure()
            for region in regions:
                rdf = spot_df[spot_df["region"] == region]
                fig.add_trace(go.Scatter(
                    x=rdf["settlement_date"], y=rdf["avg_spot_price"],
                    mode="lines", name=region, line=dict(width=1.5),
                ))
            fig.add_hline(y=0, line_dash="dash", line_color="gray", line_width=1)
            fig.update_layout(
                height=320, margin=dict(l=0, r=0, t=0, b=0),
                yaxis_title=f"{currency}/MWh", xaxis_title="",
                legend=dict(orientation="h", yanchor="bottom", y=1.01, x=0),
            )
            st.subheader(f"Spot Price — Daily Average ({currency}/MWh)")
            st.plotly_chart(fig, use_container_width=True)

            daily_avg = spot_df.groupby("settlement_date")["avg_spot_price"].mean().reset_index()
            col1, col2, col3 = st.columns(3)
            col1.metric("Avg Spot Price", f"{currency}{daily_avg['avg_spot_price'].mean():.1f}/MWh")
            col2.metric("Max Day", f"{currency}{daily_avg['avg_spot_price'].max():.1f}/MWh")
            col3.metric("Min Day", f"{currency}{daily_avg['avg_spot_price'].min():.1f}/MWh")

    # ── Ancillary Markets ─────────────────────────────────────────────────────
    with tab_ancillary:
        st.header(f"Ancillary Markets — {cfg.ancillary_label}")
        anc_df = _get_ancillary(prefix, date_start, date_end)
        if anc_df.empty:
            st.info(
                f"No {cfg.ancillary_label} data yet. "
                f"Awaiting Modo API {cfg.code} ancillary endpoints."
            )
        else:
            svcs = sorted(anc_df["service"].unique().tolist())
            sel_svcs = st.multiselect("Services", svcs, default=svcs[:6] if len(svcs) >= 6 else svcs)
            if sel_svcs:
                filtered = anc_df[anc_df["service"].isin(sel_svcs)]
                col1, col2 = st.columns(2)
                with col1:
                    st.subheader(f"Clearing Price ({currency}/MW/h)")
                    fig = px.line(
                        filtered.groupby(["settlement_date", "service"])["clearing_price"].mean().reset_index(),
                        x="settlement_date", y="clearing_price", color="service",
                        labels={"clearing_price": f"{currency}/MW/h", "settlement_date": ""},
                    )
                    fig.update_layout(height=300, margin=dict(l=0, r=0, t=0, b=0))
                    st.plotly_chart(fig, use_container_width=True)
                with col2:
                    st.subheader("Cleared Volume (MW)")
                    fig2 = px.line(
                        filtered.groupby(["settlement_date", "service"])["volume_mw"].sum().reset_index(),
                        x="settlement_date", y="volume_mw", color="service",
                        labels={"volume_mw": "MW", "settlement_date": ""},
                    )
                    fig2.update_layout(height=300, margin=dict(l=0, r=0, t=0, b=0))
                    st.plotly_chart(fig2, use_container_width=True)
                summary = filtered.groupby("service").agg(
                    avg_price=("clearing_price", "mean"),
                    max_price=("clearing_price", "max"),
                    min_price=("clearing_price", "min"),
                    avg_volume=("volume_mw", "mean"),
                ).round(2).reset_index()
                summary.columns = ["Service", f"Avg Price ({currency}/MW/h)", "Max", "Min", "Avg Volume (MW)"]
                st.dataframe(summary, use_container_width=True, hide_index=True)

    # ── BESS Benchmarking ─────────────────────────────────────────────────────
    with tab_bess:
        st.header("BESS Benchmarking Index")
        bc1, bc2 = st.columns(2)
        bess_from = bc1.date_input("From", value=today - timedelta(days=90), key=f"{cfg.code}_bess_from")
        bess_to   = bc2.date_input("To",   value=today - timedelta(days=1),  key=f"{cfg.code}_bess_to")
        _bs = bess_from.isoformat()
        _be = bess_to.isoformat()

        col1, col2 = st.columns([2, 1])
        with col1:
            st.subheader(f"Daily Revenue Index ({currency}/MW/day)")
            daily_idx = _get_daily_index(prefix, _bs, _be)
            if daily_idx.empty:
                st.info("No daily index data. Run a backfill in Data Management.")
            else:
                components = daily_idx[daily_idx["market"] != "total"]
                total_line = daily_idx[daily_idx["market"] == "total"].sort_values("settlement_date")
                fig = px.bar(components, x="settlement_date", y="revenue_permw", color="market",
                             labels={"revenue_permw": f"{currency}/MW/day", "settlement_date": ""},
                             barmode="relative")
                if not total_line.empty:
                    fig.add_scatter(x=total_line["settlement_date"], y=total_line["revenue_permw"],
                                    mode="lines", name="total",
                                    line=dict(color="black", width=2, dash="dash"))
                fig.update_layout(height=350, margin=dict(l=0, r=0, t=0, b=0))
                st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.subheader("Market Avg")
            if not daily_idx.empty:
                mkt_avg = (daily_idx[daily_idx["market"] != "total"]
                           .groupby("market")[["revenue_permw"]].mean()
                           .round(2).reset_index().sort_values("revenue_permw", ascending=False))
                mkt_avg.columns = ["Market", f"Avg {currency}/MW/day"]
                st.dataframe(mkt_avg, use_container_width=True, hide_index=True)

        st.subheader(f"Monthly Revenue Index ({currency}/MW/month)")
        monthly_idx = _get_monthly_index(prefix, _bs, _be)
        if monthly_idx.empty:
            st.info("No monthly index data.")
        else:
            fig2 = px.bar(
                monthly_idx[monthly_idx["market"] != "total"],
                x="month", y="revenue_permw", color="market",
                labels={"revenue_permw": f"{currency}/MW/month", "month": ""},
                barmode="stack",
            )
            fig2.update_layout(height=320, margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Asset Leaderboard — Top 20")
        leader_df = _get_leaderboard(prefix, _bs, _be)
        if leader_df.empty:
            st.info("No leaderboard data.")
        else:
            for col in ["total_revenue", "rated_power_mw", "energy_capacity_mwh"]:
                if col in leader_df.columns:
                    leader_df[col] = pd.to_numeric(leader_df[col], errors="coerce").round(1)
            st.dataframe(leader_df.sort_values("total_revenue", ascending=False, na_position="last"),
                         use_container_width=True, hide_index=True)

    # ── Asset Map ─────────────────────────────────────────────────────────────
    with tab_map:
        st.header(f"{cfg.name} BESS Assets")
        assets_df = _get_assets(prefix)
        if assets_df.empty:
            st.info("No asset data. Run a backfill in Data Management.")
        else:
            col1, col2 = st.columns(2)
            col1.metric("Total Assets", len(assets_df))
            col2.metric("Total Capacity",
                        f"{pd.to_numeric(assets_df['rated_power_mw'], errors='coerce').sum():.0f} MW")

            mc1, mc2, mc3 = st.columns(3)
            map_from = mc1.date_input("From", value=today - timedelta(days=90), key=f"{cfg.code}_map_from")
            map_to   = mc2.date_input("To",   value=today - timedelta(days=1),  key=f"{cfg.code}_map_to")
            map_mkt  = mc3.selectbox("Colour by", ["total", "wholesale", "frequency_response", "ancillary"],
                                     key=f"{cfg.code}_map_mkt")

            rev_df = _get_asset_revenue_map(prefix, map_from.isoformat(), map_to.isoformat(), map_mkt)

            # Capacity-by-owner bar chart (no geo data for new markets initially)
            cat_df = (assets_df[["rated_power_mw", "owner"]].dropna(subset=["owner"])
                      .assign(rated_power_mw=lambda d: pd.to_numeric(d["rated_power_mw"], errors="coerce"))
                      .groupby("owner", as_index=False)["rated_power_mw"].sum()
                      .sort_values("rated_power_mw", ascending=True))
            if not cat_df.empty:
                fig_cat = px.bar(cat_df, x="rated_power_mw", y="owner", orientation="h",
                                 labels={"rated_power_mw": "MW", "owner": ""},
                                 title="Capacity by Owner")
                fig_cat.update_layout(height=max(300, len(cat_df) * 20),
                                      margin=dict(l=0, r=10, t=30, b=0),
                                      yaxis=dict(tickfont=dict(size=9)))
                st.plotly_chart(fig_cat, use_container_width=True)

            with st.expander("Asset details"):
                show_cols = [c for c in ["asset", "rated_power_mw", "energy_capacity_mwh",
                                         "owner", "operator"] if c in assets_df.columns]
                st.dataframe(assets_df[show_cols], use_container_width=True, hide_index=True)

    # ── Knowledge Base ────────────────────────────────────────────────────────
    with tab_knowledge:
        st.header(f"{cfg.name} Knowledge Base")
        st.info("**Auto-updated daily** · Knowledge ingested at **03:30 SGT** · Modo AI at **04:00 SGT**", icon="🔄")

        # --- KB metrics dashboard ---
        try:
            _kbs = _kb_stats_metrics(prefix)
            _km1, _km2, _km3 = st.columns(3)
            _km1.metric("📄 KB Documents", f"{_kbs['n_docs']:,}")
            _km2.metric("💡 Expert Insights", f"{_kbs['n_insights']:,}")
            _km3.metric("⏱ Last Ingestion", _kbs["last_ingest_ago"])
            _kd1, _kd2 = st.columns(2)
            with _kd1:
                st.caption(
                    f"**Digest rate** — {_kbs['digested']}/{_kbs['total_digestible']} "
                    f"docs converted to insights"
                )
                st.progress(_kbs["digest_pct"], text=f"{_kbs['digest_pct']:.0%}")
            with _kd2:
                _sched_lbl = (
                    f"{_kbs['successes']}/{_kbs['total_runs']} runs succeeded (last 30 days)"
                    if _kbs["total_runs"] > 0 else "no ingestion runs in last 30 days"
                )
                st.caption(f"**Ingestion success** — {_sched_lbl}")
                st.progress(
                    _kbs["sched_pct"],
                    text=f"{_kbs['sched_pct']:.0%}" if _kbs["total_runs"] > 0 else "—",
                )
            st.divider()
        except Exception:
            pass

        kc1, kc2 = st.columns([2, 1])
        with kc1:
            kb_counts = _knowledge_doc_counts(prefix)
            if kb_counts.empty:
                st.info("Knowledge base is empty. Run ingestion from Data Management.")
            else:
                st.dataframe(kb_counts, use_container_width=True, hide_index=True)
        with kc2:
            if st.button("Refresh KB stats", key=f"{cfg.code}_kb_refresh"):
                _knowledge_doc_counts.clear()
                st.rerun()
            if st.button("Run Knowledge Ingest Now", type="primary", key=f"{cfg.code}_kb_ingest"):
                with st.spinner("Fetching from all knowledge sources…"):
                    result = _run_knowledge_ingest_job(trigger="manual")
                if result["status"] == "success":
                    st.success(f"Done — {result['total']} new docs in {result['duration']:.1f}s")
                else:
                    st.error(f"Failed: {result['error']}")
                _knowledge_doc_counts.clear()
                st.rerun()

        st.divider()
        st.subheader("Search Knowledge Base")
        kb_query = st.text_input("Search query",
                                  placeholder=f"e.g. {cfg.name} BESS revenue outlook",
                                  key=f"{cfg.code}_kb_query")
        if kb_query:
            _search_knowledge.clear()
            results_df = _search_knowledge(prefix, kb_query, limit=10)
            if results_df.empty:
                st.info("No results found.")
            else:
                for _, row in results_df.iterrows():
                    with st.expander(
                        f"[{row['source']}] {row['title'] or 'Untitled'} — {row['published_date']}",
                        expanded=False,
                    ):
                        if row.get("url"):
                            st.markdown(f"[View source]({row['url']})")
                        st.text(row["snippet"])

    # ── Strategist Agent ──────────────────────────────────────────────────────
    with tab_strategist:
        st.header(f"Strategist — {cfg.name} Market Analysis")
        n_insights = _get_insight_count()
        st.caption(
            f"Grounded on DB data only · Memory persists across sessions · "
            f"Expert memory: {n_insights} accumulated insights"
        )

        # Knowledge Gap Interview
        with st.expander("Teach the Agent — Knowledge Gap Interview", expanded=False):
            for _k, _v in [("interview_questions", []), ("interview_idx", 0),
                            ("interview_answers", 0), ("interview_modo_queried", False),
                            ("interview_modo_results", {}), ("interview_pending_qs", [])]:
                _skey = f"{cfg.code}_{_k}"
                if _skey not in st.session_state:
                    st.session_state[_skey] = _v

            _iq  = st.session_state[f"{cfg.code}_interview_questions"]
            _ii  = st.session_state[f"{cfg.code}_interview_idx"]
            _pqs = st.session_state[f"{cfg.code}_interview_pending_qs"]

            if not _iq:
                st.markdown("Audits the KB, identifies gaps, queries Modo AI, then asks you the rest.")
                if st.button("Generate Knowledge Gap Questions", key=f"{cfg.code}_gen_interview"):
                    with st.spinner("Auditing knowledge base…"):
                        _new_qs = _generate_interview_questions()
                    if _new_qs:
                        for _k2, _v2 in [("interview_questions", _new_qs), ("interview_idx", 0),
                                         ("interview_answers", 0), ("interview_modo_queried", False),
                                         ("interview_modo_results", {}), ("interview_pending_qs", [])]:
                            st.session_state[f"{cfg.code}_{_k2}"] = _v2
                        st.rerun()
                    else:
                        st.error("Could not generate questions.")
            elif not st.session_state[f"{cfg.code}_interview_modo_queried"]:
                for _qi, _qo in enumerate(_iq):
                    st.markdown(f"{_qi+1}. **[{_qo['topic']}]** {_qo['question']}")
                    st.caption(f"   *{_qo.get('why_asking','')}*")
                st.divider()
                _col_m, _col_u = st.columns(2)
                with _col_m:
                    if st.button("Query Modo AI First (recommended)", key=f"{cfg.code}_interview_modo",
                                 type="primary"):
                        with st.spinner("Querying Modo AI (~2-4 min)…"):
                            try:
                                from services.intl_market_common.modo_ai_base import distill_gap_questions
                                from services.intl_market_common.expert_memory_base import digest_kb_docs
                                _qs_text = [_qo["question"] for _qo in _iq]
                                _mres = distill_gap_questions(_qs_text, cfg, prefix)
                                digest_kb_docs(_ANTHROPIC_KEY, prefix, cfg.name, limit=len(_iq) + 5)
                            except Exception as _me:
                                st.error(f"Modo query failed: {_me}")
                                _mres = {}
                        st.session_state[f"{cfg.code}_interview_modo_results"] = _mres
                        st.session_state[f"{cfg.code}_interview_pending_qs"] = [
                            _qo for _qo in _iq if not _mres.get(_qo["question"])
                        ]
                        st.session_state[f"{cfg.code}_interview_modo_queried"] = True
                        st.session_state[f"{cfg.code}_interview_idx"] = 0
                        st.rerun()
                with _col_u:
                    if st.button("Answer Yourself (skip Modo)", key=f"{cfg.code}_interview_skip"):
                        st.session_state[f"{cfg.code}_interview_pending_qs"] = list(_iq)
                        st.session_state[f"{cfg.code}_interview_modo_queried"] = True
                        st.session_state[f"{cfg.code}_interview_idx"] = 0
                        st.rerun()
            elif _ii >= len(_pqs):
                _mres = st.session_state[f"{cfg.code}_interview_modo_results"]
                _n_modo = sum(1 for v in _mres.values() if v)
                _n_user = st.session_state[f"{cfg.code}_interview_answers"]
                st.success(f"Modo AI answered {_n_modo}, you answered {_n_user}. All stored as high-confidence insights.")
                if st.button("Start New Interview", key=f"{cfg.code}_new_interview"):
                    for _k2 in ["interview_questions", "interview_pending_qs"]:
                        st.session_state[f"{cfg.code}_{_k2}"] = []
                    st.session_state[f"{cfg.code}_interview_modo_results"] = {}
                    st.session_state[f"{cfg.code}_interview_idx"] = 0
                    st.session_state[f"{cfg.code}_interview_answers"] = 0
                    st.session_state[f"{cfg.code}_interview_modo_queried"] = False
                    st.rerun()
            else:
                _q = _pqs[_ii]
                st.progress(_ii / max(len(_pqs), 1), text=f"Question {_ii+1} of {len(_pqs)}")
                st.markdown(f"**[{_q['topic']}]** {_q['question']}")
                st.caption(f"*{_q.get('why_asking','')}*")
                _ans = st.text_area("Your answer:", key=f"{cfg.code}_interview_ans_{_ii}", height=120)
                _cs, _csk = st.columns([2, 1])
                with _cs:
                    if st.button("Submit Answer", key=f"{cfg.code}_interview_submit_{_ii}", type="primary"):
                        if _ans.strip():
                            _store_interview_answer(_q["question"], _ans.strip(), _q["topic"])
                            st.session_state[f"{cfg.code}_interview_idx"] += 1
                            st.session_state[f"{cfg.code}_interview_answers"] += 1
                            st.rerun()
                        else:
                            st.warning("Please enter an answer.")
                with _csk:
                    if st.button("Skip", key=f"{cfg.code}_interview_skip_{_ii}"):
                        st.session_state[f"{cfg.code}_interview_idx"] += 1
                        st.rerun()

        # Session resume
        if f"{cfg.code}_strat_session_id" not in st.session_state:
            st.session_state[f"{cfg.code}_strat_session_id"] = str(uuid.uuid4())
        if f"{cfg.code}_strat_history" not in st.session_state:
            st.session_state[f"{cfg.code}_strat_history"] = []

        if not st.session_state[f"{cfg.code}_strat_history"]:
            _recent = _list_recent_sessions(prefix)
            if not _recent.empty:
                with st.expander("Resume a previous conversation?", expanded=False):
                    for _, _srow in _recent.iterrows():
                        _lbl = (f"{_srow['session_id'][:8]}… — "
                                f"{_srow['updated_at'].strftime('%Y-%m-%d %H:%M')} — "
                                f"{int(_srow['msg_count'])} messages")
                        if st.button(_lbl, key=f"{cfg.code}_resume_{_srow['session_id']}"):
                            st.session_state[f"{cfg.code}_strat_session_id"] = _srow["session_id"]
                            st.session_state[f"{cfg.code}_strat_history"] = _load_session(_srow["session_id"])
                            st.rerun()

        for msg in st.session_state[f"{cfg.code}_strat_history"]:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

        user_input = st.chat_input(f"Ask about {cfg.name} market fundamentals, prices, ancillary services…")
        if user_input:
            st.session_state[f"{cfg.code}_strat_history"].append({"role": "user", "content": user_input})
            with st.chat_message("user"):
                st.markdown(user_input)
            with st.chat_message("assistant"):
                with st.spinner("Analysing…"):
                    api_msgs = [{"role": m["role"], "content": m["content"]}
                                for m in st.session_state[f"{cfg.code}_strat_history"]]
                    try:
                        reply, _, tool_events = _run_agent_turn(
                            api_msgs, _build_strategist_system(user_input),
                            _STRATEGIST_TOOLS, _dispatch_strategist,
                        )
                    except Exception as _err:
                        reply = f"⚠️ API error: {_err}. Please try again."
                        tool_events = []
                st.markdown(reply)
                if tool_events:
                    with st.expander(f"Tools used ({len(tool_events)})", expanded=False):
                        for ev in tool_events:
                            st.caption(f"**{ev['tool']}** → {ev['result'][:120]}…")

            st.session_state[f"{cfg.code}_strat_history"].append({"role": "assistant", "content": reply})
            try:
                _save_session(st.session_state[f"{cfg.code}_strat_session_id"],
                              st.session_state[f"{cfg.code}_strat_history"])
            except Exception:
                pass
            try:
                from services.intl_market_common.expert_memory_base import extract_insights
                n_ins = extract_insights(user_input, reply, _ANTHROPIC_KEY, prefix, cfg.name)
                if n_ins:
                    st.toast(f"Stored {n_ins} expert insight(s)")
            except Exception:
                pass

        if st.session_state.get(f"{cfg.code}_strat_history") and st.button("Clear chat", key=f"{cfg.code}_clear_strat"):
            st.session_state[f"{cfg.code}_strat_history"] = []
            st.session_state[f"{cfg.code}_strat_session_id"] = str(uuid.uuid4())
            st.rerun()

    # ── Quant Agent ───────────────────────────────────────────────────────────
    with tab_quant:
        st.header(f"Quant — {cfg.name} BESS Investment Economics")
        st.caption(f"Grounded on Modo index data · Parametric IRR model · {currency}")

        if f"{cfg.code}_quant_history" not in st.session_state:
            st.session_state[f"{cfg.code}_quant_history"] = []

        for msg in st.session_state[f"{cfg.code}_quant_history"]:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

        user_input_q = st.chat_input(f"Ask about {cfg.name} BESS revenues, leaderboard, or IRR…")
        if user_input_q:
            st.session_state[f"{cfg.code}_quant_history"].append({"role": "user", "content": user_input_q})
            with st.chat_message("user"):
                st.markdown(user_input_q)
            with st.chat_message("assistant"):
                with st.spinner("Calculating…"):
                    api_msgs_q = [{"role": m["role"], "content": m["content"]}
                                  for m in st.session_state[f"{cfg.code}_quant_history"]]
                    try:
                        reply_q, _, tool_events_q = _run_agent_turn(
                            api_msgs_q, _build_quant_system(), _QUANT_TOOLS, _dispatch_quant,
                        )
                    except Exception as _err:
                        reply_q = f"⚠️ API error: {_err}. Please try again."
                        tool_events_q = []
                st.markdown(reply_q)
                if tool_events_q:
                    with st.expander(f"Tools used ({len(tool_events_q)})", expanded=False):
                        for ev in tool_events_q:
                            st.caption(f"**{ev['tool']}** → {ev['result'][:120]}…")

            st.session_state[f"{cfg.code}_quant_history"].append({"role": "assistant", "content": reply_q})
            suggestions_q = _extract_memories(user_input_q, reply_q)
            for sug in suggestions_q:
                _save_memory(sug["category"], sug["subject"], sug["content"], source="auto")
            if suggestions_q:
                st.toast(f"Saved {len(suggestions_q)} memory item(s)")

        if st.session_state.get(f"{cfg.code}_quant_history") and st.button("Clear chat", key=f"{cfg.code}_clear_quant"):
            st.session_state[f"{cfg.code}_quant_history"] = []
            st.rerun()

    # ── Library ───────────────────────────────────────────────────────────────
    with tab_library:
        from services.common.report_library_ui import render_library_tab
        render_library_tab(cfg.code, cfg.name, cfg.code)

    # ── Data Management ───────────────────────────────────────────────────────
    with tab_mgmt:
        st.header("Data Management")
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Table Coverage")
            if st.button("Refresh counts", key=f"{cfg.code}_refresh_counts"):
                _table_counts.clear()
            counts_df = _table_counts(prefix)
            st.dataframe(counts_df, use_container_width=True, hide_index=True)
        with col2:
            st.subheader("Run Backfill")
            st.caption(f"Fetches Modo {cfg.name} data and upserts into DB.")
            bf_start = st.date_input("Backfill from", value=today - timedelta(days=30), key=f"{cfg.code}_bf_start")
            bf_end   = st.date_input("Backfill to",   value=today,                      key=f"{cfg.code}_bf_end")
            if st.button("Run Backfill", type="primary", key=f"{cfg.code}_run_backfill"):
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
            kb_only = st.multiselect("Sources (all if empty)", ["modo_ai"], key=f"{cfg.code}_kb_only")
        with kb_col2:
            if st.button("Run Knowledge Ingest", type="secondary", key=f"{cfg.code}_run_kb"):
                with st.spinner("Fetching knowledge…"):
                    kr = _run_knowledge_ingest_job(only=kb_only or None, trigger="manual")
                if kr["status"] == "success":
                    st.success(f"{kr['total']} new docs in {kr['duration']:.1f}s")
                else:
                    st.error(f"Knowledge ingest failed: {kr['error']}")
                _knowledge_doc_counts.clear()

        st.divider()
        st.subheader("Modo AI Distillation")
        st.caption(
            f"Logs into app.modoenergy.com and asks {len(cfg.standard_questions)} standard {cfg.name} "
            f"BESS market questions. Runs automatically at 04:00 SGT nightly."
        )
        if st.button("Run Modo AI Distillation Now", type="secondary", key=f"{cfg.code}_modo_ai_btn"):
            with st.spinner("Opening Modo app and querying AI agent (~2-3 min)…"):
                try:
                    from services.intl_market_common.modo_ai_base import ModoAIConnector
                    connector = ModoAIConnector(cfg)
                    n = _run_connector_to_db(connector, _conn(), prefix)
                    if n == 0:
                        st.warning("Modo AI distillation complete — 0 new docs inserted.")
                    else:
                        st.success(f"Modo AI distillation complete — {n} new docs inserted.")
                except Exception as _ma_e:
                    st.error(f"Modo AI distillation failed: {_ma_e}")
            _knowledge_doc_counts.clear()

        st.divider()
        st.subheader("Expert Memory — KB Digestion")
        st.caption("Digests KB docs into durable market insights. Runs at 03:45 SGT nightly.")
        if st.button("Digest KB into Expert Memory", key=f"{cfg.code}_digest_kb"):
            with st.spinner("Extracting insights (1-2 min)…"):
                try:
                    from services.intl_market_common.expert_memory_base import digest_kb_docs
                    n_dk = digest_kb_docs(_ANTHROPIC_KEY, prefix, cfg.name, limit=200)
                    st.success(f"Extracted {n_dk} new insights.")
                except Exception as _dk_e:
                    st.error(f"KB digest failed: {_dk_e}")

        st.divider()
        st.subheader("Upload Documents to Knowledge Base")
        up_tab1, up_tab2 = st.tabs(["Upload Files", "Fetch from URL"])
        with up_tab1:
            uploaded_files = st.file_uploader(
                "Choose files", type=["pdf", "xlsx", "xls", "docx", "doc", "txt"],
                accept_multiple_files=True, key=f"{cfg.code}_kb_upload",
            )
            if uploaded_files and st.button("Ingest uploaded files", type="primary", key=f"{cfg.code}_kb_upload_btn"):
                prog = st.progress(0)
                ok, errs = [], []
                for i, f in enumerate(uploaded_files):
                    res = _ingest_uploaded_file(f.name, f.read())
                    (ok if res["status"] == "success" else errs).append((f.name, res))
                    prog.progress((i + 1) / len(uploaded_files))
                prog.empty()
                if ok:
                    st.success(f"Ingested {len(ok)} file(s).")
                for fname, r in errs:
                    st.error(f"✗ {fname}: {r['msg']}")
                _knowledge_doc_counts.clear()
                st.rerun()
        with up_tab2:
            fetch_url = st.text_input("Article URL", key=f"{cfg.code}_fetch_url")
            if st.button("Fetch and ingest", type="primary", key=f"{cfg.code}_kb_fetch") and fetch_url:
                with st.spinner("Fetching…"):
                    res = _ingest_url(fetch_url.strip())
                if res["status"] == "success":
                    st.success(res["msg"])
                    _knowledge_doc_counts.clear()
                else:
                    st.error(res["msg"])

        st.divider()
        st.subheader("Daily Market Report")
        rpt_col1, rpt_col2 = st.columns(2)
        with rpt_col1:
            rpt_date = st.date_input("Report date", value=today - timedelta(days=1), key=f"{cfg.code}_rpt_date")
            rpt_email = st.text_input("Send to", value="chen_dpeng@hotmail.com", key=f"{cfg.code}_rpt_email")
        with rpt_col2:
            st.write("")
            st.write("")
            if st.button("Send Report Now", type="primary", key=f"{cfg.code}_send_rpt"):
                smtp_user = os.environ.get("SMTP_USER", "")
                smtp_pass = os.environ.get("SMTP_PASSWORD", "")
                if not smtp_user or not smtp_pass:
                    st.error("SMTP credentials not configured.")
                else:
                    with st.spinner("Generating PDF and sending email…"):
                        try:
                            import importlib.util as _ilu
                            _rpt_path = _app_dir / "daily_report.py"
                            _spec = _ilu.spec_from_file_location("daily_report_ui", _rpt_path)
                            _mod = _ilu.module_from_spec(_spec)
                            _spec.loader.exec_module(_mod)
                            pdf_bytes, ai_cmnt = _mod.generate_report_pdf(rpt_date)
                            _mod.send_daily_report_email(pdf_bytes, rpt_date, rpt_email, ai_commentary=ai_cmnt)
                            st.success(f"Report sent to {rpt_email} ({len(pdf_bytes):,} bytes)")
                        except Exception as _rpt_exc:
                            st.error(f"Report failed: {_rpt_exc}")

        st.divider()
        st.subheader("Scheduled Downloads")
        try:
            sched = _start_scheduler(cfg.code, cfg.name, prefix, _ANTHROPIC_KEY,
                                     str(_app_dir / "daily_report.py"))
            jobs = sched.get_jobs()
            if jobs:
                next_run = min((j.next_run_time for j in jobs if j.next_run_time), default=None)
                next_str = next_run.strftime("%Y-%m-%d %H:%M SGT") if next_run else "—"
                st.success(f"Scheduler running · Next: **{next_str}**")
            else:
                st.warning("Scheduler has no active jobs.")
        except Exception as e:
            st.error(f"Scheduler error: {e}")

        st.caption("Recent ingestion runs")
        if st.button("Refresh logs", key=f"{cfg.code}_refresh_logs"):
            _get_ingestion_logs.clear()
        logs_df = _get_ingestion_logs(prefix, limit=20)
        if logs_df.empty:
            st.info("No ingestion runs recorded yet.")
        else:
            for _, row in logs_df.iterrows():
                icon = "✅" if row["status"] == "success" else "❌"
                with st.expander(
                    f"{icon} {row['run_at_sgt']}  [{row['trigger']}]  "
                    f"{row['date_from']} → {row['date_to']}  ({row['duration_seconds']}s)",
                    expanded=(row["status"] == "error"),
                ):
                    if row["status"] == "error" and row["error_msg"]:
                        st.error(row["error_msg"])
                    if row["rows_ingested"]:
                        st.json(row["rows_ingested"])

        st.divider()
        st.subheader("Agent Memory")
        mems = _load_memories(app_key)
        if mems.empty:
            st.info("No memories saved yet.")
        else:
            for _, row in mems.iterrows():
                c1, c2 = st.columns([10, 1])
                with c1:
                    st.markdown(f"**[{row['category']}]** {row['subject']}: {row['content']}")
                    st.caption(f"{row['source']} · {row['created_at']}")
                with c2:
                    if st.button("🗑", key=f"{cfg.code}_del_{row['id']}"):
                        _delete_memory(row["id"])
                        st.rerun()

        st.divider()
        st.subheader("Add Memory Manually")
        with st.form(f"{cfg.code}_add_mem"):
            cat = st.selectbox("Category",
                               ["market_view", "methodology", "asset_note", "investment_thesis", "red_flag"])
            subj = st.text_input("Subject (≤8 words)")
            cont = st.text_area("Content (one sentence)")
            if st.form_submit_button("Save"):
                _save_memory(cat, subj, cont, source="manual")
                st.success("Saved")
