"""Standalone scheduler service for GB market.

Runs as a separate process at container startup (via run.sh) so that scheduled
jobs fire even when no user has visited the Streamlit app.  Has no dependency
on the Streamlit runtime — uses its own DB connections and imports service
modules directly.

Jobs (all times Asia/Singapore):
  03:00  Daily market data ingestion  (Modo Energy API → RDS)
  03:30  Knowledge-base ingestion
  03:45  KB digest → expert insights
  04:00  Modo AI distillation
  04:30  Pricing batch
  06:00  Daily report → email + WeCom
  09:15  Elexon ops ingest (settlement system prices + wind forecast)
"""
import json
import logging
import os
import pathlib
import sys
import time
from datetime import date, timedelta

sys.path.insert(0, "/app")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [gb-sched] %(levelname)s %(name)s: %(message)s",
    force=True,
)
logger = logging.getLogger("gb-scheduler")

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _new_conn():
    import psycopg2
    url = (
        os.environ.get("PGURL")
        or os.environ.get("DATABASE_URL")
        or "postgresql://postgres:root@127.0.0.1:5433/marketdata"
    )
    conn = psycopg2.connect(url, connect_timeout=10)
    conn.autocommit = True
    return conn


def _is_report_enabled() -> bool:
    try:
        conn = _new_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM intl_market.platform_settings "
                "WHERE market_code = 'gb' AND key = 'daily_report_enabled'"
            )
            row = cur.fetchone()
        conn.close()
        if row is None:
            return True
        return row[0].lower() in ("true", "1", "yes")
    except Exception:
        return True


def _log_ingestion_run(trigger, date_from, date_to, status, rows, error_msg, duration):
    try:
        conn = _new_conn()
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO intl_market.gb_ingestion_log "
                "(trigger, date_from, date_to, status, rows_ingested, error_msg, duration_seconds) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (trigger, date_from, date_to, status,
                 json.dumps(rows) if rows else None, error_msg, round(duration, 1)),
            )
        conn.close()
    except Exception as exc:
        logger.warning("Could not write to gb_ingestion_log: %s", exc)


# ---------------------------------------------------------------------------
# Job functions
# ---------------------------------------------------------------------------

def _daily_market_job():
    import importlib.util
    yesterday = date.today() - timedelta(days=1)
    # Main market data ingestion
    t0 = time.time()
    try:
        from services.modo_energy.gb_ingestion import run_gb_backfill
        run_gb_backfill(yesterday, yesterday)
        duration = time.time() - t0
        _log_ingestion_run("scheduled", yesterday, yesterday, "success", None, None, duration)
        logger.info("Market ingestion completed for %s (%.1fs)", yesterday, duration)
    except Exception as exc:
        duration = time.time() - t0
        _log_ingestion_run("scheduled", yesterday, yesterday, "error", None, str(exc), duration)
        logger.error("Market ingestion failed: %s", exc)
    # Fuel mix
    try:
        _fm_path = pathlib.Path(__file__).with_name("fuel_mix_ingest.py")
        _fm_spec = importlib.util.spec_from_file_location("fuel_mix_ingest", _fm_path)
        _fm_mod  = importlib.util.module_from_spec(_fm_spec)
        _fm_spec.loader.exec_module(_fm_mod)
        conn = _new_conn()
        n = _fm_mod.ingest_fuel_mix(yesterday, conn)
        conn.close()
        logger.info("Fuel mix ingest: %d rows for %s", n, yesterday)
    except Exception as exc:
        logger.warning("Fuel mix ingest failed: %s", exc)


def _daily_knowledge_job():
    try:
        from services.gb_knowledge.ingest import run_knowledge_ingest
        results = run_knowledge_ingest(only=None, verbose=False)
        total = sum(results.values())
        logger.info("Knowledge ingest: %d total items", total)
    except Exception as exc:
        logger.error("Knowledge ingest failed: %s", exc)


def _kb_digest_job():
    try:
        from services.gb_knowledge.expert_memory import digest_kb_docs
        anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
        n = digest_kb_docs(anthropic_key, limit=100)
        logger.info("KB digest: %d new insights", n)
    except Exception as exc:
        logger.error("KB digest failed: %s", exc)


def _modo_ai_job():
    try:
        from services.gb_knowledge.modo_ai import ModoAIConnector
        from services.gb_knowledge.base import get_db_conn, ensure_table, upsert_doc
        conn = get_db_conn()
        ensure_table(conn)
        connector = ModoAIConnector()
        n = connector.run(conn)
        conn.close()
        logger.info("Modo AI distillation: %d new docs", n)
    except Exception as exc:
        logger.error("Modo AI distillation failed: %s", exc)


def _pricing_batch_job():
    import importlib.util
    yesterday = date.today() - timedelta(days=1)
    try:
        _pb_path = pathlib.Path(__file__).with_name("pricing_batch.py")
        _pb_spec = importlib.util.spec_from_file_location("pricing_batch", _pb_path)
        _pb_mod  = importlib.util.module_from_spec(_pb_spec)
        _pb_spec.loader.exec_module(_pb_mod)
        conn = _new_conn()
        result = _pb_mod.run_pricing_batch(yesterday, conn)
        conn.close()
        logger.info("Pricing batch: %s", result)
    except Exception as exc:
        logger.error("Pricing batch failed: %s", exc)


def _elexon_ops_job():
    yesterday = date.today() - timedelta(days=1)
    try:
        from services.gb_knowledge.elexon_ops import run_elexon_ops_ingest
        result = run_elexon_ops_ingest(yesterday)
        logger.info("Elexon ops ingest: %s", result)
    except Exception as exc:
        logger.error("Elexon ops ingest failed: %s", exc)


def _daily_report_job():
    if not _is_report_enabled():
        logger.info("Daily report disabled for GB — skipping")
        return
    import importlib.util
    today     = date.today()
    yesterday = today - timedelta(days=1)

    # Check if ingestion completed today; if not, run it first
    ingestion_done = False
    try:
        conn = _new_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM intl_market.gb_ingestion_log "
                "WHERE trigger IN ('scheduled', 'report_triggered') "
                "AND status = 'success' "
                "AND date_from = %s AND run_at::date = %s",
                (yesterday, today),
            )
            ingestion_done = cur.fetchone()[0] > 0
        conn.close()
    except Exception as exc:
        logger.warning("Report job: could not check ingestion log: %s", exc)

    if not ingestion_done:
        logger.info("Report job: ingestion not complete, running it now")
        _daily_market_job()

    _rpt_path = pathlib.Path(__file__).with_name("daily_report.py")
    _spec = importlib.util.spec_from_file_location("daily_report", _rpt_path)
    _mod  = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)

    rpt_conn  = _new_conn()
    rpt_date  = _mod._get_latest_data_date(rpt_conn)
    rpt_conn.close()

    pdf_bytes, ai_commentary = _mod.generate_report_pdf(rpt_date)

    # Email
    try:
        _mod.send_daily_report_email(pdf_bytes, rpt_date, ai_commentary=ai_commentary)
        logger.info("Daily report emailed for %s (%d bytes)", rpt_date, len(pdf_bytes))
    except Exception as exc:
        logger.error("Daily report email failed: %s", exc)

    # WeCom
    wecom_url = os.environ.get("WECOM_WEBHOOK_URL", "")
    if wecom_url:
        try:
            _mod.send_daily_report_wecom(pdf_bytes, rpt_date,
                                         webhook_url=wecom_url,
                                         ai_commentary=ai_commentary)
            logger.info("Daily report sent to WeCom for %s", rpt_date)
        except Exception as exc:
            logger.error("Daily report WeCom failed: %s", exc)


# ---------------------------------------------------------------------------
# Scheduler setup
# ---------------------------------------------------------------------------

def start_scheduler():
    from apscheduler.schedulers.background import BackgroundScheduler
    scheduler = BackgroundScheduler(timezone="Asia/Singapore")
    scheduler.add_job(_daily_market_job,   "cron", hour=3,  minute=0,
                      id="gb_daily_market",    misfire_grace_time=3600)
    scheduler.add_job(_daily_knowledge_job, "cron", hour=3, minute=30,
                      id="gb_daily_knowledge", misfire_grace_time=3600)
    scheduler.add_job(_kb_digest_job,       "cron", hour=3, minute=45,
                      id="gb_kb_digest",       misfire_grace_time=3600)
    scheduler.add_job(_modo_ai_job,         "cron", hour=4, minute=0,
                      id="gb_modo_ai",         misfire_grace_time=3600)
    scheduler.add_job(_pricing_batch_job,   "cron", hour=4, minute=30,
                      id="gb_pricing_batch",   misfire_grace_time=3600)
    scheduler.add_job(_daily_report_job,    "cron", hour=6, minute=0,
                      id="gb_daily_report",    misfire_grace_time=3600)
    scheduler.add_job(_elexon_ops_job,      "cron", hour=9, minute=15,
                      id="gb_elexon_ops",      misfire_grace_time=3600)
    scheduler.start()
    return scheduler


if __name__ == "__main__":
    logger.info("[SCHEDULER] GB standalone scheduler service starting")
    # Brief wait for DB readiness on fresh container
    time.sleep(8)
    scheduler = start_scheduler()
    logger.info("[SCHEDULER] GB scheduler running. Jobs: %s",
                [j.id for j in scheduler.get_jobs()])
    # Keep process alive — APScheduler runs jobs in daemon threads
    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("[SCHEDULER] GB scheduler stopped")
