"""Standalone scheduler service for template-based market apps (AU, ERCOT, PJM, CAISO).

Run as a separate process at container startup via run.sh so that scheduled
jobs fire even when no user has visited the Streamlit app.

Usage:
    python services/intl_market_common/scheduler_service.py --code au --app-dir apps/au-market
    python services/intl_market_common/scheduler_service.py --code ercot --app-dir apps/ercot-market
    python services/intl_market_common/scheduler_service.py --code pjm --app-dir apps/pjm-market
    python services/intl_market_common/scheduler_service.py --code caiso --app-dir apps/caiso-market

Jobs (all times Asia/Singapore):
  03:00  Daily market ingestion  (Modo Energy API → RDS)
  03:30  Knowledge-base ingestion  (optional — skipped if services/{code}_knowledge absent)
  03:45  KB digest → expert insights  (same guard)
  04:00  Modo AI distillation  (same guard)
  06:00  Daily report → email + WeCom
"""
import argparse
import importlib
import logging
import os
import pathlib
import sys
import time
from datetime import date, timedelta

sys.path.insert(0, "/app")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [{code}-sched] %(levelname)s %(name)s: %(message)s",
    force=True,
)


def _build_logger(code: str):
    log = logging.getLogger(f"{code}-scheduler")
    # patch format with code filled in
    for h in logging.getLogger().handlers:
        h.setFormatter(logging.Formatter(
            f"%(asctime)s [{code}-sched] %(levelname)s %(name)s: %(message)s"
        ))
    return log


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


def _is_report_enabled(code: str) -> bool:
    try:
        conn = _new_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM intl_market.platform_settings "
                "WHERE market_code = %s AND key = 'daily_report_enabled'",
                (code,),
            )
            row = cur.fetchone()
        conn.close()
        if row is None:
            return True
        return row[0].lower() in ("true", "1", "yes")
    except Exception:
        return True


def _make_jobs(code: str, api_key: str, app_dir: pathlib.Path, logger):
    """Return the job functions for a given market code."""

    def _daily_market_job():
        yesterday = date.today() - timedelta(days=1)
        try:
            mod = importlib.import_module(f"services.modo_energy.{code}_ingestion")
            mod.run_ingestion(yesterday, yesterday)
            logger.info("Market ingestion completed for %s (%s)", code, yesterday)
        except Exception as exc:
            logger.error("Daily market job failed for %s: %s", code, exc)

    def _daily_knowledge_job():
        try:
            mod = importlib.import_module(f"services.{code}_knowledge.ingest")
            mod.run_knowledge_ingest(verbose=False)
            logger.info("Knowledge ingest completed for %s", code)
        except ModuleNotFoundError:
            logger.debug("No knowledge module for %s — skipping", code)
        except Exception as exc:
            logger.error("Daily knowledge job failed for %s: %s", code, exc)

    def _kb_digest_job():
        try:
            from services.intl_market_common.expert_memory_base import digest_kb_docs
            prefix = f"/{code}-market"
            n = digest_kb_docs(api_key, prefix, code.upper(), limit=100)
            logger.info("KB digest for %s: %d new insights", code, n)
        except ModuleNotFoundError:
            logger.debug("No expert_memory_base for %s — skipping", code)
        except Exception as exc:
            logger.error("KB digest failed for %s: %s", code, exc)

    def _modo_ai_job():
        try:
            from services.intl_market_common.modo_ai_base import ModoAIConnector
            cfg_mod = importlib.import_module(f"services.{code}_knowledge.config")
            cfg = cfg_mod.MARKET_CONFIG
            connector = ModoAIConnector(cfg)
            conn = _new_conn()
            # _run_connector_to_db equivalent — inline here to avoid Streamlit dep
            prefix = f"/{code}-market"
            from services.gb_knowledge.base import ensure_table, upsert_doc
            ensure_table(conn)
            n = 0
            for doc in connector.fetch_all():
                if upsert_doc(conn, **doc):
                    n += 1
            conn.close()
            logger.info("Modo AI distillation for %s: %d new docs", code, n)
        except ModuleNotFoundError:
            logger.debug("No Modo AI module for %s — skipping", code)
        except Exception as exc:
            logger.error("Modo AI job failed for %s: %s", code, exc)

    def _daily_report_job():
        if not _is_report_enabled(code):
            logger.info("Daily report disabled for %s — skipping", code)
            return
        try:
            import importlib.util
            rpt_path = app_dir / "daily_report.py"
            if not rpt_path.exists():
                logger.warning("No daily_report.py for %s", code)
                return
            spec = importlib.util.spec_from_file_location("daily_report", rpt_path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            conn = _new_conn()
            rpt_date = mod._get_latest_data_date(conn)
            conn.close()
            pdf_bytes, ai_commentary = mod.generate_report_pdf(rpt_date)
            # Email
            try:
                mod.send_daily_report_email(pdf_bytes, rpt_date, ai_commentary=ai_commentary)
                logger.info("Daily report emailed for %s (%s)", code, rpt_date)
            except Exception as exc:
                logger.error("Daily report email failed for %s: %s", code, exc)
            # WeCom
            wecom_url = os.environ.get("WECOM_WEBHOOK_URL", "")
            if wecom_url:
                try:
                    mod.send_daily_report_wecom(pdf_bytes, rpt_date,
                                                webhook_url=wecom_url,
                                                ai_commentary=ai_commentary)
                    logger.info("Daily report WeCom sent for %s (%s)", code, rpt_date)
                except Exception as exc:
                    logger.error("Daily report WeCom failed for %s: %s", code, exc)
        except Exception as exc:
            logger.error("Daily report job failed for %s: %s", code, exc)

    return (
        _daily_market_job,
        _daily_knowledge_job,
        _kb_digest_job,
        _modo_ai_job,
        _daily_report_job,
    )


def start_scheduler(code: str, api_key: str, app_dir: pathlib.Path, logger):
    from apscheduler.schedulers.background import BackgroundScheduler
    (
        _daily_market_job,
        _daily_knowledge_job,
        _kb_digest_job,
        _modo_ai_job,
        _daily_report_job,
    ) = _make_jobs(code, api_key, app_dir, logger)

    scheduler = BackgroundScheduler(timezone="Asia/Singapore")
    scheduler.add_job(_daily_market_job,    "cron", hour=3, minute=0,
                      id=f"{code}_daily_market",    misfire_grace_time=3600)
    scheduler.add_job(_daily_knowledge_job, "cron", hour=3, minute=30,
                      id=f"{code}_daily_knowledge", misfire_grace_time=3600)
    scheduler.add_job(_kb_digest_job,       "cron", hour=3, minute=45,
                      id=f"{code}_kb_digest",       misfire_grace_time=3600)
    scheduler.add_job(_modo_ai_job,         "cron", hour=4, minute=0,
                      id=f"{code}_modo_ai",         misfire_grace_time=3600)
    scheduler.add_job(_daily_report_job,    "cron", hour=6, minute=0,
                      id=f"{code}_daily_report",    misfire_grace_time=3600)
    scheduler.start()
    return scheduler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--code", required=True, help="Market code, e.g. au, ercot, pjm, caiso")
    parser.add_argument("--app-dir", required=True, help="Path to the app directory, e.g. apps/au-market")
    args = parser.parse_args()

    code = args.code
    app_dir = pathlib.Path(args.app_dir)
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    logger = _build_logger(code)

    logger.info("[SCHEDULER] %s standalone scheduler service starting", code.upper())
    time.sleep(8)  # Brief wait for DB readiness

    scheduler = start_scheduler(code, api_key, app_dir, logger)
    logger.info("[SCHEDULER] %s scheduler running. Jobs: %s",
                code.upper(), [j.id for j in scheduler.get_jobs()])

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("[SCHEDULER] %s scheduler stopped", code.upper())


if __name__ == "__main__":
    main()
