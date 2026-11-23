"""Headless daily jobs for the Philippines market app.

The Streamlit app starts the same jobs on an in-process scheduler, but only once a
browser session has loaded the page. This entrypoint runs them from a one-shot ECS
task (EventBridge rule bess-ph-scheduler-daily) so data stays current with the web
UI stopped.

    python -m services.ph_knowledge.run_jobs                 # daily: prices + KB ingest + digest
    python -m services.ph_knowledge.run_jobs --only prices --days-back 120
    python -m services.ph_knowledge.run_jobs --only prices --days-back 400 --overwrite

Exit code 1 if any step failed.
"""
import argparse
import logging
import os
import sys
from datetime import date

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ph-jobs] %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("ph-jobs")

_WEEKLY_DOC_SOURCES = [
    "wesm_market_watch", "wesm_assessment", "wesm_retail_assessment",
    "wesm_overriding_constraints", "iemop_knowledge_center",
    "wesm_bess_study", "iemop_market_data",
]


def _prices(days_back: int, overwrite: bool) -> None:
    import psycopg2
    from services.ph_knowledge.wesm_scraper import run_wesm_price_scrape

    conn = psycopg2.connect(os.environ["PGURL"], keepalives=1, keepalives_idle=30)
    conn.autocommit = True
    try:
        results = run_wesm_price_scrape(conn, days_back=days_back, overwrite=overwrite)
    finally:
        conn.close()
    found = {d: n for d, n in results.items() if n != 0}
    failed = sorted(d for d, n in results.items() if n < 0)
    written = sum(n for n in results.values() if n > 0)
    logger.info("prices: %d rows written across %d dates (window %d days, overwrite=%s)",
                written, len([n for n in found.values() if n > 0]), days_back, overwrite)
    if found:
        logger.info("prices: dates written %s .. %s", min(found), max(found))
    if failed:
        raise RuntimeError(f"price store failed for {failed}")


def _kb_ingest() -> None:
    from services.ph_knowledge.ingest import run_knowledge_ingest

    logger.info("kb ingest: %s", run_knowledge_ingest(verbose=False))
    if date.today().weekday() == 0:  # Monday: WESM / IEMOP publication crawl
        logger.info("kb weekly docs: %s", run_knowledge_ingest(only=_WEEKLY_DOC_SOURCES, verbose=False))


def _digest() -> None:
    from services.intl_market_common.expert_memory_base import digest_kb_docs
    from services.ph_knowledge.config import MARKET_CONFIG as cfg

    n = digest_kb_docs(os.environ.get("ANTHROPIC_API_KEY", ""), cfg.table_prefix, cfg.name, limit=100)
    logger.info("digest: %s new insights", n)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["prices", "kb", "digest"], action="append")
    ap.add_argument("--days-back", type=int, default=7)
    ap.add_argument("--overwrite", action="store_true", help="restate stored prices that differ from source")
    args = ap.parse_args(argv)

    steps = {
        "prices": lambda: _prices(args.days_back, args.overwrite),
        "kb": _kb_ingest,
        "digest": _digest,
    }
    failed = []
    for name in args.only or list(steps):
        try:
            steps[name]()
        except Exception:
            logger.exception("step %s failed", name)
            failed.append(name)
    if failed:
        logger.error("FAILED steps: %s", failed)
        return 1
    logger.info("all steps ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
