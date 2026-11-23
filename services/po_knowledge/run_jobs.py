"""Headless daily jobs for the Poland market app.

Same jobs as the Streamlit app's in-process scheduler, runnable from a one-shot ECS
task (EventBridge rule bess-po-scheduler-daily) so data stays current with the web
UI stopped. Every step is idempotent, so the weekly/monthly scrapes simply run daily.

    python -m services.po_knowledge.run_jobs
    python -m services.po_knowledge.run_jobs --only prices --days-back 120

Exit code 1 if any step failed.
"""
import argparse
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [po-jobs] %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("po-jobs")


def _connect():
    import psycopg2

    conn = psycopg2.connect(os.environ["PGURL"], keepalives=1, keepalives_idle=30)
    conn.autocommit = True
    return conn


def _prices(days_back: int) -> None:
    from services.po_knowledge.entso_scraper import run_entso_price_scrape

    conn = _connect()
    try:
        results = run_entso_price_scrape(conn, days_back=days_back)
    finally:
        conn.close()
    failed = sorted(d for d, n in results.items() if n < 0)
    logger.info("prices: %d rows written (window %d days)", sum(n for n in results.values() if n > 0), days_back)
    if failed:
        raise RuntimeError(f"price store failed for {failed}")


def _ancillary() -> None:
    from services.po_knowledge.entso_scraper import scrape_po_afrr_prices, scrape_po_fcr_prices

    conn = _connect()
    try:
        logger.info("ancillary: FCR=%d rows, aFRR=%d rows",
                    scrape_po_fcr_prices(conn, weeks_back=4), scrape_po_afrr_prices(conn, weeks_back=4))
    finally:
        conn.close()


def _capacity() -> None:
    from services.po_knowledge.entso_scraper import scrape_po_capacity_market

    conn = _connect()
    try:
        logger.info("capacity market: %d rows", scrape_po_capacity_market(conn))
    finally:
        conn.close()


def _kb_ingest() -> None:
    from services.po_knowledge.ingest import run_knowledge_ingest

    logger.info("kb ingest: %s", run_knowledge_ingest(verbose=False))


def _digest() -> None:
    from services.intl_market_common.expert_memory_base import digest_kb_docs
    from services.po_knowledge.config import MARKET_CONFIG as cfg

    n = digest_kb_docs(os.environ.get("ANTHROPIC_API_KEY", ""), cfg.table_prefix, cfg.name, limit=100)
    logger.info("digest: %s new insights", n)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["prices", "ancillary", "capacity", "kb", "digest"], action="append")
    ap.add_argument("--days-back", type=int, default=7)
    args = ap.parse_args(argv)

    steps = {
        "prices": lambda: _prices(args.days_back),
        "ancillary": _ancillary,
        "capacity": _capacity,
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
