"""
Backfill news articles for hermes.news_sources from a given start date.

Paginates Sogou to retrieve WeChat articles older than the standard 72h window,
fetches each article body, scores with Claude Haiku, and ingests into the
Strategist knowledge base.

Usage:
    python scripts/backfill_news.py [--start-date 2025-01-01] [--source-id 5] [--dry-run]

Environment:
    PGURL           PostgreSQL connection string
    ANTHROPIC_API_KEY
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

# Ensure repo root is on PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services.hermes.news_screener import backfill_source, get_sources

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill_news")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Hermes news sources")
    parser.add_argument("--start-date", default="2025-01-01", help="ISO date to backfill from (default: 2025-01-01)")
    parser.add_argument("--source-id", type=int, default=None, help="Backfill only this source ID")
    parser.add_argument("--dry-run", action="store_true", help="Discover articles but do not ingest")
    args = parser.parse_args()

    pg_url = os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", "")
    if not pg_url:
        sys.exit("PGURL env var not set")

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key and not args.dry_run:
        logger.warning("ANTHROPIC_API_KEY not set — articles will be ingested without AI scoring")

    try:
        start_date = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc)
    except ValueError:
        sys.exit(f"Invalid --start-date: {args.start_date}")

    sources = get_sources(pg_url, active_only=False)
    if args.source_id:
        sources = [s for s in sources if s["id"] == args.source_id]
        if not sources:
            sys.exit(f"Source ID {args.source_id} not found")

    logger.info("Backfilling %d source(s) from %s", len(sources), args.start_date)

    total_ingested = 0
    total_skipped = 0
    total_errors = 0

    for src in sources:
        logger.info("── Source: %s (type=%s)", src["name"], src["source_type"])
        if args.dry_run:
            from services.hermes.news_screener import _discover_wechat_paginated, _discover_web_articles, _discover_rss_articles
            if src["source_type"] == "wechat":
                arts = _discover_wechat_paginated(src, start_date)
            elif src["source_type"] == "rss":
                arts = _discover_rss_articles(src)
            else:
                arts = _discover_web_articles(src)
            logger.info("  [DRY-RUN] Would ingest %d articles", len(arts))
            for a in arts[:5]:
                logger.info("    • %s  (%s)", a.get("title", "?")[:80], a.get("published_at", "?"))
            if len(arts) > 5:
                logger.info("    … and %d more", len(arts) - 5)
            continue

        summary = backfill_source(src, start_date, pg_url, api_key)
        total_ingested += summary["ingested"]
        total_skipped += summary["skipped"]
        total_errors += summary["errors"]
        logger.info(
            "  → discovered=%d  ingested=%d  skipped=%d  errors=%d",
            summary["discovered"], summary["ingested"], summary["skipped"], summary["errors"],
        )

    if not args.dry_run:
        logger.info(
            "Backfill complete: ingested=%d  skipped=%d  errors=%d",
            total_ingested, total_skipped, total_errors,
        )


if __name__ == "__main__":
    main()
