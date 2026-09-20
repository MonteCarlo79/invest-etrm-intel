# services/citic_futures/run_weekly.py
"""Local orchestrator: scan → ingest new weekly folders → record in
staging.citic_ingest_log. The digest itself is generated and sent by the
ECS-side job (services/citic_futures/digest_job.py, hermes cron) — Anthropic
calls are geo-blocked from the Mac, so no LLM work happens here.

Usage (env from config/.env):
    python services/citic_futures/run_weekly.py [--base-dir data/zhongxin-futures]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

logger = logging.getLogger("citic_weekly")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-dir", default="data/zhongxin-futures")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from services.citic_futures.ingest_weekly import (
        find_pending_weekly_folders, ingest_folder, mark_folder_done)
    from services.citic_futures.digest_job import record_ingest

    base_dir = Path(args.base_dir)
    pg_url = os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", "")
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")  # vision extraction only; degrades to text-only if blocked
    if not pg_url:
        raise SystemExit("PGURL not set")

    pending = find_pending_weekly_folders(base_dir)
    if not pending:
        logger.info("no pending weekly folders")
        return

    for folder in pending:
        logger.info("processing %s", folder.name)
        result = ingest_folder(folder, api_key=api_key)
        logger.info("ingested %d new, %d dup, %d failed",
                    result["ingested"], result["skipped_dup"], len(result["failed"]))
        if result["failed"]:
            logger.warning("folder %s had %d failed files — NOT marking done", folder.name, len(result["failed"]))
            continue
        doc_ids = [doc_id for doc_id, _name, _g in result["relevant_docs"]]
        record_ingest(pg_url, folder.name, doc_ids,
                      result["ingested"], result["skipped_dup"])
        mark_folder_done(base_dir, folder)
        logger.info("recorded ingest for digest job; marked done: %s", folder.name)


if __name__ == "__main__":
    main()
