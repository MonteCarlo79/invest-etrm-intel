"""File watcher that runs periodically to ingest new settlement PDFs.

Usage:
    # Run once (e.g., from Windows Task Scheduler):
    python -m services.settlement_ingest.watcher

    # Or import and call:
    from services.settlement_ingest.watcher import run_watcher
    run_watcher()
"""
from __future__ import annotations

import os
import sys
import logging
from datetime import datetime

# Ensure repo root is in path
_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, _repo_root)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_repo_root, "config", ".env"), override=False)
except ImportError:
    pass

from services.settlement_ingest.scanner import scan_and_ingest, INVOICE_ROOT

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def run_watcher():
    """Run a single scan-and-ingest pass."""
    logger.info(f"Settlement watcher started. Root: {INVOICE_ROOT}")

    results = scan_and_ingest()

    ingested = [r for r in results if r.get("status") == "ingested"]
    skipped = [r for r in results if r.get("status") in ("already_ingested", "skipped")]
    errors = [r for r in results if r.get("status") == "error"]

    logger.info(f"Scan complete: {len(ingested)} ingested, {len(skipped)} skipped, {len(errors)} errors")

    for r in ingested:
        logger.info(f"  INGESTED: {r['path']} → {r['asset']} ({r['month']}, {r['type']}, {r['items']} items)")

    for r in errors:
        logger.error(f"  ERROR: {r['path']} → {r.get('error')}")

    return results


if __name__ == "__main__":
    run_watcher()
