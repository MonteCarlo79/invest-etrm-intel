"""
services/monitoring/run_realization_monitor.py

Batch job: compute and persist daily realization status for all BESS assets.

Reads from: reports.bess_asset_daily_attribution
Writes to:  monitoring.asset_realization_status

Must run AFTER the daily attribution job (run_daily_attribution.py or
run_pnl_refresh.py) has populated the attribution table for today.

Usage:
    python -m services.monitoring.run_realization_monitor
    python -m services.monitoring.run_realization_monitor --date 2026-04-18
    python -m services.monitoring.run_realization_monitor --lookback 14
"""
from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import date, timedelta
from typing import List

from services.common.db_utils import get_engine
from services.monitoring.realization_monitor import (
    compute_realization_status,
    upsert_realization_status,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

_ASSET_CODES: List[str] = [
    "suyou", "wulate", "wuhai", "wulanchabu",
    "hetao", "hangjinqi", "siziwangqi", "gushanliang",
]
_DEFAULT_LOOKBACK = int(os.getenv("REALIZATION_LOOKBACK_DAYS", "30"))


def ensure_monitoring_schema(engine) -> None:
    """Create monitoring schema and table if they don't exist."""
    import pathlib
    from sqlalchemy import text

    ddl_path = pathlib.Path(__file__).parents[2] / "db" / "ddl" / "monitoring" / "asset_realization_status.sql"
    if ddl_path.exists():
        ddl = ddl_path.read_text()
        with engine.begin() as conn:
            for stmt in ddl.split(";"):
                sql = stmt.strip()
                if sql and not sql.startswith("--"):
                    conn.execute(text(sql))
    else:
        logger.warning("DDL file not found at %s — table must exist already", ddl_path)


def run_for_date(snapshot_date: date, engine, lookback_days: int = _DEFAULT_LOOKBACK) -> int:
    """Compute and upsert realization status for all assets on snapshot_date."""
    t0 = time.monotonic()
    rows = []
    for asset_code in _ASSET_CODES:
        try:
            row = compute_realization_status(asset_code, snapshot_date, engine, lookback_days)
            rows.append(row)
            logger.debug(
                "%s %s: status=%s ratio=%s",
                asset_code, snapshot_date,
                row["status_level"],
                f"{row['realization_ratio']:.2f}" if row["realization_ratio"] is not None else "N/A",
            )
        except Exception as exc:
            logger.error("Failed computing realization for %s on %s: %s", asset_code, snapshot_date, exc)

    if rows:
        upsert_realization_status(engine, rows)

    # B5: structured MONITORING_RUN event for ops dashboards / log aggregation
    status_counts = {}
    for r in rows:
        lvl = r["status_level"]
        status_counts[lvl] = status_counts.get(lvl, 0) + 1
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    logger.info(
        "MONITORING_RUN job=realization_monitor date=%s assets=%d lookback_days=%d "
        "NORMAL=%d WARN=%d ALERT=%d CRITICAL=%d DATA_ABSENT=%d INDETERMINATE=%d elapsed_ms=%d",
        snapshot_date, len(rows), lookback_days,
        status_counts.get("NORMAL", 0),
        status_counts.get("WARN", 0),
        status_counts.get("ALERT", 0),
        status_counts.get("CRITICAL", 0),
        status_counts.get("DATA_ABSENT", 0),
        status_counts.get("INDETERMINATE", 0),
        elapsed_ms,
    )
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run daily realization monitor batch job")
    parser.add_argument("--date", type=str, default=None,
                        help="ISO snapshot date (default: today)")
    parser.add_argument("--lookback", type=int, default=_DEFAULT_LOOKBACK,
                        help=f"Rolling window in days (default: {_DEFAULT_LOOKBACK})")
    args = parser.parse_args()

    snapshot_date = date.fromisoformat(args.date) if args.date else date.today()
    engine = get_engine()

    ensure_monitoring_schema(engine)
    count = run_for_date(snapshot_date, engine, args.lookback)
    logger.info("run_realization_monitor complete — %d rows written", count)


if __name__ == "__main__":
    main()
