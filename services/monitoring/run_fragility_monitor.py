"""
services/monitoring/run_fragility_monitor.py

Batch job: compute and persist daily fragility status for all BESS assets.

Reads from: monitoring.asset_realization_status (must be populated first)
            reports.bess_asset_daily_attribution (for trend computation)
Writes to:  monitoring.asset_fragility_status

Run order:
    1. services/monitoring/run_daily_attribution.py   (or run_pnl_refresh.py)
    2. services/monitoring/run_realization_monitor.py
    3. services/monitoring/run_fragility_monitor.py   ← this job

Usage:
    python -m services.monitoring.run_fragility_monitor
    python -m services.monitoring.run_fragility_monitor --date 2026-04-18
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import date
from typing import List

from services.common.db_utils import get_engine
from services.monitoring.fragility_monitor import (
    compute_fragility_status,
    upsert_fragility_status,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

_ASSET_CODES: List[str] = [
    "suyou", "wulate", "wuhai", "wulanchabu",
    "hetao", "hangjinqi", "siziwangqi", "gushanliang",
]
_DEFAULT_LOOKBACK = int(os.getenv("REALIZATION_LOOKBACK_DAYS", "30"))


def ensure_fragility_schema(engine) -> None:
    """Create monitoring schema and fragility table if they don't exist."""
    import pathlib
    from sqlalchemy import text

    ddl_path = pathlib.Path(__file__).parents[2] / "db" / "ddl" / "monitoring" / "asset_fragility_status.sql"
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
    """Compute and upsert fragility status for all assets on snapshot_date."""
    rows = []
    for asset_code in _ASSET_CODES:
        try:
            row = compute_fragility_status(asset_code, snapshot_date, engine, lookback_days)
            rows.append(row)
            logger.debug(
                "%s %s: fragility=%s score=%.3f",
                asset_code, snapshot_date,
                row["fragility_level"], row["composite_score"],
            )
        except Exception as exc:
            logger.error("Failed computing fragility for %s on %s: %s", asset_code, snapshot_date, exc)

    if rows:
        upsert_fragility_status(engine, rows)
        high_count = sum(1 for r in rows if r["fragility_level"] in ("HIGH", "CRITICAL"))
        logger.info(
            "Fragility status written for %d assets on %s. HIGH/CRITICAL: %d.",
            len(rows), snapshot_date, high_count,
        )
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run daily fragility monitor batch job")
    parser.add_argument("--date", type=str, default=None,
                        help="ISO snapshot date (default: today)")
    parser.add_argument("--lookback", type=int, default=_DEFAULT_LOOKBACK,
                        help=f"Realization lookback window in days (default: {_DEFAULT_LOOKBACK})")
    args = parser.parse_args()

    snapshot_date = date.fromisoformat(args.date) if args.date else date.today()
    engine = get_engine()

    ensure_fragility_schema(engine)
    count = run_for_date(snapshot_date, engine, args.lookback)
    logger.info("run_fragility_monitor complete — %d rows written", count)


if __name__ == "__main__":
    main()
