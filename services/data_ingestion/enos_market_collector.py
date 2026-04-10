"""
EnOS Mengxi market data collector.
Wraps bess-marketdata-ingestion/providers/mengxi/ logic.
Adds: RunContext, control table updates, structured logging, dry_run.

Observability notes:
- rows_written = SUM(COUNT(*) across all 8 target tables for the date range),
  queried after the subprocess completes.
- dataset_status updated for every target table, not just the representative one.
"""
from __future__ import annotations
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text

from services.data_ingestion.shared.context import RunContext
from services.data_ingestion.shared.db import get_engine
from services.data_ingestion.shared.logging import get_logger
from services.data_ingestion.shared.control import start_run, finish_run, update_dataset_status

logger = get_logger("enos_market_collector")
COLLECTOR = "enos_market"

# All tables written by bess-marketdata-ingestion/providers/mengxi/load_excel_to_marketdata.py
# Source: batch_downloader.py TABLE_SOURCE_RULES keys + md_avg_bid_price
ENOS_TARGET_TABLES = [
    "marketdata.md_rt_nodal_price",
    "marketdata.md_rt_total_cleared_energy",
    "marketdata.md_da_cleared_energy",
    "marketdata.md_da_fuel_summary",
    "marketdata.md_avg_bid_price",
    "marketdata.md_id_cleared_energy",
    "marketdata.md_id_fuel_summary",
    "marketdata.md_settlement_ref_price",
]


def _count_rows_written(start_date, end_date) -> int:
    """Sum rows across all target tables for the given date range.
    Skips tables that don't exist yet (first run) rather than raising.
    """
    engine = get_engine()
    total = 0
    with engine.connect() as conn:
        for table in ENOS_TARGET_TABLES:
            try:
                n = conn.execute(
                    text(f"SELECT COUNT(*) FROM {table} WHERE data_date BETWEEN :s AND :e"),
                    {"s": start_date, "e": end_date},
                ).scalar() or 0
                total += n
            except Exception:
                pass  # table may not exist on very first run
    return total


def run(ctx: RunContext):
    run_id = start_run(
        collector=COLLECTOR, mode=ctx.mode,
        start_date=ctx.start_date, end_date=ctx.end_date,
        dataset_filter=ctx.dataset_filter, dry_run=ctx.dry_run,
        ecs_task_id=os.environ.get("ECS_TASK_ID"),
    )
    logger.info(json.dumps({
        "event": "run_start", "run_id": run_id, "mode": ctx.mode,
        "start": str(ctx.start_date), "end": str(ctx.end_date),
        "dry_run": ctx.dry_run,
    }))

    if ctx.dry_run:
        logger.info(json.dumps({"event": "dry_run_skip"}))
        finish_run(run_id, "skipped")
        return

    try:
        pipeline_script = (
            Path(__file__).parents[2]
            / "bess-marketdata-ingestion/providers/mengxi/run_pipeline.py"
        )
        env = os.environ.copy()
        env["RUN_MODE"]   = ctx.mode
        env["START_DATE"] = str(ctx.start_date)
        env["END_DATE"]   = str(ctx.end_date)
        # Prepend sys.executable's directory so bare "python" in run_pipeline's
        # subprocess calls (batch_downloader.py, load_excel_to_marketdata.py)
        # resolves to the same interpreter we're running under.
        python_dir = str(Path(sys.executable).parent)
        env["PATH"] = python_dir + os.pathsep + env.get("PATH", "")
        subprocess.run(
            [sys.executable, str(pipeline_script)],
            env=env, check=True,
            cwd=str(pipeline_script.parent),
        )
        logger.info(json.dumps({"event": "pipeline_ok", "script": pipeline_script.name}))

        rows_written = _count_rows_written(ctx.start_date, ctx.end_date)
        logger.info(json.dumps({
            "event": "rows_counted",
            "total": rows_written,
            "tables": len(ENOS_TARGET_TABLES),
        }))

        finish_run(run_id, "success", rows_written=rows_written)
        for table in ENOS_TARGET_TABLES:
            update_dataset_status(COLLECTOR, table, last_date=ctx.end_date)

    except Exception as e:
        logger.error(json.dumps({"event": "pipeline_error", "error": str(e)}))
        finish_run(run_id, "failed", error_message=str(e))
        for table in ENOS_TARGET_TABLES:
            update_dataset_status(COLLECTOR, table, failed=True)
        raise


if __name__ == "__main__":
    ctx = RunContext.from_env_and_args(COLLECTOR)
    run(ctx)
