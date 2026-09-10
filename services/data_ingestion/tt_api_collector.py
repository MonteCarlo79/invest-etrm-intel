"""
TT DAAS API collector.
Wraps province_misc_to_db_v2.py (province alias + misc tables)
and column_to_matrix_all.py (node-level 15min/hourly matrix writes).
Adds: RunContext, control table updates, structured logging, dry_run.

Import note: province_misc_to_db_v2 reads HIST_START_DATE / HIST_END_DATE at
module-level (os.getenv at import time). The env vars are therefore set BEFORE
the lazy import inside run(), ensuring the correct dates are captured. This works
correctly in ECS (fresh process) and in local invocations. Do NOT move the import
to the top of the file.
"""
from __future__ import annotations
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from services.data_ingestion.shared.context import RunContext
from services.data_ingestion.shared.logging import get_logger, emit_metrics
from services.data_ingestion.shared.control import start_run, finish_run, update_dataset_status

logger = get_logger("tt_api_collector")
COLLECTOR = "tt_api"


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
    }))

    if ctx.dry_run:
        logger.info(json.dumps({"event": "dry_run_skip"}))
        finish_run(run_id, "skipped")
        return

    # Set env vars BEFORE importing province_misc_to_db_v2 — its module-level
    # constants HIST_START_DATE / HIST_END_DATE are read at import time.
    os.environ["HIST_START_DATE"] = str(ctx.start_date)
    os.environ["HIST_END_DATE"]   = str(ctx.end_date)
    os.environ["FULL_HISTORY"]    = "false"

    try:
        # Lazy import: captures the env vars set above at module init time.
        from services.loader.province_misc_to_db_v2 import main as province_main
        province_main()
        logger.info(json.dumps({"event": "province_misc_ok",
                                "start": str(ctx.start_date), "end": str(ctx.end_date)}))

        from column_to_matrix_all import Column_to_Matrix, MARKET_MAP
        markets = (ctx.dataset_filter or ",".join(MARKET_MAP.keys())).split(",")
        markets_run = []
        for market in markets:
            market = market.strip()
            if market in MARKET_MAP:
                Column_to_Matrix("", market)
                markets_run.append(market)
        logger.info(json.dumps({"event": "column_to_matrix_ok", "markets": markets_run}))

        finish_run(run_id, "success", rows_written=len(markets_run))
        update_dataset_status(COLLECTOR, "public.hist_*", last_date=ctx.end_date)
    except Exception as e:
        logger.error(json.dumps({"event": "collector_error", "error": str(e)}))
        finish_run(run_id, "failed", error_message=str(e))
        update_dataset_status(COLLECTOR, "public.hist_*", failed=True)
        raise


if __name__ == "__main__":
    ctx = RunContext.from_env_and_args(COLLECTOR)
    run(ctx)
