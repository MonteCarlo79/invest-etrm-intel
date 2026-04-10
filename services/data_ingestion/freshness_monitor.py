"""
Freshness monitor + gap remediation.
1. Reads ops.ingestion_expected_freshness
2. For each dataset, checks MAX(date_col) in target table
3. If lag > max_lag_days, inserts gap into ops.ingestion_gap_queue
4. Optionally launches ECS reconcile tasks for pending gaps (ECS_DISPATCH=true)
"""
from __future__ import annotations
import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from services.data_ingestion.shared.db import get_engine
from services.data_ingestion.shared.logging import get_logger
from services.data_ingestion.shared.control import queue_gap
from sqlalchemy import text

logger = get_logger("freshness_monitor")


def check_freshness(engine, today: date) -> list[dict]:
    gaps = []
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT dataset, collector, date_column, max_lag_days
            FROM ops.ingestion_expected_freshness
            WHERE active = TRUE
        """)).fetchall()

    for dataset, collector, date_col, max_lag in rows:
        try:
            with engine.connect() as conn:
                val = conn.execute(
                    text(f"SELECT MAX({date_col}::date) FROM {dataset}")
                ).scalar()
        except Exception as e:
            logger.warning(json.dumps({
                "event": "freshness_check_error",
                "dataset": dataset, "error": str(e),
            }))
            continue

        if val is None:
            logger.warning(json.dumps({"event": "empty_table", "dataset": dataset}))
            continue

        lag = (today - val).days
        if lag > max_lag:
            gap_start = val + timedelta(days=1)
            gap_end   = today - timedelta(days=1)
            logger.info(json.dumps({
                "event": "gap_found", "dataset": dataset,
                "lag_days": lag,
                "gap_start": str(gap_start), "gap_end": str(gap_end),
            }))
            queue_gap(collector, dataset, gap_start, gap_end)
            gaps.append({
                "dataset": dataset, "collector": collector,
                "gap_start": gap_start, "gap_end": gap_end,
            })
    return gaps


def dispatch_gaps(engine, gaps: list[dict]):
    """Launch ECS tasks for each pending gap (if ECS_DISPATCH=true).

    Required IAM permissions on the ECS task role (aws_iam_role.task_role):
      ecs:RunTask       on arn:aws:ecs:<region>:<account>:task-definition/<family>:*
      iam:PassRole      on ecs_execution_role_arn and ecs_task_role_arn
      ecs:DescribeTasks on the cluster (optional, for status checks)

    Enable via ECS_DISPATCH=true in the task environment ONLY after confirming:
      1. IAM permissions above are in place.
      2. At least one collector is producing data (ops.ingestion_dataset_status populated).
    """
    if os.environ.get("ECS_DISPATCH", "").lower() not in ("1", "true"):
        logger.info(json.dumps({"event": "dispatch_skipped", "reason": "ECS_DISPATCH not set"}))
        return

    import boto3
    ecs = boto3.client("ecs", region_name=os.environ.get("AWS_REGION", "ap-southeast-1"))
    cluster = os.environ["ECS_CLUSTER"]

    collector_task_def = {
        "enos_market": os.environ.get("ENOS_MARKET_TASK_DEF", ""),
        "tt_api":      os.environ.get("TT_API_TASK_DEF", ""),
        "lingfeng":    os.environ.get("LINGFENG_TASK_DEF", ""),
    }

    for gap in gaps:
        task_def = collector_task_def.get(gap["collector"])
        if not task_def:
            logger.warning(json.dumps({
                "event": "dispatch_skipped_no_task_def",
                "collector": gap["collector"],
            }))
            continue
        try:
            ecs.run_task(
                cluster=cluster,
                taskDefinition=task_def,
                launchType="FARGATE",
                networkConfiguration={"awsvpcConfiguration": {
                    "subnets": os.environ["PRIVATE_SUBNETS"].split(","),
                    "securityGroups": os.environ["TASK_SECURITY_GROUPS"].split(","),
                    "assignPublicIp": "DISABLED",
                }},
                overrides={"containerOverrides": [{
                    "name": gap["collector"].replace("_", "-") + "-collector",
                    "environment": [
                        {"name": "RUN_MODE",   "value": "reconcile"},
                        {"name": "START_DATE", "value": str(gap["gap_start"])},
                        {"name": "END_DATE",   "value": str(gap["gap_end"])},
                    ],
                }]},
            )
            logger.info(json.dumps({
                "event": "gap_dispatched",
                "collector": gap["collector"],
                "task_def": task_def,
                "start": str(gap["gap_start"]), "end": str(gap["gap_end"]),
            }))
        except Exception as e:
            logger.error(json.dumps({
                "event": "dispatch_error",
                "collector": gap["collector"],
                "task_def": task_def,
                "start": str(gap["gap_start"]), "end": str(gap["gap_end"]),
                "error": str(e),
                "hint": "Check ecs:RunTask + iam:PassRole permissions on the ECS task role",
            }))


if __name__ == "__main__":
    _engine = get_engine()
    _today = date.today()
    _gaps = check_freshness(_engine, _today)
    logger.info(json.dumps({"event": "gaps_found", "count": len(_gaps)}))
    dispatch_gaps(_engine, _gaps)
