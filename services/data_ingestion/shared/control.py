"""ops schema control table helpers."""
from __future__ import annotations
from typing import Optional
from sqlalchemy import text
from .db import get_engine


def start_run(collector: str, mode: str, start_date, end_date,
              dataset_filter=None, dry_run=False, ecs_task_id=None) -> int:
    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(text("""
            INSERT INTO ops.ingestion_job_runs
              (collector, run_mode, start_date, end_date, dataset_filter, dry_run, ecs_task_id)
            VALUES (:c, :m, :s, :e, :df, :dr, :t)
            RETURNING id
        """), {"c": collector, "m": mode, "s": start_date, "e": end_date,
               "df": dataset_filter, "dr": dry_run, "t": ecs_task_id})
        return row.scalar()


def finish_run(run_id: int, status: str, rows_written: Optional[int] = None,
               error_message: Optional[str] = None):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE ops.ingestion_job_runs
               SET status=:s, rows_written=:r, error_message=:e,
                   finished_at=now()
             WHERE id=:id
        """), {"s": status, "r": rows_written, "e": error_message, "id": run_id})


def update_dataset_status(collector: str, dataset: str, last_date=None,
                          failed: bool = False):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO ops.ingestion_dataset_status
              (collector, dataset, last_run_at, last_success_at, last_date_seen, failure_count)
            VALUES (:c, :d, now(),
                    CASE WHEN :ok THEN now() ELSE NULL END,
                    :ld, CASE WHEN :ok THEN 0 ELSE 1 END)
            ON CONFLICT (collector, dataset) DO UPDATE SET
              last_run_at     = now(),
              last_success_at = CASE WHEN :ok THEN now()
                                     ELSE ingestion_dataset_status.last_success_at END,
              last_date_seen  = COALESCE(:ld, ingestion_dataset_status.last_date_seen),
              failure_count   = CASE WHEN :ok THEN 0
                                     ELSE ingestion_dataset_status.failure_count + 1 END,
              updated_at      = now()
        """), {"c": collector, "d": dataset, "ok": not failed, "ld": last_date})


def queue_gap(collector: str, dataset: str, gap_start, gap_end):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO ops.ingestion_gap_queue
              (dataset, collector, gap_start, gap_end)
            VALUES (:d, :c, :s, :e)
            ON CONFLICT (dataset, gap_start, gap_end, status) DO NOTHING
        """), {"d": dataset, "c": collector, "s": gap_start, "e": gap_end})
