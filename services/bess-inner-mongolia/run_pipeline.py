# -*- coding: utf-8 -*-

import sys
import os
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text

from batch_downloader import batch_download
from load_excel_to_marketdata import main

from common.job_control import (
    ensure_job_table,
    ensure_job_row,
    set_job_status
)

JOB_NAME = "inner_mongolia"


def ensure_pipeline_tables(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.pipeline_file_log (
                id SERIAL PRIMARY KEY,
                job_name TEXT NOT NULL,
                file_date DATE NOT NULL,
                stage TEXT NOT NULL,
                message TEXT,
                created_at TIMESTAMP DEFAULT now()
            );
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_pipeline_file_log_job
            ON public.pipeline_file_log (job_name);
        """))


def log_file_stage(engine, job_name, file_date, stage, message=""):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO pipeline_file_log (job_name, file_date, stage, message)
            VALUES (:job, :date, :stage, :msg)
        """), {
            "job": job_name,
            "date": file_date,
            "stage": stage,
            "msg": message
        })


if __name__ == "__main__":

    pgurl = os.getenv("PGURL")
    if not pgurl:
        raise RuntimeError("PGURL not set")

    engine = create_engine(pgurl)

    ensure_job_table()
    ensure_job_row(JOB_NAME)
    ensure_pipeline_tables(engine)

    start = sys.argv[1]
    end = sys.argv[2]

    try:
        # -----------------------------
        # MARK RUNNING
        # -----------------------------
        set_job_status(JOB_NAME, "running", 0, "Starting pipeline")

        start_dt = datetime.strptime(start, "%Y-%m-%d")
        end_dt = datetime.strptime(end, "%Y-%m-%d")
        total_days = (end_dt - start_dt).days + 1

        current = start_dt
        i = 0

        # -----------------------------
        # DOWNLOAD LOOP
        # -----------------------------
        while current <= end_dt:

            date_str = current.strftime("%Y-%m-%d")

            log_file_stage(engine, JOB_NAME, date_str, "downloading", "Downloading date")

            batch_download(date_str, date_str)

            i += 1
            progress = 10 + int((i / total_days) * 40)

            set_job_status(
                JOB_NAME,
                "running",
                progress,
                f"Downloaded {date_str}"
            )

            current += timedelta(days=1)

        # -----------------------------
        # LOAD PHASE
        # -----------------------------
        log_file_stage(engine, JOB_NAME, start, "loading", "Loading to DB")

        set_job_status(JOB_NAME, "running", 60, "Loading to database")

        main("/tmp/output")

        # -----------------------------
        # COMPLETE
        # -----------------------------
        log_file_stage(engine, JOB_NAME, start, "completed", "Done")

        set_job_status(JOB_NAME, "completed", 100, "Completed successfully")

    except Exception as e:

        log_file_stage(engine, JOB_NAME, start, "failed", str(e))

        set_job_status(
            JOB_NAME,
            "failed",
            0,
            f"Failed: {str(e)}"
        )

        raise