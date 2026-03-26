# common/job_control.py

from sqlalchemy import create_engine, text
import os

def get_engine():
    return create_engine(os.getenv("PGURL"))

def ensure_job_table():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS pipeline_job_status (
                job_name TEXT PRIMARY KEY,
                status TEXT,
                progress_percent INT DEFAULT 0,
                message TEXT,
                started_at TIMESTAMP,
                updated_at TIMESTAMP DEFAULT now()
            );
        """))

def ensure_job_row(job_name):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO pipeline_job_status (job_name, status, progress_percent, message)
            VALUES (:job, 'idle', 0, 'Not started')
            ON CONFLICT (job_name) DO NOTHING
        """), {"job": job_name})

def set_job_status(job_name, status, progress=None, message=None):
    engine = get_engine()

    sql = """
        UPDATE pipeline_job_status
        SET status = :status,
            updated_at = now()
    """

    params = {
        "job": job_name,
        "status": status
    }

    if progress is not None:
        sql += ", progress_percent = :progress"
        params["progress"] = progress

    if message is not None:
        sql += ", message = :message"
        params["message"] = message

    if status == "running":
        sql += ", started_at = now()"

    sql += " WHERE job_name = :job"

    with engine.begin() as conn:
        conn.execute(text(sql), params)