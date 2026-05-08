import json
import os
import subprocess
import time
from datetime import datetime, timedelta
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import psycopg2

OUTPUT_DIR = "./output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

mode = os.getenv("RUN_MODE", "daily")

START_DATE = os.getenv("START_DATE")
END_DATE = os.getenv("END_DATE")
RECONCILE_DAYS = os.getenv("RECONCILE_DAYS")
REMEDIATION_BATCH_SIZE = int(os.getenv("REMEDIATION_BATCH_SIZE", "7"))

MARKET_LAG_DAYS = int(os.getenv("MARKET_LAG_DAYS", "1"))
DB_DSN = os.getenv("PGURL") or os.getenv("DB_DSN")
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "").strip()
ALERT_CONTEXT = os.getenv("ALERT_CONTEXT", "mengxi-ingestion")
PIPELINE_NAME = os.getenv("PIPELINE_NAME", "bess-mengxi-ingestion")
DB_CONNECT_TIMEOUT_SECONDS = int(os.getenv("DB_CONNECT_TIMEOUT_SECONDS", "30"))

today = datetime.utcnow().date()
latest_available = today - timedelta(days=MARKET_LAG_DAYS)

print("========== PIPELINE CONFIG ==========")
print("RUN_MODE:", mode)
print("START_DATE:", START_DATE)
print("END_DATE:", END_DATE)
print("RECONCILE_DAYS:", RECONCILE_DAYS)
print("REMEDIATION_BATCH_SIZE:", REMEDIATION_BATCH_SIZE)
print("MARKET_LAG_DAYS:", MARKET_LAG_DAYS)
print("LATEST_AVAILABLE_DATE:", latest_available)
print("=====================================")


# ------------------------------------------------
# DB CONNECTION CHECK
# ------------------------------------------------

def extract_db_host(dsn):

    if not dsn:
        return None

    parsed = urlparse(dsn)
    return parsed.hostname


def send_alert(payload):

    if not ALERT_WEBHOOK_URL:
        return

    body = json.dumps(payload).encode("utf-8")
    request = Request(
        ALERT_WEBHOOK_URL,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urlopen(request, timeout=10) as response:
            print("Alert sent with status:", response.status)
    except Exception as alert_error:
        print("Alert delivery failed:", str(alert_error))


def build_db_timeout_alert(max_attempts, delay, last_error):

    summary = (
        "Mengxi ingestion alert: DB connectivity timeout. "
        "The ECS task could not reach Postgres on port 5432 after repeated retries. "
        "This is likely an infra/network reachability issue rather than a SQL/query error."
    )

    return {
        "text": summary,
        "pipeline_name": PIPELINE_NAME,
        "alert_context": ALERT_CONTEXT,
        "run_mode": mode,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "error_class": "db_connect_timeout",
        "db_host": extract_db_host(DB_DSN),
        "retry_attempts": max_attempts,
        "retry_delay_seconds": delay,
        "db_connect_timeout_seconds": DB_CONNECT_TIMEOUT_SECONDS,
        "utc_timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "error_message": str(last_error),
    }

def query_load_issues(window_start, window_end):
    """Return (load_issues, quality_issues, missing_no_record) for the given date window."""
    if not DB_DSN:
        return [], [], []
    try:
        conn = psycopg2.connect(DB_DSN, connect_timeout=DB_CONNECT_TIMEOUT_SECONDS)
        cur = conn.cursor()

        cur.execute(
            """
            SELECT file_date, status, message
            FROM marketdata.md_load_log
            WHERE file_date BETWEEN %s::date AND %s::date
              AND status IN ('failed', 'partial_success')
            ORDER BY file_date
            """,
            (window_start, window_end),
        )
        load_issues = [
            {"date": str(r[0]), "status": r[1], "message": (r[2] or "")[:500]}
            for r in cur.fetchall()
        ]

        cur.execute(
            """
            SELECT data_date, interval_coverage, notes
            FROM marketdata.data_quality_status
            WHERE data_date BETWEEN %s::date AND %s::date
              AND is_complete = FALSE
            ORDER BY data_date
            """,
            (window_start, window_end),
        )
        quality_issues = [
            {"date": str(r[0]), "coverage": float(r[1] or 0), "notes": (r[2] or "")[:300]}
            for r in cur.fetchall()
        ]

        # Weekdays with no quality record (download likely failed entirely)
        cur.execute(
            """
            WITH weekdays AS (
                SELECT d::date AS dt
                FROM generate_series(%s::date, %s::date, interval '1 day') d
                WHERE extract(isodow from d) < 6
                  AND d::date < CURRENT_DATE
            )
            SELECT w.dt
            FROM weekdays w
            LEFT JOIN marketdata.data_quality_status dq
              ON dq.data_date = w.dt AND dq.province = 'mengxi'
            WHERE dq.data_date IS NULL
            ORDER BY w.dt
            """,
            (window_start, window_end),
        )
        missing_no_record = [str(r[0]) for r in cur.fetchall()]

        conn.close()
        return load_issues, quality_issues, missing_no_record

    except Exception as e:
        print(f"[WARN] Could not query load issues: {e}")
        return [], [], []


def build_load_issues_alert(load_issues, quality_issues, missing_no_record, window_start, window_end):
    failed_dates = [i["date"] for i in load_issues if i["status"] == "failed"]
    partial_dates = [i["date"] for i in load_issues if i["status"] == "partial_success"]
    incomplete_quality = [i["date"] for i in quality_issues]

    parts = []
    if failed_dates:
        parts.append(f"failed loads ({len(failed_dates)}): {', '.join(failed_dates)}")
    if partial_dates:
        parts.append(f"partial loads ({len(partial_dates)}): {', '.join(partial_dates)}")
    if incomplete_quality:
        parts.append(f"incomplete data ({len(incomplete_quality)}): {', '.join(incomplete_quality)}")
    if missing_no_record:
        parts.append(f"no record at all ({len(missing_no_record)}): {', '.join(missing_no_record)}")

    summary = (
        f"Mengxi ingestion alert: data quality issues for {window_start} → {window_end}. "
        + "; ".join(parts)
    )
    return {
        "text": summary,
        "pipeline_name": PIPELINE_NAME,
        "alert_context": ALERT_CONTEXT,
        "run_mode": mode,
        "window_start": window_start,
        "window_end": window_end,
        "error_class": "load_data_quality",
        "utc_timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "failed_dates": failed_dates,
        "partial_dates": partial_dates,
        "incomplete_quality_dates": incomplete_quality,
        "missing_no_record_dates": missing_no_record,
    }


def check_and_alert_load_issues(window_start, window_end):
    load_issues, quality_issues, missing_no_record = query_load_issues(window_start, window_end)
    if load_issues or quality_issues or missing_no_record:
        payload = build_load_issues_alert(
            load_issues, quality_issues, missing_no_record, window_start, window_end
        )
        print("[ALERT] Load/quality issues:", payload["text"])
        send_alert(payload)
    else:
        print(f"[OK] Data quality check passed for {window_start} → {window_end}")


def build_pipeline_crash_alert(error, window_start, window_end):
    summary = (
        f"Mengxi ingestion alert: pipeline crashed during {mode} mode "
        f"({window_start} → {window_end}). "
        f"{type(error).__name__}: {str(error)[:500]}"
    )
    return {
        "text": summary,
        "pipeline_name": PIPELINE_NAME,
        "alert_context": ALERT_CONTEXT,
        "run_mode": mode,
        "window_start": window_start,
        "window_end": window_end,
        "error_class": "pipeline_crash",
        "utc_timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "error_message": str(error),
    }


def wait_for_db(max_attempts=10, delay=10):

    if not DB_DSN:
        print("No DB connection string provided — skipping DB check")
        return

    for attempt in range(1, max_attempts + 1):

        try:
            print(f"Checking DB connectivity (attempt {attempt})...")

            conn = psycopg2.connect(DB_DSN, connect_timeout=DB_CONNECT_TIMEOUT_SECONDS)
            conn.close()

            print("Database connection successful")
            return

        except Exception as e:
            print("DB connection failed:", str(e))

            if attempt == max_attempts:
                send_alert(build_db_timeout_alert(max_attempts, delay, e))
                raise RuntimeError("Database not reachable") from e

            time.sleep(delay)


# ------------------------------------------------
# STEP 1 — VERIFY DB FIRST
# ------------------------------------------------

wait_for_db()


# ------------------------------------------------
# SHARED EXEC HELPERS
# ------------------------------------------------

def run_loader_with_retry():
    print("Loading Excel files to database")

    max_attempts = 5

    for attempt in range(1, max_attempts + 1):
        try:
            wait_for_db()
            subprocess.run(
                ["python", "load_excel_to_marketdata.py", OUTPUT_DIR],
                check=True
            )
            return
        except subprocess.CalledProcessError:
            print(f"Load attempt {attempt} failed")
            if attempt == max_attempts:
                raise
            print("Retrying in 15 seconds...")
            time.sleep(15)


def run_downloader():
    subprocess.run(["python", "batch_downloader.py"], check=True)


def resolve_window_for_non_daily():
    if not START_DATE:
        raise RuntimeError("START_DATE must be set for reconcile/remediation mode")

    start_date = datetime.strptime(START_DATE, "%Y-%m-%d").date()

    if END_DATE:
        end_date = datetime.strptime(END_DATE, "%Y-%m-%d").date()
    elif RECONCILE_DAYS:
        end_date = start_date + timedelta(days=int(RECONCILE_DAYS) - 1)
    else:
        end_date = latest_available

    return start_date, end_date


def chunked(values, size):
    for i in range(0, len(values), size):
        yield values[i:i + size]


# ------------------------------------------------
# STEP 2 — DOWNLOAD DATA
# ------------------------------------------------

_window_start = None
_window_end = None

try:
    if mode == "reconcile":

        print("Running reconciliation mode")

        start_date, end_date = resolve_window_for_non_daily()

        print(f"Reconciling window: {start_date} → {end_date}")

        _window_start = start_date.strftime("%Y-%m-%d")
        _window_end = end_date.strftime("%Y-%m-%d")
        os.environ["START_DATE"] = _window_start
        os.environ["END_DATE"] = _window_end
        os.environ.pop("EXACT_DATES", None)
        run_downloader()
        run_loader_with_retry()
        check_and_alert_load_issues(_window_start, _window_end)

    elif mode == "remediation":
        print("Running remediation mode (targeted missing dates)")

        start_date, end_date = resolve_window_for_non_daily()
        _window_start = start_date.strftime("%Y-%m-%d")
        _window_end = end_date.strftime("%Y-%m-%d")

        print(f"Remediation window: {_window_start} → {_window_end}")

        from batch_downloader import get_missing_dates
        missing_dates = get_missing_dates(_window_start, _window_end)

        if not missing_dates:
            print("No missing dates found in remediation window")
        else:
            print(f"Targeted remediation dates: {len(missing_dates)}")
            for chunk in chunked(missing_dates, max(1, REMEDIATION_BATCH_SIZE)):
                print(f"Remediation chunk: {chunk[0]} → {chunk[-1]} ({len(chunk)} dates)")
                os.environ["EXACT_DATES"] = json.dumps(chunk, ensure_ascii=False)
                os.environ["START_DATE"] = chunk[0]
                os.environ["END_DATE"] = chunk[-1]
                run_downloader()
                run_loader_with_retry()
        check_and_alert_load_issues(_window_start, _window_end)

    else:
        target_day = latest_available.strftime("%Y-%m-%d")
        _window_start = target_day
        _window_end = target_day
        print("Daily ingestion for:", target_day)
        os.environ["START_DATE"] = target_day
        os.environ["END_DATE"] = target_day
        os.environ.pop("EXACT_DATES", None)
        subprocess.run(
            ["python", "batch_downloader.py", target_day, target_day],
            check=True
        )
        run_loader_with_retry()
        check_and_alert_load_issues(_window_start, _window_end)

    print("Pipeline completed successfully")

except Exception as _pipeline_error:
    print(f"[FATAL] Pipeline crashed: {_pipeline_error}")
    send_alert(build_pipeline_crash_alert(
        _pipeline_error,
        _window_start or "unknown",
        _window_end or "unknown",
    ))
    raise
