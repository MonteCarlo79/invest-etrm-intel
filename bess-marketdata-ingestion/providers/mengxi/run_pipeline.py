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

if mode == "reconcile":

    print("Running reconciliation mode")

    start_date, end_date = resolve_window_for_non_daily()

    print(f"Reconciling window: {start_date} → {end_date}")

    os.environ["START_DATE"] = start_date.strftime("%Y-%m-%d")
    os.environ["END_DATE"] = end_date.strftime("%Y-%m-%d")
    os.environ.pop("EXACT_DATES", None)
    run_downloader()
    run_loader_with_retry()

elif mode == "remediation":
    print("Running remediation mode (targeted missing dates)")

    start_date, end_date = resolve_window_for_non_daily()
    window_start = start_date.strftime("%Y-%m-%d")
    window_end = end_date.strftime("%Y-%m-%d")

    print(f"Remediation window: {window_start} → {window_end}")

    from batch_downloader import get_missing_dates
    missing_dates = get_missing_dates(window_start, window_end)

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

else:
    target_day = latest_available.strftime("%Y-%m-%d")
    print("Daily ingestion for:", target_day)
    os.environ["START_DATE"] = target_day
    os.environ["END_DATE"] = target_day
    os.environ.pop("EXACT_DATES", None)
    subprocess.run(
        ["python", "batch_downloader.py", target_day, target_day],
        check=True
    )
    run_loader_with_retry()

print("Pipeline completed successfully")
