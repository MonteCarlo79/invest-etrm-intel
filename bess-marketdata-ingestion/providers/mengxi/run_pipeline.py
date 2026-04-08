import os
import subprocess
import time
import json
import urllib.request
import urllib.error
import psycopg2
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone

OUTPUT_DIR = "./output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

mode = os.getenv("RUN_MODE", "daily")

START_DATE = os.getenv("START_DATE")
END_DATE = os.getenv("END_DATE")
RECONCILE_DAYS = os.getenv("RECONCILE_DAYS")

MARKET_LAG_DAYS = int(os.getenv("MARKET_LAG_DAYS", "1"))
DB_DSN = os.getenv("PGURL") or os.getenv("DB_DSN")
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "").strip()
ALERT_CONTEXT = os.getenv("ALERT_CONTEXT", "bess-mengxi-ingestion").strip()


def extract_db_host(dsn: str | None) -> str | None:
    if not dsn:
        return None

    try:
        parsed = urlparse(dsn)
        if parsed.hostname:
            return parsed.hostname
    except Exception:
        pass

    return None


def send_alert(payload: dict) -> None:
    if not ALERT_WEBHOOK_URL:
        return

    try:
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            ALERT_WEBHOOK_URL,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"Alert webhook sent with status {getattr(resp, 'status', 'unknown')}")
    except urllib.error.URLError as e:
        print("Alert webhook failed:", str(e))
    except Exception as e:
        print("Unexpected alert failure:", str(e))


def emit_db_timeout_alert(max_attempts: int, delay: int, connect_timeout: int, last_error: str) -> None:
    payload = {
        "context": ALERT_CONTEXT,
        "pipeline": "bess-mengxi-ingestion",
        "error_class": "db_connect_timeout",
        "message": "Mengxi ingestion alert: DB connectivity timeout. The ECS task could not reach Postgres on port 5432 after repeated retries. This is likely an infra/network reachability issue rather than a SQL/query error.",
        "run_mode": mode,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "reconcile_days": RECONCILE_DAYS,
        "market_lag_days": MARKET_LAG_DAYS,
        "db_host": extract_db_host(DB_DSN),
        "max_attempts": max_attempts,
        "retry_delay_seconds": delay,
        "connect_timeout_seconds": connect_timeout,
        "last_error": last_error,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    send_alert(payload)


today = datetime.utcnow().date()
latest_available = today - timedelta(days=MARKET_LAG_DAYS)

print("========== PIPELINE CONFIG ==========")
print("RUN_MODE:", mode)
print("START_DATE:", START_DATE)
print("END_DATE:", END_DATE)
print("RECONCILE_DAYS:", RECONCILE_DAYS)
print("MARKET_LAG_DAYS:", MARKET_LAG_DAYS)
print("LATEST_AVAILABLE_DATE:", latest_available)
print("=====================================")


# ------------------------------------------------
# DB CONNECTION CHECK
# ------------------------------------------------

def wait_for_db(max_attempts=10, delay=10, connect_timeout=5):

    if not DB_DSN:
        print("No DB connection string provided — skipping DB check")
        return

    last_error = None

    for attempt in range(1, max_attempts + 1):

        try:
            print(f"Checking DB connectivity (attempt {attempt})...")

            conn = psycopg2.connect(DB_DSN, connect_timeout=connect_timeout)
            conn.close()

            print("Database connection successful")
            return

        except Exception as e:
            last_error = str(e)
            print("DB connection failed:", last_error)

            if attempt == max_attempts:
                emit_db_timeout_alert(
                    max_attempts=max_attempts,
                    delay=delay,
                    connect_timeout=connect_timeout,
                    last_error=last_error,
                )
                raise RuntimeError("Database not reachable") from e

            time.sleep(delay)


# ------------------------------------------------
# STEP 1 — VERIFY DB FIRST
# ------------------------------------------------

wait_for_db()


# ------------------------------------------------
# STEP 2 — DOWNLOAD DATA
# ------------------------------------------------

if mode == "reconcile":

    print("Running reconciliation mode")

    if not START_DATE:
        raise RuntimeError("START_DATE must be set for reconcile mode")

    start_date = datetime.strptime(START_DATE, "%Y-%m-%d").date()

    if END_DATE:
        end_date = datetime.strptime(END_DATE, "%Y-%m-%d").date()

    elif RECONCILE_DAYS:
        end_date = start_date + timedelta(days=int(RECONCILE_DAYS) - 1)

    else:
        end_date = latest_available

    print(f"Reconciling window: {start_date} → {end_date}")

    subprocess.run(
        [
            "python",
            "batch_downloader.py",
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d")
        ],
        check=True
    )

else:

    target_day = latest_available.strftime("%Y-%m-%d")

    print("Daily ingestion for:", target_day)

    subprocess.run(
        ["python", "batch_downloader.py", target_day, target_day],
        check=True
    )


# ------------------------------------------------
# STEP 3 — LOAD WITH RETRY
# ------------------------------------------------

print("Loading Excel files to database")

max_attempts = 5

for attempt in range(1, max_attempts + 1):

    try:

        wait_for_db()

        subprocess.run(
            ["python", "load_excel_to_marketdata.py", OUTPUT_DIR],
            check=True
        )

        print("Pipeline completed successfully")
        break

    except subprocess.CalledProcessError as e:

        print(f"Load attempt {attempt} failed")

        if attempt == max_attempts:
            raise

        print("Retrying in 15 seconds...")
        time.sleep(15)