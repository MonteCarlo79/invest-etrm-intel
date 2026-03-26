import os
import subprocess
import time
import psycopg2
from datetime import datetime, timedelta

OUTPUT_DIR = "./output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

mode = os.getenv("RUN_MODE", "daily")

START_DATE = os.getenv("START_DATE")
END_DATE = os.getenv("END_DATE")
RECONCILE_DAYS = os.getenv("RECONCILE_DAYS")

MARKET_LAG_DAYS = int(os.getenv("MARKET_LAG_DAYS", "1"))
DB_DSN = os.getenv("PGURL") or os.getenv("DB_DSN")

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

def wait_for_db(max_attempts=10, delay=10):

    if not DB_DSN:
        print("No DB connection string provided — skipping DB check")
        return

    for attempt in range(1, max_attempts + 1):

        try:
            print(f"Checking DB connectivity (attempt {attempt})...")

            conn = psycopg2.connect(DB_DSN, connect_timeout=5)
            conn.close()

            print("Database connection successful")
            return

        except Exception as e:
            print("DB connection failed:", str(e))

            if attempt == max_attempts:
                raise RuntimeError("Database not reachable")

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