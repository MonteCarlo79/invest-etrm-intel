# -*- coding: utf-8 -*-
"""
Created on Tue Jan  6 14:37:57 2026

@author: dipeng.chen
"""

import requests
from datetime import datetime, timedelta
from pathlib import Path
from tqdm import tqdm
import os
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlalchemy as sa
import pandas as pd
# ================= CONFIG ==================

BASE_URL = "https://app-portal-cn-ft.enos-iot.com/mengxi-data-sync/v1/api/details/6.52"

HEADERS = {
    "accept": "*/*",
    # "accept-encoding": "gzip, deflate, br",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
    "connection": "keep-alive",
    "referer": "https://app-portal-cn-ft.enos-iot.com/mengxi-data-sync/details/6.52",
    # "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    # "sec-ch-ua-mobile": "?0",
    # "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# Ignore corrupted or incomplete files
MIN_FILE_SIZE_MB = 4

# Default reconciliation range
DEFAULT_START_DATE = "2026-01-01"
# Optional reconciliation controls (from ECS env vars)

ENV_START_DATE = os.getenv("START_DATE")
ENV_END_DATE = os.getenv("END_DATE")
ENV_RECONCILE_DAYS = os.getenv("RECONCILE_DAYS")
# ==========================================
def excel_contains_data(filepath: Path):

    try:
        df = pd.read_excel(
            filepath,
            sheet_name="实时节点电价",
            nrows=200
        )

        if len(df) > 50:
            return True

        print(f"[INVALID] {filepath.name} only {len(df)} rows")
        return False

    except Exception as e:
        print(f"[INVALID EXCEL] {filepath.name} ({e})")
        return False
def get_engine():

    db_dsn = os.getenv("DB_DSN")

    if not db_dsn:
        raise RuntimeError("DB_DSN environment variable not set")

    return sa.create_engine(db_dsn, pool_pre_ping=True)

def get_missing_dates(start_date_str, end_date_str):

    engine = get_engine()

    start = datetime.strptime(start_date_str, "%Y-%m-%d")
    end = datetime.strptime(end_date_str, "%Y-%m-%d")

    all_dates = {dt.date() for dt in daterange(start, end)}

    query = """
        SELECT DISTINCT data_date
        FROM marketdata.md_rt_nodal_price
        WHERE data_date BETWEEN :start AND :end
    """

    with engine.connect() as conn:

        rows = conn.execute(
            sa.text(query),
            {"start": start_date_str, "end": end_date_str}
        )

        existing = {r[0] for r in rows}

    missing = sorted(all_dates - existing)

    return [d.strftime("%Y-%m-%d") for d in missing]

def download_missing_dates(start_date_str, end_date_str):

    missing_dates = get_missing_dates(start_date_str, end_date_str)

    today = datetime.today().date()
    lag = int(os.getenv("MARKET_LAG_DAYS", "1"))
    cutoff = datetime.today().date() - timedelta(days=lag)
    
    missing_dates = [
        d for d in missing_dates
        if datetime.strptime(d, "%Y-%m-%d").date() <= cutoff
    ]

    if not missing_dates:
        print("Database already complete for this period.")
        return

    print(f"Missing {len(missing_dates)} days:", missing_dates[:10], "...")

    failed = []

    max_workers = int(os.getenv("MAX_DOWNLOAD_WORKERS", 1))
    

    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        futures = {executor.submit(download_excel_for_date, d): d for d in missing_dates}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading"):

            d = futures[future]

            try:
                if not future.result():
                    failed.append(d)

            except Exception as e:
                print(f"[ERROR] {d} {e}")
                failed.append(d)

    if failed:
        print("Failed dates:", failed)

def daterange(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)
def resolve_reconcile_window():

    start_date = ENV_START_DATE or DEFAULT_START_DATE

    start = datetime.strptime(start_date, "%Y-%m-%d")

    # Priority 1 — explicit END_DATE
    if ENV_END_DATE:

        end = datetime.strptime(ENV_END_DATE, "%Y-%m-%d")

    # Priority 2 — RECONCILE_DAYS
    elif ENV_RECONCILE_DAYS:

        days = int(ENV_RECONCILE_DAYS)
        end = start + timedelta(days=days - 1)

    # Default — reconcile until today
    else:
        lag_days = int(os.getenv("MARKET_LAG_DAYS", "1"))
        end = datetime.today() - timedelta(days=lag_days)
        
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

def get_session() -> requests.Session:

    session = requests.Session()

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update(HEADERS)
    return session


SESSION = get_session()


def file_is_valid(filepath: Path):

    if not filepath.exists():
        return False

    size_mb = filepath.stat().st_size / (1024 * 1024)

    if size_mb < MIN_FILE_SIZE_MB:
        print(f"[CORRUPTED] {filepath.name} ({size_mb:.2f}MB)")
        return False

    return True


def download_excel_for_date(date_str: str):
    
    time.sleep(float(os.getenv("REQUEST_DELAY", 1)))
    filename = OUTPUT_DIR / f"data_{date_str}.xlsx"

    if file_is_valid(filename):
        return True

    params = {"startDay": date_str}
    timeout = (30, 300)

    try:

        r = SESSION.get(BASE_URL, params=params, timeout=timeout)

        if r.status_code != 200:
            print(f"[WARN] {date_str} HTTP {r.status_code}")
            return False

        content_type = (r.headers.get("Content-Type") or "").lower()

        if "spreadsheetml" not in content_type and "application/vnd.ms-excel" not in content_type:
            print(f"[WARN] {date_str} not Excel (content-type={content_type})")
            return False

        content = r.content
        
        
        
        print(
            f"[DEBUG] {date_str} "
            f"len={len(content)} "
            f"type={r.headers.get('Content-Type')} "
        )

        with open(filename, "wb") as f:
            f.write(content)

        size_mb = filename.stat().st_size / (1024 * 1024)

        if size_mb < MIN_FILE_SIZE_MB:
            print(f"[SMALL FILE] {date_str} ({size_mb:.2f}MB) validating...")

        if not excel_contains_data(filename):

            print(f"[REJECT] {date_str} invalid Excel")
            filename.unlink(missing_ok=True)
            return False

        print(f"[OK] {date_str} ({size_mb:.2f}MB)")

        time.sleep(int(os.getenv("REQUEST_DELAY", 2)))

        return True

    except requests.exceptions.ReadTimeout:
        print(f"[WARN] {date_str} read timeout")
        return False

    except requests.exceptions.RequestException as e:
        print(f"[WARN] {date_str} request failed: {e}")
        return False


# ================= NEW SELF-HEALING LOGIC =================

def download_latest_available():

    today = datetime.today()

    for offset in range(0, 3):  # try today, yesterday, T-2

        dt = today - timedelta(days=offset)
        date_str = dt.strftime("%Y-%m-%d")

        print(f"\nTrying latest data date: {date_str}")

        success = download_excel_for_date(date_str)

        if success:
            print(f"[SUCCESS] Latest available data: {date_str}")
            return

    print("[ERROR] No recent data available in last 3 days")


# ==========================================================


def batch_download(start_date_str, end_date_str):

    start = datetime.strptime(start_date_str, "%Y-%m-%d")
    end = datetime.strptime(end_date_str, "%Y-%m-%d")

    dates = [dt.strftime("%Y-%m-%d") for dt in daterange(start, end)]

    failed_dates = []

    max_workers = int(os.getenv("MAX_DOWNLOAD_WORKERS", 1))

    print(f"Downloading {len(dates)} days using {max_workers} parallel workers")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        futures = {executor.submit(download_excel_for_date, d): d for d in dates}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading"):

            date_str = futures[future]

            try:
                success = future.result()

                if not success:
                    failed_dates.append(date_str)

            except Exception as e:
                print(f"[ERROR] {date_str} {e}")
                failed_dates.append(date_str)

    print("\nDownload completed.")

    if failed_dates:
        print("Failed for dates:", failed_dates)


if __name__ == "__main__":

    import sys

    print("=== Downloader Config ===")
    print("RUN_MODE:", os.getenv("RUN_MODE"))
    print("START_DATE:", ENV_START_DATE)
    print("END_DATE:", ENV_END_DATE)
    print("RECONCILE_DAYS:", ENV_RECONCILE_DAYS)
    print("MAX_DOWNLOAD_WORKERS:", os.getenv("MAX_DOWNLOAD_WORKERS"))
    print("=========================")

    # Manual range mode
    if len(sys.argv) == 3:

        start_date = sys.argv[1]
        end_date = sys.argv[2]

        print(f"Manual range mode: {start_date} → {end_date}")
        batch_download(start_date, end_date)

    else:

        mode = os.getenv("RUN_MODE", "daily")

        if mode == "reconcile":

            start_date, end_date = resolve_reconcile_window()

            print(f"Reconciling missing data: {start_date} → {end_date}")

            download_missing_dates(start_date, end_date)

        else:

            print("Daily ingestion mode")
            download_latest_available()