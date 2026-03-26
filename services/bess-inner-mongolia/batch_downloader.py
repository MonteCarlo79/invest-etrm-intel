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

# ================= CONFIG ==================

# Base API endpoint for downloads
BASE_URL = "https://app-portal-cn-ft.enos-iot.com/mengxi-data-sync/v1/api/details/6.52"

# HTTP headers copied from your browser
HEADERS = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
    "connection": "keep-alive",
    "referer": "https://app-portal-cn-ft.enos-iot.com/mengxi-data-sync/details/6.52",
    "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
}

# Output directory for saved files
OUTPUT_DIR = Path("/tmp/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ==========================================

def daterange(start_date, end_date):
    """Yield datetime objects from start_date to end_date (inclusive)."""
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


# ... keep your existing CONFIG (BASE_URL, HEADERS, OUTPUT_DIR) ...

def get_session() -> requests.Session:
    """
    Create a session with retries/backoff for transient network/server issues.
    """
    session = requests.Session()

    retry = Retry(
        total=6,                 # total retry attempts
        connect=6,
        read=6,
        status=6,
        backoff_factor=1.2,      # 1.2s, 2.4s, 4.8s, ...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # Keep your browser-like headers
    session.headers.update(HEADERS)
    return session


SESSION = get_session()

def download_excel_for_date(date_str: str) -> bool:
    """
    Download Excel for a given date.
    Returns True if successful and file size >= MIN_FILE_SIZE_MB.
    """

    params = {"startDay": date_str}

    MIN_FILE_SIZE_MB = int(os.getenv("MIN_FILE_SIZE_MB", "7"))
    MIN_BYTES = MIN_FILE_SIZE_MB * 1024 * 1024

    timeout = (10, 180)

    try:
        with SESSION.get(BASE_URL, params=params, timeout=timeout, stream=True) as r:

            if r.status_code != 200:
                print(f"[WARN] {date_str} HTTP {r.status_code}, content-type={r.headers.get('Content-Type')}")
                return False

            content_type = (r.headers.get("Content-Type") or "").lower()
            if "spreadsheetml" not in content_type and "application/vnd.ms-excel" not in content_type:
                snippet = ""
                try:
                    snippet = r.text[:200].replace("\n", " ")
                except Exception:
                    pass
                print(f"[WARN] {date_str} not Excel (content-type={content_type}); snippet={snippet!r}")
                return False

            filename = OUTPUT_DIR / f"data_{date_str}.xlsx"

            bytes_written = 0
            with open(filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 256):
                    if chunk:
                        f.write(chunk)
                        bytes_written += len(chunk)

            if bytes_written == 0:
                print(f"[WARN] {date_str} downloaded 0 bytes")
                return False

            # ✅ SIZE VALIDATION AFTER DOWNLOAD
            if bytes_written < MIN_BYTES:
                print(
                    f"[WARN] {date_str} file too small "
                    f"({bytes_written/1024/1024:.2f}MB < {MIN_FILE_SIZE_MB}MB). "
                    "Treating as not ready."
                )

                # Remove partial file to avoid accidental upload
                try:
                    filename.unlink()
                except Exception:
                    pass

                return False

            print(f"[OK] {date_str} downloaded ({bytes_written/1024/1024:.2f}MB)")
            return True

    except requests.exceptions.ReadTimeout:
        print(f"[WARN] {date_str} read timed out.")
        return False

    except requests.exceptions.RequestException as e:
        print(f"[WARN] {date_str} request failed: {type(e).__name__}: {e}")
        return False

def batch_download(start_date_str, end_date_str):
    start = datetime.strptime(start_date_str, "%Y-%m-%d")
    end   = datetime.strptime(end_date_str,   "%Y-%m-%d")

    failed_dates = []

    for dt in tqdm(list(daterange(start, end)), desc="Downloading"):
        date_str = dt.strftime("%Y-%m-%d")
        success = download_excel_for_date(date_str)

        if not success:
            failed_dates.append(date_str)

    print("\nDownload completed.")
    if failed_dates:
        print("Failed for dates:", failed_dates)

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python batch_downloader.py [start_date] [end_date]")
        print("Example: python batch_downloader.py 2025-12-29 2025-12-31")
        sys.exit(1)

    batch_download(sys.argv[1], sys.argv[2])
