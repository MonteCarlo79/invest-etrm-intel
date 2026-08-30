"""Fengxing nodal catch-up with wave-riding: per-day done-marker, backoff retry
on the SAME day (never advances past a failure), resume across restarts.

Usage: python3 scripts/catchup_fengxing_nodal.py [start_date] [end_date]
Defaults: 2026-07-01 .. 2026-08-29
"""
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

_env = _REPO / "config" / ".env"
if _env.exists():
    from dotenv import load_dotenv
    load_dotenv(str(_env))

import psycopg2
from sqlalchemy import create_engine
from services.fengxing.nodal_price import _fetch_day, upsert

PROVINCES = ["山西", "陕西", "湖南", "浙江", "云南", "贵州", "广东", "广西", "海南", "甘肃",
             "山东", "河北南网", "黑龙江", "辽宁", "蒙西", "湖北", "安徽", "江西"]
DONE_LOG = _REPO / "scripts" / ".fengxing_catchup_done"


def _done() -> set[str]:
    if DONE_LOG.exists():
        return set(l.strip() for l in DONE_LOG.read_text().splitlines() if l.strip())
    return set()


def _mark(key: str) -> None:
    with DONE_LOG.open("a", encoding="utf-8") as f:
        f.write(key + "\n")


def main():
    start = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date(2026, 7, 1)
    end = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else date(2026, 8, 29)
    pgurl = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    engine = create_engine(pgurl, connect_args={
        "connect_timeout": 10, "keepalives": 1, "keepalives_idle": 30,
        "keepalives_interval": 10, "keepalives_count": 3, "tcp_user_timeout": 120000})
    api_key = os.environ["FENGXING_API_KEY"]
    done = _done()

    days = []
    d = start
    while d <= end:
        days.append(d)
        d += timedelta(days=1)

    deferred = []
    for prov in PROVINCES:
        for day in days:
            key = f"{prov}/{day.isoformat()}"
            if key in done:
                continue
            ok = False
            for attempt in range(5):
                try:
                    rows = _fetch_day(day, api_key, prov)
                    n = upsert(rows, engine)
                    _mark(key)
                    ok = True
                    print(f"OK {key} ({n:,} rows)", flush=True)
                    break
                except Exception as exc:
                    wait = 20 * (attempt + 1)
                    print(f"  retry {key} attempt {attempt+1} ({type(exc).__name__}); sleep {wait}s", flush=True)
                    time.sleep(wait)
            if not ok:
                deferred.append(key)
                print(f"DEFER {key}", flush=True)
        print(f"PROVINCE_DONE {prov}", flush=True)

    print(f"CATCHUP_COMPLETE deferred={len(deferred)}", flush=True)
    for k in deferred:
        print("DEFERRED:", k, flush=True)


main()
