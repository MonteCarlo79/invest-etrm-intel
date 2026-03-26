import os
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List, Tuple

from sqlalchemy import create_engine, text

from batch_downloader import batch_download
from load_excel_to_marketdata import main as load_to_db


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default


PGURL = os.getenv("PGURL")
if not PGURL:
    raise RuntimeError("PGURL not set")

DB_SCHEMA = os.getenv("DB_SCHEMA", "marketdata")
MIN_FILE_SIZE_MB = env_int("MIN_FILE_SIZE_MB", 7)
BACKFILL_DAYS = env_int("BACKFILL_DAYS", 7)

MIN_BYTES = MIN_FILE_SIZE_MB * 1024 * 1024

# Use Fargate-safe paths
OUTPUT_DIR = Path("/tmp/output")
READY_DIR = Path("/tmp/output_ready")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
READY_DIR.mkdir(parents=True, exist_ok=True)

engine = create_engine(PGURL)


def get_latest_success_date() -> date | None:
    with engine.connect() as conn:
        return conn.execute(text(f"""
            SELECT max(file_date)
            FROM {DB_SCHEMA}.md_load_log
            WHERE status='success'
        """)).scalar()


def get_missing_dates(window_start: date, window_end: date) -> List[date]:
    """
    Find dates that are either:
    - not loaded
    - OR marked incomplete in data_quality_status
    """
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT data_date, is_complete, notes
            FROM {DB_SCHEMA}.data_quality_status
            WHERE data_date BETWEEN :s AND :e
        """), {"s": window_start, "e": window_end}).fetchall()

    bad_dates = set()

    for r in rows:
        d, is_complete, notes = r

        notes = notes or ""
        has_error = "[SHEET FAIL]" in notes or "DiskFull" in notes

        if (not is_complete) or has_error:
            bad_dates.add(d)

    # ALSO include dates that never appeared at all
    existing_dates = {r[0] for r in rows}
    d = window_start
    while d <= window_end:
        if d not in existing_dates:
            bad_dates.add(d)
        d += timedelta(days=1)

    return sorted(bad_dates)


def validate_and_stage_files(dates: List[date]) -> Tuple[List[date], List[date]]:
    """
    Move "ready" files to READY_DIR if size >= MIN_BYTES.
    Returns (ready_dates, partial_dates).
    """
    ready = []
    partial = []

    # clear READY_DIR each run
    for p in READY_DIR.glob("data_*.xlsx"):
        try:
            p.unlink()
        except Exception:
            pass

    for d in dates:
        fn = OUTPUT_DIR / f"data_{d.isoformat()}.xlsx"
        if not fn.exists():
            partial.append(d)
            continue

        size = fn.stat().st_size
        if size < MIN_BYTES:
            partial.append(d)
            continue

        # stage to READY_DIR
        staged = READY_DIR / fn.name
        if staged.exists():
            staged.unlink()
        fn.replace(staged)
        ready.append(d)

    return ready, partial


def anomaly_summary(missing: List[date], partial: List[date], loaded: List[date]) -> str:
    lines = []
    if missing:
        lines.append(f"[ANOMALY] Missing days (no success log): {', '.join([d.isoformat() for d in missing])}")
    if partial:
        lines.append(f"[ANOMALY] Partial/unready downloads (<{MIN_FILE_SIZE_MB}MB or missing file): {', '.join([d.isoformat() for d in partial])}")
    if loaded:
        lines.append(f"[OK] Loaded days: {', '.join([d.isoformat() for d in loaded])}")

    if not lines:
        return "[OK] No anomalies detected."

    # Simple operational guidance
    if partial:
        lines.append("[ACTION] Will retry partial days automatically in next scheduled run(s).")
    return "\n".join(lines)
def get_successfully_loaded_dates(dates: List[date]) -> List[date]:
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT data_date
            FROM {DB_SCHEMA}.data_quality_status
            WHERE data_date = ANY(:dates)
              AND is_complete = true
        """), {"dates": dates}).fetchall()

    return [r[0] for r in rows]

def run():
    today = datetime.today().date()
    target_end = today - timedelta(days=1)  # always aim to load through yesterday

    # backfill window: last BACKFILL_DAYS up to yesterday
    window_start = max(target_end - timedelta(days=BACKFILL_DAYS), date(2000, 1, 1))
    window_end = target_end

    # determine missing days in the recent window
    missing_days = get_missing_dates(window_start, window_end)

    if not missing_days:
        print(f"[OK] No missing days between {window_start} and {window_end}.")
        return

    # Download missing range as one call (your downloader supports ranges)
    start = missing_days[0]
    end = missing_days[-1]

    print(f"[INFO] Missing days detected: {len(missing_days)} day(s). Downloading {start} → {end}")
    batch_download(start.isoformat(), end.isoformat())

    # validate file readiness (size gate) and stage
    ready_days, partial_days = validate_and_stage_files(missing_days)

    loaded_days: List[date] = []
    if ready_days:
        print(f"[INFO] Ready files: {len(ready_days)}. Loading to DB from {READY_DIR}")
        os.environ["FORCE_RELOAD"] = "true"
        load_to_db(str(READY_DIR))
        loaded_days = get_successfully_loaded_dates(ready_days)

    report = anomaly_summary(missing_days, partial_days, loaded_days)
    print(report)

    # If we want the ECS task to be marked failed when there are partial days, exit nonzero:
    # This makes CloudWatch/EventBridge failures visible.
    if partial_days:
        raise SystemExit(2)


if __name__ == "__main__":
    run()