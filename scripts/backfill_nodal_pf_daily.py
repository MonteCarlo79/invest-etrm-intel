"""
Backfill reports.nodal_pf_daily for all dates in a given range.

For each date, computes perfect-foresight 2h/4h BESS scores for all plants in
marketdata.station_master and upserts into reports.nodal_pf_daily.

Skips dates that already have a full set of rows (≥ MIN_PLANTS plants stored),
so it is safe to re-run after interruption.

Usage:
    py scripts/backfill_nodal_pf_daily.py
    py scripts/backfill_nodal_pf_daily.py --start 2025-01-01 --end 2026-07-08
    py scripts/backfill_nodal_pf_daily.py --start 2026-01-01  # from date to today
    py scripts/backfill_nodal_pf_daily.py --dry-run           # show gaps only
"""
import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from dotenv import load_dotenv
load_dotenv(_REPO / "config" / ".env", override=False)

_MIN_PLANTS = 20  # dates with fewer rows than this are considered incomplete


def _iter_dates(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def _already_done(cur, d: date, min_plants: int) -> int:
    """Return number of plants stored for date d (0 = missing, <min_plants = partial)."""
    cur.execute(
        "SELECT COUNT(*) FROM reports.nodal_pf_daily WHERE data_date = %s", (d,)
    )
    return cur.fetchone()[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2025-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default=str(date.today() - timedelta(days=1)),
                        help="End date YYYY-MM-DD (inclusive, default: yesterday)")
    parser.add_argument("--dry-run", action="store_true", help="Show missing dates only")
    parser.add_argument("--force", action="store_true",
                        help="Re-compute even for dates that already have data")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)

    pg_url = os.environ.get("PGURL", "")
    if not pg_url:
        sys.exit("PGURL not set in config/.env")

    import psycopg2
    conn = psycopg2.connect(pg_url)

    # Load plant names from station_master
    with conn.cursor() as cur:
        cur.execute("SELECT plant_name FROM marketdata.station_master ORDER BY plant_name")
        plant_names = [r[0] for r in cur.fetchall()]
    print(f"Plants: {len(plant_names)}")

    # Ensure table exists
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS reports.nodal_pf_daily (
                data_date   DATE        NOT NULL,
                plant_name  TEXT        NOT NULL,
                score_2h    FLOAT,
                score_4h    FLOAT,
                computed_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (data_date, plant_name)
            )
        """)
    conn.commit()

    # Find dates to process
    dates_all = list(_iter_dates(start, end))
    dates_todo = []
    with conn.cursor() as cur:
        for d in dates_all:
            n = _already_done(cur, d, _MIN_PLANTS)
            if args.force or n < _MIN_PLANTS:
                dates_todo.append((d, n))

    conn.close()

    print(f"Date range: {start} → {end}  ({len(dates_all)} days total)")
    print(f"To process: {len(dates_todo)}  (already complete: {len(dates_all) - len(dates_todo)})")

    if args.dry_run:
        print("\nMissing / partial dates:")
        for d, n in dates_todo:
            label = "missing" if n == 0 else f"partial ({n} plants)"
            print(f"  {d}  {label}")
        return

    if not dates_todo:
        print("Nothing to do — all dates already have sufficient data.")
        return

    from services.hermes.mengxi_ranking_report import compute_and_store_nodal_pf_daily

    total = len(dates_todo)
    ok = skipped = errors = 0

    for i, (d, existing_n) in enumerate(dates_todo, 1):
        label = f"[{i:4d}/{total}]  {d}"
        print(f"{label}  ...", end="", flush=True)
        try:
            n = compute_and_store_nodal_pf_daily(pg_url, d, plant_names, milp_timeout_s=120.0)
            if n == 0:
                print(f"  no data (skipped)")
                skipped += 1
            else:
                print(f"  OK {n} plants")
                ok += 1
        except Exception as exc:
            print(f"  ERROR: {exc}")
            errors += 1

    print(f"\nDone — {ok} ok / {skipped} no-data / {errors} errors  (out of {total} dates)")


if __name__ == "__main__":
    main()
