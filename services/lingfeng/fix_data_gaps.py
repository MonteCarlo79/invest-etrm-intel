"""
fix_data_gaps.py

Diagnostic + cleanup runner for two known data gaps:
  A) 蒙东 zeros, Feb–Apr 2026
  B) 福建 SSL gap, Nov 2025–Mar 2026

Run from repo root:
    python services/lingfeng/fix_data_gaps.py
    python services/lingfeng/fix_data_gaps.py --dry-run   # diagnostics only, no deletions

After this succeeds, run the re-ingest commands printed at the end.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Load config/.env
_REPO = Path(__file__).resolve().parent.parent.parent
_ENV_FILE = _REPO / "config" / ".env"

if _ENV_FILE.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(str(_ENV_FILE))
    except ImportError:
        pass

import psycopg2


def _connect():
    url = os.getenv("PGURL") or os.getenv("DATABASE_URL")
    if url:
        return psycopg2.connect(url)
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "marketdata"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
    )


def _query(cur, sql: str, params=None) -> list:
    cur.execute(sql, params)
    return cur.fetchall()


def _run_diagnostics(cur) -> None:
    print("\n" + "="*60)
    print("DIAGNOSTICS")
    print("="*60)

    # 蒙东 zeros
    print("\n--- 蒙东: spot_prices_hourly, Feb–Apr 2026 (zero/null rt_price by day) ---")
    rows = _query(cur, """
        SELECT date(datetime) AS day,
               count(*) AS total,
               sum(case when rt_price = 0 OR rt_price IS NULL then 1 else 0 end) AS zero_rt,
               sum(case when da_price = 0 OR da_price IS NULL then 1 else 0 end) AS zero_da
        FROM marketdata.spot_prices_hourly
        WHERE province = '蒙东'
          AND datetime >= '2026-02-01'
          AND datetime < '2026-05-01'
        GROUP BY 1 ORDER BY 1
    """)
    if rows:
        print(f"  {'day':<12} {'total':>7} {'zero_rt':>9} {'zero_da':>9}")
        for r in rows:
            flag = " <-- ALL ZERO" if r[2] == r[1] else (" <-- partial" if r[2] > 0 else "")
            print(f"  {str(r[0]):<12} {r[1]:>7} {r[2]:>9} {r[3]:>9}{flag}")
    else:
        print("  (no rows in this range)")

    print("\n--- 蒙东: audit.province_progress ---")
    rows = _query(cur, "SELECT * FROM audit.province_progress WHERE province = '蒙东'")
    for r in rows:
        print(f"  {r}")

    # 福建 coverage
    print("\n--- 福建: coverage by month (Nov 2025–Mar 2026) ---")
    rows = _query(cur, """
        SELECT date_trunc('month', datetime)::date AS month,
               count(*) AS rows,
               count(case when rt_price > 0 then 1 end) AS nonzero_rt,
               min(datetime)::date AS first_day,
               max(datetime)::date AS last_day
        FROM marketdata.spot_prices_hourly
        WHERE province = '福建'
          AND datetime >= '2025-11-01'
          AND datetime < '2026-04-01'
        GROUP BY 1 ORDER BY 1
    """)
    if rows:
        expected_rows = 30 * 24  # approximate
        print(f"  {'month':<12} {'rows':>6} {'nonzero_rt':>10} {'first':>12} {'last':>12}")
        for r in rows:
            flag = " <-- INCOMPLETE" if r[1] < expected_rows * 0.8 else ""
            print(f"  {str(r[0]):<12} {r[1]:>6} {r[2]:>10} {str(r[3]):>12} {str(r[4]):>12}{flag}")
    else:
        print("  (no rows in this range)")


_CLEANUP_MENGDONG = [
    ("Delete audit.province_progress for 蒙东",
     "DELETE FROM audit.province_progress WHERE province = '蒙东' AND duration_h = 0.0"),
    ("Delete bess_capture_daily for 蒙东 Feb–Apr 2026",
     "DELETE FROM marketdata.bess_capture_daily WHERE province='蒙东' AND date>='2026-02-01' AND date<'2026-05-01'"),
    ("Delete spot_dispatch_hourly_theoretical for 蒙东 Feb–Apr 2026",
     "DELETE FROM marketdata.spot_dispatch_hourly_theoretical WHERE province='蒙东' AND datetime>='2026-02-01' AND datetime<'2026-05-01'"),
    ("Delete spot_dispatch_hourly_rt_forecast for 蒙东 Feb–Apr 2026",
     "DELETE FROM marketdata.spot_dispatch_hourly_rt_forecast WHERE province='蒙东' AND datetime>='2026-02-01' AND datetime<'2026-05-01'"),
    ("Delete spot_prices_hourly_rt_forecast for 蒙东 Feb–Apr 2026",
     "DELETE FROM marketdata.spot_prices_hourly_rt_forecast WHERE province='蒙东' AND datetime>='2026-02-01' AND datetime<'2026-05-01'"),
]

_CLEANUP_FUJIAN = [
    ("Delete bess_capture_daily for 福建 Nov 2025–Mar 2026",
     "DELETE FROM marketdata.bess_capture_daily WHERE province='福建' AND date>='2025-11-01' AND date<'2026-04-01'"),
    ("Delete spot_dispatch_hourly_theoretical for 福建 Nov 2025–Mar 2026",
     "DELETE FROM marketdata.spot_dispatch_hourly_theoretical WHERE province='福建' AND datetime>='2025-11-01' AND datetime<'2026-04-01'"),
    ("Delete spot_dispatch_hourly_rt_forecast for 福建 Nov 2025–Mar 2026",
     "DELETE FROM marketdata.spot_dispatch_hourly_rt_forecast WHERE province='福建' AND datetime>='2025-11-01' AND datetime<'2026-04-01'"),
    ("Delete spot_prices_hourly_rt_forecast for 福建 Nov 2025–Mar 2026",
     "DELETE FROM marketdata.spot_prices_hourly_rt_forecast WHERE province='福建' AND datetime>='2025-11-01' AND datetime<'2026-04-01'"),
]


def _run_cleanup(conn, cur, steps: list, label: str, dry_run: bool) -> None:
    print(f"\n{'='*60}")
    print(f"CLEANUP: {label}")
    print(f"{'='*60}")
    for desc, sql in steps:
        if dry_run:
            print(f"  [DRY-RUN] {desc}")
        else:
            cur.execute(sql)
            n = cur.rowcount
            print(f"  [OK] {desc}  ({n} rows deleted)")
    if not dry_run:
        conn.commit()


def _print_next_steps() -> None:
    print("\n" + "="*60)
    print("NEXT STEPS — run these commands from repo root:")
    print("="*60)
    print("""
--- A) 蒙东 re-ingest + capture ---
python services/lingfeng/run_daily.py ^
    --markets 蒙东 ^
    --start-date 2026-02-01 ^
    --end-date 2026-04-30 ^
    --models ols_rt_time_v1,naive_rt_ar17,ols_fundamentals_v1 ^
    --force-capture

--- B) 福建 re-ingest (small chunks to avoid SSL) ---
python services/lingfeng/run_daily.py ^
    --markets 福建 ^
    --start-date 2025-11-01 ^
    --end-date 2026-03-31 ^
    --models ols_rt_time_v1,naive_rt_ar17,ols_fundamentals_v1 ^
    --chunk-days 7 ^
    --force-capture
""")


def main() -> None:
    ap = argparse.ArgumentParser(description="Diagnose and fix 蒙东/福建 data gaps.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print diagnostics and what would be deleted, without modifying DB")
    ap.add_argument("--only", choices=["mengdong", "fujian"],
                    help="Run only one of the two fixes")
    args = ap.parse_args()

    conn = _connect()
    cur = conn.cursor()

    try:
        _run_diagnostics(cur)

        if args.dry_run:
            print("\n[DRY-RUN mode — no changes will be made]")

        if args.only != "fujian":
            _run_cleanup(conn, cur, _CLEANUP_MENGDONG, "FIX A: 蒙东 zeros", args.dry_run)

        if args.only != "mengdong":
            _run_cleanup(conn, cur, _CLEANUP_FUJIAN, "FIX B: 福建 SSL gap", args.dry_run)

        if not args.dry_run:
            print("\n[All cleanup complete]")
        _print_next_steps()

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
