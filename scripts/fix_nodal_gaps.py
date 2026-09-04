"""
Refill missing / empty nodal CSV files from the Fengxing API.

Scans data/nodal/<province>/ for months with fewer than MIN_ROWS rows (or
entirely absent), fetches day-by-day from the API, merges with any existing
rows (dedup by node_name+metric_time), and writes back.

Usage:
    py scripts/fix_nodal_gaps.py                         # all provinces with gaps
    py scripts/fix_nodal_gaps.py --province 陕西 安徽    # specific provinces
    py scripts/fix_nodal_gaps.py --province 陕西 --start 2025-02 --end 2025-12
    py scripts/fix_nodal_gaps.py --dry-run               # show gaps only, no fetch

Reads FENGXING_API_KEY from config/.env.
"""
import argparse
import csv
import io
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from dotenv import load_dotenv
load_dotenv(_REPO / "config" / ".env", override=False)

_NODAL_ROOT = _REPO / "data" / "nodal"
_FIELDNAMES = ["node_name", "metric_time", "time_order_96", "market_name", "avg_node_price"]
_MIN_ROWS = 1000       # kept for CLI --min-rows but size check is primary
_MIN_SIZE_KB = 500     # file smaller than this (KB) is considered incomplete
_DAY_DELAY = 0.15      # seconds between API calls


def _iter_months(start_ym: str, end_ym: str):
    """Yield 'YYYY-MM' strings from start_ym to end_ym inclusive."""
    sy, sm = map(int, start_ym.split("-"))
    ey, em = map(int, end_ym.split("-"))
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield f"{y}-{m:02d}"
        m += 1
        if m > 12:
            m, y = 1, y + 1


def _month_days(ym: str):
    """Return list of date objects for every day in YYYY-MM."""
    y, m = map(int, ym.split("-"))
    d = date(y, m, 1)
    days = []
    while d.month == m:
        days.append(d)
        d += timedelta(days=1)
    return days


def _file_size_kb(path: Path) -> float:
    """Return file size in KB, 0 if missing."""
    if not path.exists():
        return 0.0
    return path.stat().st_size / 1024


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        text = path.read_text(encoding="utf-8-sig")
        return list(csv.DictReader(io.StringIO(text)))
    except Exception:
        return []


def _write_csv(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=_FIELDNAMES, extrasaction="ignore",
                       lineterminator="\r\n")
    w.writeheader()
    w.writerows(rows)
    path.write_bytes(buf.getvalue().encode("utf-8-sig"))


def _merge(existing: list[dict], new_rows: list[dict]) -> list[dict]:
    seen = {(r.get("node_name"), r.get("metric_time")) for r in existing}
    merged = list(existing)
    for r in new_rows:
        k = (r.get("node_name"), r.get("metric_time"))
        if k not in seen:
            seen.add(k)
            merged.append(r)
    return merged


def scan_gaps(provinces: list[str], start_ym: str, end_ym: str, min_rows: int = _MIN_ROWS) -> dict:
    """Return {province: [(ym, row_count), ...]} of months that need filling."""
    gaps = {}
    for prov in provinces:
        prov_dir = _NODAL_ROOT / prov
        needed = []
        for ym in _iter_months(start_ym, end_ym):
            fpath = prov_dir / f"{prov}_{ym}.csv"
            kb = _file_size_kb(fpath)
            if kb < _MIN_SIZE_KB:
                needed.append((ym, kb))
        if needed:
            gaps[prov] = needed
    return gaps


def fill_gaps(gaps: dict, api_key: str, dry_run: bool = False):
    from services.fengxing.nodal_price import _fetch_day

    total_months = sum(len(v) for v in gaps.values())
    done = 0

    for prov, months in gaps.items():
        print(f"\n{'='*60}")
        print(f"Province: {prov}  ({len(months)} months to fill)")
        print(f"{'='*60}")

        for ym, existing_kb in months:
            print(f"\n  [{ym}]  file size: {existing_kb:.1f} KB")
            if dry_run:
                print(f"    DRY-RUN — would fetch {len(_month_days(ym))} days")
                continue

            fpath = _NODAL_ROOT / prov / f"{prov}_{ym}.csv"
            existing = _read_csv(fpath) if existing_kb > 0 else []

            # Fetch each day in the month
            month_rows: list[dict] = []
            days = _month_days(ym)
            day_ok = day_fail = day_empty = 0
            for d in days:
                try:
                    rows = _fetch_day(d, api_key, prov)
                    if rows:
                        month_rows.extend(rows)
                        day_ok += 1
                        print(f"    {d}  OK  {len(rows):,} rows", flush=True)
                    else:
                        day_empty += 1
                        print(f"    {d}  empty", flush=True)
                except Exception as exc:
                    day_fail += 1
                    print(f"    {d}  FAIL  {exc}", flush=True)
                time.sleep(_DAY_DELAY)

            # Merge and write
            merged = _merge(existing, month_rows)
            n_new = len(merged) - len(existing)
            _write_csv(fpath, merged)
            print(f"  → {ym}: {day_ok} ok / {day_empty} empty / {day_fail} fail  "
                  f"+{n_new} new rows → total {len(merged):,}")
            done += 1

    if not dry_run:
        print(f"\nDone — filled {done}/{total_months} month-files.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--province", nargs="+", help="Province(s) to process (default: all with gaps)")
    parser.add_argument("--start", default="2025-01", help="Start month YYYY-MM (default: 2025-01)")
    parser.add_argument("--end", default="2026-06", help="End month YYYY-MM (default: 2026-06)")
    parser.add_argument("--dry-run", action="store_true", help="Show gaps without fetching")
    parser.add_argument("--min-rows", type=int, default=_MIN_ROWS,
                        help=f"Rows below this threshold trigger a refetch (default: {_MIN_ROWS})")
    args = parser.parse_args()

    min_rows = args.min_rows

    api_key = os.environ.get("FENGXING_API_KEY", "")
    if not api_key and not args.dry_run:
        sys.exit("FENGXING_API_KEY not set in config/.env")

    # Determine province list
    if args.province:
        provinces = args.province
    else:
        provinces = [p.name for p in sorted(_NODAL_ROOT.iterdir()) if p.is_dir()]

    print(f"Scanning {len(provinces)} province(s) from {args.start} to {args.end} "
          f"(min_rows={min_rows}) ...\n")

    gaps = scan_gaps(provinces, args.start, args.end, min_rows)

    if not gaps:
        print("No gaps found — all months have sufficient data.")
        return

    # Summary
    print(f"Found gaps in {len(gaps)} province(s):")
    for prov, months in gaps.items():
        total_missing = sum(1 for _, kb in months if kb == 0)
        total_thin = sum(1 for _, kb in months if 0 < kb < _MIN_SIZE_KB)
        print(f"  {prov}: {len(months)} months  "
              f"(missing={total_missing}, thin={total_thin})")
        for ym, kb in months:
            label = "missing" if kb == 0 else f"{kb:.0f} KB"
            print(f"    {ym}  {label}")

    if args.dry_run:
        print("\nDry-run complete.")
        return

    print(f"\nStarting gap fill...")
    fill_gaps(gaps, api_key, dry_run=False)


if __name__ == "__main__":
    main()
