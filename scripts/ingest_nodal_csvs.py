"""
One-time backfill: read all *_YYYY-MM.csv files under data/nodal/<province>/
and upsert into marketdata.md_shanxi_nodal_price_96.

Uses PostgreSQL COPY protocol (streams the whole file in one shot, then a
single bulk INSERT ... ON CONFLICT from a TEMP staging table) which is
~100x faster than individual INSERT statements.

Run from repo root:
    py scripts/ingest_nodal_csvs.py

Optional filters:
    py scripts/ingest_nodal_csvs.py --province 云南 安徽
    py scripts/ingest_nodal_csvs.py --since 2026-01
    py scripts/ingest_nodal_csvs.py --resume   # skip files already logged as done
"""
from __future__ import annotations

import argparse
import io
import os
import re
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

_env_file = _REPO / "config" / ".env"
if _env_file.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(str(_env_file))
    except ImportError:
        pass

import pandas as pd
from sqlalchemy import create_engine, text

from services.fengxing.nodal_price import init_table

_PAT = re.compile(r"^(.+)_(\d{4}-\d{2})\.csv$")
_DONE_LOG = _REPO / "scripts" / ".ingest_nodal_done"
_COLS = ["node_name", "metric_time", "time_order_96", "market_name", "avg_node_price"]


# ---------------------------------------------------------------------------
# COPY-based bulk upsert
# ---------------------------------------------------------------------------

def _bulk_upsert(path: Path, engine) -> int:
    """
    Load one CSV file into RDS using COPY → temp staging table → INSERT ON CONFLICT.
    Returns the number of rows upserted.
    """
    print(f"  reading CSV ...", end=" ", flush=True)
    t = time.time()
    df = pd.read_csv(path, encoding="utf-8-sig")
    print(f"{len(df):,} rows in {time.time()-t:.1f}s")

    missing = [c for c in _COLS if c not in df.columns]
    if missing:
        raise ValueError(f"CSV missing columns: {missing}  (has: {list(df.columns)})")

    df = df[_COLS].copy()
    df["time_order_96"] = pd.to_numeric(df["time_order_96"], errors="coerce").fillna(0).astype(int)
    df["avg_node_price"] = pd.to_numeric(df["avg_node_price"], errors="coerce")
    df["node_name"] = df["node_name"].astype(str)
    df["market_name"] = df["market_name"].fillna("").astype(str)

    print(f"  serialising to buffer ...", end=" ", flush=True)
    t = time.time()
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)
    print(f"{time.time()-t:.1f}s")

    staging = f"stg_nodal_{int(time.time() * 1000) % 2_000_000_000}"

    print(f"  connecting to DB ...", end=" ", flush=True)
    t = time.time()
    raw = engine.raw_connection()
    print(f"{time.time()-t:.1f}s")

    try:
        cur = raw.cursor()
        # Set statement timeout so DB ops can't hang indefinitely
        cur.execute("SET statement_timeout = '300s'")

        cur.execute(f"""
            CREATE TEMP TABLE "{staging}" (
                node_name      TEXT,
                metric_time    TIMESTAMPTZ,
                time_order_96  SMALLINT,
                market_name    TEXT,
                avg_node_price NUMERIC(12,4)
            )
        """)

        print(f"  COPY to staging ...", end=" ", flush=True)
        t = time.time()
        cur.copy_expert(f'COPY "{staging}" FROM STDIN WITH (FORMAT CSV)', buf)
        print(f"{time.time()-t:.1f}s")

        print(f"  INSERT ON CONFLICT ...", end=" ", flush=True)
        t = time.time()
        cur.execute(f"""
            INSERT INTO marketdata.md_shanxi_nodal_price_96
                (node_name, metric_time, time_order_96, market_name, avg_node_price)
            SELECT node_name, metric_time, time_order_96, market_name, avg_node_price
            FROM "{staging}"
            ON CONFLICT (node_name, metric_time, time_order_96) DO UPDATE SET
                market_name    = EXCLUDED.market_name,
                avg_node_price = EXCLUDED.avg_node_price,
                inserted_at    = NOW()
        """)
        n = cur.rowcount
        print(f"{n:,} rows in {time.time()-t:.1f}s")

        raw.commit()
        return n

    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()


# ---------------------------------------------------------------------------
# File scanning
# ---------------------------------------------------------------------------

def scan(nodal_root: Path, provinces: list[str] | None, since: str | None) -> list[dict]:
    entries = []
    for prov_dir in sorted(nodal_root.iterdir()):
        if not prov_dir.is_dir():
            continue
        if provinces and prov_dir.name not in provinces:
            continue
        for f in sorted(prov_dir.iterdir()):
            m = _PAT.match(f.name)
            if not m:
                continue
            month = m.group(2)
            if since and month < since:
                continue
            entries.append({"province": prov_dir.name, "month": month, "path": f})
    return entries


def _load_done() -> set[str]:
    if _DONE_LOG.exists():
        return set(_DONE_LOG.read_text(encoding="utf-8").splitlines())
    return set()


def _mark_done(key: str) -> None:
    with open(_DONE_LOG, "a", encoding="utf-8") as f:
        f.write(key + "\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="Bulk-ingest nodal CSV files to RDS via COPY")
    p.add_argument("--province", nargs="*", help="Province name(s) to include (default: all)")
    p.add_argument("--since", help="Skip months before YYYY-MM")
    p.add_argument("--no-resume", action="store_true",
                   help="Re-process files already marked done (default: skip completed files)")
    args = p.parse_args()

    pgurl = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not pgurl:
        sys.exit("PGURL not set — check config/.env")

    engine = create_engine(pgurl, pool_pre_ping=True)
    init_table(engine)

    nodal_root = _REPO / "data" / "nodal"
    entries = scan(nodal_root, args.province, args.since)

    if not entries:
        print("No CSV files found.")
        return

    done = set() if args.no_resume else _load_done()
    pending = [e for e in entries if f"{e['province']}/{e['month']}" not in done]

    print(f"Found {len(entries)} file(s) total, {len(pending)} to process.\n")

    total_rows = 0
    errors: list[str] = []
    t0_all = time.time()

    for i, e in enumerate(pending, 1):
        label = f"{e['province']} / {e['month']}"
        size_mb = e["path"].stat().st_size / 1024 / 1024
        print(f"[{i}/{len(pending)}] {label}  ({size_mb:.1f} MB)")
        t0 = time.time()
        try:
            n = _bulk_upsert(e["path"], engine)
            elapsed = time.time() - t0
            total_rows += n
            print(f"  ✓ {n:,} rows  ({elapsed:.1f}s)")
            _mark_done(f"{e['province']}/{e['month']}")
        except Exception as exc:
            elapsed = time.time() - t0
            print(f"  ✗ ERROR ({elapsed:.1f}s): {exc}")
            errors.append(label)

    elapsed_all = time.time() - t0_all
    print(f"\nDone in {elapsed_all/60:.1f} min. "
          f"{total_rows:,} rows upserted, {len(errors)} error(s).")
    if errors:
        print(f"Failed files: {errors}")
        print("Re-run with --resume to retry only failed files.")


if __name__ == "__main__":
    main()
