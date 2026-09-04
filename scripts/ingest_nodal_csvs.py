"""
One-time backfill: read all *_YYYY-MM.csv files under data/nodal/<province>/
and upsert into marketdata.md_shanxi_nodal_price_96.

Per-chunk upserts, each on a fresh DB connection: the home-network path to
RDS drops long-lived connections after ~30 min (SSL SYSCALL timeout), and
TEMP staging tables don't survive a reconnect — so every 100k-row chunk is
its own short idempotent transaction (INSERT ... ON CONFLICT), retried once
on connection errors. Slower than single-shot COPY, but it actually finishes.

Run from repo root:
    py scripts/ingest_nodal_csvs.py

Optional filters:
    py scripts/ingest_nodal_csvs.py --province 云南 安徽
    py scripts/ingest_nodal_csvs.py --since 2026-01
    py scripts/ingest_nodal_csvs.py --resume   # skip files already logged as done
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import threading
import time
from datetime import timedelta, timezone
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
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text

from services.fengxing.nodal_price import init_table

_PAT = re.compile(r"^(.+)_(\d{4}-\d{2})(?:\.part(\d{3}))?\.csv$")
_DONE_LOG = _REPO / "scripts" / ".ingest_nodal_done"
_COLS = ["node_name", "metric_time", "time_order_96", "market_name", "avg_node_price"]


class _ChunkTimeout(Exception):
    """Wall-clock watchdog: catches connections that LOOK alive (middlebox ACKs
    keepalives) but never deliver a response — neither tcp_user_timeout nor
    statement_timeout fire in that state."""


def _force_close(pg_conn) -> None:
    """shutdown() the socket from a watchdog thread — this reliably wakes a
    recv blocked inside libpq (SIGALRM cannot: libpq retries EINTR internally,
    so a Python signal handler never gets control)."""
    import socket as _socket
    try:
        dup = _socket.fromfd(pg_conn.fileno(), _socket.AF_INET, _socket.SOCK_STREAM)
        dup.shutdown(_socket.SHUT_RDWR)
        dup.close()
    except Exception:
        pass


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

    # Normalise metric_time to the API-path convention (tz-aware CST midnight
    # + 15min*(slot-1)). Some CSVs (e.g. 陕西 2026) mix date-only and full
    # "+08:00" timestamp formats and list the same data twice — reconstructing
    # from date+slot unifies formats; exact duplicates are then dropped.
    _CST = timezone(timedelta(hours=8))
    day = pd.to_datetime(df["metric_time"].astype(str).str[:10], errors="coerce")
    valid = day.notna() & df["time_order_96"].between(1, 96)
    n_invalid = int((~valid).sum())
    df = df[valid].copy()
    df["metric_time"] = day[valid].dt.tz_localize(_CST) + pd.to_timedelta(
        15 * (df["time_order_96"] - 1), unit="min")
    before = len(df)
    df = df.drop_duplicates(subset=["node_name", "metric_time", "time_order_96"], keep="last")
    if n_invalid or len(df) < before:
        print(f"  normalised metric_time; dropped {n_invalid:,} invalid + {before - len(df):,} duplicate rows")

    # Each chunk upserts straight into the target table on its own connection.
    # A dead connection now costs at most one chunk (retried once), not the
    # whole file — and ON CONFLICT keeps re-runs idempotent.
    _CHUNK = 100_000
    n_chunks = (len(df) - 1) // _CHUNK + 1
    _UPSERT_SQL = """
        INSERT INTO marketdata.md_shanxi_nodal_price_96
            (node_name, metric_time, time_order_96, market_name, avg_node_price)
        VALUES %s
        ON CONFLICT (node_name, metric_time, time_order_96) DO UPDATE SET
            market_name    = EXCLUDED.market_name,
            avg_node_price = EXCLUDED.avg_node_price,
            inserted_at    = NOW()
    """

    total = 0
    print(f"  upserting ({n_chunks} chunk(s), fresh connection per chunk) ...")
    t = time.time()
    for ci, start_row in enumerate(range(0, len(df), _CHUNK), 1):
        chunk = df.iloc[start_row: start_row + _CHUNK]
        rows = list(chunk.itertuples(index=False, name=None))
        print(f"    chunk {ci}/{n_chunks} ({len(chunk):,} rows) ...", end=" ", flush=True)
        tc = time.time()
        last_exc = None
        for attempt in (1, 2, 3):
            raw = None
            try:
                raw = engine.raw_connection()
                cur = raw.cursor()
                cur.execute("SET statement_timeout = '600s'")
                # Watchdog: if this attempt exceeds 300s wall-clock, kill the
                # socket from a timer thread (SIGALRM can't interrupt libpq).
                wd = threading.Timer(300, _force_close, args=(raw.driver_connection,))
                wd.start()
                try:
                    execute_values(cur, _UPSERT_SQL, rows, page_size=10_000)
                    raw.commit()
                finally:
                    wd.cancel()
                last_exc = None
                break
            except (psycopg2.OperationalError, psycopg2.InterfaceError, _ChunkTimeout) as exc:
                last_exc = exc
                print(f"\n    chunk {ci} attempt {attempt} failed ({type(exc).__name__}); retrying on fresh connection ...", flush=True)
                time.sleep(5)
            finally:
                if raw is not None:
                    try:
                        raw.close()
                    except Exception:
                        pass
        if last_exc is not None:
            raise last_exc
        total += len(rows)
        print(f"{time.time()-tc:.1f}s")
    print(f"  upsert done in {time.time()-t:.1f}s total")
    return total


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
            entries.append({"province": prov_dir.name, "month": month,
                            "part": m.group(3), "path": f})
    return entries


def _key(e: dict) -> str:
    """Done-marker key: province/month[.partNNN] for split part-files."""
    k = f"{e['province']}/{e['month']}"
    return k + (f".part{e['part']}" if e.get("part") else "")


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

    # Wall-clock watchdog for silent network stalls (see _force_close) —
    # armed per chunk attempt in _bulk_upsert.

    # Blackholed connections (this network drops them silently) must die fast:
    # tcp_user_timeout makes the client error out after 2 min of unacked data
    # instead of hanging for 15+ min on TCP retransmits.
    engine = create_engine(
        pgurl,
        pool_pre_ping=True,
        connect_args={
            "connect_timeout": 15,
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 3,
            "tcp_user_timeout": 120000,
        },
    )
    # init_table DDL (CREATE TABLE/INDEX IF NOT EXISTS) takes a ShareLock that
    # blocks behind orphaned idle-in-transaction writers from killed runs —
    # and this script has been killed a lot. The table already exists in
    # practice, so skip the DDL when present (to_regclass is catalog-only).
    with engine.connect() as _c:
        _exists = _c.execute(text(
            "SELECT to_regclass('marketdata.md_shanxi_nodal_price_96') IS NOT NULL")).scalar()
    if not _exists:
        init_table(engine)

    nodal_root = _REPO / "data" / "nodal"
    entries = scan(nodal_root, args.province, args.since)

    if not entries:
        print("No CSV files found.")
        return

    done = set() if args.no_resume else _load_done()
    pending = [e for e in entries if _key(e) not in done]

    print(f"Found {len(entries)} file(s) total, {len(pending)} to process.\n")

    total_rows = 0
    errors: list[str] = []
    t0_all = time.time()

    for i, e in enumerate(pending, 1):
        label = _key(e).replace("/", " / ")
        size_mb = e["path"].stat().st_size / 1024 / 1024
        print(f"[{i}/{len(pending)}] {label}  ({size_mb:.1f} MB)")
        t0 = time.time()
        try:
            n = _bulk_upsert(e["path"], engine)
            elapsed = time.time() - t0
            total_rows += n
            print(f"  ✓ {n:,} rows  ({elapsed:.1f}s)")
            _mark_done(_key(e))
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
