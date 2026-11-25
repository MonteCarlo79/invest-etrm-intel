"""Dump a laptop-sized copy of the platform database to S3.

Runs as a one-off ECS task (the Mac cannot reach RDS). Read-only on the
database. Launched by scripts/replica.sh, which stages this file in S3.

Layout written to s3://<bucket>/replica/<stamp>/:
  dump.tar            pg_dump directory format: all schema + data of every
                      table except the big ones below and _stg_* scratch tables
  win/<table>.csv.gz  recent window (or column subset) of each big table
  manifest.json       written last; replica/latest.json points at it
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from urllib.parse import unquote, urlparse

BUCKET = os.environ.get("REPLICA_BUCKET", "bess-uploader-data-chen-singp-2026")
PREFIX = "replica"
WORK = "/tmp/replica"

# table -> (column list or "*", WHERE clause). {d} = window in days.
BIG = {
    "marketdata.md_shanxi_nodal_price_96": ("*", "metric_time >= now() - interval '{d} days'", 14),
    "marketdata.md_rt_nodal_price": ("*", "data_date >= current_date - {d}", 90),
    "marketdata.md_mengxi_nodal_price_96_filled": ("*", "metric_time >= now() - interval '{d} days'", 90),
    "marketdata.md_id_cleared_energy": ("*", "data_date >= current_date - {d}", 90),
    "marketdata.md_da_cleared_energy": ("*", "data_date >= current_date - {d}", 90),
    "intl_market.gb_bess_leaderboard": ("*", "settlement_date >= current_date - {d}", 365),
    # embeddings are 14 GB of the table; text search works without them
    "staging.spot_knowledge_chunks": ("id, doc_id, page_no, chunk_index, chunk_text", "true", 0),
}


def _log(msg: str) -> None:
    print(f"[replica] {msg}", flush=True)


def _pg_env() -> dict:
    u = urlparse(os.environ["PGURL"].replace("postgresql+psycopg2", "postgresql"))
    env = dict(os.environ)
    env.update(
        PGHOST=u.hostname, PGPORT=str(u.port or 5432), PGUSER=unquote(u.username or ""),
        PGPASSWORD=unquote(u.password or ""), PGDATABASE=u.path.lstrip("/"),
        PGSSLMODE="require", PGCONNECT_TIMEOUT="20",
    )
    return env


def _ensure_client() -> None:
    if shutil.which("pg_dump"):
        return
    _log("installing postgresql-client-18")
    run = lambda c: subprocess.run(c, shell=True, check=True, stdout=subprocess.DEVNULL)  # noqa: E731
    run("apt-get update -qq && apt-get install -y -qq postgresql-common ca-certificates")
    run("/usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y")
    run("apt-get install -y -qq postgresql-client-18")


def _windows() -> dict:
    """Per-table window days; REPLICA_WINDOWS='table=days,...' overrides."""
    days = {t: d for t, (_, _, d) in BIG.items()}
    for item in filter(None, os.environ.get("REPLICA_WINDOWS", "").split(",")):
        t, d = item.split("=")
        if t.strip() in days:
            days[t.strip()] = int(d)
    return days


def main() -> int:
    import boto3

    t0 = time.time()
    env = _pg_env()
    _ensure_client()
    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(f"{WORK}/win")

    def psql(sql: str) -> str:
        return subprocess.run(["psql", "-AtX", "-c", sql], env=env, check=True,
                              capture_output=True, text=True).stdout

    stg = [t for t in psql(
        "select n.nspname||'.'||c.relname from pg_class c join pg_namespace n "
        "on n.oid=c.relnamespace where c.relkind='r' and c.relname like '\\_stg\\_%'"
    ).split("\n") if t]

    cmd = ["pg_dump", "-Fd", "-j", "4", "-Z", "6", "--no-owner", "--no-privileges",
           "-f", f"{WORK}/dump"]
    for t in stg:
        cmd += ["--exclude-table", t]
    for t in BIG:
        cmd += ["--exclude-table-data", t]
    _log(f"pg_dump: {len(stg)} scratch tables skipped, {len(BIG)} big tables schema-only")
    subprocess.run(cmd, env=env, check=True)
    subprocess.run(["tar", "-cf", f"{WORK}/dump.tar", "-C", WORK, "dump"], check=True)
    shutil.rmtree(f"{WORK}/dump")
    _log(f"pg_dump done: {os.path.getsize(f'{WORK}/dump.tar') / 1e6:.0f} MB, {time.time() - t0:.0f}s")

    days = _windows()
    windows = {}
    for t, (cols, where, _) in BIG.items():
        t1 = time.time()
        out = f"{WORK}/win/{t}.csv.gz"
        sel = f"select {cols} from {t} where {where.format(d=days[t])}"
        with open(out, "wb") as fh:
            p1 = subprocess.Popen(["psql", "-X", "-c", f"copy ({sel}) to stdout with (format csv)"],
                                  env=env, stdout=subprocess.PIPE)
            p2 = subprocess.Popen(["gzip", "-6"], stdin=p1.stdout, stdout=fh)
            p1.stdout.close()
            if p2.wait() or p1.wait():
                raise RuntimeError(f"window copy failed: {t}")
        windows[t] = {"columns": cols, "days": days[t], "bytes": os.path.getsize(out)}
        _log(f"window {t}: {windows[t]['bytes'] / 1e6:.0f} MB, {time.time() - t1:.0f}s")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    s3 = boto3.client("s3")
    base = f"{PREFIX}/{stamp}"
    files = {"dump.tar": os.path.getsize(f"{WORK}/dump.tar")}
    s3.upload_file(f"{WORK}/dump.tar", BUCKET, f"{base}/dump.tar")
    for t, w in windows.items():
        s3.upload_file(f"{WORK}/win/{t}.csv.gz", BUCKET, f"{base}/win/{t}.csv.gz")
        files[f"win/{t}.csv.gz"] = w["bytes"]
    manifest = {"stamp": stamp, "base": base, "files": files, "windows": windows,
                "server_version": psql("show server_version").strip()}
    body = json.dumps(manifest, ensure_ascii=False, indent=1).encode()
    s3.put_object(Bucket=BUCKET, Key=f"{base}/manifest.json", Body=body)
    s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}/latest.json", Body=body)

    # keep only this snapshot
    pag = s3.get_paginator("list_objects_v2")
    old = [o["Key"] for pg in pag.paginate(Bucket=BUCKET, Prefix=f"{PREFIX}/")
           for o in pg.get("Contents", [])
           if not o["Key"].startswith(base + "/")
           and o["Key"] not in (f"{PREFIX}/latest.json", f"{PREFIX}/dump.py")]
    for i in range(0, len(old), 1000):
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": k} for k in old[i:i + 1000]]})

    _log(f"DONE {stamp}: {sum(files.values()) / 1e6:.0f} MB in {time.time() - t0:.0f}s, "
         f"{len(old)} old objects removed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
