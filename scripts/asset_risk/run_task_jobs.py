"""One-off asset-risk jobs run inside the asset-risk image via ECS run-task (ruling R5).

Self-contained: imports only stdlib, sqlalchemy/boto3 and libs/ (PYTHONPATH=/app in the image),
so it can be base64-shipped into the container. DB via the PGURL env. Dry-run unless --apply.

    python run_task_jobs.py ensure [--apply]
        Create marketdata.rm_ic_assumptions + marketdata.rm_review_metrics (CREATE IF NOT EXISTS).
    python run_task_jobs.py seed --s3-prefix s3://bucket/prefix [--apply]
    python run_task_jobs.py seed --local-dir DIR [--apply]          # tests / local
        .xlsx -> parse_ic_workbook + save_ic; .pptx -> parse_bess_deck + save_review (sha256 of bytes).
        Prints per file: rows, notes, assets, months. Files are processed in key order; for IC a
        later workbook replaces an earlier one per asset.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import sys
import tempfile
from pathlib import Path

for _p in ("/app", str(Path(__file__).resolve().parents[2])):
    if _p not in sys.path and Path(_p, "libs").is_dir():
        sys.path.insert(0, _p)

from libs.asset_risk import ic_case  # noqa: E402
from libs.asset_risk.review import store as review_store  # noqa: E402
from libs.asset_risk.review.parse_bess import parse_bess_deck  # noqa: E402

TABLES = ["marketdata.rm_ic_assumptions", "marketdata.rm_review_metrics"]
EXTS = (".xlsx", ".pptx")


def _engine():
    url = os.environ.get("PGURL")
    if not url:
        raise SystemExit("PGURL not set")
    from sqlalchemy import create_engine
    return create_engine(url)


def cmd_ensure(engine, apply: bool, out=print) -> int:
    if not apply:
        out("-- dry-run: would ensure " + ", ".join(TABLES) + " (use --apply)")
        return 0
    ic_case.ensure_table(engine)
    review_store.ensure_table(engine)
    for t in TABLES:
        out(f"ensured {t}")
    return 0


def _parse_s3(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise SystemExit(f"bad --s3-prefix {uri!r}; expected s3://bucket/prefix")
    bucket, _, prefix = uri[5:].partition("/")
    return bucket, prefix


def fetch_s3(uri: str, dest: Path, client=None) -> list[Path]:
    """Download every object under the prefix to dest; returns local paths in key order."""
    import boto3
    client = client or boto3.client("s3")
    bucket, prefix = _parse_s3(uri)
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", []) if not o["Key"].endswith("/")]
    paths = []
    for i, key in enumerate(sorted(keys)):
        p = dest / f"{i:03d}_{Path(key).name}"
        client.download_file(bucket, key, str(p))
        paths.append(p)
    return paths


def _display_name(p: Path, from_s3: bool) -> str:
    return p.name.split("_", 1)[1] if from_s3 else p.name


def cmd_seed(engine, files: list[Path], apply: bool, from_s3: bool = False, out=print) -> int:
    if apply:
        ic_case.ensure_table(engine)
        review_store.ensure_table(engine)
    failed = parsed_any = 0
    for p in files:
        name = _display_name(p, from_s3)
        ext = p.suffix.lower()
        if ext not in EXTS or name.startswith("~$"):
            out(f"-- {name}: skipped (not .xlsx/.pptx)")
            continue
        data = p.read_bytes()
        try:
            if ext == ".xlsx":
                df, notes = ic_case.parse_ic_workbook(p, name)
                months = "n/a (IC op_year 1-20)"
            else:
                df, notes = parse_bess_deck(p, name)
                months = sorted({str(m)[:7] for m in df["month"]}) if not df.empty else []
        except Exception as e:  # noqa: BLE001 — one bad file must not stop the batch
            out(f"-- {name}: PARSE ERROR {type(e).__name__}: {e}")
            failed += 1
            continue
        assets = sorted(df["asset_code"].unique()) if not df.empty else []
        out(f"-- {name}: rows={len(df)} notes={len(notes)} assets={assets} months={months}")
        for n in notes:
            out(f"   note: {n}")
        if df.empty:
            continue
        parsed_any += 1
        if not apply:
            continue
        if ext == ".xlsx":
            ic_case.save_ic(engine, df)
            out(f"   saved {len(df)} IC rows")
        else:
            n = review_store.save_review(engine, df, name, hashlib.sha256(data).hexdigest())
            out(f"   saved {n} review rows" if n else "   skipped (file hash already stored)")
    out("-- applied" if apply else "-- dry-run: nothing written (use --apply)")
    if failed:
        out(f"-- {failed} file(s) failed to parse")
    return 1 if failed or not parsed_any else 0


def main(argv=None, engine=None, s3_client=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("ensure")
    e.add_argument("--apply", action="store_true")
    s = sub.add_parser("seed")
    src = s.add_mutually_exclusive_group(required=True)
    src.add_argument("--s3-prefix")
    src.add_argument("--local-dir")
    s.add_argument("--apply", action="store_true")
    a = ap.parse_args(argv)
    if a.apply and engine is None:
        engine = _engine()
    if a.cmd == "ensure":
        return cmd_ensure(engine, a.apply)
    if a.local_dir:
        files = sorted(p for p in Path(a.local_dir).iterdir() if p.is_file())
        return cmd_seed(engine, files, a.apply)
    with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
        files = fetch_s3(a.s3_prefix, Path(tmp), s3_client)
        if not files:
            print(f"-- nothing under {a.s3_prefix}")
            return 1
        return cmd_seed(engine, files, a.apply, from_s3=True)


if __name__ == "__main__":
    sys.exit(main())
