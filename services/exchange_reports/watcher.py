"""Scheduled watcher: ingest exchange monthly reports from OneDrive proactively.

The desk saves exchange monthly reports into the OneDrive folder
``etrm/bess-platform/data/exchange-monthly-reports/<province>月报/…`` (synced to
OneDrive cloud regardless of the workstation's state). Once a day, Hermes lists
that tree via the Graph API, skips files already in staging.exchange_monthly_reports
(by file_name — cheap, before downloading), downloads the rest, and runs the
standard ingest_report pipeline (metrics → staging.exchange_monthly_metrics,
doc → shared KB as monthly_report, sha256 dedup as backstop for renames).

Runs on ECS in the Hermes scheduler — no workstation dependency. Also usable
as a CLI for backfills:

    python -m services.exchange_reports.watcher --days 365
    python -m services.exchange_reports.watcher --all --dry-run
"""
from __future__ import annotations

import datetime as dt
import logging
import os
import sys
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_ONEDRIVE_ROOT = "etrm/bess-platform/data/exchange-monthly-reports"
_PDF_SUFFIXES = (".pdf",)


def _list_tree(onedrive, root: str, max_depth: int = 3) -> list[dict]:
    """List files under root up to max_depth levels: root(1) → province dirs(2)
    → nested date-stamped aggregate dirs(3, e.g. 各省披露月报-2026-07-29)."""
    out: list[dict] = []

    def _walk(path: str, depth: int) -> None:
        try:
            items = onedrive.list_items(path)
        except Exception as exc:
            logger.warning("exchange watch: list_items failed for %s: %s", path, exc)
            return
        for it in items:
            if "folder" in it:
                if depth < max_depth:
                    _walk(f"{path}/{it['name']}", depth + 1)
            else:
                out.append(it)

    _walk(root, 1)
    return out


def _within_days(item: dict, days: Optional[int]) -> bool:
    if days is None:
        return True
    ts = item.get("lastModifiedDateTime", "")
    try:
        modified = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return True  # unknown date → treat as new
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
    return modified >= cutoff


def _existing_filenames(pg_url: str) -> set[str]:
    import psycopg2
    conn = psycopg2.connect(pg_url, options="-c statement_timeout=15000")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT file_name FROM staging.exchange_monthly_reports")
            return {r[0] for r in cur.fetchall() if r[0]}
    finally:
        conn.close()


def scan_exchange_reports_onedrive(
    onedrive,
    pg_url: str,
    api_key: str,
    feishu=None,
    owner_open_id: str = "",
    days: Optional[int] = 45,
    dry_run: bool = False,
    root: str = _ONEDRIVE_ROOT,
) -> dict:
    """One scan pass. Returns {scanned, candidates, ingested, duplicate, failed, skipped, results}."""
    from services.exchange_reports.ingestor import ingest_report

    summary: dict = {"scanned": 0, "candidates": 0, "ingested": 0,
                     "duplicate": 0, "failed": 0, "skipped": 0, "results": []}

    if onedrive is None:
        logger.warning("exchange watch: OneDrive not configured — skipping scan")
        summary["error"] = "onedrive_not_configured"
        return summary

    files = [
        it for it in _list_tree(onedrive, root)
        if it.get("name", "").lower().endswith(_PDF_SUFFIXES)
    ]
    summary["scanned"] = len(files)

    try:
        known = _existing_filenames(pg_url)
    except Exception as exc:
        logger.error("exchange watch: cannot load existing filenames: %s", exc)
        summary["error"] = f"db_error: {exc}"
        return summary

    candidates = [it for it in files if it["name"] not in known and _within_days(it, days)]
    summary["candidates"] = len(candidates)
    summary["skipped"] = summary["scanned"] - len(candidates)

    for it in candidates:
        name = it["name"]
        if dry_run:
            summary["results"].append({"file": name, "status": "dry_run"})
            continue
        try:
            file_bytes = onedrive.read_file(it["id"])
            result = ingest_report(
                file_bytes=file_bytes,
                filename=name,
                province=None,  # auto-detect (Claude when filename alone is ambiguous)
                pg_url=pg_url,
                anthropic_api_key=api_key,
            )
            status = result.get("status", "failed")
            summary[status if status in ("ingested", "duplicate", "failed") else "failed"] += 1
            summary["results"].append({
                "file": name, "status": status,
                "province": result.get("province"),
                "report_month": str(result.get("report_month", "")),
            })
            logger.info("exchange watch: %s → %s (%s %s)", name, status,
                        result.get("province"), result.get("report_month"))
        except Exception as exc:
            summary["failed"] += 1
            summary["results"].append({"file": name, "status": "failed", "error": str(exc)[:200]})
            logger.error("exchange watch: ingest failed for %s: %s", name, exc, exc_info=True)

    # Notify only when something new actually landed
    if feishu and owner_open_id and summary["ingested"] > 0 and not dry_run:
        landed = [r for r in summary["results"] if r["status"] == "ingested"]
        lines = "、".join(f"{r.get('province') or '?'} {r.get('report_month') or ''}" for r in landed[:8])
        try:
            feishu.send_text(
                open_id=owner_open_id,
                text=(f"📥 自动入库交易所月报：{lines}"
                      f"（新入库 {summary['ingested']} 份；跳过已入库 {summary['skipped']} 份）"),
            )
        except Exception as exc:
            logger.warning("exchange watch: feishu notify failed: %s", exc)

    return summary


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Scan OneDrive for new exchange monthly reports and ingest them")
    ap.add_argument("--days", type=int, default=45, help="only files modified within N days (default 45)")
    ap.add_argument("--all", action="store_true", help="no date limit (full backfill)")
    ap.add_argument("--dry-run", action="store_true", help="list candidates without ingesting")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(repo_root))
    try:
        from dotenv import load_dotenv
        load_dotenv(repo_root / "config" / ".env", override=False)
    except ImportError:
        pass

    from services.hermes.onedrive_client import get_shared_onedrive_client
    pg_url = os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", "")
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    client = get_shared_onedrive_client(pg_url)

    summary = scan_exchange_reports_onedrive(
        onedrive=client, pg_url=pg_url, api_key=api_key,
        days=None if args.all else args.days, dry_run=args.dry_run,
    )
    print(f"scanned={summary['scanned']} candidates={summary['candidates']} "
          f"ingested={summary['ingested']} duplicate={summary['duplicate']} "
          f"failed={summary['failed']} skipped={summary['skipped']}")
    for r in summary["results"][:40]:
        print(" ", r)
    if summary.get("error"):
        print("ERROR:", summary["error"])
        return 1
    return 1 if summary["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
