"""
services/ops/watch_dispatch_files.py

File-drop trigger for Inner Mongolia BESS daily dispatch reports.

Watches a local directory for new .xlsx files.  When a new dispatch file
arrives it:
  1. Runs the ops ingestion pipeline for that file.
  2. Infers the report date from the filename.
  3. Runs the trading performance agent for that date and sends an email.

Usage
-----
  # Watch default 2026 directory
  py services/ops/watch_dispatch_files.py

  # Watch a specific directory
  py services/ops/watch_dispatch_files.py --watch-dir data/operations/bess/inner-mongolia/2026

  # Dry run — ingest only, no agent, no email
  py services/ops/watch_dispatch_files.py --dry-run

  # Override recipient
  py services/ops/watch_dispatch_files.py --email-to ops@example.com

  # Force re-ingest even if file was already processed
  py services/ops/watch_dispatch_files.py --force

Required environment variables
-------------------------------
  PGURL or DB_DSN           — PostgreSQL connection string
  ANTHROPIC_API_KEY         — Claude API key (for agent step)
  SMTP_HOST                 — SMTP server hostname
  SMTP_PORT                 — SMTP port (default 587)
  SMTP_USER                 — SMTP login
  SMTP_PASSWORD             — SMTP password
  SMTP_FROM                 — Sender address
  REPORT_EMAIL_TO           — Comma-separated recipient list
                              (overridden by --email-to)

Stop
----
  Ctrl-C  (or kill the process)
"""
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import date as _date
from pathlib import Path

from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent
from watchdog.observers import Observer

# ---------------------------------------------------------------------------
# Path setup — allow running as a script or module
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
_LOG_DIR = _ROOT / "logs"
_LOG_DIR.mkdir(exist_ok=True)
_LOG_FILE = _LOG_DIR / "watch_dispatch_files.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(_LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
_DEFAULT_WATCH_DIR = str(_ROOT / "data" / "operations" / "bess" / "inner-mongolia" / "2026")
_DEFAULT_EMAIL = "chen_dpeng@hotmail.com"
_SETTLE_SECONDS = 4   # wait after event before processing (file copy may still be in progress)
_DEBOUNCE_SECONDS = 30  # ignore repeat events on the same path within this window


# ---------------------------------------------------------------------------
# Date inference
# ---------------------------------------------------------------------------

def _infer_date(path: str) -> _date | None:
    """Extract the report date from a dispatch file path using the existing date_parser."""
    try:
        from services.ops_ingestion.inner_mongolia.date_parser import parse_date
        # Infer year from /YYYY/ path component
        import re
        normalised = path.replace("\\", "/")
        m = re.search(r"/(\d{4})/", normalised)
        year_hint = int(m.group(1)) if m else None
        return parse_date(path, year_hint=year_hint)
    except Exception as exc:
        log.warning("Could not parse date from %s: %s", path, exc)
        return None


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def _run_ingestion(path: str, force: bool, dry_run: bool) -> bool:
    """Run the ops ingestion entrypoint for a single file. Returns True on success."""
    cmd = [
        sys.executable, "-m",
        "services.ops_ingestion.inner_mongolia.entrypoint",
        "--file", path,
    ]
    if force:
        cmd.append("--force")
    if dry_run:
        cmd.append("--dry-run")

    log.info("Ingesting: %s", os.path.basename(path))
    result = subprocess.run(cmd, cwd=str(_ROOT), capture_output=False)
    if result.returncode != 0:
        log.error("Ingestion failed (exit %d): %s", result.returncode, os.path.basename(path))
        return False
    log.info("Ingestion complete: %s", os.path.basename(path))
    return True


def _run_agent(report_date: _date, email_to: str, dry_run: bool) -> bool:
    """Run the trading performance agent for the given date. Returns True on success."""
    cmd = [
        sys.executable,
        str(_ROOT / "services" / "ops" / "run_trading_agent.py"),
        "--date", report_date.isoformat(),
    ]
    if dry_run:
        cmd.append("--dry-run")
    else:
        cmd.append("--send-email")

    env = os.environ.copy()
    env["REPORT_EMAIL_TO"] = email_to

    log.info("Running trading agent for %s (email → %s)", report_date, email_to)
    result = subprocess.run(cmd, cwd=str(_ROOT), env=env, capture_output=False)
    if result.returncode != 0:
        log.error("Trading agent failed (exit %d) for %s", result.returncode, report_date)
        return False
    log.info("Trading agent complete for %s", report_date)
    return True


# ---------------------------------------------------------------------------
# File event handler
# ---------------------------------------------------------------------------

class DispatchFileHandler(FileSystemEventHandler):

    def __init__(self, email_to: str, force: bool, dry_run: bool):
        self._email_to = email_to
        self._force = force
        self._dry_run = dry_run
        self._last_processed: dict[str, float] = {}   # path → epoch time

    def _should_process(self, path: str) -> bool:
        if not path.endswith(".xlsx"):
            return False
        if os.path.basename(path).startswith("~$"):  # Excel temp/lock file
            return False
        last = self._last_processed.get(path, 0)
        if time.time() - last < _DEBOUNCE_SECONDS:
            log.debug("Debounced: %s", os.path.basename(path))
            return False
        return True

    def _handle(self, path: str) -> None:
        if not self._should_process(path):
            return

        self._last_processed[path] = time.time()

        # Wait for the file to finish copying
        log.info("New file detected: %s — settling %ss…", os.path.basename(path), _SETTLE_SECONDS)
        time.sleep(_SETTLE_SECONDS)

        # Verify file is still there (could be a temp file that vanished)
        if not os.path.isfile(path):
            log.warning("File gone after settle: %s — skipping", path)
            return

        # Step 1: ingest
        ok = _run_ingestion(path, force=self._force, dry_run=self._dry_run)
        if not ok:
            log.error("Skipping agent step due to ingestion failure.")
            return

        # Step 2: infer date and run agent
        report_date = _infer_date(path)
        if report_date is None:
            log.warning("Cannot infer date from %s — skipping agent.", os.path.basename(path))
            return

        _run_agent(report_date, self._email_to, dry_run=self._dry_run)

    def on_created(self, event):
        if not event.is_directory:
            self._handle(event.src_path)

    def on_moved(self, event):
        # Handles files moved/renamed into the watched directory (e.g. OneDrive sync)
        if not event.is_directory:
            self._handle(event.dest_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Watch a directory for new dispatch .xlsx files and trigger ingestion + report.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--watch-dir",
        default=_DEFAULT_WATCH_DIR,
        metavar="DIR",
        help=f"Directory to watch (default: {_DEFAULT_WATCH_DIR})",
    )
    p.add_argument(
        "--email-to",
        default=os.environ.get("REPORT_EMAIL_TO", _DEFAULT_EMAIL),
        metavar="EMAIL",
        help=f"Report recipient email (default: {_DEFAULT_EMAIL})",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Pass --force to ingestion (reprocess even if file hash already in registry)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Ingest and run agent in dry-run mode (no DB writes, no email sent)",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    watch_dir = os.path.abspath(args.watch_dir)
    if not os.path.isdir(watch_dir):
        log.error("Watch directory not found: %s", watch_dir)
        sys.exit(1)

    # Override REPORT_EMAIL_TO so subprocess picks it up
    os.environ["REPORT_EMAIL_TO"] = args.email_to

    log.info("=" * 60)
    log.info("BESS Dispatch File Watcher")
    log.info("  Watching : %s", watch_dir)
    log.info("  Email to : %s", args.email_to)
    log.info("  Dry run  : %s", args.dry_run)
    log.info("  Log file : %s", _LOG_FILE)
    log.info("=" * 60)
    log.info("Press Ctrl-C to stop.")

    handler = DispatchFileHandler(
        email_to=args.email_to,
        force=args.force,
        dry_run=args.dry_run,
    )

    observer = Observer()
    observer.schedule(handler, watch_dir, recursive=False)
    observer.start()

    try:
        while observer.is_alive():
            observer.join(timeout=5)
    except KeyboardInterrupt:
        log.info("Stopping watcher…")
    finally:
        observer.stop()
        observer.join()
        log.info("Watcher stopped.")


if __name__ == "__main__":
    main()
