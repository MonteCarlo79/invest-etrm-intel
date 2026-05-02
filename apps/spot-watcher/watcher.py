"""
Spot market file watcher — local Windows service.

Watches data/spot reports/{year}/ for new PDF files and triggers the
ingestion pipeline automatically.

Usage:
    python apps/spot-watcher/watcher.py                   # watch 2026
    python apps/spot-watcher/watcher.py --year 2026
    python apps/spot-watcher/watcher.py --year 2026 --dir "C:/custom/path"

The watcher runs until Ctrl-C. Detected PDFs are debounced (3 seconds) to
handle OneDrive sync creating temp files before the final PDF is stable.
Already-processed files are skipped via SHA-256 hash tracking.
"""
from __future__ import annotations

import hashlib
import logging
import sys
import time
from pathlib import Path
from threading import Lock, Timer
from typing import Dict, Optional

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))

try:
    from dotenv import load_dotenv
    for _env in [
        _REPO / "config" / ".env",   # RDS PGURL — load first so it wins
        _REPO / ".env",
        _REPO / "apps" / "spot-agent" / ".env",
    ]:
        if _env.exists():
            load_dotenv(_env)         # load_dotenv never overwrites already-set vars
except ImportError:
    pass

from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent  # type: ignore
from watchdog.observers import Observer  # type: ignore

from pipeline import run as run_pipeline

_log = logging.getLogger(__name__)

DEBOUNCE_SECONDS = 3.0


def _sha256(path: Path) -> Optional[str]:
    try:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


class _PdfHandler(FileSystemEventHandler):
    def __init__(self):
        self._seen_hashes: set[str] = set()
        self._pending_timers: Dict[str, Timer] = {}
        self._lock = Lock()

    def on_created(self, event: FileCreatedEvent) -> None:
        self._handle(event.src_path)

    def on_modified(self, event: FileModifiedEvent) -> None:
        self._handle(event.src_path)

    def _handle(self, path_str: str) -> None:
        path = Path(path_str)
        if path.suffix.lower() != ".pdf":
            return
        if path.name.startswith("~$") or path.name.startswith("."):
            return  # skip temp files

        with self._lock:
            # Cancel any existing debounce timer for this path
            key = str(path)
            if key in self._pending_timers:
                self._pending_timers[key].cancel()
            t = Timer(DEBOUNCE_SECONDS, self._process, args=(path,))
            self._pending_timers[key] = t
            t.start()

    def _process(self, path: Path) -> None:
        with self._lock:
            self._pending_timers.pop(str(path), None)

        if not path.exists():
            return

        file_hash = _sha256(path)
        if file_hash is None:
            _log.warning("[WATCHER] Could not hash %s; skipping", path.name)
            return

        with self._lock:
            if file_hash in self._seen_hashes:
                _log.info("[WATCHER] %s already processed (hash match); skipping", path.name)
                return
            self._seen_hashes.add(file_hash)

        _log.info("[WATCHER] New PDF detected: %s", path.name)
        try:
            result = run_pipeline(path)
            _log.info(
                "[WATCHER] Done %s — dates=%s upserted=%d discrepancies=%d errors=%d",
                path.name,
                result.get("dates", []),
                result.get("upserted", 0),
                len(result.get("discrepancies", [])),
                len(result.get("errors", [])),
            )
            if result.get("discrepancies"):
                for d in result["discrepancies"]:
                    _log.warning("[CROSSCHECK] %s", d)
            if result.get("errors"):
                for e in result["errors"]:
                    _log.error("[ERROR] %s", e)
        except Exception as exc:
            _log.exception("[WATCHER] Pipeline failed for %s: %s", path.name, exc)


def start_watching(watch_dir: Path, year: int = 2026) -> None:
    handler = _PdfHandler()
    observer = Observer()
    observer.schedule(handler, str(watch_dir), recursive=False)
    observer.start()

    _log.info("[WATCHER] Watching %s (year=%d)", watch_dir, year)
    _log.info("[WATCHER] Press Ctrl-C to stop.")

    # Pre-scan: process any PDFs already present but not yet in seen hashes.
    # This handles files added while the watcher was not running.
    existing = sorted(watch_dir.glob("*.pdf"))
    if existing:
        _log.info("[WATCHER] Pre-scanning %d existing PDFs...", len(existing))
        for pdf in existing:
            handler._handle(str(pdf))
        # Let debounce timers fire before announcing we're live
        time.sleep(DEBOUNCE_SECONDS + 0.5)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        _log.info("[WATCHER] Stopping...")
    finally:
        observer.stop()
        observer.join()
        # Cancel any pending debounce timers
        with handler._lock:
            for t in handler._pending_timers.values():
                t.cancel()


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Watch spot reports folder and auto-ingest new PDFs")
    parser.add_argument("--year", type=int, default=2026, help="Year subfolder to watch (default: 2026)")
    parser.add_argument(
        "--dir",
        help="Override watch directory (default: data/spot reports/{year}/)",
    )
    args = parser.parse_args()

    if args.dir:
        watch_dir = Path(args.dir)
    else:
        watch_dir = _REPO / "data" / "spot reports" / str(args.year)

    if not watch_dir.exists():
        print(f"[ERROR] Watch directory does not exist: {watch_dir}", file=sys.stderr)
        sys.exit(1)

    start_watching(watch_dir, year=args.year)
