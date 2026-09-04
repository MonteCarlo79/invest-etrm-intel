"""Single-pass watcher: ingest drop-in files from assets/operating/复盘/ into the
operating-assets knowledge base (staging.ops_knowledge_*).

Designed for scheduled invocation (launchd, hourly). Each run:
  1. scans the tree (skips ~$ Excel locks and *_Error.txt OneDrive stubs)
  2. skips files already in the local checkpoint log (fast path; sha256 in
     register_and_ingest is the real dedup)
  3. ingests each file with a per-file timeout (OneDrive hydration stall mitigation)
  4. appends completed paths to the checkpoint (local disk: ~/Library/Logs/bess-ops-kb/)

CLI: python -m services.knowledge_pool.ops_watcher [--root PATH] [--dry-run] [--timeout N]
"""
from __future__ import annotations

import argparse
import concurrent.futures
import logging
import os
import sys
from pathlib import Path

log = logging.getLogger(__name__)

SUPPORTED_EXTENSIONS = {
    ".pdf", ".pptx", ".ppt", ".docx", ".doc", ".xlsx", ".xls",
    ".txt", ".html", ".htm", ".png", ".jpg", ".jpeg", ".webp",
}

_REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ROOT = _REPO_ROOT / "assets" / "operating" / "复盘"
LOG_DIR = Path.home() / "Library" / "Logs" / "bess-ops-kb"
CHECKPOINT = LOG_DIR / "checkpoint.log"


def _collect_files(root: Path) -> list[Path]:
    files = []
    for p in sorted(root.rglob("*")):
        if not p.is_file() or p.name.startswith("~$") or p.name.endswith("_Error.txt"):
            continue
        if p.suffix.lower() in SUPPORTED_EXTENSIONS:
            files.append(p)
    return files


def _load_checkpoint(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {ln.strip() for ln in path.read_text().splitlines() if ln.strip()}


def _append_checkpoint(path: Path, file_path: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(file_path + "\n")


def _load_asset_names() -> dict[str, int]:
    from shared.agents.db import get_conn
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT name, id FROM marketdata.rm_assets")
        return {r[0]: r[1] for r in cur.fetchall()}


def run_ops_watcher(root: Path = DEFAULT_ROOT, timeout: int = 120,
                    checkpoint: Path = CHECKPOINT, dry_run: bool = False) -> dict:
    """One scan+ingest pass. Returns a summary dict."""
    from services.knowledge_pool.ops_knowledge_docs import register_and_ingest

    if not root.is_dir():
        log.error("Review root not found: %s", root)
        return {"error": f"root not found: {root}"}

    files = _collect_files(root)
    done = _load_checkpoint(checkpoint)
    pending = [f for f in files if str(f) not in done]
    log.info("ops-watcher: %d file(s) under %s (%d new)", len(files), root, len(pending))

    summary = {"total": len(files), "pending": len(pending),
               "added": 0, "duplicates": 0, "errors": 0, "details": []}
    if dry_run:
        for f in pending:
            log.info("[dry-run] would ingest: %s", f.name)
            summary["details"].append(f"dry-run: {f.name}")
        return summary

    asset_names = _load_asset_names()

    def _ingest_one(path: Path):
        from services.knowledge_pool.ops_knowledge_docs import register_and_ingest as _r
        return _r(path.read_bytes(), path.name, source_path=str(path),
                  asset_names=asset_names)

    for f in pending:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            fut = pool.submit(_ingest_one, f)
            try:
                doc_id, is_new, category = fut.result(timeout=timeout)
            except concurrent.futures.TimeoutError:
                log.error("[timeout>%ds] %s", timeout, f.name)
                summary["errors"] += 1
                summary["details"].append(f"timeout: {f.name}")
                continue
            except Exception as exc:
                log.error("[error] %s — %s", f.name, exc)
                summary["errors"] += 1
                summary["details"].append(f"error: {f.name}: {exc}")
                continue
        tag = "added" if is_new else "duplicate"
        summary["added" if is_new else "duplicates"] += 1
        summary["details"].append(f"{tag}: {f.name} (doc_id={doc_id}, category={category})")
        log.info("%s: %s (doc_id=%d, category=%s)", tag, f.name, doc_id, category)
        _append_checkpoint(checkpoint, str(f))

    log.info("ops-watcher done: %d added, %d duplicates, %d errors",
             summary["added"], summary["duplicates"], summary["errors"])
    return summary


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    p = argparse.ArgumentParser(description="Ingest assets/operating/复盘/ into the ops KB")
    p.add_argument("--root", default=str(DEFAULT_ROOT))
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--timeout", type=int, default=120)
    args = p.parse_args(argv)
    summary = run_ops_watcher(root=Path(args.root), timeout=args.timeout, dry_run=args.dry_run)
    print(summary)
    return 1 if summary.get("errors") else 0


if __name__ == "__main__":
    sys.exit(main())
