"""
Bulk Knowledge Base Ingestion
==============================
Walks a local folder recursively and ingests all supported files into the
spot-market knowledge pool (staging.spot_knowledge_docs / staging.spot_knowledge_chunks).

Usage:
    python scripts/ingest_knowledge_bulk.py --dir /path/to/knowledge/folder

Options:
    --dir       Root folder to walk (required)
    --category  Force a category for all files (default: auto-detect)
                Choices: market_rules annual_report policy_doc technical_spec
                         research_report other
    --ext       Comma-separated list of extensions to include
                Default: pdf,pptx,ppt,docx,doc,xlsx,xls,txt,png,jpg,jpeg,webp
    --dry-run   Print what would be ingested without actually doing it
    --workers   Number of parallel threads (default: 3, max: 8)
    --env       Path to .env file (default: auto-detect from repo root)

Environment variables required:
    PGURL (or DB_DSN)  — PostgreSQL connection string
    ANTHROPIC_API_KEY  — for image vision + auto-categorization + LLM fallback

Example:
    python scripts/ingest_knowledge_bulk.py \\
        --dir "C:/KnowledgeBase" \\
        --workers 4

Output:
    Prints a progress line per file: [ADDED] / [SKIP] / [ERROR]
    Prints a summary at the end: added / skipped / failed counts.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# ── repo root on sys.path ─────────────────────────────────────────────────────
_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

# ── load .env ─────────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv, find_dotenv
    _env_file = _REPO / "config" / ".env"
    if _env_file.exists():
        load_dotenv(_env_file)
    else:
        load_dotenv(find_dotenv())
except ImportError:
    pass

VALID_APPS = {"shared", "strategist", "trader"}

# Folder-name substrings that signal a document belongs to the Trader agent
_TRADER_PATH_MARKERS = {
    "5-交易数据", "交易数据",
    "电力市场结算情况",   # monthly settlement reports
    "调频结果数据",        # frequency regulation result data
}


def _resolve_app(path: Path, app_override: str | None) -> str:
    """Return the app scope for a file.

    If --app was given explicitly, use that.  Otherwise auto-detect based on
    folder path: files under any folder whose name contains a trading-data
    marker are tagged 'trader'; everything else is 'shared'.
    """
    if app_override:
        return app_override
    path_str = str(path)
    for marker in _TRADER_PATH_MARKERS:
        if marker in path_str:
            return "trader"
    return "shared"


SUPPORTED_EXTENSIONS = {
    ".pdf", ".pptx", ".ppt", ".docx", ".doc",
    ".xlsx", ".xls", ".txt",
    ".png", ".jpg", ".jpeg", ".webp",
}

VALID_CATEGORIES = {
    "market_rules", "annual_report", "policy_doc",
    "technical_spec", "research_report", "other",
}


def _collect_files(
    root: Path,
    allowed_exts: set[str],
    exclude_patterns: list[str] | None = None,
) -> list[Path]:
    """Return all files under root whose extension is in allowed_exts.

    Files whose name ends with '_Error.txt' (OneDrive download stubs) are
    always skipped.  Additional glob-style patterns can be passed via
    ``exclude_patterns`` — any file whose full path string contains one of
    the patterns is excluded.
    """
    results = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        # Skip OneDrive download-error stubs
        if path.name.endswith("_Error.txt"):
            continue
        if path.suffix.lower() not in allowed_exts:
            continue
        if exclude_patterns:
            path_str = str(path)
            if any(pat in path_str for pat in exclude_patterns):
                continue
        results.append(path)
    return results


def _ingest_file(
    path: Path,
    category_override: str | None,
    app: str,
    api_key: str | None,
    dry_run: bool,
) -> tuple[str, str]:
    """
    Ingest a single file.
    Returns (status, message) where status is "added", "skip", or "error".
    """
    if dry_run:
        return "dry", f"{path}  [app={app}]"

    try:
        from services.knowledge_pool.knowledge_docs import register_and_ingest
        file_bytes = path.read_bytes()
        doc_id, is_new, category = register_and_ingest(
            file_bytes=file_bytes,
            filename=path.name,
            category_override=category_override,
            app=app,
            api_key=api_key,
        )
        if is_new:
            return "added", f"{path.name}  [app={app}  category={category}  doc_id={doc_id}]"
        else:
            return "skip", f"{path.name}  [already ingested, doc_id={doc_id}]"
    except Exception as exc:
        return "error", f"{path.name}  {exc}"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bulk-ingest a folder of documents into the spot-market knowledge pool."
    )
    parser.add_argument("--dir", required=True, help="Root folder to walk")
    parser.add_argument("--category", default=None,
                        choices=sorted(VALID_CATEGORIES),
                        help="Force a category for all files (default: auto-detect)")
    parser.add_argument("--ext", default=None,
                        help="Comma-separated extensions, e.g. pdf,docx (default: all supported)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print files that would be ingested without doing anything")
    parser.add_argument("--workers", type=int, default=3,
                        help="Parallel worker threads (default: 3, max: 8)")
    parser.add_argument("--exclude", default=None,
                        help="Comma-separated path substrings to skip, "
                             "e.g. '各省现货价格及边界数据,交易数据'")
    parser.add_argument("--app", default=None,
                        choices=sorted(VALID_APPS),
                        help="Force an app scope for all files (default: auto-detect). "
                             "Files under '5-交易数据' folders are auto-tagged 'trader'; "
                             "everything else defaults to 'shared'.")
    args = parser.parse_args()

    root = Path(args.dir)
    if not root.exists():
        print(f"[ERROR] Directory not found: {root}", file=sys.stderr)
        sys.exit(1)

    # Resolve extensions
    if args.ext:
        allowed_exts = {"." + e.lstrip(".").lower() for e in args.ext.split(",")}
        invalid = allowed_exts - SUPPORTED_EXTENSIONS
        if invalid:
            print(f"[WARN] Unsupported extensions will be skipped: {invalid}")
        allowed_exts &= SUPPORTED_EXTENSIONS
    else:
        allowed_exts = SUPPORTED_EXTENSIONS

    workers = max(1, min(args.workers, 8))

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("[WARN] ANTHROPIC_API_KEY not set — image descriptions and LLM "
              "auto-categorization will be skipped.")

    exclude_patterns = [p.strip() for p in args.exclude.split(",")] if args.exclude else None

    # Discover files
    files = _collect_files(root, allowed_exts, exclude_patterns)
    if not files:
        print(f"No matching files found under: {root}")
        sys.exit(0)

    print(f"Found {len(files)} file(s) under {root}")
    if args.dry_run:
        print("[DRY RUN] The following files would be ingested:")
        for f in files:
            print(f"  [{_resolve_app(f, args.app):>10}]  {f}")
        sys.exit(0)

    # Ingest
    counts = {"added": 0, "skip": 0, "error": 0}
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _ingest_file, f, args.category,
                _resolve_app(f, args.app),
                api_key, False,
            ): f
            for f in files
        }
        done = 0
        for future in as_completed(futures):
            done += 1
            status, msg = future.result()
            counts[status] = counts.get(status, 0) + 1
            icon = {"added": "[ADDED]", "skip": "[SKIP ]", "error": "[ERROR]"}.get(status, status)
            pct = done / len(files) * 100
            print(f"{icon}  ({done:>4}/{len(files)}, {pct:4.0f}%)  {msg}")

    elapsed = time.time() - t0
    print(
        f"\nDone in {elapsed:.1f}s — "
        f"added: {counts.get('added', 0)}  "
        f"skipped: {counts.get('skip', 0)}  "
        f"errors: {counts.get('error', 0)}"
    )
    if counts.get("error", 0) > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
