"""
Knowledge Synthesis Pipeline Runner
=====================================
Runs all 5 phases of knowledge intelligence building:

  Phase 1: Document synthesis (summaries + Q&A pairs + entities)
  Phase 2: Knowledge graph (entity deduplication + relationships)
  Phase 3: Policy timeline (temporal regulatory context)
  Phase 4: Expert memory (extracts insights from today's conversation logs)

Usage:
    # Run everything (typical first-time run)
    py scripts/run_synthesis_pipeline.py

    # Phase 1 only, shared docs first
    py scripts/run_synthesis_pipeline.py --phase 1 --app shared

    # Phase 1 with limit for testing
    py scripts/run_synthesis_pipeline.py --phase 1 --limit 20

    # Resume: will skip already-synthesized docs
    py scripts/run_synthesis_pipeline.py --phase 1

    # After Phase 1 completes, run knowledge graph + policy timeline
    py scripts/run_synthesis_pipeline.py --phase 2 --phase 3

    # Extract insights from today's sessions
    py scripts/run_synthesis_pipeline.py --phase 4

Options:
    --phase     Which phase(s) to run (1, 2, 3, 4). Default: all
    --app       Filter docs by app: shared | trader (Phase 1 only)
    --limit     Max docs to process in Phase 1 (useful for testing)
    --workers   Parallel workers (default: 3)
    --dry-run   Show what would be done without calling the API
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from dotenv import load_dotenv
for _env in [_REPO / "config" / ".env", _REPO / ".env"]:
    if _env.exists():
        load_dotenv(_env)
        break


def _require_api_key() -> str:
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        print("[ERROR] ANTHROPIC_API_KEY not set in environment.", file=sys.stderr)
        sys.exit(1)
    return key


def run_phase_1(api_key: str, app: str | None, limit: int | None, workers: int) -> None:
    print("\n" + "=" * 60)
    print("PHASE 1: Document Synthesis")
    print("=" * 60)

    from services.knowledge_pool.synthesis import SynthesisPipeline
    pipeline = SynthesisPipeline(api_key=api_key, workers=workers)

    # Run shared first (strategist), then trader
    scopes = [app] if app else ["shared", "trader"]
    for scope in scopes:
        print(f"\n→ Processing scope: {scope}")
        counts = pipeline.run(app_filter=scope, limit=limit, verbose=True)
        print(
            f"  Phase 1 [{scope}] — ok: {counts['ok']}  errors: {counts['error']}"
        )


def run_phase_2(api_key: str, workers: int) -> None:
    print("\n" + "=" * 60)
    print("PHASE 2: Knowledge Graph")
    print("=" * 60)

    from services.knowledge_pool.knowledge_graph import build_knowledge_graph
    counts = build_knowledge_graph(api_key=api_key, verbose=True)
    print(f"  Entities: {counts['entities']}  Relations: {counts['relations']}")


def run_phase_3(api_key: str, workers: int) -> None:
    print("\n" + "=" * 60)
    print("PHASE 3: Policy Timeline")
    print("=" * 60)

    from services.knowledge_pool.knowledge_graph import build_policy_timeline
    counts = build_policy_timeline(api_key=api_key, workers=workers, verbose=True)
    print(
        f"  Processed: {counts['processed']}  "
        f"Policies found: {counts['policies_found']}  "
        f"Errors: {counts['errors']}"
    )


def run_phase_4(api_key: str) -> None:
    print("\n" + "=" * 60)
    print("PHASE 4: Expert Memory Extraction")
    print("=" * 60)

    from services.knowledge_pool.expert_memory import extract_and_store_insights, get_memory_stats
    n = extract_and_store_insights(api_key=api_key)
    print(f"  Insights extracted from today's session: {n}")
    stats = get_memory_stats()
    print(
        f"  Total expert memory: {stats.get('total', 0)} insights  "
        f"({stats.get('high_conf', 0)} high-confidence)"
    )


def show_dry_run(app: str | None, limit: int | None) -> None:
    print("\n[DRY RUN] Would process:")

    from services.knowledge_pool.synthesis import _get_unprocessed_doc_ids
    scopes = [app] if app else ["shared", "trader"]
    for scope in scopes:
        ids = _get_unprocessed_doc_ids(app_filter=scope, limit=limit)
        print(f"  Phase 1 [{scope}]: {len(ids)} documents pending synthesis")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run knowledge intelligence pipeline phases"
    )
    parser.add_argument(
        "--phase", type=int, action="append", dest="phases",
        choices=[1, 2, 3, 4],
        help="Phase(s) to run (can specify multiple). Default: all",
    )
    parser.add_argument("--app", choices=["shared", "trader"], default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    phases = sorted(set(args.phases)) if args.phases else [1, 2, 3, 4]

    if args.dry_run:
        show_dry_run(args.app, args.limit)
        return

    api_key = _require_api_key()

    if 1 in phases:
        run_phase_1(api_key, args.app, args.limit, args.workers)

    if 2 in phases:
        run_phase_2(api_key, args.workers)

    if 3 in phases:
        run_phase_3(api_key, args.workers)

    if 4 in phases:
        run_phase_4(api_key)

    print("\n" + "=" * 60)
    print("Pipeline complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
