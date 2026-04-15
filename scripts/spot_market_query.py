#!/usr/bin/env python3
"""
Spot market knowledge pool — retrieval CLI.

Searches staging.spot_report_chunks and staging.spot_report_facts.

Usage:
    # Full-text search across all chunks
    python scripts/spot_market_query.py search "新能源出力下降"

    # Search within a province
    python scripts/spot_market_query.py search "均价偏高" --province 山东

    # Search within a date range
    python scripts/spot_market_query.py search "原因为" --from 2025-07-01 --to 2025-07-31

    # Show structured price facts for a province
    python scripts/spot_market_query.py facts --province 山东 --type price_da

    # List all driver facts for a date range
    python scripts/spot_market_query.py facts --type driver --from 2025-07-16

    # List all registered documents
    python scripts/spot_market_query.py docs

    # List all registered notes
    python scripts/spot_market_query.py notes
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

try:
    from dotenv import load_dotenv
    for _env_candidate in [
        _REPO / ".env",
        _REPO / "apps" / "spot-agent" / ".env",
    ]:
        if _env_candidate.exists():
            load_dotenv(_env_candidate)
            break
except ImportError:
    pass

from services.knowledge_pool.retrieval import (
    search_chunks, get_facts, get_note_index, get_document_list,
)


def _fmt_date(d) -> str:
    return str(d) if d else "—"


def cmd_search(args):
    results = search_chunks(
        query=args.query,
        province_cn=args.province,
        date_from=dt.date.fromisoformat(args.from_date) if args.from_date else None,
        date_to=dt.date.fromisoformat(args.to_date) if args.to_date else None,
        chunk_type=args.chunk_type,
        limit=args.limit,
    )

    if not results:
        print("No results found.", flush=True)
        return

    print(f"{len(results)} result(s) for '{args.query}':\n", flush=True)
    for i, r in enumerate(results, 1):
        snippet = r["chunk_text"][:200].replace("\n", " ")
        print(
            f"[{i}] doc={r['document_id']} page={r['page_no']} "
            f"type={r['chunk_type']} date={_fmt_date(r['report_date'])}\n"
            f"     {snippet}...\n",
            flush=True,
        )


def cmd_facts(args):
    results = get_facts(
        fact_type=args.type,
        province_cn=args.province,
        date_from=dt.date.fromisoformat(args.from_date) if args.from_date else None,
        date_to=dt.date.fromisoformat(args.to_date) if args.to_date else None,
        limit=args.limit,
    )

    if not results:
        print("No facts found.", flush=True)
        return

    print(f"{len(results)} fact(s):\n", flush=True)
    for r in results:
        if r["metric_value"] is not None:
            val_str = f"{r['metric_name']}={r['metric_value']} {r['metric_unit'] or ''}"
        else:
            val_str = r["fact_text"][:100] if r["fact_text"] else ""
        print(
            f"  {_fmt_date(r['report_date'])} | {r['province_cn'] or 'National'} | "
            f"{r['fact_type']} | {val_str}",
            flush=True,
        )


def cmd_docs(args):
    docs = get_document_list(status=args.status)
    if not docs:
        print("No documents found.", flush=True)
        return

    print(f"{len(docs)} document(s):\n", flush=True)
    for d in docs:
        print(
            f"  id={d['id']} | {d['file_name']} | "
            f"status={d['ingest_status']} | pages={d['page_count']} | "
            f"dates={_fmt_date(d['report_date_min'])}→{_fmt_date(d['report_date_max'])}",
            flush=True,
        )


def cmd_notes(args):
    notes = get_note_index(note_type=args.type)
    if not notes:
        print("No notes found.", flush=True)
        return

    print(f"{len(notes)} note(s):\n", flush=True)
    for n in notes:
        print(
            f"  [{n['note_type']}] {n['note_title']} | "
            f"{_fmt_date(n['report_date_min'])}→{_fmt_date(n['report_date_max'])} | "
            f"{n['note_path']}",
            flush=True,
        )


def main():
    parser = argparse.ArgumentParser(description="Query the spot market knowledge pool")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # search subcommand
    s_search = sub.add_parser("search", help="Full-text search over text chunks")
    s_search.add_argument("query", help="Search query (Chinese or English)")
    s_search.add_argument("--province", help="Filter by province CN name (e.g. 山东)")
    s_search.add_argument("--from", dest="from_date", help="Start date YYYY-MM-DD")
    s_search.add_argument("--to", dest="to_date", help="End date YYYY-MM-DD")
    s_search.add_argument("--chunk-type", choices=["body", "table", "header", "reason"])
    s_search.add_argument("--limit", type=int, default=20)

    # facts subcommand
    s_facts = sub.add_parser("facts", help="Show structured facts")
    s_facts.add_argument(
        "--type",
        choices=["price_da", "price_rt", "driver", "interprovincial", "section_marker"],
        help="Fact type filter",
    )
    s_facts.add_argument("--province", help="Filter by province CN name")
    s_facts.add_argument("--from", dest="from_date", help="Start date YYYY-MM-DD")
    s_facts.add_argument("--to", dest="to_date", help="End date YYYY-MM-DD")
    s_facts.add_argument("--limit", type=int, default=50)

    # docs subcommand
    s_docs = sub.add_parser("docs", help="List registered source documents")
    s_docs.add_argument(
        "--status",
        choices=["pending", "parsed", "error", "empty"],
        help="Filter by ingest status",
    )

    # notes subcommand
    s_notes = sub.add_parser("notes", help="List registered markdown notes")
    s_notes.add_argument(
        "--type",
        choices=["daily_report", "province", "concept", "index"],
        help="Filter by note type",
    )

    args = parser.parse_args()

    dispatch = {
        "search": cmd_search,
        "facts": cmd_facts,
        "docs": cmd_docs,
        "notes": cmd_notes,
    }
    dispatch[args.cmd](args)


if __name__ == "__main__":
    main()
