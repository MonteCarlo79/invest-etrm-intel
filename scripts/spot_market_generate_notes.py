#!/usr/bin/env python3
"""
Spot market knowledge pool — note generation CLI.

Generates / updates Obsidian-compatible markdown notes in knowledge/spot_market/:
  01_daily_reports/  — one note per report date
  02_provinces/      — one rolling note per province
  03_concepts/       — one note per recurring market driver concept
  04_indices/        — master index note

Usage:
    python scripts/spot_market_generate_notes.py            # generate all note types
    python scripts/spot_market_generate_notes.py --type daily
    python scripts/spot_market_generate_notes.py --type province
    python scripts/spot_market_generate_notes.py --type concept
    python scripts/spot_market_generate_notes.py --type index
    python scripts/spot_market_generate_notes.py --date 2025-07-16   # single daily note

Requires PGURL (or DB_URL / DATABASE_URL) in environment or .env file.
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

try:
    from dotenv import load_dotenv
    _env = _REPO / ".env"
    if _env.exists():
        load_dotenv(_env)
except ImportError:
    pass

from services.knowledge_pool.db import get_conn
from services.knowledge_pool.markdown_notes import (
    generate_daily_report_note,
    generate_province_note,
    generate_concept_note,
    generate_index_note,
    CONCEPT_PATTERNS,
)


def _all_parsed_documents() -> list[dict]:
    """Return all parsed documents with their date ranges."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, source_path, report_date_min, report_date_max
                FROM staging.spot_report_documents
                WHERE ingest_status = 'parsed'
                ORDER BY report_date_min
                """
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def _all_provinces() -> list[tuple[str, str]]:
    """Return all (province_cn, province_en) pairs seen in facts."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT province_cn, province_en
                FROM staging.spot_report_facts
                WHERE province_cn IS NOT NULL
                ORDER BY province_cn
                """
            )
            return cur.fetchall()


def generate_daily_notes(only_date: dt.date | None = None) -> int:
    docs = _all_parsed_documents()
    generated = 0

    for doc in docs:
        doc_id = doc["id"]
        source_path = doc["source_path"]
        d_min = doc["report_date_min"]
        d_max = doc["report_date_max"]

        if not d_min or not d_max:
            continue

        cur = d_min
        while cur <= d_max:
            if only_date and cur != only_date:
                cur += dt.timedelta(days=1)
                continue

            try:
                path = generate_daily_report_note(cur, doc_id, source_path)
                print(f"  [NOTE] {path.name}", flush=True)
                generated += 1
            except Exception as e:
                print(f"  [ERROR] daily note {cur}: {e}", flush=True)

            cur += dt.timedelta(days=1)

    return generated


def generate_province_notes() -> int:
    provinces = _all_provinces()
    generated = 0

    for cn, en in provinces:
        try:
            path = generate_province_note(cn, en or cn)
            print(f"  [NOTE] {path.name}", flush=True)
            generated += 1
        except Exception as e:
            print(f"  [ERROR] province note {cn}: {e}", flush=True)

    return generated


def generate_concept_notes() -> int:
    generated = 0
    for concept_key in CONCEPT_PATTERNS:
        try:
            path = generate_concept_note(concept_key)
            print(f"  [NOTE] {path.name}", flush=True)
            generated += 1
        except Exception as e:
            print(f"  [ERROR] concept note {concept_key}: {e}", flush=True)
    return generated


def main():
    parser = argparse.ArgumentParser(description="Generate knowledge pool markdown notes")
    parser.add_argument(
        "--type",
        choices=["daily", "province", "concept", "index", "all"],
        default="all",
        help="Note type to generate (default: all)",
    )
    parser.add_argument(
        "--date",
        help="Generate a single daily note for this date (YYYY-MM-DD); implies --type daily",
    )
    args = parser.parse_args()

    only_date = None
    if args.date:
        only_date = dt.date.fromisoformat(args.date)
        args.type = "daily"

    note_type = args.type

    if note_type in ("daily", "all"):
        print("[DAILY] Generating daily report notes...", flush=True)
        n = generate_daily_notes(only_date)
        print(f"  → {n} daily notes written", flush=True)

    if note_type in ("province", "all"):
        print("[PROVINCE] Generating province notes...", flush=True)
        n = generate_province_notes()
        print(f"  → {n} province notes written", flush=True)

    if note_type in ("concept", "all"):
        print("[CONCEPT] Generating concept notes...", flush=True)
        n = generate_concept_notes()
        print(f"  → {n} concept notes written", flush=True)

    if note_type in ("index", "all"):
        print("[INDEX] Generating index note...", flush=True)
        path = generate_index_note()
        print(f"  → {path}", flush=True)

    print("[DONE]", flush=True)


if __name__ == "__main__":
    main()
