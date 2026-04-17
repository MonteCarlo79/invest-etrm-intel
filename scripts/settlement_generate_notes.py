#!/usr/bin/env python3
"""
Settlement knowledge pool — note generation CLI.

Generates / updates Obsidian-compatible markdown notes in knowledge/settlement/:
  {asset}/YYYY-MM.md    — monthly_asset notes (charge breakdown + recon)
  {asset}/index.md      — asset_summary rolling index
  components/           — charge_component cross-asset notes
  reconciliation/       — recon diff notes (only when ≥2 invoice versions exist)
  index.md              — master settlement index

Usage:
    python scripts/settlement_generate_notes.py              # all note types
    python scripts/settlement_generate_notes.py --type monthly
    python scripts/settlement_generate_notes.py --type summary
    python scripts/settlement_generate_notes.py --type component
    python scripts/settlement_generate_notes.py --type reconciliation
    python scripts/settlement_generate_notes.py --type index
    python scripts/settlement_generate_notes.py --asset suyou
    python scripts/settlement_generate_notes.py --asset suyou --year 2025 --month 10
    python scripts/settlement_generate_notes.py --dry-run    # list notes without writing

Requires PGURL (or DB_URL / DATABASE_URL) in environment or .env file.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

try:
    from dotenv import load_dotenv
    for _env_candidate in [
        _REPO / ".env",
        _REPO / "config" / ".env",
        _REPO / "apps" / "spot-agent" / ".env",
    ]:
        if _env_candidate.exists():
            load_dotenv(_env_candidate)
            break
except ImportError:
    pass

from services.knowledge_pool.db import get_conn
from services.knowledge_pool.settlement_markdown_notes import (
    generate_monthly_asset_note,
    generate_asset_summary_note,
    generate_charge_component_note,
    generate_reconciliation_note,
    generate_settlement_index_note,
)


def _all_parsed_asset_periods() -> list[tuple[str, int, int]]:
    """Return (asset_slug, year, month) for all parsed docs with known asset."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT asset_slug, settlement_year, settlement_month
                FROM staging.settlement_report_documents
                WHERE ingest_status = 'parsed' AND asset_slug IS NOT NULL
                ORDER BY asset_slug, settlement_year, settlement_month
                """
            )
            return [(r[0], r[1], r[2]) for r in cur.fetchall()]


def _all_parsed_assets() -> list[str]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT asset_slug
                FROM staging.settlement_report_documents
                WHERE ingest_status = 'parsed' AND asset_slug IS NOT NULL
                ORDER BY asset_slug
                """
            )
            return [r[0] for r in cur.fetchall()]


def _all_components() -> list[str]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT component_name
                FROM staging.settlement_report_facts
                WHERE component_name IS NOT NULL
                  AND fact_type IN ('charge_component', 'total_amount')
                ORDER BY component_name
                """
            )
            return [r[0] for r in cur.fetchall()]


def _reconciliation_combos() -> list[tuple[str, int, int, str]]:
    """Return (asset_slug, year, month, invoice_type) that have recon rows."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT asset_slug, settlement_year, settlement_month, invoice_type
                FROM staging.settlement_reconciliation
                ORDER BY asset_slug, settlement_year, settlement_month, invoice_type
                """
            )
            return [(r[0], r[1], r[2], r[3]) for r in cur.fetchall()]


def main():
    parser = argparse.ArgumentParser(description="Generate settlement knowledge notes")
    parser.add_argument(
        "--type",
        choices=["monthly", "summary", "component", "reconciliation", "index", "all"],
        default="all",
        help="Note type to generate (default: all)",
    )
    parser.add_argument("--asset", help="Filter to a single asset slug")
    parser.add_argument("--year", type=int, help="Filter to a specific year")
    parser.add_argument("--month", type=int, help="Filter to a specific month (requires --year)")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be generated without writing files",
    )
    args = parser.parse_args()

    note_type = args.type
    dry_run = args.dry_run

    generated = 0
    skipped = 0

    def _log(path: Path, label: str):
        nonlocal generated
        print(f"  [OK] {label}: {path.relative_to(_REPO)}")
        generated += 1

    def _dry(label: str):
        nonlocal generated
        print(f"  [DRY] {label}")
        generated += 1

    # ── Monthly asset notes ─────────────────────────────────────────────────
    if note_type in ("monthly", "all"):
        combos = _all_parsed_asset_periods()
        if args.asset:
            combos = [(s, y, m) for s, y, m in combos if s == args.asset]
        if args.year:
            combos = [(s, y, m) for s, y, m in combos if y == args.year]
        if args.month:
            combos = [(s, y, m) for s, y, m in combos if m == args.month]

        print(f"\nGenerating monthly_asset notes ({len(combos)} combos)...")
        for slug, year, month in combos:
            if dry_run:
                _dry(f"monthly_asset: {slug} {year}-{month:02d}")
            else:
                path = generate_monthly_asset_note(slug, year, month)
                _log(path, f"monthly_asset {slug} {year}-{month:02d}")

    # ── Asset summary notes ─────────────────────────────────────────────────
    if note_type in ("summary", "all"):
        assets = _all_parsed_assets()
        if args.asset:
            assets = [a for a in assets if a == args.asset]

        print(f"\nGenerating asset_summary notes ({len(assets)} assets)...")
        for slug in assets:
            if dry_run:
                _dry(f"asset_summary: {slug}")
            else:
                path = generate_asset_summary_note(slug)
                _log(path, f"asset_summary {slug}")

    # ── Charge component notes ──────────────────────────────────────────────
    if note_type in ("component", "all") and not args.asset:
        components = _all_components()
        print(f"\nGenerating charge_component notes ({len(components)} components)...")
        for cn in components:
            if dry_run:
                _dry(f"charge_component: {cn}")
            else:
                path = generate_charge_component_note(cn)
                _log(path, f"charge_component {cn}")

    # ── Reconciliation notes ────────────────────────────────────────────────
    if note_type in ("reconciliation", "all"):
        recon_combos = _reconciliation_combos()
        if args.asset:
            recon_combos = [r for r in recon_combos if r[0] == args.asset]
        if args.year:
            recon_combos = [r for r in recon_combos if r[1] == args.year]

        print(f"\nGenerating reconciliation notes ({len(recon_combos)} combos)...")
        for slug, year, month, itype in recon_combos:
            if dry_run:
                _dry(f"reconciliation: {slug} {year}-{month:02d} {itype}")
            else:
                path = generate_reconciliation_note(slug, year, month, itype)
                if path:
                    _log(path, f"reconciliation {slug} {year}-{month:02d} {itype}")
                else:
                    skipped += 1

    # ── Index note ──────────────────────────────────────────────────────────
    if note_type in ("index", "all"):
        print("\nGenerating settlement index note...")
        if dry_run:
            _dry("settlement index")
        else:
            path = generate_settlement_index_note()
            _log(path, "settlement index")

    print(f"\nDone. Generated: {generated}  Skipped: {skipped}")


if __name__ == "__main__":
    main()
