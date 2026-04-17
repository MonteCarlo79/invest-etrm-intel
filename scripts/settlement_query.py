#!/usr/bin/env python3
"""
Settlement knowledge pool — retrieval CLI.

Queries staging.settlement_report_chunks, staging.settlement_report_facts,
staging.settlement_reconciliation, and staging.settlement_report_documents.

Usage:
    # Full-text search across all settlement invoice chunks
    python scripts/settlement_query.py search "市场上网电费"

    # Search scoped to one asset
    python scripts/settlement_query.py search "调频" --asset suyou

    # Search within a specific invoice type
    python scripts/settlement_query.py search "容量补偿" --invoice-type capacity_compensation

    # Show charge breakdown for an asset × period
    python scripts/settlement_query.py facts --asset suyou --year 2025 --month 10

    # Show only total_amount facts for all assets in 2025
    python scripts/settlement_query.py facts --fact-type total_amount --year 2025

    # Monthly totals time-series for an asset
    python scripts/settlement_query.py totals --asset wulate

    # All flagged reconciliation differences
    python scripts/settlement_query.py recon --flagged

    # Reconciliation for a specific asset × month
    python scripts/settlement_query.py recon --asset wulanchabu --year 2025 --month 1

    # List all settlement documents (optionally filtered)
    python scripts/settlement_query.py docs
    python scripts/settlement_query.py docs --status error
    python scripts/settlement_query.py docs --invoice-type grid_withdrawal

    # List registered notes
    python scripts/settlement_query.py notes
    python scripts/settlement_query.py notes --type monthly_asset

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

from services.knowledge_pool.settlement_retrieval import (
    search_settlement_chunks,
    get_settlement_facts,
    get_settlement_totals,
    get_reconciliation_deltas,
    get_settlement_documents,
    get_settlement_note_index,
)

_INVOICE_TYPE_LABELS = {
    "grid_injection":        "上网",
    "grid_withdrawal":       "下网",
    "rural_grid":            "农网",
    "capacity_compensation": "容量补偿",
}


def _fmt(v, width: int = 14) -> str:
    if v is None:
        return "—".ljust(width)
    if isinstance(v, float):
        return f"{v:,.2f}".rjust(width)
    return str(v).ljust(width)


def cmd_search(args):
    results = search_settlement_chunks(
        query=args.query,
        asset_slug=args.asset,
        invoice_type=args.invoice_type,
        settlement_year=args.year,
        settlement_month=args.month,
        chunk_type=args.chunk_type,
        limit=args.limit,
    )
    if not results:
        print("No results.", flush=True)
        return

    print(f"{len(results)} result(s) for '{args.query}':\n")
    for i, r in enumerate(results, 1):
        snippet = r["chunk_text"][:200].replace("\n", " ")
        itype = _INVOICE_TYPE_LABELS.get(r["invoice_type"], r["invoice_type"])
        print(
            f"[{i}] doc={r['document_id']} asset={r['asset_slug'] or '—'} "
            f"{r['settlement_year']}-{r['settlement_month']:02d} "
            f"type={itype} chunk_type={r['chunk_type']}\n"
            f"     {snippet}\n"
        )


def cmd_facts(args):
    rows = get_settlement_facts(
        asset_slug=args.asset,
        invoice_type=args.invoice_type,
        fact_type=args.fact_type,
        component_name=args.component,
        component_group=args.group,
        settlement_year=args.year,
        settlement_month=args.month,
        limit=args.limit,
    )
    if not rows:
        print("No facts found.", flush=True)
        return

    print(f"{len(rows)} fact(s):\n")
    print(f"{'Asset':<14} {'Period':<8} {'Type':<8} {'InvType':<8} {'Component':<22} {'Value':>14} {'Unit':<8} {'Conf'}")
    print("-" * 100)
    for r in rows:
        period = f"{r['settlement_year']}-{r['settlement_month']:02d}"
        itype = _INVOICE_TYPE_LABELS.get(r["invoice_type"], r["invoice_type"])[:8]
        val = r["metric_value"]
        val_str = f"{float(val):>14,.2f}" if val is not None else "—".rjust(14)
        print(
            f"{(r['asset_slug'] or '—'):<14} {period:<8} {(r['fact_type'] or '')[:8]:<8} "
            f"{itype:<8} {(r['component_name'] or '—'):<22} {val_str} {(r['metric_unit'] or ''):<8} "
            f"{r['confidence']}"
        )


def cmd_totals(args):
    rows = get_settlement_totals(
        asset_slug=args.asset,
        settlement_year=args.year,
        invoice_type=args.invoice_type,
    )
    if not rows:
        print("No total_amount facts found.", flush=True)
        return

    print(f"{len(rows)} total(s):\n")
    print(f"{'Asset':<14} {'Period':<8} {'Invoice Type':<22} {'Total (yuan)':>16} {'Half':<8}")
    print("-" * 80)
    for r in rows:
        period = f"{r['settlement_year']}-{r['settlement_month']:02d}"
        itype = _INVOICE_TYPE_LABELS.get(r["invoice_type"], r["invoice_type"])
        val = r["metric_value"]
        val_str = f"{float(val):>16,.2f}" if val is not None else "—".rjust(16)
        print(
            f"{(r['asset_slug'] or '—'):<14} {period:<8} {itype:<22} {val_str} {r['period_half']:<8}"
        )


def cmd_recon(args):
    rows = get_reconciliation_deltas(
        asset_slug=args.asset,
        settlement_year=args.year,
        settlement_month=args.month,
        invoice_type=args.invoice_type,
        flagged_only=args.flagged,
        limit=args.limit,
    )
    if not rows:
        label = "flagged" if args.flagged else "any"
        print(f"No {label} reconciliation rows found.", flush=True)
        return

    flagged_count = sum(1 for r in rows if r["flagged"])
    print(f"{len(rows)} reconciliation row(s), {flagged_count} flagged:\n")
    print(
        f"{'Asset':<14} {'Period':<8} {'Inv':<8} {'Component':<22} "
        f"{'Value A':>14} {'Value B':>14} {'Delta':>12} {'Δ%':>8} {'Flag'}"
    )
    print("-" * 110)
    for r in rows:
        period = f"{r['settlement_year']}-{r['settlement_month']:02d}"
        itype = _INVOICE_TYPE_LABELS.get(r["invoice_type"], r["invoice_type"])[:8]
        flag_marker = " ⚠" if r["flagged"] else ""
        delta = r["delta"]
        dpct = r["delta_pct"]
        delta_str = f"{float(delta):>12,.2f}" if delta is not None else "—".rjust(12)
        dpct_str = f"{float(dpct):>7.2f}%" if dpct is not None else "—".rjust(8)
        va_str = f"{float(r['value_a']):>14,.2f}" if r["value_a"] is not None else "—".rjust(14)
        vb_str = f"{float(r['value_b']):>14,.2f}" if r["value_b"] is not None else "—".rjust(14)
        print(
            f"{(r['asset_slug'] or '—'):<14} {period:<8} {itype:<8} "
            f"{(r['component_name'] or r['fact_type'] or '—'):<22} "
            f"{va_str} {vb_str} {delta_str} {dpct_str}{flag_marker}"
        )
        if r["flagged"] and r.get("flag_reason"):
            print(f"  {'':14}  → {r['flag_reason']}")


def cmd_docs(args):
    rows = get_settlement_documents(
        status=args.status,
        asset_slug=args.asset,
        invoice_type=args.invoice_type,
        settlement_year=args.year,
    )
    if not rows:
        print("No documents found.", flush=True)
        return

    status_counts: dict[str, int] = {}
    for r in rows:
        status_counts[r["ingest_status"]] = status_counts.get(r["ingest_status"], 0) + 1

    print(f"{len(rows)} document(s): " + "  ".join(f"{s}={c}" for s, c in sorted(status_counts.items())) + "\n")
    print(f"{'ID':>5} {'Period':<8} {'Asset':<14} {'InvType':<10} {'Half':<8} {'Status':<12} {'Pages':>5}  File")
    print("-" * 110)
    for r in rows:
        period = f"{r['settlement_year']}-{r['settlement_month']:02d}"
        itype = _INVOICE_TYPE_LABELS.get(r["invoice_type"], r["invoice_type"])[:10]
        pages = str(r["page_count"] or "?")
        err_hint = f"  [{r['parse_error'][:40]}]" if r.get("parse_error") else ""
        print(
            f"{r['id']:>5} {period:<8} {(r['asset_slug'] or '—'):<14} {itype:<10} "
            f"{r['period_half']:<8} {r['ingest_status']:<12} {pages:>5}  {r['file_name']}{err_hint}"
        )


def cmd_notes(args):
    rows = get_settlement_note_index(note_type=args.type)
    if not rows:
        print("No settlement notes registered.", flush=True)
        return

    print(f"{len(rows)} note(s):\n")
    for r in rows:
        period = ""
        if r["settlement_year"] and r["settlement_month"]:
            period = f" {r['settlement_year']}-{r['settlement_month']:02d}"
        print(
            f"  [{r['note_type']}] {r['note_title'] or r['note_key']}{period}\n"
            f"    {r['note_path']}"
        )


def main():
    parser = argparse.ArgumentParser(description="Settlement knowledge pool query tool")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # ── search ──────────────────────────────────────────────────────────────
    sp = sub.add_parser("search", help="Full-text search over invoice chunks")
    sp.add_argument("query", help="Search text (Chinese OK)")
    sp.add_argument("--asset", help="Filter to asset slug")
    sp.add_argument("--invoice-type", dest="invoice_type")
    sp.add_argument("--year", type=int)
    sp.add_argument("--month", type=int)
    sp.add_argument("--chunk-type", dest="chunk_type",
                    choices=["body", "table", "header", "amount_line"])
    sp.add_argument("--limit", type=int, default=20)

    # ── facts ───────────────────────────────────────────────────────────────
    sp = sub.add_parser("facts", help="Structured fact lookup")
    sp.add_argument("--asset", help="Filter to asset slug")
    sp.add_argument("--invoice-type", dest="invoice_type")
    sp.add_argument("--fact-type", dest="fact_type",
                    choices=["energy_mwh", "energy_kwh", "charge_component",
                             "total_amount", "capacity_compensation"])
    sp.add_argument("--component", help="Filter by canonical component name")
    sp.add_argument("--group", dest="group",
                    help="Component group filter (energy/system/total/...)")
    sp.add_argument("--year", type=int)
    sp.add_argument("--month", type=int)
    sp.add_argument("--limit", type=int, default=200)

    # ── totals ──────────────────────────────────────────────────────────────
    sp = sub.add_parser("totals", help="Monthly 总电费 totals per asset")
    sp.add_argument("--asset", help="Filter to asset slug")
    sp.add_argument("--year", type=int)
    sp.add_argument("--invoice-type", dest="invoice_type")

    # ── recon ───────────────────────────────────────────────────────────────
    sp = sub.add_parser("recon", help="Reconciliation delta query")
    sp.add_argument("--asset", help="Filter to asset slug")
    sp.add_argument("--year", type=int)
    sp.add_argument("--month", type=int)
    sp.add_argument("--invoice-type", dest="invoice_type")
    sp.add_argument("--flagged", action="store_true",
                    help="Show only flagged differences")
    sp.add_argument("--limit", type=int, default=200)

    # ── docs ────────────────────────────────────────────────────────────────
    sp = sub.add_parser("docs", help="List settlement documents")
    sp.add_argument("--status",
                    choices=["pending", "parsed", "empty", "unresolved_asset", "error"])
    sp.add_argument("--asset")
    sp.add_argument("--invoice-type", dest="invoice_type")
    sp.add_argument("--year", type=int)

    # ── notes ───────────────────────────────────────────────────────────────
    sp = sub.add_parser("notes", help="List registered settlement notes")
    sp.add_argument("--type", dest="type",
                    choices=["monthly_asset", "asset_summary",
                             "charge_component", "reconciliation"])

    args = parser.parse_args()

    dispatch = {
        "search": cmd_search,
        "facts":  cmd_facts,
        "totals": cmd_totals,
        "recon":  cmd_recon,
        "docs":   cmd_docs,
        "notes":  cmd_notes,
    }
    dispatch[args.cmd](args)


if __name__ == "__main__":
    main()
