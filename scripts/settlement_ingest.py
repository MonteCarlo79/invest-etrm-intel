#!/usr/bin/env python3
"""
Settlement knowledge pool — PDF ingestion CLI.

Scans data/raw/settlement/invoices/{asset_dir}/{year}年结算单/*.pdf
and data/raw/settlement/compensation/*.pdf.

Usage:
    python scripts/settlement_ingest.py --year 2025
    python scripts/settlement_ingest.py --asset-dir "B-6 内蒙苏右" --year 2025
    python scripts/settlement_ingest.py --compensation-only --year 2025
    python scripts/settlement_ingest.py --init-db
    python scripts/settlement_ingest.py --year 2025 --force
    python scripts/settlement_ingest.py --asset-dir "B-6 内蒙苏右" --year 2025 --limit-months 3
"""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

try:
    from dotenv import load_dotenv
    for _env in [_REPO / ".env", _REPO / "apps" / "spot-agent" / ".env"]:
        if _env.exists():
            load_dotenv(_env)
            break
except ImportError:
    pass

from services.knowledge_pool.db import get_conn
from services.knowledge_pool.settlement_ingestion import (
    extract_and_store_settlement_pages,
    build_and_store_settlement_chunks,
)
from services.knowledge_pool.settlement_fact_extraction import (
    extract_facts_for_settlement_document,
    run_reconciliation_check,
)

# ── Constants ────────────────────────────────────────────────────────────────
DEFAULT_INVOICES_DIR = _REPO / "data" / "raw" / "settlement" / "invoices"
DEFAULT_COMP_DIR     = _REPO / "data" / "raw" / "settlement" / "compensation"

# Filename regex patterns (two naming conventions)
# Pattern A: "10月 【B-6-上】..."  or  "10月 【B-6-下-交易中心】..."
_RE_FNAME_A = re.compile(
    r"^(\d{1,2})\s*月.{0,10}【(B-[\w\u5916\u5916]+)-([上下])(?:-([^】]+))?】",
    re.UNICODE,
)
# Pattern B: "B-6景蓝乌尔图储能2026年01月上网..."
_RE_FNAME_B = re.compile(
    r"(B-[\w\u5916]+).{0,30}(\d{4})\s*年\s*0?(\d{1,2})\s*月.{0,20}(上网|下网|农网)",
    re.UNICODE,
)
# Compensation filename: "2025年10月储能容量补偿..."
_RE_COMP_FNAME = re.compile(r"(\d{4})\s*年\s*0?(\d{1,2})\s*月.*?补偿")

_DIRECTION_MAP = {
    "上":  "grid_injection",
    "下":  "grid_withdrawal",
    "上网": "grid_injection",
    "下网": "grid_withdrawal",
    "农网": "rural_grid",
}

_PERIOD_HALF_KEYWORDS = {
    "交易中心": "issuer_trading_center",
    "宣定":     "issuer_trading_center",
    "场站":     "issuer_plant",
    "含1月调试期": "commissioning_supplement",
    "含调试期":    "commissioning_supplement",
    "试运行":      "commissioning_supplement",
    "上半月":      "first_half",
    "下半月":      "second_half",
}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def resolve_asset_slug(b_code: str) -> str | None:
    """Query core.asset_alias_map for invoice_dir_code → asset_slug."""
    b_code = b_code.replace("【外】", "外").replace("外】", "外")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT asset_code FROM core.asset_alias_map "
                "WHERE alias_type='invoice_dir_code' AND lower(alias_value)=lower(%s)",
                (b_code,),
            )
            row = cur.fetchone()
    return row[0] if row else None


def get_dispatch_name_map() -> dict[str, str]:
    """Return {dispatch_unit_name_cn: asset_slug} for capacity_compensation matching."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT alias_value, asset_code FROM core.asset_alias_map "
                "WHERE alias_type='dispatch_unit_name_cn'"
            )
            return {row[0]: row[1] for row in cur.fetchall()}


def parse_filename(fname: str, year_from_dir: int) -> dict | None:
    """
    Parse invoice filename → {settlement_month, settlement_year, b_code, invoice_type,
                               period_half, period_notes}.
    Returns None if the file should be skipped (compensation table, unrecognized pattern).
    """
    stem = Path(fname).stem

    # Skip compensation tables placed in asset dirs
    if "容量补偿" in stem:
        return None

    # Pattern A: "10月 【B-6-上】..."
    m = _RE_FNAME_A.match(stem)
    if m:
        month = int(m.group(1))
        b_code = m.group(2)
        direction = m.group(3)
        sub_tag = m.group(4) or ""
        invoice_type = _DIRECTION_MAP.get(direction, "grid_injection")
        period_half = "full"
        period_notes = None
        for kw, ph in _PERIOD_HALF_KEYWORDS.items():
            if kw in stem:
                period_half = ph
                period_notes = kw
                break
        return {
            "settlement_month": month,
            "settlement_year": year_from_dir,
            "b_code": b_code,
            "invoice_type": invoice_type,
            "period_half": period_half,
            "period_notes": period_notes,
        }

    # Pattern B: "B-6景蓝乌尔图储能2026年01月上网..."
    m = _RE_FNAME_B.search(stem)
    if m:
        b_code = m.group(1)
        year = int(m.group(2))
        month = int(m.group(3))
        direction_str = m.group(4)
        invoice_type = _DIRECTION_MAP.get(direction_str, "grid_injection")
        period_half = "full"
        period_notes = None
        for kw, ph in _PERIOD_HALF_KEYWORDS.items():
            if kw in stem:
                period_half = ph
                period_notes = kw
                break
        return {
            "settlement_month": month,
            "settlement_year": year,
            "b_code": b_code,
            "invoice_type": invoice_type,
            "period_half": period_half,
            "period_notes": period_notes,
        }

    return None


def register_settlement_document(
    pdf: Path,
    asset_slug: str | None,
    invoice_dir_code: str | None,
    settlement_year: int,
    settlement_month: int,
    period_half: str,
    invoice_type: str,
    period_notes: str | None,
) -> tuple[int, bool]:
    """Register in staging.settlement_report_documents. Returns (doc_id, is_new)."""
    file_hash = sha256_file(pdf)
    file_size = pdf.stat().st_size

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM staging.settlement_report_documents WHERE file_hash=%s",
                (file_hash,),
            )
            row = cur.fetchone()
            if row:
                return row[0], False

            cur.execute(
                """
                INSERT INTO staging.settlement_report_documents
                    (source_path, file_name, asset_slug, invoice_dir_code,
                     settlement_year, settlement_month, period_half, invoice_type,
                     period_notes, file_hash, file_size_bytes, ingest_status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending')
                RETURNING id
                """,
                (str(pdf), pdf.name, asset_slug, invoice_dir_code,
                 settlement_year, settlement_month, period_half, invoice_type,
                 period_notes, file_hash, file_size),
            )
            doc_id = cur.fetchone()[0]
        conn.commit()
    return doc_id, True


def set_status(doc_id: int, status: str, page_count: int | None = None,
               parse_error: str | None = None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE staging.settlement_report_documents
                SET ingest_status=%s, page_count=COALESCE(%s,page_count),
                    parse_error=%s, updated_at=now()
                WHERE id=%s
                """,
                (status, page_count, parse_error, doc_id),
            )
        conn.commit()


def ingest_one(
    pdf: Path,
    asset_slug: str | None,
    invoice_dir_code: str | None,
    settlement_year: int,
    settlement_month: int,
    period_half: str,
    invoice_type: str,
    period_notes: str | None,
    force: bool,
    asset_map: dict,
) -> str:
    """
    Ingest a single settlement PDF.
    Returns: 'processed' | 'skipped' | 'empty' | 'error' | 'unresolved'
    """
    if asset_slug is None and invoice_type != "capacity_compensation":
        print(f"  [UNRESOLVED] {pdf.name} — no asset_slug for {invoice_dir_code}", flush=True)
        doc_id, _ = register_settlement_document(
            pdf, None, invoice_dir_code, settlement_year, settlement_month,
            period_half, invoice_type, period_notes,
        )
        set_status(doc_id, "unresolved_asset")
        return "unresolved"

    doc_id, is_new = register_settlement_document(
        pdf, asset_slug, invoice_dir_code, settlement_year, settlement_month,
        period_half, invoice_type, period_notes,
    )

    if not is_new and not force:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ingest_status FROM staging.settlement_report_documents WHERE id=%s",
                    (doc_id,),
                )
                row = cur.fetchone()
        if row and row[0] in ("parsed", "empty"):
            print(f"  [SKIP] {pdf.name} (doc_id={doc_id}, status={row[0]})", flush=True)
            return "skipped"

    print(f"  [INGEST] {pdf.name} (doc_id={doc_id})", flush=True)

    # On force re-ingest: purge prior facts/pages/chunks so stale rows don't persist
    if not is_new and force:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM staging.settlement_report_facts WHERE document_id=%s", (doc_id,))
                cur.execute("DELETE FROM staging.settlement_report_chunks WHERE document_id=%s", (doc_id,))
                cur.execute("DELETE FROM staging.settlement_report_pages WHERE document_id=%s", (doc_id,))
            conn.commit()

    try:
        page_count, date_min, date_max = extract_and_store_settlement_pages(
            doc_id, pdf, settlement_year, settlement_month,
        )
        print(f"    pages={page_count}", flush=True)

        # Check total chars extracted
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(SUM(char_count),0) FROM staging.settlement_report_pages "
                    "WHERE document_id=%s",
                    (doc_id,),
                )
                total_chars = cur.fetchone()[0]

        if total_chars == 0:
            set_status(doc_id, "empty", page_count=page_count,
                       parse_error="no_text_layer; likely scanned image")
            print(f"    [EMPTY] no text extracted (scanned PDF)", flush=True)
            return "empty"

        chunk_count = build_and_store_settlement_chunks(doc_id)
        print(f"    chunks={chunk_count}", flush=True)

        fact_count = extract_facts_for_settlement_document(
            doc_id=doc_id,
            invoice_type=invoice_type,
            asset_slug=asset_slug or "unknown",
            settlement_year=settlement_year,
            settlement_month=settlement_month,
            period_half=period_half,
            asset_map=asset_map,
        )
        print(f"    facts={fact_count}", flush=True)

        # Update date range
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE staging.settlement_report_documents "
                    "SET report_date_min=%s, report_date_max=%s WHERE id=%s",
                    (date_min, date_max, doc_id),
                )
            conn.commit()

        set_status(doc_id, "parsed", page_count=page_count)

        # Reconciliation check (skip for capacity_compensation — multi-asset)
        if asset_slug and invoice_type != "capacity_compensation":
            recon_count = run_reconciliation_check(
                doc_id, asset_slug, settlement_year, settlement_month,
                invoice_type, period_half,
            )
            if recon_count:
                print(f"    recon_rows={recon_count}", flush=True)

        return "processed"

    except Exception as e:
        set_status(doc_id, "error", parse_error=str(e)[:500])
        print(f"    [ERROR] {e}", flush=True)
        return "error"


def init_settlement_tables():
    ddl_path = _REPO / "db" / "ddl" / "staging" / "settlement_report_knowledge.sql"
    if not ddl_path.exists():
        print(f"[ERROR] DDL not found: {ddl_path}", flush=True)
        sys.exit(1)
    pgurl = (os.environ.get("PGURL") or os.environ.get("DB_URL") or
             os.environ.get("DATABASE_URL") or os.environ.get("MARKETDATA_DB_URL"))
    if not pgurl:
        print("[ERROR] No PGURL/DB_URL in environment", flush=True)
        sys.exit(1)
    subprocess.run(["psql", pgurl, "-f", str(ddl_path)], check=True)
    print("[DB] Settlement tables initialised.", flush=True)


def _print_stats(stats: dict):
    print("\n[DONE] " + "  ".join(f"{k}={v}" for k, v in stats.items() if v), flush=True)


def main():
    parser = argparse.ArgumentParser(description="Ingest settlement PDFs into knowledge pool")
    parser.add_argument("--year", type=int, help="Settlement year (e.g. 2025)")
    parser.add_argument("--asset-dir", help="Asset directory name e.g. 'B-6 内蒙苏右'")
    parser.add_argument("--compensation-only", action="store_true",
                        help="Only process capacity compensation PDFs")
    parser.add_argument("--limit-months", type=int,
                        help="Only process first N distinct months per asset dir")
    parser.add_argument("--force", action="store_true",
                        help="Re-ingest files already marked parsed/empty")
    parser.add_argument("--init-db", action="store_true",
                        help="Create settlement tables from DDL before ingesting")
    args = parser.parse_args()

    if args.init_db:
        init_settlement_tables()

    asset_map = get_dispatch_name_map()

    stats: dict[str, int] = {"processed": 0, "skipped": 0, "empty": 0,
                              "error": 0, "unresolved": 0}

    # ── Capacity compensation PDFs ────────────────────────────────────────────
    if not args.asset_dir:
        if not DEFAULT_COMP_DIR.exists():
            print(f"[INFO] Compensation dir not found: {DEFAULT_COMP_DIR}", flush=True)
        else:
            comp_pdfs = sorted(DEFAULT_COMP_DIR.glob("*.pdf"))
            if args.year:
                comp_pdfs = [p for p in comp_pdfs if f"{args.year}年" in p.name]
            for pdf in comp_pdfs:
                m = _RE_COMP_FNAME.search(pdf.name)
                if not m:
                    print(f"  [SKIP-FNAME] {pdf.name}", flush=True)
                    continue
                year, month = int(m.group(1)), int(m.group(2))
                result = ingest_one(
                    pdf=pdf, asset_slug=None, invoice_dir_code=None,
                    settlement_year=year, settlement_month=month,
                    period_half="full", invoice_type="capacity_compensation",
                    period_notes=None, force=args.force, asset_map=asset_map,
                )
                stats[result] = stats.get(result, 0) + 1

    if args.compensation_only:
        _print_stats(stats)
        return

    # ── Per-asset invoice PDFs ────────────────────────────────────────────────
    if not DEFAULT_INVOICES_DIR.exists():
        print(f"[ERROR] Invoices dir not found: {DEFAULT_INVOICES_DIR}", flush=True)
        sys.exit(1)

    asset_dirs: list[Path] = []
    if args.asset_dir:
        d = DEFAULT_INVOICES_DIR / args.asset_dir
        if not d.exists():
            print(f"[ERROR] Asset dir not found: {d}", flush=True)
            sys.exit(1)
        asset_dirs = [d]
    else:
        asset_dirs = sorted(p for p in DEFAULT_INVOICES_DIR.iterdir() if p.is_dir())

    for asset_dir in asset_dirs:
        if not asset_dir.is_dir():
            continue

        b_match = re.search(r"(B-[\w\u5916]+)", asset_dir.name)
        dir_b_code = b_match.group(1) if b_match else None
        asset_slug = resolve_asset_slug(dir_b_code) if dir_b_code else None
        if asset_slug is None:
            print(f"[WARN] {asset_dir.name} — could not resolve B-code '{dir_b_code}'", flush=True)

        print(f"\n[DIR] {asset_dir.name}  slug={asset_slug}", flush=True)

        # Year subdirectories: "{year}年结算单"
        year_dirs: list[Path] = []
        if args.year:
            y = asset_dir / f"{args.year}年结算单"
            if y.exists():
                year_dirs = [y]
            else:
                print(f"  [INFO] No year dir for {args.year}: {y}", flush=True)
        else:
            year_dirs = sorted(asset_dir.glob("*年结算单"))

        for year_dir in year_dirs:
            yr_match = re.search(r"(\d{4})年", year_dir.name)
            if not yr_match:
                continue
            dir_year = int(yr_match.group(1))

            pdfs = sorted(year_dir.glob("*.pdf"))

            if args.limit_months:
                seen_months: set[int] = set()
                filtered: list[Path] = []
                for pdf in pdfs:
                    info = parse_filename(pdf.name, dir_year)
                    if info:
                        m_key = info["settlement_month"]
                        seen_months.add(m_key)
                        if len(seen_months) > args.limit_months:
                            break
                    filtered.append(pdf)
                pdfs = filtered

            for pdf in pdfs:
                info = parse_filename(pdf.name, dir_year)
                if info is None:
                    print(f"  [SKIP-FNAME] {pdf.name}", flush=True)
                    continue

                result = ingest_one(
                    pdf=pdf,
                    asset_slug=asset_slug,
                    invoice_dir_code=dir_b_code,
                    settlement_year=info["settlement_year"],
                    settlement_month=info["settlement_month"],
                    period_half=info["period_half"],
                    invoice_type=info["invoice_type"],
                    period_notes=info["period_notes"],
                    force=args.force,
                    asset_map=asset_map,
                )
                stats[result] = stats.get(result, 0) + 1

    _print_stats(stats)


if __name__ == "__main__":
    main()
