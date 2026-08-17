"""Settlement folder scanner — watches for new PDF files and triggers ingestion.

Scans data/raw/settlement/invoices/ recursively for PDF files.
Tracks ingested files in rm_settlements to avoid re-processing.
Can run as a scheduled task (Windows Task Scheduler / cron) or manually from the app.
"""
from __future__ import annotations

import os
import hashlib
from pathlib import Path

from shared.agents.db import get_conn
from services.settlement_ingest.folder_mapper import resolve_folder_to_asset
from services.settlement_ingest.parser_charge import parse_charging_cost_pdf
from services.settlement_ingest.parser_discharge import parse_discharge_settlement_pdf


INVOICE_ROOT = os.environ.get(
    "SETTLEMENT_INVOICE_ROOT",
    os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "settlement", "invoices")
)


def file_sha256(path: str) -> str:
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def is_already_ingested(file_hash: str) -> bool:
    """Check if a file has already been ingested (by hash)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM marketdata.rm_settlements WHERE raw_data->>'file_hash' = %s",
                (file_hash,)
            )
            return cur.fetchone() is not None


def classify_pdf(filename: str) -> str:
    """Classify PDF as 'charge' or 'discharge' based on filename.

    Returns: 'charge', 'discharge', 'voucher', or 'unknown'
    """
    # Skip invoice copies (发票) — not settlement data
    if "发票" in filename:
        return "skip"

    # Trading-center settlement vouchers (发电侧/用户侧结算凭证): their content
    # duplicates the 上网/下网结算单 (same settlement, exchange version).
    # Skip deliberately — ingesting both would double-count.
    if "结算凭证" in filename:
        return "voucher"

    # === Discharge (上网 = power sold to grid) ===
    # Explicit 上网 keyword
    if "上网" in filename:
        return "discharge"
    # 【B-X-上】 bracket convention (e.g. 【B-7-上】, 【B-11-上】)
    if "上】" in filename or "-上】" in filename or "上]" in filename:
        return "discharge"
    # "上网结算单" without brackets (e.g. B-11四子王旗2026-03月上网结算单)
    # Already caught by 上网 above

    # === Charge (下网 = power bought from grid) ===
    # Explicit 下网 keyword
    if "下网" in filename:
        return "charge"
    # 农网 (agricultural grid) = charging cost variant
    if "农网" in filename:
        return "charge"
    # 【B-X-下】 bracket convention
    if "下】" in filename or "-下】" in filename or "下]" in filename:
        return "charge"
    # 电费清单 = charging cost detailed bill
    if "电费清单" in filename or "清单" in filename:
        return "charge"
    # "下网结算单" without brackets (e.g. B-11四子王旗2026-03月下网结算单)
    # Already caught by 下网 above
    # Files with just 下 + month (e.g. "1月【B-11-下】四子王旗.pdf")
    if "【" in filename and "下" in filename:
        return "charge"

    # === Fallback heuristics ===
    # 电费结算单 without 上/下/清单 = discharge (e.g. 内蒙古悦杭...电费结算单)
    if "电费结算单" in filename or "结算单" in filename:
        return "discharge"
    # Just "下网电费" or similar fragments
    if "下" in filename and ("电费" in filename or "结算" in filename):
        return "charge"
    if "上" in filename and ("电费" in filename or "结算" in filename):
        return "discharge"

    return "unknown"


def extract_month_from_filename(filename: str) -> str | None:
    """Extract settlement month (YYYY-MM-DD first of month) from filename.

    Patterns:
    - 2026-01电费结算单 → 2026-01-01
    - 杭锦旗1月份电费清单 → requires year from parent folder
    - 2026年1月上网电费结算单 → 2026-01-01
    """
    import re
    # Pattern: YYYY-MM or YYYY.MM (vendor uses both separators)
    m = re.search(r'(\d{4})[-.](\d{1,2})', filename)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-01"
    # Pattern: YYYY年M月
    m = re.search(r'(\d{4})年(\d{1,2})月', filename)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-01"
    # Pattern: N月份 (need year from context)
    m = re.search(r'(\d{1,2})月份?', filename)
    if m:
        # Will be combined with year from folder path
        return f"NEED_YEAR-{int(m.group(1)):02d}-01"
    return None


def extract_year_from_path(path: str) -> int | None:
    """Extract year from folder path (e.g. /2026/ or /2026年结算单/)."""
    import re
    m = re.search(r'[/\\](\d{4})', path)
    return int(m.group(1)) if m else None


def resolve_settlement_month(pdf_path) -> str | None:
    """Resolve settlement month (YYYY-MM-01) for an invoice PDF.

    Ladder: filename date → filename month + folder year → PDF content
    (extract_billing_period, lazy import so tests can patch it at its home).
    Never stamps the current year — returns None when no source yields a date
    (phantom-month rule, commit 1064925).
    """
    month_str = extract_month_from_filename(Path(pdf_path).name)
    if month_str and month_str.startswith("NEED_YEAR"):
        year = extract_year_from_path(str(pdf_path))
        month_str = month_str.replace("NEED_YEAR", str(year)) if year else None
    if month_str:
        return month_str
    try:
        from services.settlement_ingest.parser_charge import extract_billing_period
        return extract_billing_period(str(pdf_path))
    except Exception:
        return None


def scan_and_ingest(root: str | None = None, dry_run: bool = False) -> list[dict]:
    """Scan all invoice folders and ingest new PDFs.

    Args:
        root: Override invoice root path (default: INVOICE_ROOT)
        dry_run: If True, report what would be ingested without writing to DB

    Returns:
        List of result dicts per file: {path, asset, month, type, status, items, error}
    """
    if root is None:
        root = INVOICE_ROOT

    results = []
    root_path = Path(root)

    if not root_path.exists():
        return [{"error": f"Root path not found: {root}"}]

    # If root is a specific asset folder (not the parent), use its name for resolution
    root_folder_name = root_path.name
    root_is_asset_folder = resolve_folder_to_asset(root_folder_name) is not None

    for pdf_path in sorted(root_path.rglob("*.pdf")):
        if pdf_path.name.startswith("~"):
            continue

        rel_path = str(pdf_path.relative_to(root_path))
        # Resolve asset from folder
        if root_is_asset_folder:
            asset_folder = root_folder_name
        else:
            folder_parts = rel_path.split(os.sep)
            asset_folder = folder_parts[0] if folder_parts else ""
        asset_name = resolve_folder_to_asset(asset_folder)

        if asset_name is None:
            results.append({"path": rel_path, "status": "skipped", "error": f"No asset mapping for folder: {asset_folder}"})
            continue

        # Check if already ingested
        fhash = file_sha256(str(pdf_path))
        if is_already_ingested(fhash):
            results.append({"path": rel_path, "asset": asset_name, "status": "already_ingested"})
            continue

        # Classify and extract month (filename → folder year → PDF content;
        # never stamps the current year — phantom-month rule, commit 1064925)
        pdf_type = classify_pdf(pdf_path.name)
        month_str = resolve_settlement_month(pdf_path)

        if not month_str:
            results.append({"path": rel_path, "asset": asset_name, "status": "skipped",
                            "error": "Cannot determine month from filename, folder path, or PDF content"})
            continue

        if dry_run:
            results.append({"path": rel_path, "asset": asset_name, "month": month_str, "type": pdf_type, "status": "dry_run"})
            continue

        # Ingest
        try:
            if pdf_type == "charge":
                items = parse_charging_cost_pdf(str(pdf_path))
            elif pdf_type == "discharge":
                items = parse_discharge_settlement_pdf(str(pdf_path))
            elif pdf_type == "voucher":
                results.append({"path": rel_path, "asset": asset_name, "status": "skipped",
                                "error": "结算凭证 (trading-center voucher) — duplicates 结算单 data, not ingested"})
                continue
            else:
                results.append({"path": rel_path, "asset": asset_name, "status": "skipped", "error": f"Unknown PDF type: {pdf_type}"})
                continue

            if not items:
                results.append({"path": rel_path, "asset": asset_name, "status": "empty", "error": "No items extracted"})
                continue

            # Write to DB
            _write_settlement(asset_name, month_str, pdf_path.name, pdf_type, fhash, items)
            results.append({"path": rel_path, "asset": asset_name, "month": month_str, "type": pdf_type, "status": "ingested", "items": len(items)})

        except Exception as e:
            results.append({"path": rel_path, "asset": asset_name, "status": "error", "error": str(e)})

    return results


def _write_settlement(asset_name: str, month: str, filename: str, file_type: str, file_hash: str, items: list[dict]):
    """Write settlement record + items to database."""
    import json

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Get book_id for this asset
            cur.execute("""
                SELECT b.id FROM marketdata.rm_books b
                JOIN marketdata.rm_assets a ON a.id = b.asset_id
                WHERE a.name = %s AND b.book_type = 'asset'
                LIMIT 1
            """, (asset_name,))
            row = cur.fetchone()
            if not row:
                raise ValueError(f"No book found for asset: {asset_name}")
            book_id = row[0]

            # Insert settlement record
            cur.execute("""
                INSERT INTO marketdata.rm_settlements
                    (book_id, settlement_month, file_name, file_type, status, raw_data)
                VALUES (%s, %s, %s, %s, 'processed', %s)
                RETURNING id
            """, (book_id, month, filename, 'pdf', json.dumps({"file_hash": file_hash, "pdf_type": file_type})))
            settlement_id = cur.fetchone()[0]

            # Insert items
            total = 0.0
            for item in items:
                cur.execute("""
                    INSERT INTO marketdata.rm_settlement_items
                        (settlement_id, category, volume_mwh, price_cny_kwh, amount_cny, peak_period, notes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    settlement_id, item["category"],
                    item.get("volume_mwh"), item.get("price_cny_kwh"),
                    item["amount_cny"], item.get("peak_period"), item.get("notes"),
                ))
                total += float(item.get("amount_cny", 0) or 0)

            # Update total
            cur.execute(
                "UPDATE marketdata.rm_settlements SET total_amount_cny = %s WHERE id = %s",
                (total, settlement_id)
            )
        conn.commit()
