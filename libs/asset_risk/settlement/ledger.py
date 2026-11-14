"""Settlement ledger writes and dedup reads (no Streamlit)."""
from __future__ import annotations

import hashlib
import json

from sqlalchemy import bindparam, text


def _content_sha256(data: bytes) -> str:
    """SHA-256 of uploaded file bytes (content follows bytes, not filename)."""
    return hashlib.sha256(data).hexdigest()


def _already_ingested(engine, book_id: int, settlement_month: str, file_hash: str) -> bool:
    """True if an identical-content settlement exists at this book+month.

    Guards the rename-reupload duplication: tab uploads historically stored no
    hash, so a renamed re-upload bypassed both overwrite (name match) and the
    scanner's hash check (observed 2026-08-17: 苏右/裕昭沙子坝/远景乌拉特 dupes).
    """
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT 1 FROM marketdata.rm_settlements
            WHERE book_id = :bid AND settlement_month = :month
              AND raw_data->>'file_hash' = :h LIMIT 1
        """), {"bid": book_id, "month": settlement_month, "h": file_hash}).first()
    return row is not None


def _has_category(engine, book_id: int, settlement_month: str, category: str) -> bool:
    """True if any settlement item of this category exists at this book+month."""
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT 1 FROM marketdata.rm_settlement_items i
            JOIN marketdata.rm_settlements s ON s.id = i.settlement_id
            WHERE s.book_id = :bid AND s.settlement_month = :month
              AND i.category = :cat LIMIT 1
        """), {"bid": book_id, "month": settlement_month, "cat": category}).first()
    return row is not None


def _delete_book_month_file(conn, book_id: int, settlement_month, file_name: str):
    """Delete settlement record(s) + items for book+month+file_name (overwrite path).

    Only called AFTER a successful parse — a refused/failed upload must never
    destroy existing data (observed 2026-09-14: overwrite checked + scanned
    charge bill refused would have deleted the hand-ingested records).
    """
    conn.execute(text("""
        DELETE FROM marketdata.rm_settlement_items
        WHERE settlement_id IN (
            SELECT id FROM marketdata.rm_settlements
            WHERE book_id = :bid AND settlement_month = :month AND file_name = :fname
        )
    """), {"bid": book_id, "month": settlement_month, "fname": file_name})


def _delete_voucher_settlements(conn, book_id: int, settlement_month) -> int:
    """Delete voucher-sourced settlements for book+month, returning count removed.

    The 凭证 fallback (voucher as sole discharge source, 乌海 2026-01/02) has a
    reverse-order hole: voucher ingested first, then the real 结算单 lands and
    nothing removes the voucher — both count (乌海 2026-08/09 double-counted
    现货收入). A 结算单 is authoritative and the voucher is its strict subset,
    so a successful non-voucher discharge parse supersedes them.
    """
    from services.settlement_ingest.scanner import classify_pdf
    rows = conn.execute(text("""
        SELECT id, file_name FROM marketdata.rm_settlements
        WHERE book_id = :bid AND settlement_month = :month
    """), {"bid": book_id, "month": settlement_month}).fetchall()
    voucher_ids = [r[0] for r in rows if classify_pdf(r[1] or "") == "voucher"]
    if not voucher_ids:
        return 0
    conn.execute(text(
        "DELETE FROM marketdata.rm_settlement_items WHERE settlement_id IN :ids"
    ).bindparams(bindparam("ids", expanding=True)), {"ids": voucher_ids})
    conn.execute(text(
        "DELETE FROM marketdata.rm_settlements WHERE id IN :ids"
    ).bindparams(bindparam("ids", expanding=True)), {"ids": voucher_ids})
    return len(voucher_ids)


def insert_settlement(conn, book_id: int, settlement_month, file_name: str, file_type: str,
                      file_hash: str | None, items: list[dict]) -> int:
    """Insert one rm_settlements row plus its items; returns the new settlement id.

    One INSERT serves both upload paths: Excel-only fields (delivery_date,
    amount_*_cny, counterparty) and the PDF-only peak_period are written when
    present on an item, else NULL.
    """
    result = conn.execute(text("""
        INSERT INTO marketdata.rm_settlements (book_id, settlement_month, file_name, file_type, status, raw_data)
        VALUES (:bid, :month, :fname, :ftype, 'processed', :rd)
        RETURNING id
    """), {"bid": book_id, "month": settlement_month, "fname": file_name, "ftype": file_type,
           "rd": json.dumps({"file_hash": file_hash}) if file_hash else None})
    settlement_id = result.scalar()

    for item in items:
        conn.execute(text("""
            INSERT INTO marketdata.rm_settlement_items
                (settlement_id, category, delivery_date, volume_mwh, price_cny_kwh,
                 amount_cny, amount_receivable_cny, amount_settled_cny, amount_diff_cny,
                 counterparty, peak_period, notes)
            VALUES (:sid, :cat, :dd, :vol, :price, :amt, :recv, :settled, :diff, :cp, :peak, :notes)
        """), {
            "sid": settlement_id, "cat": item["category"],
            "dd": item.get("delivery_date"), "vol": item.get("volume_mwh"),
            "price": item.get("price_cny_kwh"), "amt": item["amount_cny"],
            "recv": item.get("amount_receivable_cny"),
            "settled": item.get("amount_settled_cny"),
            "diff": item.get("amount_diff_cny"),
            "cp": item.get("counterparty"), "peak": item.get("peak_period"),
            "notes": item.get("notes"),
        })
    return settlement_id
