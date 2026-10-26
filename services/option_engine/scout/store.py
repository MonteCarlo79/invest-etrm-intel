# services/option_engine/scout/store.py
"""Persistence for generative-scout opportunity cards (Task 1).

Table ``marketdata.option_opportunity_cards``, DDL in
``db/ddl/marketdata/option_opportunity_cards.sql``, applied via the
ensure-pattern like ``services/option_engine/snapshot.py``. This module is
the ONLY writer of that table (write allowlist).

I/O only via the caller's SQLAlchemy engine — no module-level engine.
"""
from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import text

_DDL_PATH = (
    Path(__file__).resolve().parents[3]
    / "db" / "ddl" / "marketdata" / "option_opportunity_cards.sql"
)

# 'promoted' is deliberately absent: promotion goes through the promotion
# flow (store.set_status, a later task), never through review_action.
_REVIEW_ACTIONS = ("approved", "rejected", "snoozed")

_COLUMNS = (
    "id", "created_at", "kind", "province", "asset_type", "thesis",
    "evidence_json", "validation", "validation_detail", "status",
    "review_note", "reviewed_at", "scan_params", "scan_date", "signature",
)

_INSERT = text("""
    INSERT INTO marketdata.option_opportunity_cards
        (kind, province, asset_type, thesis, evidence_json, validation,
         validation_detail, scan_params, scan_date, signature)
    VALUES (:kind, :province, :asset_type, :thesis,
            CAST(:evidence_json AS jsonb), :validation,
            CAST(:validation_detail AS jsonb), CAST(:scan_params AS jsonb),
            :scan_date, :signature)
    RETURNING id
""")

_UPDATE_REVIEW = text("""
    UPDATE marketdata.option_opportunity_cards
    SET status = :status, review_note = :note, reviewed_at = NOW()
    WHERE id = :cid
""")

_UPDATE_CARRIED = text("""
    UPDATE marketdata.option_opportunity_cards
    SET status = 'carried'
    WHERE id = :cid
""")


def build_signature(card: dict) -> str:
    """Stable dedup key — independent of thesis text and evidence drift."""
    return (
        f"{card['kind']}|{card['province']}"
        f"|{card['asset_type']}|{card['scan_key']}"
    )


def ensure_table(eng) -> None:
    """Apply the DDL file (idempotent CREATE ... IF NOT EXISTS).

    Strip ``--`` comments BEFORE splitting on ";" (snapshot.py /
    deal_committee pattern): a semicolon inside a comment would otherwise
    split mid-comment and leave a chunk starting with comment text.
    """
    raw = _DDL_PATH.read_text(encoding="utf-8")
    bare = "\n".join(line.split("--", 1)[0] for line in raw.splitlines())
    with eng.begin() as conn:
        for stmt in bare.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))


def save_card(eng, card: dict) -> int:
    """Insert one card, return its id. ``card`` keys are the table column
    names; the three JSON fields accept dicts. ``signature`` falls back to
    ``build_signature(card)`` when absent."""
    ensure_table(eng)
    params = {
        "kind": card.get("kind"),
        "province": card.get("province"),
        "asset_type": card.get("asset_type"),
        "thesis": card.get("thesis"),
        "evidence_json": json.dumps(card.get("evidence_json") or {}),
        "validation": card.get("validation", "pending"),
        "validation_detail": json.dumps(card.get("validation_detail") or {}),
        "scan_params": json.dumps(card.get("scan_params") or {}),
        "scan_date": card.get("scan_date"),
        "signature": card.get("signature") or build_signature(card),
    }
    with eng.begin() as conn:
        return conn.execute(_INSERT, params).scalar_one()


def list_cards(eng, status: str | None = None, kind: str | None = None,
               limit: int = 100) -> list[dict]:
    """Cards newest-first (scan_date, then id), optionally filtered by
    status and/or kind."""
    sql = (
        f"SELECT {', '.join(_COLUMNS)}"
        " FROM marketdata.option_opportunity_cards"
    )
    clauses, params = [], {}
    if status is not None:
        clauses.append("status = :status")
        params["status"] = status
    if kind is not None:
        clauses.append("kind = :kind")
        params["kind"] = kind
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY scan_date DESC, id DESC LIMIT :limit"
    params["limit"] = int(limit)
    with eng.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().fetchall()
    return [dict(r) for r in rows]


def review_action(eng, card_id: int, action: str, note: str) -> None:
    """Record a human review decision: sets status + review_note +
    reviewed_at. Only approved/rejected/snoozed pass through here —
    'promoted' is reserved for the promotion flow."""
    if action not in _REVIEW_ACTIONS:
        raise ValueError(
            f"review_action allows {sorted(_REVIEW_ACTIONS)}, got {action!r}"
        )
    with eng.begin() as conn:
        conn.execute(
            _UPDATE_REVIEW,
            {"cid": card_id, "status": action, "note": note},
        )


def mark_carried(eng, card_id: int) -> None:
    """Set status='carried' — card carried forward to the next scan."""
    with eng.begin() as conn:
        conn.execute(_UPDATE_CARRIED, {"cid": card_id})


def recent_same_signature(eng, signature: str, within_days: int = 40
                          ) -> dict | None:
    """Latest-by-scan_date non-rejected card with this signature inside the
    window — the delta gate's lookup. A rejected thesis must not suppress a
    re-fire, so rejected cards are excluded."""
    sql = text(f"""
        SELECT {', '.join(_COLUMNS)}
        FROM marketdata.option_opportunity_cards
        WHERE signature = :signature
          AND status <> 'rejected'
          AND scan_date >= CURRENT_DATE - make_interval(days => :within_days)
        ORDER BY scan_date DESC, id DESC
        LIMIT 1
    """)
    with eng.connect() as conn:
        row = conn.execute(sql, {
            "signature": signature, "within_days": int(within_days),
        }).mappings().fetchone()
    return dict(row) if row is not None else None
