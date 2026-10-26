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
import math
from datetime import date, datetime
from pathlib import Path

import numpy as np
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

_UPDATE_VALIDATION = text("""
    UPDATE marketdata.option_opportunity_cards
    SET validation = :verdict,
        validation_detail = CAST(:detail AS jsonb)
    WHERE id = :cid
""")

# The validator's verdict vocabulary (Task 5) plus the pre-validation
# default. Review-status words ('approved'/'rejected'/...) never pass here —
# those stay with review_action / the promotion flow.
_VALIDATION_VERDICTS = ("confirmed", "weakened", "no_evidence", "pending")


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


def _json_safe(obj):
    """Recursively convert numpy scalars/arrays to plain Python and map
    non-finite floats to None (jsonb rejects NaN/Inf literals). Ported from
    ``services/option_engine/snapshot.py::_json_safe`` — its docstring
    records the numpy-persistence lesson — and extended: pandas Timestamps
    and date/datetime objects become ISO strings (Timestamp subclasses
    datetime, so one branch covers both, and the extension must live here
    because it has to recurse at any nesting depth)."""
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, np.ndarray):
        return _json_safe(obj.tolist())
    if isinstance(obj, (bool, np.bool_)):
        return bool(obj)
    if isinstance(obj, (int, np.integer)):
        return int(obj)
    if isinstance(obj, (float, np.floating)):
        f = float(obj)
        return f if math.isfinite(f) else None
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj


def _build_params(card: dict) -> dict:
    """INSERT params for one card. The three JSON fields are serialized
    numpy/pandas-safe via ``_json_safe``; ``signature`` falls back to
    ``build_signature(card)`` when absent."""
    return {
        "kind": card.get("kind"),
        "province": card.get("province"),
        "asset_type": card.get("asset_type"),
        "thesis": card.get("thesis"),
        "evidence_json": json.dumps(_json_safe(card.get("evidence_json") or {})),
        "validation": card.get("validation", "pending"),
        "validation_detail": json.dumps(_json_safe(card.get("validation_detail") or {})),
        "scan_params": json.dumps(_json_safe(card.get("scan_params") or {})),
        "scan_date": card.get("scan_date"),
        "signature": card.get("signature") or build_signature(card),
    }


def save_card(eng, card: dict) -> int:
    """Insert one card, return its id. ``card`` keys are the table column
    names; the three JSON fields accept dicts (numpy/pandas-safe
    serialization). ``scan_date`` is required in practice: the delta gate
    (``recent_same_signature``) windows on it, so a NULL scan_date makes the
    card invisible to re-fire suppression."""
    ensure_table(eng)
    with eng.begin() as conn:
        return conn.execute(_INSERT, _build_params(card)).scalar_one()


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


def set_validation(eng, card_id: int, verdict: str, detail: dict) -> None:
    """Set validation + validation_detail on a saved card — the dedicated
    writer for (re-)validation of an existing row (the orchestrator saves
    new cards with the verdict already set; this serves re-validation
    without touching review status). Guarded to the validator's verdict
    vocabulary; review actions stay with ``review_action``. ``detail`` is
    serialized numpy/pandas-safe like the other JSON fields."""
    if verdict not in _VALIDATION_VERDICTS:
        raise ValueError(
            f"set_validation allows {sorted(_VALIDATION_VERDICTS)}, "
            f"got {verdict!r}"
        )
    with eng.begin() as conn:
        conn.execute(_UPDATE_VALIDATION, {
            "cid": card_id,
            "verdict": verdict,
            "detail": json.dumps(_json_safe(detail or {})),
        })


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
