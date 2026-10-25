# services/option_engine/snapshot.py
"""Persistence for option-engine value snapshots (spec §5.7).

Table ``marketdata.option_value_snapshot``, DDL in
``db/ddl/marketdata/option_value_snapshot.sql``, applied via the
ensure-pattern like ``deal_committee.sql``. Only ``batch.py`` writes; app
tabs read latest per key. Numpy paths are never persisted (deal_committee
lesson).

I/O only via the caller's SQLAlchemy engine — no module-level engine.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
from sqlalchemy import text

_DDL_PATH = (
    Path(__file__).resolve().parents[2]
    / "db" / "ddl" / "marketdata" / "option_value_snapshot.sql"
)

_COLUMNS = (
    "province", "asset_type", "duration_h", "asof_date",
    "window_start", "window_end",
    "intrinsic_yuan_kw", "extrinsic_yuan_kw", "total_yuan_kw",
    "capture_adj_yuan_kw",
    "policy_json", "greeks_json", "mc_params",
)
_JSON_FIELDS = ("policy_json", "greeks_json", "mc_params")
_FLOAT_FIELDS = (
    "intrinsic_yuan_kw", "extrinsic_yuan_kw", "total_yuan_kw",
    "capture_adj_yuan_kw",
)

_UPSERT = text("""
    INSERT INTO marketdata.option_value_snapshot
        (province, asset_type, duration_h, asof_date, window_start, window_end,
         intrinsic_yuan_kw, extrinsic_yuan_kw, total_yuan_kw, capture_adj_yuan_kw,
         policy_json, greeks_json, mc_params)
    VALUES (:province, :asset_type, :duration_h, :asof_date,
            :window_start, :window_end,
            :intrinsic_yuan_kw, :extrinsic_yuan_kw, :total_yuan_kw,
            :capture_adj_yuan_kw,
            CAST(:policy_json AS jsonb), CAST(:greeks_json AS jsonb),
            CAST(:mc_params AS jsonb))
    ON CONFLICT (province, asset_type, duration_h, asof_date) DO UPDATE SET
        window_start        = EXCLUDED.window_start,
        window_end          = EXCLUDED.window_end,
        intrinsic_yuan_kw   = EXCLUDED.intrinsic_yuan_kw,
        extrinsic_yuan_kw   = EXCLUDED.extrinsic_yuan_kw,
        total_yuan_kw       = EXCLUDED.total_yuan_kw,
        capture_adj_yuan_kw = EXCLUDED.capture_adj_yuan_kw,
        policy_json         = EXCLUDED.policy_json,
        greeks_json         = EXCLUDED.greeks_json,
        mc_params           = EXCLUDED.mc_params,
        created_at          = NOW()
""")


def ensure_table(eng) -> None:
    """Apply the DDL file (idempotent CREATE ... IF NOT EXISTS).

    Strip ``--`` comments BEFORE splitting on ";" (deal_committee/library.py
    pattern): a semicolon inside a comment would otherwise split mid-comment
    and leave a chunk starting with comment text. Safe here: the DDL has no
    string literals containing "--".
    """
    raw = _DDL_PATH.read_text(encoding="utf-8")
    bare = "\n".join(line.split("--", 1)[0] for line in raw.splitlines())
    with eng.begin() as conn:
        for stmt in bare.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))


def _json_safe(obj):
    """Recursively convert numpy scalars/arrays to plain Python and map
    non-finite floats to None (jsonb rejects NaN/Inf literals)."""
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
    return obj


def _finite_or_none(v):
    """float → None when None or non-finite (NaN/Inf stay out of the DB)."""
    if v is None:
        return None
    f = float(v)
    return f if math.isfinite(f) else None


def write_snapshot(eng, row: dict) -> None:
    """Upsert one snapshot row keyed by (province, asset_type, duration_h,
    asof_date). ``row`` keys are the table column names (see ``_COLUMNS``);
    the three JSON fields accept dicts/lists (numpy-safe serialization).
    """
    ensure_table(eng)
    params = {k: row.get(k) for k in _COLUMNS}
    for f in _FLOAT_FIELDS:
        params[f] = _finite_or_none(params[f])
    params["duration_h"] = float(params["duration_h"])
    for f in _JSON_FIELDS:
        params[f] = json.dumps(_json_safe(params[f]))
    with eng.begin() as conn:
        conn.execute(_UPSERT, params)


def read_latest(eng, province: str, asset_type: str, duration_h: float
                ) -> dict | None:
    """Latest snapshot by asof_date for the key, as a dict; None if absent."""
    sql = text("""
        SELECT province, asset_type, duration_h, asof_date,
               window_start, window_end,
               intrinsic_yuan_kw, extrinsic_yuan_kw, total_yuan_kw,
               capture_adj_yuan_kw,
               policy_json, greeks_json, mc_params, created_at
        FROM marketdata.option_value_snapshot
        WHERE province = :province AND asset_type = :asset_type
          AND duration_h = :duration_h
        ORDER BY asof_date DESC
        LIMIT 1
    """)
    with eng.connect() as conn:
        row = conn.execute(sql, {
            "province": province, "asset_type": asset_type,
            "duration_h": float(duration_h),
        }).mappings().fetchone()
    return dict(row) if row is not None else None
