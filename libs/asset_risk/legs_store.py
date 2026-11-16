"""Persistent store for the waterfall's expensive LP legs (benchmark, forecast).

One row per (asset_id, month, leg). Only complete months (month_end + 3 days <
today) are stored; the current/incomplete month is always recomputed.
"""
from __future__ import annotations

import calendar
import hashlib
from datetime import date, timedelta
from typing import Callable

from sqlalchemy import text

DDL = """CREATE TABLE IF NOT EXISTS marketdata.rm_waterfall_legs (
  asset_id INT NOT NULL, month TEXT NOT NULL, leg TEXT NOT NULL CHECK (leg IN ('benchmark','forecast')),
  arb_cny DOUBLE PRECISION, dis_mwh DOUBLE PRECISION, chg_mwh DOUBLE PRECISION, cap_cny DOUBLE PRECISION,
  days_solved INT, days_skipped INT, age INT, note TEXT, price_source TEXT, inputs_key TEXT,
  computed_at TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY (asset_id, month, leg))"""

_FLOAT_COLS = ("arb_cny", "dis_mwh", "chg_mwh", "cap_cny")
_INT_COLS = ("days_solved", "days_skipped", "age")
_VALUE_COLS = _FLOAT_COLS + _INT_COLS
_UPSERT = text(
    "INSERT INTO marketdata.rm_waterfall_legs (asset_id, month, leg, "
    + ", ".join(_VALUE_COLS) + ", note, price_source, inputs_key) VALUES (:asset_id, :month, :leg, "
    + ", ".join(f":{c}" for c in _VALUE_COLS) + ", :note, :price_source, :inputs_key) "
    "ON CONFLICT (asset_id, month, leg) DO UPDATE SET "
    + ", ".join(f"{c} = EXCLUDED.{c}" for c in _VALUE_COLS + ("note", "price_source", "inputs_key"))
    + ", computed_at = CURRENT_TIMESTAMP")
_SELECT = text("SELECT " + ", ".join(_VALUE_COLS) + ", note, inputs_key FROM marketdata.rm_waterfall_legs "
               "WHERE asset_id = :asset_id AND month = :month AND leg = :leg")


DEGENERATE_NOTES = frozenset({"no prices", "no complete price day", "no forecast", "lp empty"})


def _is_persistable(leg: str, result: dict) -> bool:
    """Zero-valued fallbacks (feed lag etc.) must not be frozen into the store."""
    if result.get("note") in DEGENERATE_NOTES:
        return False
    if leg == "benchmark" and (result.get("days_solved") or 0) == 0:
        return False
    # A benchmark with skipped days is only frozen when a nodal-price fallback
    # succeeded; otherwise the gap may be feed lag that later fills in.
    if (leg == "benchmark" and (result.get("days_skipped") or 0) > 0
            and "节点" not in (result.get("note") or "")):
        return False
    return True


# Bump when the price series feeding the legs changes, so stored legs are recomputed.
# px2: slot-wise merge cleared > node RT (md_rt_nodal_price) > filled (2026-11-16).
PRICE_SOURCE_VERSION = "px2"


def legs_inputs_key(capacity_mw, duration_h, commission_iso, capcomp_rate) -> str:
    """Stable fingerprint of the asset parameters a stored leg was solved with."""
    def _num(v):
        return "None" if v is None else f"{round(float(v), 4):.4f}"
    canon = "|".join([_num(capacity_mw), _num(duration_h),
                      "None" if not commission_iso else str(commission_iso)[:10],
                      _num(capcomp_rate), PRICE_SOURCE_VERSION])
    return hashlib.sha1(canon.encode("utf-8")).hexdigest()


def ensure_table(engine) -> None:
    ddl = DDL
    if engine.dialect.name == "sqlite":
        ddl = ddl.replace("DEFAULT NOW()", "DEFAULT CURRENT_TIMESTAMP")
    with engine.begin() as conn:
        conn.execute(text(ddl))


def is_month_complete(month: str, today: date) -> bool:
    y, m = int(month[:4]), int(month[5:7])
    month_end = date(y, m, calendar.monthrange(y, m)[1])
    return month_end + timedelta(days=3) < today


def get_or_compute(engine, leg: str, asset_id: int, month: str, compute: Callable[[], dict],
                   force: bool = False, today: date | None = None,
                   inputs_key: str | None = None) -> dict:
    key = {"asset_id": int(asset_id), "month": month, "leg": leg}
    if not force:
        with engine.begin() as conn:
            row = conn.execute(_SELECT, key).mappings().first()
        # A row solved with different asset inputs is a miss (recompute + upsert).
        if row is not None and (inputs_key is None or row["inputs_key"] == inputs_key):
            return {k: row[k] for k in _VALUE_COLS} | {"note": row["note"] or ""}
    result = compute()
    if is_month_complete(month, today or date.today()) and _is_persistable(leg, result):
        params = dict(key)
        for c in _FLOAT_COLS:
            params[c] = float(result[c]) if result.get(c) is not None else None
        for c in _INT_COLS:
            params[c] = int(result[c]) if result.get(c) is not None else None
        params["note"] = result.get("note")
        params["price_source"] = result.get("note") or ""
        params["inputs_key"] = inputs_key
        with engine.begin() as conn:
            conn.execute(_UPSERT, params)
    return result
