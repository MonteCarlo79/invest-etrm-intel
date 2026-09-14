"""Fuel & fleet ETL — upsert + conflict detection for marketdata.province_fuel_fleet.

Entry points:
    ensure_table(cur)                                 — idempotent DDL
    upsert_fuel_fleet_rows(rows, pg_url, source)      — upsert extracted rows
    resolve_fuel_fleet_conflict(row_id_keep, row_id_drop, pg_url)
"""
from __future__ import annotations

import json
import logging
from datetime import date
from typing import Optional

import psycopg2

logger = logging.getLogger(__name__)

_ENSURE_SQL = """
CREATE TABLE IF NOT EXISTS marketdata.province_fuel_fleet (
    id                  SERIAL PRIMARY KEY,
    province            TEXT        NOT NULL,
    effective_date      DATE        NOT NULL,
    coal_price_yuan_t   NUMERIC,
    gas_price_yuan_m3   NUMERIC,
    fleet_segments      JSONB       NOT NULL,
    source              TEXT,
    status              TEXT        NOT NULL DEFAULT 'draft',
    notes               TEXT,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pff_prov_date_src
    ON marketdata.province_fuel_fleet (province, effective_date, COALESCE(source, ''));
CREATE INDEX IF NOT EXISTS idx_pff_prov_date
    ON marketdata.province_fuel_fleet (province, effective_date DESC);
"""

_INSERT_SQL = """
INSERT INTO marketdata.province_fuel_fleet
    (province, effective_date, coal_price_yuan_t, gas_price_yuan_m3,
     fleet_segments, source, status, notes)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (province, effective_date, COALESCE(source, '')) DO UPDATE SET
    coal_price_yuan_t = EXCLUDED.coal_price_yuan_t,
    gas_price_yuan_m3 = EXCLUDED.gas_price_yuan_m3,
    fleet_segments    = EXCLUDED.fleet_segments,
    notes             = EXCLUDED.notes,
    ingested_at       = NOW()
RETURNING id
"""

_FETCH_CONFIRMED_SQL = """
SELECT id, coal_price_yuan_t FROM marketdata.province_fuel_fleet
WHERE province = %s AND effective_date = %s AND status = 'confirmed'
ORDER BY ingested_at DESC LIMIT 1
"""

_SET_STATUS_SQL = "UPDATE marketdata.province_fuel_fleet SET status = %s, ingested_at = NOW() WHERE id = %s"

_CONFLICT_THRESHOLD = 0.05


def ensure_table(cur) -> None:
    cur.execute(_ENSURE_SQL)


def _parse_date(d) -> Optional[date]:
    if isinstance(d, date):
        return d
    if isinstance(d, str):
        try:
            return date.fromisoformat(d[:10])
        except ValueError:
            return None
    return None


def _valid_segments(segs) -> bool:
    if not isinstance(segs, list) or not segs:
        return False
    for s in segs:
        if not isinstance(s, dict):
            return False
        if s.get("fuel") not in ("coal", "gas"):
            return False
        try:
            float(s["capacity_mw"]); float(s["heat_rate_kj_kwh"]); float(s["vom_yuan_mwh"])
        except (KeyError, TypeError, ValueError):
            return False
    return True


def _values_conflict(existing: float, new_val: float) -> bool:
    if existing == 0:
        return new_val != 0
    return abs(new_val - existing) / abs(existing) > _CONFLICT_THRESHOLD


def upsert_fuel_fleet_rows(rows: list[dict], pg_url: str, source: str) -> dict:
    upserted, conflicts, errors = 0, 0, []
    conn = psycopg2.connect(pg_url)
    try:
        with conn.cursor() as cur:
            ensure_table(cur)
            for row in rows:
                province = str(row.get("province", "")).strip()
                if not province:
                    errors.append("missing province"); continue
                eff = _parse_date(row.get("effective_date"))
                if not eff:
                    errors.append(f"{province}: invalid effective_date"); continue
                segs = row.get("fleet_segments")
                if not _valid_segments(segs):
                    errors.append(f"{province}: invalid fleet_segments"); continue
                coal = row.get("coal_price_yuan_t")
                gas = row.get("gas_price_yuan_m3")
                # conflict check on coal price vs existing confirmed
                cur.execute(_FETCH_CONFIRMED_SQL, (province, eff))
                existing = cur.fetchall()
                status = "draft"
                if existing and coal is not None and existing[0][1] is not None:
                    if _values_conflict(float(existing[0][1]), float(coal)):
                        cur.execute(_SET_STATUS_SQL, ("conflict", existing[0][0]))
                        status = "conflict"
                        conflicts += 1
                cur.execute(_INSERT_SQL, (
                    province, eff, coal, gas,
                    json.dumps(segs, ensure_ascii=False), source, status,
                    row.get("notes")))
                upserted += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"upserted": upserted, "conflicts": conflicts, "errors": errors}


def resolve_fuel_fleet_conflict(row_id_keep: int, row_id_drop: int, pg_url: str) -> None:
    conn = psycopg2.connect(pg_url)
    try:
        with conn.cursor() as cur:
            cur.execute(_SET_STATUS_SQL, ("confirmed", row_id_keep))
            cur.execute(_SET_STATUS_SQL, ("superseded", row_id_drop))
        conn.commit()
    finally:
        conn.close()
