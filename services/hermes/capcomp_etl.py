"""
Province Capacity Compensation + FR Market ETL
===============================================
Handles DB upsert and conflict detection for:
  - province_cap_comp     : 储能容量补偿标准 (yuan/kW) + 年最高净负荷峰值时段 (h)
  - province_fr_market    : 调频容量价格 (yuan/kW/h) + 全省调频总资金池 (亿元/年)

Conflict detection:
  If a 'confirmed' row already exists for (province, effective_date) with a value
  that differs by >5%, both the existing and the new row are marked 'conflict'.

Entry points:
  upsert_cap_comp_rows(rows, pg_url, source)       — upsert cap_comp data
  upsert_fr_rows(rows, pg_url, source)             — upsert fr_market data
  resolve_conflict(table, row_id_keep, row_id_drop, pg_url)  — UI conflict resolution
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

import psycopg2

logger = logging.getLogger(__name__)

# ── DDL ───────────────────────────────────────────────────────────────────────

_ENSURE_CAP_COMP_SQL = """
CREATE TABLE IF NOT EXISTS marketdata.province_cap_comp (
    id                  SERIAL PRIMARY KEY,
    province            TEXT        NOT NULL,
    effective_date      DATE        NOT NULL,
    cap_comp_yuan_kw    NUMERIC,
    peak_duration_hours NUMERIC,
    source              TEXT,
    status              TEXT        NOT NULL DEFAULT 'confirmed',
    notes               TEXT,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE marketdata.province_cap_comp ADD COLUMN IF NOT EXISTS notes TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_pcc_prov_date_src
    ON marketdata.province_cap_comp (province, effective_date, COALESCE(source, ''));
CREATE INDEX IF NOT EXISTS idx_pcc_prov_date
    ON marketdata.province_cap_comp (province, effective_date DESC);
"""

_ENSURE_FR_SQL = """
CREATE TABLE IF NOT EXISTS marketdata.province_fr_market (
    id                   SERIAL PRIMARY KEY,
    province             TEXT        NOT NULL,
    effective_date       DATE        NOT NULL,
    fr_price_yuan_kw_h   NUMERIC,
    fr_pool_billion_yuan NUMERIC,
    source               TEXT,
    status               TEXT        NOT NULL DEFAULT 'confirmed',
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pfr_prov_date_src
    ON marketdata.province_fr_market (province, effective_date, COALESCE(source, ''));
CREATE INDEX IF NOT EXISTS idx_pfr_prov_date
    ON marketdata.province_fr_market (province, effective_date DESC);
"""

# ── SQL statements ─────────────────────────────────────────────────────────────

_INSERT_CAP_COMP_SQL = """
INSERT INTO marketdata.province_cap_comp
    (province, effective_date, cap_comp_yuan_kw, peak_duration_hours, source, status)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (province, effective_date, COALESCE(source, '')) DO UPDATE SET
    cap_comp_yuan_kw    = EXCLUDED.cap_comp_yuan_kw,
    peak_duration_hours = EXCLUDED.peak_duration_hours,
    status              = EXCLUDED.status,
    ingested_at         = NOW()
RETURNING id
"""

_INSERT_FR_SQL = """
INSERT INTO marketdata.province_fr_market
    (province, effective_date, fr_price_yuan_kw_h, fr_pool_billion_yuan, source, status)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (province, effective_date, COALESCE(source, '')) DO UPDATE SET
    fr_price_yuan_kw_h   = EXCLUDED.fr_price_yuan_kw_h,
    fr_pool_billion_yuan = EXCLUDED.fr_pool_billion_yuan,
    status               = EXCLUDED.status,
    ingested_at          = NOW()
RETURNING id
"""

_FETCH_CONFIRMED_CAP_SQL = """
SELECT id, cap_comp_yuan_kw
FROM marketdata.province_cap_comp
WHERE province = %s AND effective_date = %s AND status = 'confirmed'
ORDER BY ingested_at DESC
LIMIT 1
"""

_FETCH_CONFIRMED_FR_SQL = """
SELECT id, fr_price_yuan_kw_h
FROM marketdata.province_fr_market
WHERE province = %s AND effective_date = %s AND status = 'confirmed'
ORDER BY ingested_at DESC
LIMIT 1
"""

_SET_CONFLICT_SQL = "UPDATE {table} SET status = 'conflict' WHERE id = %s"

_RESOLVE_KEEP_SQL = "UPDATE {table} SET status = 'confirmed', ingested_at = NOW() WHERE id = %s"
_RESOLVE_DROP_SQL = "UPDATE {table} SET status = 'superseded', ingested_at = NOW() WHERE id = %s"

_CONFLICT_THRESHOLD = 0.05  # 5% difference triggers conflict


def _values_conflict(existing: float, new_val: float) -> bool:
    """Return True if new_val differs from existing by more than the threshold."""
    if existing == 0:
        return new_val != 0
    return abs(new_val - existing) / abs(existing) > _CONFLICT_THRESHOLD


def _parse_date(d) -> Optional[date]:
    if isinstance(d, date):
        return d
    if isinstance(d, str):
        try:
            return date.fromisoformat(d[:10])
        except ValueError:
            return None
    if isinstance(d, int) and 2020 <= d <= 2030:
        return date(d, 1, 1)
    return None


# ── Public upsert functions ────────────────────────────────────────────────────

def upsert_cap_comp_rows(rows: list[dict], pg_url: str, source: str) -> dict:
    """
    Upsert 容量补偿 rows into province_cap_comp.

    Each row: {province, effective_date, cap_comp_yuan_kw, [peak_duration_hours]}
    Conflict detection: if confirmed row already exists with >5% diff in cap_comp_yuan_kw,
    both rows are marked 'conflict'.

    Returns {upserted, conflicts, errors}.
    """
    upserted = 0
    conflicts = 0
    errors: list[str] = []

    conn = psycopg2.connect(pg_url)
    try:
        with conn.cursor() as cur:
            cur.execute(_ENSURE_CAP_COMP_SQL)

            for row in rows:
                province = str(row.get("province", "")).strip()
                if not province:
                    continue
                eff_date = _parse_date(row.get("effective_date"))
                if not eff_date:
                    errors.append(f"{province}: invalid effective_date {row.get('effective_date')}")
                    continue
                cap_val = row.get("cap_comp_yuan_kw")
                if cap_val is None:
                    errors.append(f"{province}: missing cap_comp_yuan_kw")
                    continue
                try:
                    cap_val = float(cap_val)
                except (TypeError, ValueError):
                    errors.append(f"{province}: non-numeric cap_comp_yuan_kw={cap_val}")
                    continue
                peak_h = row.get("peak_duration_hours")
                if peak_h is not None:
                    try:
                        peak_h = float(peak_h)
                    except (TypeError, ValueError):
                        peak_h = None

                # Conflict detection
                status = "confirmed"
                cur.execute(_FETCH_CONFIRMED_CAP_SQL, (province, eff_date))
                existing_row = cur.fetchone()
                if existing_row and _values_conflict(float(existing_row[1]), cap_val):
                    # Mark existing as conflict
                    cur.execute(_SET_CONFLICT_SQL.format(table="marketdata.province_cap_comp"),
                                (existing_row[0],))
                    status = "conflict"
                    conflicts += 1
                    logger.info(
                        "cap_comp conflict: %s %s existing=%.4f new=%.4f (src=%s)",
                        province, eff_date, existing_row[1], cap_val, source,
                    )

                # Prefer per-row source (e.g. KB filename, policy doc) over generic tag
                row_source = str(row.get("source") or source)[:500]
                try:
                    cur.execute(_INSERT_CAP_COMP_SQL,
                                (province, eff_date, cap_val, peak_h, row_source, status))
                    upserted += 1
                except Exception as exc:
                    errors.append(f"{province}/{eff_date}: {exc}")
                    logger.error("cap_comp upsert failed %s/%s: %s", province, eff_date, exc)

        conn.commit()
    finally:
        conn.close()

    return {"upserted": upserted, "conflicts": conflicts, "errors": errors}


def upsert_fr_rows(rows: list[dict], pg_url: str, source: str) -> dict:
    """
    Upsert 调频市场 rows into province_fr_market.

    Each row: {province, effective_date, fr_price_yuan_kw_h, [fr_pool_billion_yuan]}
    Conflict detection: if confirmed row already exists with >5% diff in fr_price_yuan_kw_h,
    both rows are marked 'conflict'.

    Returns {upserted, conflicts, errors}.
    """
    upserted = 0
    conflicts = 0
    errors: list[str] = []

    conn = psycopg2.connect(pg_url)
    try:
        with conn.cursor() as cur:
            cur.execute(_ENSURE_FR_SQL)

            for row in rows:
                province = str(row.get("province", "")).strip()
                if not province:
                    continue
                eff_date = _parse_date(row.get("effective_date"))
                if not eff_date:
                    errors.append(f"{province}: invalid effective_date {row.get('effective_date')}")
                    continue
                fr_price = row.get("fr_price_yuan_kw_h")
                if fr_price is not None:
                    try:
                        fr_price = float(fr_price)
                    except (TypeError, ValueError):
                        errors.append(f"{province}: non-numeric fr_price_yuan_kw_h={fr_price}")
                        continue
                fr_pool = row.get("fr_pool_billion_yuan")
                if fr_pool is not None:
                    try:
                        fr_pool = float(fr_pool)
                    except (TypeError, ValueError):
                        fr_pool = None
                # Require at least unit price or total pool
                if fr_price is None and fr_pool is None:
                    errors.append(f"{province}: missing fr_price_yuan_kw_h and fr_pool_billion_yuan")
                    continue

                # Conflict detection (only when unit price is present)
                status = "confirmed"
                if fr_price is not None:
                    cur.execute(_FETCH_CONFIRMED_FR_SQL, (province, eff_date))
                    existing_row = cur.fetchone()
                    if existing_row and existing_row[1] is not None and _values_conflict(float(existing_row[1]), fr_price):
                        cur.execute(_SET_CONFLICT_SQL.format(table="marketdata.province_fr_market"),
                                    (existing_row[0],))
                        status = "conflict"
                        conflicts += 1
                        logger.info(
                            "fr_market conflict: %s %s existing=%.4f new=%.4f (src=%s)",
                            province, eff_date, existing_row[1], fr_price, source,
                        )

                # Prefer per-row source over generic tag
                row_source = str(row.get("source") or source)[:500]
                try:
                    cur.execute(_INSERT_FR_SQL,
                                (province, eff_date, fr_price, fr_pool, row_source, status))
                    upserted += 1
                except Exception as exc:
                    errors.append(f"{province}/{eff_date}: {exc}")
                    logger.error("fr_market upsert failed %s/%s: %s", province, eff_date, exc)

        conn.commit()
    finally:
        conn.close()

    return {"upserted": upserted, "conflicts": conflicts, "errors": errors}


def resolve_conflict(table: str, row_id_keep: int, row_id_drop: int, pg_url: str) -> dict:
    """
    Resolve a data conflict:
      - row_id_keep → status='confirmed'
      - row_id_drop → status='superseded'

    table must be 'province_cap_comp' or 'province_fr_market'.
    Returns {ok: bool, error: str|None}.
    """
    allowed = {"province_cap_comp", "province_fr_market"}
    if table not in allowed:
        return {"ok": False, "error": f"Invalid table: {table}"}

    qualified = f"marketdata.{table}"
    conn = psycopg2.connect(pg_url)
    try:
        with conn.cursor() as cur:
            cur.execute(_RESOLVE_KEEP_SQL.format(table=qualified), (row_id_keep,))
            cur.execute(_RESOLVE_DROP_SQL.format(table=qualified), (row_id_drop,))
        conn.commit()
        logger.info("Conflict resolved: %s keep=%d drop=%d", table, row_id_keep, row_id_drop)
        return {"ok": True, "error": None}
    except Exception as exc:
        conn.rollback()
        logger.error("resolve_conflict failed: %s", exc)
        return {"ok": False, "error": str(exc)}
    finally:
        conn.close()
