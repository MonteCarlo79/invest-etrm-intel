"""GB fuel mix ingestion — NESO CKAN → intl_market.gb_fuel_mix.

Fetches half-hourly (48 SP/day) generation mix data from the National Grid ESO
Historic Generation Mix dataset (CKAN resource f93d1835-75bc-43e5-84ad-12472b180a98)
and writes structured rows to intl_market.gb_fuel_mix.

Called nightly from the 03:00 SGT market-data job and can also be triggered
manually from the Data Management tab.
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, timezone

import requests

logger = logging.getLogger(__name__)

NESO_CKAN_BASE = "https://api.neso.energy/api/3/action"
NESO_GEN_RESOURCE = "f93d1835-75bc-43e5-84ad-12472b180a98"
_REQUEST_DELAY = 0.5

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS intl_market.gb_fuel_mix (
    settlement_date           DATE     NOT NULL,
    settlement_period         SMALLINT NOT NULL,
    gas_mw                    NUMERIC,
    coal_mw                   NUMERIC,
    nuclear_mw                NUMERIC,
    wind_mw                   NUMERIC,
    wind_emb_mw               NUMERIC,
    solar_mw                  NUMERIC,
    hydro_mw                  NUMERIC,
    imports_mw                NUMERIC,
    biomass_mw                NUMERIC,
    storage_mw                NUMERIC,
    other_mw                  NUMERIC,
    generation_mw             NUMERIC,
    demand_mw                 NUMERIC,
    carbon_intensity_gco2_kwh NUMERIC,
    PRIMARY KEY (settlement_date, settlement_period)
);
"""

# Mapping from CKAN column names to DB column names
_COLUMN_MAP = {
    "GAS":            "gas_mw",
    "COAL":           "coal_mw",
    "NUCLEAR":        "nuclear_mw",
    "WIND":           "wind_mw",
    "WIND_EMB":       "wind_emb_mw",
    "SOLAR":          "solar_mw",
    "HYDRO":          "hydro_mw",
    "IMPORTS":        "imports_mw",
    "BIOMASS":        "biomass_mw",
    "STORAGE":        "storage_mw",
    "OTHER":          "other_mw",
    "GENERATION":     "generation_mw",
    "DEMAND":         "demand_mw",
    "CARBON_INTENSITY": "carbon_intensity_gco2_kwh",
}


def _ensure_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(_CREATE_TABLE_SQL)


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _datetime_to_settlement_period(dt_str: str) -> tuple[date, int] | None:
    """Parse NESO CKAN DATETIME string → (settlement_date, settlement_period 1-48).

    Each 30-min half-hourly period starts at the boundary:
      SP 1  = 00:00–00:30
      SP 2  = 00:30–01:00
      ...
      SP 48 = 23:30–24:00
    """
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        dt_utc = dt.astimezone(timezone.utc)
        # GB settlement is in UTC (not local time)
        settlement_date = dt_utc.date()
        hour = dt_utc.hour
        minute = dt_utc.minute
        # Settlement period: 30-min blocks from midnight, 1-based
        sp = hour * 2 + (1 if minute >= 30 else 0) + 1  # 1-indexed
        if sp < 1 or sp > 48:
            return None
        return settlement_date, sp
    except (ValueError, AttributeError):
        return None


def ingest_fuel_mix(settlement_date: date, conn) -> int:
    """Fetch NESO CKAN half-hourly generation mix for settlement_date.

    Upserts rows to intl_market.gb_fuel_mix.
    Returns the count of rows upserted.
    """
    _ensure_table(conn)

    date_from = settlement_date.isoformat()
    date_to = (settlement_date + timedelta(days=1)).isoformat()

    session = requests.Session()
    session.headers.update({"User-Agent": "BESS-Platform/1.0", "Accept": "application/json"})

    # NESO CKAN SQL-style filter: fetch records between date boundaries
    # The DATETIME column stores UTC timestamps.
    params = {
        "resource_id": NESO_GEN_RESOURCE,
        "limit": 60,  # 48 SPs + safety margin
        "filters": '{"' + "DATETIME" + '":""}',  # can't filter range in simple mode
        "sort": "DATETIME asc",
        "q": f'"{date_from}" AND "{date_to}"',
    }
    # Use SQL endpoint for proper date range filtering
    sql_url = f"{NESO_CKAN_BASE}/datastore_search_sql"
    sql_query = (
        f'SELECT * FROM "{NESO_GEN_RESOURCE}" '
        f"WHERE \"DATETIME\" >= '{date_from}T00:00:00' "
        f"AND \"DATETIME\" < '{date_to}T00:00:00' "
        f"ORDER BY \"DATETIME\" ASC"
    )
    try:
        resp = session.get(sql_url, params={"sql": sql_query}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        logger.warning("NESO CKAN SQL request failed: %s — falling back to filter search", exc)
        data = None

    # Fall back to filter-based search if SQL endpoint fails
    if not data or not data.get("success"):
        fallback_url = f"{NESO_CKAN_BASE}/datastore_search"
        fallback_params = {
            "resource_id": NESO_GEN_RESOURCE,
            "limit": 60,
            "sort": "DATETIME asc",
            "filters": "{}",
            "q": date_from,
        }
        try:
            resp = session.get(fallback_url, params=fallback_params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            logger.error("NESO CKAN fallback request failed: %s", exc)
            return 0

        time.sleep(_REQUEST_DELAY)

        if not data or not data.get("success"):
            logger.warning("NESO CKAN returned no data for %s", date_from)
            return 0

        # Filter to only records for this date
        all_records = data.get("result", {}).get("records", [])
        records = []
        for rec in all_records:
            dt_str = rec.get("DATETIME", "")
            if dt_str.startswith(date_from):
                records.append(rec)
    else:
        records = data.get("result", {}).get("records", [])

    if not records:
        logger.info("No NESO generation mix records for %s", date_from)
        return 0

    # Build rows to upsert
    rows_to_upsert: list[dict] = []
    for rec in records:
        dt_str = rec.get("DATETIME", "")
        parsed = _datetime_to_settlement_period(dt_str)
        if parsed is None:
            continue
        rec_date, sp = parsed
        if rec_date != settlement_date:
            continue

        row = {"settlement_date": rec_date, "settlement_period": sp}
        for ckan_col, db_col in _COLUMN_MAP.items():
            row[db_col] = _safe_float(rec.get(ckan_col))
        rows_to_upsert.append(row)

    if not rows_to_upsert:
        logger.info("No valid SP rows parsed for %s", date_from)
        return 0

    # Upsert
    db_cols = list(_COLUMN_MAP.values())
    upsert_sql = (
        "INSERT INTO intl_market.gb_fuel_mix "
        "(settlement_date, settlement_period, "
        + ", ".join(db_cols)
        + ") VALUES ("
        + ", ".join(["%s"] * (2 + len(db_cols)))
        + ") ON CONFLICT (settlement_date, settlement_period) DO UPDATE SET "
        + ", ".join(f"{c} = EXCLUDED.{c}" for c in db_cols)
    )

    cur = conn.cursor()
    count = 0
    for row in rows_to_upsert:
        values = [row["settlement_date"], row["settlement_period"]] + [
            row.get(c) for c in db_cols
        ]
        try:
            cur.execute(upsert_sql, values)
            count += 1
        except Exception as exc:
            logger.warning("Failed to upsert SP %d for %s: %s", row["settlement_period"], date_from, exc)
            conn.rollback()

    logger.info("Ingested %d fuel mix rows for %s", count, date_from)
    return count


def ingest_fuel_mix_range(start_date: date, end_date: date, conn) -> int:
    """Ingest fuel mix for a date range (inclusive). Returns total rows upserted."""
    total = 0
    d = start_date
    while d <= end_date:
        n = ingest_fuel_mix(d, conn)
        total += n
        d += timedelta(days=1)
        if d <= end_date:
            time.sleep(_REQUEST_DELAY)
    return total
