"""Elexon REMIT (generation unavailability) ingestion.

Fetches REMIT messages from the public Elexon Insights API (same base as
elexon_ops.py — no auth) into intl_market.gb_remit_messages, and writes a
daily digest into the GB knowledge base for the Strategist agent.

NOTE: field names are mapped tolerantly (multiple candidate keys) because the
live response shape is unverified — the API is IP-blocked from the dev
machine.  First successful run logs one raw message; fix the mapper in one
follow-up commit if Elexon's real keys differ.

Usage:
    python -m services.gb_knowledge.elexon_remit [--days 2]
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests

logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

_API_BASE = "https://data.elexon.co.uk/bmrs/api/v1"
_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "BESS-Platform-GBOps/1.0 (internal; contact: ops@bess-platform.internal)",
}
_TIMEOUT = 30

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS intl_market.gb_remit_messages (
    message_id   TEXT PRIMARY KEY,
    published_at TIMESTAMPTZ,
    event_start  TIMESTAMPTZ,
    event_end    TIMESTAMPTZ,
    asset_name   TEXT,
    fuel_type    TEXT,
    affected_mw  NUMERIC,
    outage_type  TEXT,
    cause        TEXT,
    raw          JSONB
);
"""
_CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS gb_remit_event_window
ON intl_market.gb_remit_messages (event_start, event_end);
"""


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE)
        cur.execute(_CREATE_INDEX)
    conn.commit()


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def _api_get(session: requests.Session, url: str, params: dict | None = None):
    resp = session.get(url, params=params, headers=_HEADERS, timeout=_TIMEOUT)
    if not resp.ok:
        # Elexon 400s carry the reason in the body — log it for CloudWatch debugging
        logger.warning("[remit] API %s %s -> %s: %s", url, params, resp.status_code, resp.text[:300])
    resp.raise_for_status()
    return resp.json()


def fetch_messages(session: requests.Session, from_dt: datetime, to_dt: datetime) -> list[dict]:
    """Fetch REMIT messages published in [from_dt, to_dt] (UTC).

    Uses the documented GET /remit/list/by-publish endpoint with the API's
    publishDateTimeFrom/To convention (same as WINDFOR in elexon_ops.py).
    """
    params = {
        "publishDateTimeFrom": from_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "publishDateTimeTo":   to_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "format": "json",
    }
    data = _api_get(session, f"{_API_BASE}/remit/list/by-publish", params)
    if isinstance(data, list):
        items = data
    else:
        items = data.get("data") or data.get("messages") or data.get("results") or []
    items = [i for i in items if isinstance(i, dict)]
    if items:
        # One raw sample per run so CloudWatch lets us verify the field mapping
        logger.info("[remit] raw sample message keys: %s", sorted(items[0].keys()))
        logger.info("[remit] raw sample message: %s", json.dumps(items[0], default=str)[:800])
    return items


# ---------------------------------------------------------------------------
# Mapping
# ---------------------------------------------------------------------------

def _first(d: dict, *keys, default=None):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


_FUEL_MAP = {
    "ccgt": "CCGT", "ocgt": "OCGT", "nuclear": "Nuclear", "coal": "Coal",
    "wind": "Wind", "wind onshore": "Wind", "wind offshore": "Wind",
    "interconnector": "Interconnector", "storage": "Storage", "battery": "Storage",
    "hydro": "Hydro", "biomass": "Biomass", "solar": "Solar", "gas": "CCGT",
}


def map_message(item: dict) -> dict | None:
    """Map one raw REMIT message to a table row, or None if it has no id."""
    mid = _first(item, "messageId", "mrid", "mRID", "id")
    if not mid:
        return None
    fuel_raw = str(_first(item, "fuelType", "fuel", "technology", default="")).lower()
    fuel = _FUEL_MAP.get(fuel_raw, "Other" if not fuel_raw else fuel_raw.title())
    outage_raw = str(_first(item, "outageType", "type", "eventType", default="")).lower()
    if "unplanned" in outage_raw:
        outage_type = "unplanned"
    elif "planned" in outage_raw:
        outage_type = "planned"
    else:
        outage_type = "unknown"
    return {
        "message_id":   str(mid),
        "published_at": _first(item, "publishedDateTime", "published", "publishedDate", "publishTime"),
        "event_start":  _first(item, "eventStart", "startTime", "startDate", "outageStart"),
        "event_end":    _first(item, "eventEnd", "endTime", "endDate", "outageEnd"),
        "asset_name":   _first(item, "assetName", "asset", "unitName", "station", "bmUnit", default=""),
        "fuel_type":    fuel,
        "affected_mw":  _first(item, "affectedCapacity", "unavailableCapacity", "mw", "affectedMW"),
        "outage_type":  outage_type,
        "cause":        _first(item, "cause", "reason", "description", default=""),
        "raw":          item,
    }


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def upsert_messages(rows: list[dict], conn) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO intl_market.gb_remit_messages (
            message_id, published_at, event_start, event_end, asset_name,
            fuel_type, affected_mw, outage_type, cause, raw
        ) VALUES (
            %(message_id)s, %(published_at)s, %(event_start)s, %(event_end)s,
            %(asset_name)s, %(fuel_type)s, %(affected_mw)s, %(outage_type)s,
            %(cause)s, %(raw)s
        )
        ON CONFLICT (message_id) DO UPDATE SET
            published_at = EXCLUDED.published_at,
            event_start  = EXCLUDED.event_start,
            event_end    = EXCLUDED.event_end,
            asset_name   = EXCLUDED.asset_name,
            fuel_type    = EXCLUDED.fuel_type,
            affected_mw  = EXCLUDED.affected_mw,
            outage_type  = EXCLUDED.outage_type,
            cause        = EXCLUDED.cause,
            raw          = EXCLUDED.raw;
    """
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(sql, {**row, "raw": json.dumps(row["raw"], default=str)})
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Daily KB digest
# ---------------------------------------------------------------------------

def significant(row: dict) -> bool:
    """Digest inclusion rule: large (>300 MW) or unplanned outages."""
    try:
        big = row.get("affected_mw") is not None and float(row["affected_mw"]) > 300
    except (TypeError, ValueError):
        big = False
    return big or row.get("outage_type") == "unplanned"


def build_digest(rows: list[dict], today: date) -> str:
    """Markdown digest of significant REMIT messages for one day."""
    sig = [r for r in rows if significant(r)]
    sig.sort(key=lambda r: float(r.get("affected_mw") or 0), reverse=True)
    lines = [
        f"# GB REMIT digest — {today.isoformat()}",
        "",
        f"{len(rows)} messages in table snapshot; {len(sig)} significant "
        f"(>300 MW or unplanned).",
        "",
    ]
    if not sig:
        lines.append("No significant outages reported.")
    for r in sig:
        tag = "UNPLANNED" if r.get("outage_type") == "unplanned" else "planned"
        mw = r.get("affected_mw")
        mw_s = f"{float(mw):,.0f} MW" if mw is not None else "capacity n/a"
        window = f"{r.get('event_start') or '?'} → {r.get('event_end') or 'open-ended'}"
        lines.append(
            f"- **[{tag}] {r.get('asset_name') or 'Unknown asset'}** "
            f"({r.get('fuel_type') or 'fuel n/a'}) — {mw_s}, {window}. "
            f"{r.get('cause') or ''}".rstrip()
        )
    return "\n".join(lines)


def write_digest(conn, today: date) -> bool:
    """Build today's digest from the table and upsert it as one KB doc per day."""
    from services.gb_knowledge.base import upsert_doc
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT message_id, asset_name, fuel_type, affected_mw, outage_type,
                   event_start, event_end, cause, published_at
            FROM intl_market.gb_remit_messages
            WHERE event_start <= NOW() + interval '7 days'
              AND (event_end IS NULL OR event_end >= NOW() - interval '1 day')
            ORDER BY published_at DESC NULLS LAST
            LIMIT 200;
            """
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    md = build_digest(rows, today)
    return upsert_doc(
        conn,
        source="elexon_remit",
        doc_type="remit_digest",
        title=f"GB REMIT digest {today.isoformat()}",
        url=f"remit://{today.isoformat()}",
        published_date=today,
        content=md,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(conn, days_back: int = 2) -> int:
    """Fetch last `days_back` days of REMIT messages and upsert. Returns row count."""
    ensure_table(conn)
    now = datetime.now(timezone.utc)
    session = requests.Session()
    items = fetch_messages(session, now - timedelta(days=days_back), now)
    rows = [r for r in (map_message(i) for i in items) if r]
    n = upsert_messages(rows, conn)
    logger.info("[remit] %d messages fetched, %d upserted", len(items), n)
    try:
        written = write_digest(conn, date.today())
        logger.info("[remit] digest written: %s", written)
    except Exception as exc:
        logger.warning("[remit] digest failed (ingest unaffected): %s", exc)
    return n


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=2)
    args = p.parse_args()
    from services.gb_knowledge.base import get_db_conn
    conn = get_db_conn()
    try:
        print("upserted:", run(conn, days_back=args.days))
    finally:
        conn.close()
