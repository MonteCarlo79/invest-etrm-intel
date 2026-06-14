"""Elexon Insights API — GB operational data scraper.

Fetches from the public Elexon Insights API (data.elexon.co.uk, no auth required):

  1. Settlement system prices (SSP/SBP, NIV, accepted volumes, price derivation)
     → intl_market.gb_elexon_sp  (PK: settlement_date, settlement_period)

  2. Wind generation forecast (latest published per hour)
     → intl_market.gb_wind_forecast  (PK: start_time)

The ELEXON_SCRIPTING_KEY env var (from the registered elexonportal.co.uk
account, vv98ppbvcb6yuku) is retained here for future use with authenticated
portal file-download endpoints (P114, BMRA archive etc.).

Usage:
    python -m services.gb_knowledge.elexon_ops              # ingest yesterday
    python -m services.gb_knowledge.elexon_ops --date 2026-06-10
    python -m services.gb_knowledge.elexon_ops --from 2026-05-01 --to 2026-06-01
"""
from __future__ import annotations

import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Iterator

import requests

logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_API_BASE = "https://data.elexon.co.uk/bmrs/api/v1"

_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "BESS-Platform-GBOps/1.0 (internal; contact: ops@bess-platform.internal)",
}

_TIMEOUT = 30
_INTER_REQUEST_SLEEP = 0.5

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_conn():
    import psycopg2
    url = os.environ.get("PGURL") or os.environ.get("DATABASE_URL", "")
    if not url:
        raise RuntimeError("Neither PGURL nor DATABASE_URL is set")
    return psycopg2.connect(url, connect_timeout=10)


_CREATE_SP_TABLE = """
CREATE TABLE IF NOT EXISTS intl_market.gb_elexon_sp (
    settlement_date          DATE     NOT NULL,
    settlement_period        SMALLINT NOT NULL,
    start_time               TIMESTAMPTZ,
    system_sell_price        NUMERIC,
    system_buy_price         NUMERIC,
    net_imbalance_volume     NUMERIC,
    total_accepted_offer_vol NUMERIC,
    total_accepted_bid_vol   NUMERIC,
    price_derivation_code    TEXT,
    reserve_scarcity_price   NUMERIC,
    bsad_defaulted           BOOLEAN,
    sell_price_adj           NUMERIC,
    buy_price_adj            NUMERIC,
    fetched_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (settlement_date, settlement_period)
);
"""

_CREATE_WF_TABLE = """
CREATE TABLE IF NOT EXISTS intl_market.gb_wind_forecast (
    start_time    TIMESTAMPTZ NOT NULL PRIMARY KEY,
    publish_time  TIMESTAMPTZ,
    generation_mw INTEGER,
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def _ensure_tables(conn) -> None:
    cur = conn.cursor()
    cur.execute(_CREATE_SP_TABLE)
    cur.execute(_CREATE_WF_TABLE)
    conn.commit()


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def _api_get(session: requests.Session, url: str, params: dict | None = None) -> dict | list:
    resp = session.get(url, params=params, headers=_HEADERS, timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# 1. Settlement system prices
# ---------------------------------------------------------------------------

def fetch_system_prices(settlement_date: date, session: requests.Session) -> list[dict]:
    """Fetch 48 half-hourly settlement system prices for *settlement_date*.

    Returns list of row dicts ready for upsert into gb_elexon_sp.
    """
    url = f"{_API_BASE}/balancing/settlement/system-prices/{settlement_date.isoformat()}"
    try:
        data = _api_get(session, url)
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            logger.info("No system prices available yet for %s", settlement_date)
            return []
        raise

    items = data if isinstance(data, list) else data.get("data", [])
    rows = []
    for item in items:
        try:
            rows.append({
                "settlement_date":          item["settlementDate"],
                "settlement_period":        int(item["settlementPeriod"]),
                "start_time":               item.get("startTime"),
                "system_sell_price":        item.get("systemSellPrice"),
                "system_buy_price":         item.get("systemBuyPrice"),
                "net_imbalance_volume":     item.get("netImbalanceVolume"),
                "total_accepted_offer_vol": item.get("totalAcceptedOfferVolume"),
                "total_accepted_bid_vol":   item.get("totalAcceptedBidVolume"),
                "price_derivation_code":    item.get("priceDerivationCode"),
                "reserve_scarcity_price":   item.get("reserveScarcityPrice"),
                "bsad_defaulted":           item.get("bsadDefaulted"),
                "sell_price_adj":           item.get("sellPriceAdjustment"),
                "buy_price_adj":            item.get("buyPriceAdjustment"),
            })
        except Exception as exc:
            logger.warning("Skipping malformed system price row: %s — %r", exc, item)
    return rows


def upsert_system_prices(rows: list[dict], conn) -> int:
    if not rows:
        return 0
    cur = conn.cursor()
    upserted = 0
    for row in rows:
        cur.execute(
            """
            INSERT INTO intl_market.gb_elexon_sp (
                settlement_date, settlement_period, start_time,
                system_sell_price, system_buy_price, net_imbalance_volume,
                total_accepted_offer_vol, total_accepted_bid_vol,
                price_derivation_code, reserve_scarcity_price,
                bsad_defaulted, sell_price_adj, buy_price_adj
            ) VALUES (
                %(settlement_date)s, %(settlement_period)s, %(start_time)s,
                %(system_sell_price)s, %(system_buy_price)s, %(net_imbalance_volume)s,
                %(total_accepted_offer_vol)s, %(total_accepted_bid_vol)s,
                %(price_derivation_code)s, %(reserve_scarcity_price)s,
                %(bsad_defaulted)s, %(sell_price_adj)s, %(buy_price_adj)s
            )
            ON CONFLICT (settlement_date, settlement_period) DO UPDATE SET
                start_time               = EXCLUDED.start_time,
                system_sell_price        = EXCLUDED.system_sell_price,
                system_buy_price         = EXCLUDED.system_buy_price,
                net_imbalance_volume     = EXCLUDED.net_imbalance_volume,
                total_accepted_offer_vol = EXCLUDED.total_accepted_offer_vol,
                total_accepted_bid_vol   = EXCLUDED.total_accepted_bid_vol,
                price_derivation_code    = EXCLUDED.price_derivation_code,
                reserve_scarcity_price   = EXCLUDED.reserve_scarcity_price,
                bsad_defaulted           = EXCLUDED.bsad_defaulted,
                sell_price_adj           = EXCLUDED.sell_price_adj,
                buy_price_adj            = EXCLUDED.buy_price_adj,
                fetched_at               = NOW()
            """,
            row,
        )
        upserted += cur.rowcount
    conn.commit()
    return upserted


# ---------------------------------------------------------------------------
# 2. Wind generation forecast
# ---------------------------------------------------------------------------

def fetch_wind_forecast(
    publish_from: datetime,
    publish_to: datetime,
    session: requests.Session,
) -> list[dict]:
    """Fetch wind generation forecasts published in [publish_from, publish_to).

    Returns list of (publish_time, start_time, generation_mw) dicts, keeping
    only the latest-published forecast per start_time.
    """
    params = {
        "publishDateTimeFrom": publish_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "publishDateTimeTo":   publish_to.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "format": "json",
    }
    try:
        data = _api_get(session, f"{_API_BASE}/datasets/WINDFOR", params)
    except requests.HTTPError as exc:
        logger.warning("WINDFOR fetch failed: %s", exc)
        return []

    items = data if isinstance(data, list) else data.get("data", [])

    # Keep latest publishTime per startTime
    latest: dict[str, dict] = {}
    for item in items:
        start_time = item.get("startTime") or ""
        if not start_time:
            continue
        existing = latest.get(start_time)
        if existing is None or item.get("publishTime", "") > existing["publish_time"]:
            latest[start_time] = {
                "start_time":    start_time,
                "publish_time":  item.get("publishTime"),
                "generation_mw": int(item["generation"]) if item.get("generation") is not None else None,
            }
    return list(latest.values())


def upsert_wind_forecast(rows: list[dict], conn) -> int:
    if not rows:
        return 0
    cur = conn.cursor()
    upserted = 0
    for row in rows:
        cur.execute(
            """
            INSERT INTO intl_market.gb_wind_forecast (start_time, publish_time, generation_mw)
            VALUES (%(start_time)s, %(publish_time)s, %(generation_mw)s)
            ON CONFLICT (start_time) DO UPDATE SET
                publish_time  = EXCLUDED.publish_time,
                generation_mw = EXCLUDED.generation_mw,
                fetched_at    = NOW()
            WHERE intl_market.gb_wind_forecast.publish_time < EXCLUDED.publish_time
               OR intl_market.gb_wind_forecast.publish_time IS NULL
            """,
            row,
        )
        upserted += cur.rowcount
    conn.commit()
    return upserted


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_elexon_ops_ingest(settlement_date: date, conn=None) -> dict[str, int]:
    """Ingest Elexon ops data for *settlement_date*.

    Returns dict with keys 'system_prices' and 'wind_forecast' showing rows
    upserted.  Creates tables on first run.
    """
    _close_conn = conn is None
    if conn is None:
        conn = _get_conn()

    try:
        _ensure_tables(conn)
        session = requests.Session()
        results: dict[str, int] = {}

        # 1. Settlement system prices
        sp_rows = fetch_system_prices(settlement_date, session)
        results["system_prices"] = upsert_system_prices(sp_rows, conn)
        logger.info("gb_elexon_sp: %d rows upserted for %s", results["system_prices"], settlement_date)
        time.sleep(_INTER_REQUEST_SLEEP)

        # 2. Wind forecast — fetch forecasts published in the 24 h window ending at
        #    23:59 UTC on settlement_date (captures intra-day updates for that day
        #    and the day-ahead forecasts published the morning before).
        wf_from = datetime(
            settlement_date.year, settlement_date.month, settlement_date.day,
            0, 0, 0, tzinfo=timezone.utc,
        )
        wf_to = wf_from + timedelta(days=1)
        wf_rows = fetch_wind_forecast(wf_from, wf_to, session)
        results["wind_forecast"] = upsert_wind_forecast(wf_rows, conn)
        logger.info("gb_wind_forecast: %d rows upserted (forecasts published %s)", results["wind_forecast"], settlement_date)

        return results

    finally:
        if _close_conn:
            conn.close()


def run_elexon_ops_range(date_from: date, date_to: date, conn=None) -> dict[str, int]:
    """Ingest Elexon ops data for each date in [date_from, date_to] inclusive."""
    _close_conn = conn is None
    if conn is None:
        conn = _get_conn()

    _ensure_tables(conn)

    totals: dict[str, int] = {"system_prices": 0, "wind_forecast": 0}
    d = date_from
    while d <= date_to:
        try:
            result = run_elexon_ops_ingest(d, conn=conn)
            for k, v in result.items():
                totals[k] = totals.get(k, 0) + v
        except Exception as exc:
            logger.error("Elexon ops ingest failed for %s: %s", d, exc)
        d += timedelta(days=1)
        time.sleep(_INTER_REQUEST_SLEEP)

    if _close_conn:
        conn.close()

    return totals


# ---------------------------------------------------------------------------
# Missing-date helper (for Data Management UI)
# ---------------------------------------------------------------------------

def get_missing_sp_dates(date_from: date, date_to: date, conn) -> list[str]:
    """Return ISO date strings in [date_from, date_to] missing from gb_elexon_sp."""
    cur = conn.cursor()
    cur.execute(
        "SELECT settlement_date FROM intl_market.gb_elexon_sp "
        "WHERE settlement_date BETWEEN %s AND %s "
        "GROUP BY settlement_date HAVING COUNT(*) >= 48",
        (date_from, date_to),
    )
    have = {str(r[0]) for r in cur.fetchall()}
    missing = []
    d = date_from
    while d <= date_to:
        if d.isoformat() not in have:
            missing.append(d.isoformat())
        d += timedelta(days=1)
    return missing


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv

    load_dotenv(
        os.path.join(os.path.dirname(__file__), "..", "..", "config", ".env"),
        override=False,
    )
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Ingest GB Elexon operational data")
    parser.add_argument("--date", help="Single date (YYYY-MM-DD). Default: yesterday.")
    parser.add_argument("--from", dest="date_from", help="Range start (YYYY-MM-DD)")
    parser.add_argument("--to",   dest="date_to",   help="Range end   (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.date_from and args.date_to:
        d_from = date.fromisoformat(args.date_from)
        d_to   = date.fromisoformat(args.date_to)
        totals = run_elexon_ops_range(d_from, d_to)
        print(f"Done (range {d_from} → {d_to}): {totals}")
    else:
        target = date.fromisoformat(args.date) if args.date else date.today() - timedelta(days=1)
        result = run_elexon_ops_ingest(target)
        print(f"Done ({target}): {result}")
