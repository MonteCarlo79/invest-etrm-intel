"""
Fengxing Nodal Price API client (v1.1).

Downloads avg_node_price (15-min, 96 intervals/day) for all nodes
in a given market from the LingFeng SaaS REST API and upserts into:
    marketdata.md_shanxi_nodal_price_96

API reference: data/nodal/实时节点电价接口使用说明文档_V1.1.pdf
  Endpoint  : POST https://lingfeng-saas.tradingthink.cn/api/open/v1/ods/data/query
  Auth      : X-API-KEY-SECRET  request header
  Request   : {"marketName": "山东", "date": "YYYY-MM-DD"}
  Response  : {"code": 200, "data": [{"nodeName": "…", "date": "…",
                "price": {"1": 300.0, …, "96": 300.0}}]}
  Rate limit: 10 req/s

Strategy: fetch ONE calendar day per API request.  One call returns all nodes
for that day (no pagination needed).  Day-by-day approach means partial results
are saved immediately — a mid-run timeout loses nothing.

Usage:
    from services.fengxing.nodal_price import download_and_upsert
    results = download_and_upsert(
        start_date=date(2026, 5, 1),
        end_date=date(2026, 5, 10),
        api_key=os.environ["FENGXING_API_KEY"],
        market_name="蒙西",
        engine=sqlalchemy_engine,
        day_cb=lambda day, status, n_rows, msg: print(day, status, msg),
    )
    # results = [{"date": …, "status": "ok"|"error", "rows": N, "msg": …}, …]
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import Callable

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

_ENDPOINT = "https://lingfeng-saas.tradingthink.cn/api/open/v1/ods/data/query"

# Per-request timeouts: (connect_timeout_s, read_timeout_s)
_TIMEOUT = (10, 120)

_MAX_RETRIES = 2      # 2 extra attempts (3 total per day)
_RETRY_DELAY = 3      # seconds between retries
_DAY_DELAY   = 0.15   # seconds between day-requests to stay under 10 req/s

_CST = timezone(timedelta(hours=8))  # China Standard Time


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def _parse_response(body: dict, market_name: str, day: date) -> list[dict]:
    """Convert the v1.1 API response into a list of row dicts.

    Response shape:
        body.data = [{"nodeName": "…", "date": "YYYY-MM-DD",
                       "price": {"1": float, …, "96": float}}, …]

    Output schema matches marketdata.md_shanxi_nodal_price_96:
        node_name, metric_time (TIMESTAMPTZ), time_order_96, market_name, avg_node_price
    """
    data = body.get("data") or []
    rows: list[dict] = []
    midnight_cst = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=_CST)
    for item in data:
        node_name = item.get("nodeName") or ""
        price_map: dict = item.get("price") or {}
        for slot_str, price_val in price_map.items():
            try:
                slot = int(slot_str)
            except (ValueError, TypeError):
                continue
            if not (1 <= slot <= 96):
                continue
            # slot 1 = 00:00–00:15 CST; metric_time = start of interval
            metric_time = midnight_cst + timedelta(minutes=15 * (slot - 1))
            rows.append({
                "node_name":      node_name,
                "metric_time":    metric_time,
                "time_order_96":  slot,
                "market_name":    market_name,
                "avg_node_price": price_val,
            })
    return rows


# ---------------------------------------------------------------------------
# Single-day fetch (with retries)
# ---------------------------------------------------------------------------

def _fetch_day(day: date, api_key: str, market_name: str) -> list[dict]:
    """Fetch all nodes for a single calendar day.

    Retries up to _MAX_RETRIES times on transient errors.
    Raises on persistent failure.
    """
    payload = {
        "marketName": market_name,
        "date":       day.strftime("%Y-%m-%d"),
    }
    headers = {
        "Content-Type":     "application/json; charset=utf-8",
        "X-API-KEY-SECRET": api_key,      # never logged
    }

    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            resp = requests.post(
                _ENDPOINT, json=payload, headers=headers,
                timeout=_TIMEOUT, verify=False,
            )
            if resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                logger.warning("Rate limited on %s, waiting %ds", day, wait)
                time.sleep(wait)
                last_exc = RuntimeError("rate_limited")
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")

            body = resp.json()
            code = body.get("code")
            if code not in (None, 0, 200):
                raise RuntimeError(f"API error {code}: {body.get('message', '')}")

            rows = _parse_response(body, market_name, day)
            logger.debug("Fetched %s %s: %d nodes × 96 = %d rows",
                         market_name, day, len(body.get("data") or []), len(rows))
            return rows

        except RuntimeError as exc:
            if "rate_limited" in str(exc):
                continue
            raise
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_DELAY * (attempt + 1))

    raise RuntimeError(f"Day {day} failed after retries: {last_exc}")


# ---------------------------------------------------------------------------
# Connectivity probe
# ---------------------------------------------------------------------------

def probe(api_key: str, market_name: str = "山东") -> str:
    """Quick connectivity check: fetch yesterday's data.

    Returns "ok" or an error string — does NOT raise.
    """
    yesterday = date.today() - timedelta(days=1)
    payload = {"marketName": market_name, "date": yesterday.strftime("%Y-%m-%d")}
    headers = {
        "Content-Type":     "application/json; charset=utf-8",
        "X-API-KEY-SECRET": api_key,
    }
    try:
        resp = requests.post(_ENDPOINT, json=payload, headers=headers,
                             timeout=(10, 20), verify=False)
        if resp.status_code == 200:
            body = resp.json()
            code = body.get("code")
            if code in (None, 0, 200):
                n_nodes = len(body.get("data") or [])
                return f"ok ({n_nodes} nodes for {yesterday})"
            return f"API error {code}: {body.get('message', '')}"
        return f"HTTP {resp.status_code}"
    except requests.Timeout:
        return "timeout — API unreachable (check network / VPN)"
    except requests.ConnectionError as exc:
        return f"connection error: {exc}"
    except Exception as exc:
        return f"error: {exc}"


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

_DDL = """
CREATE SCHEMA IF NOT EXISTS marketdata;

CREATE TABLE IF NOT EXISTS marketdata.md_shanxi_nodal_price_96 (
    node_name       TEXT        NOT NULL,
    metric_time     TIMESTAMPTZ NOT NULL,
    time_order_96   SMALLINT    NOT NULL,
    market_name     TEXT,
    avg_node_price  NUMERIC(12, 4),
    inserted_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (node_name, metric_time, time_order_96)
);

CREATE INDEX IF NOT EXISTS ix_shanxi_nodal_metric_time
    ON marketdata.md_shanxi_nodal_price_96 (metric_time);
"""


def init_table(engine) -> None:
    from sqlalchemy import text as _text
    with engine.begin() as conn:
        for stmt in _DDL.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(_text(stmt))


_UPSERT_SQL = """
INSERT INTO marketdata.md_shanxi_nodal_price_96
    (node_name, metric_time, time_order_96, market_name, avg_node_price)
VALUES
    (:node_name, :metric_time, :time_order_96, :market_name, :avg_node_price)
ON CONFLICT (node_name, metric_time, time_order_96)
DO UPDATE SET
    market_name    = EXCLUDED.market_name,
    avg_node_price = EXCLUDED.avg_node_price,
    inserted_at    = NOW()
"""


def _coerce_row(row: dict) -> dict:
    return {
        "node_name":      str(row.get("node_name", "") or ""),
        "metric_time":    row.get("metric_time"),
        "time_order_96":  int(row.get("time_order_96", 0) or 0),
        "market_name":    str(row.get("market_name", "") or "") or None,
        "avg_node_price": row.get("avg_node_price"),
    }


def upsert(rows: list[dict], engine) -> int:
    if not rows:
        return 0
    from sqlalchemy import text as _text
    batch_size = 2000
    total = 0
    with engine.begin() as conn:
        for i in range(0, len(rows), batch_size):
            batch = [_coerce_row(r) for r in rows[i : i + batch_size]]
            conn.execute(_text(_UPSERT_SQL), batch)
            total += len(batch)
    return total


# ---------------------------------------------------------------------------
# Main entry point: day-by-day download + upsert
# ---------------------------------------------------------------------------

DayResult = dict  # {date, status: "ok"|"error", rows: int, msg: str}


def download_and_upsert(
    start_date: date,
    end_date: date,
    api_key: str,
    engine,
    market_name: str = "蒙西",
    day_cb: Callable[[date, str, int, str], None] | None = None,
) -> list[DayResult]:
    """Fetch day-by-day and upsert immediately.

    day_cb(day, status, n_rows, message) is called after each day completes.
    Returns list of per-day result dicts.
    """
    init_table(engine)

    days = []
    d = start_date
    while d <= end_date:
        days.append(d)
        d += timedelta(days=1)

    results: list[DayResult] = []

    for day in days:
        try:
            rows = _fetch_day(day, api_key, market_name)
            n = upsert(rows, engine)
            result: DayResult = {"date": day, "status": "ok", "rows": n, "msg": f"{n:,} rows"}
        except Exception as exc:
            result = {"date": day, "status": "error", "rows": 0, "msg": str(exc)}

        results.append(result)
        if day_cb:
            day_cb(day, result["status"], result["rows"], result["msg"])

        time.sleep(_DAY_DELAY)

    return results
