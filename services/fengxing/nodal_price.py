"""
Fengxing Shanxi Nodal Price API client.

Downloads avg_node_price (15-min, 96 intervals/day) for all Shanxi nodes
from the LingFeng SaaS REST API and upserts into:
    marketdata.md_shanxi_nodal_price_96

API reference: data/nodal/山西节点电价接口使用说明文档_V1.0.pdf
  Endpoint  : POST https://lingfeng-saas.tradingthink.cn/api/base/metrics/data/query
  Auth      : X-API-KEY-SECRET  request header
  Metric    : avg_node_price
  Columns   : market_name, node_name, metric_time, time_order_96, avg_node_price
  Page size : max 50 000 rows   (1-based pageNum)
  Rate limit: 10 req/s

Strategy: fetch ONE calendar day per API request.  Shanxi has ~200 nodes
× 96 intervals = ~19 200 rows/day, well within the 50 000-row page limit.
Fetching day-by-day means:
  • each request is small and fast (< 5 s typical)
  • partial results are saved immediately — a mid-run timeout loses nothing
  • progress is reported per-day so failures are easy to identify

Usage:
    from services.fengxing.nodal_price import download_and_upsert
    results = download_and_upsert(
        start_date=date(2026, 5, 1),
        end_date=date(2026, 5, 10),
        api_key=os.environ["FENGXING_API_KEY"],
        engine=sqlalchemy_engine,
        day_cb=lambda day, status, n_rows, msg: print(day, status, msg),
    )
    # results = [{"date": ..., "status": "ok"|"error", "rows": N, "msg": ...}, ...]
"""
from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from typing import Callable

import requests

logger = logging.getLogger(__name__)

_ENDPOINT = "https://lingfeng-saas.tradingthink.cn/api/base/metrics/data/query"
_METRIC_NAME = "avg_node_price"
_COLUMNS = ["market_name", "node_name", "metric_time", "time_order_96"]
_PAGE_SIZE = 50_000

# Per-request timeouts: (connect_timeout_s, read_timeout_s)
# Short connect timeout catches unreachable hosts quickly.
# Read timeout is generous — a busy server on a cross-border link can be slow.
_TIMEOUT = (10, 90)

_MAX_RETRIES = 2          # 2 attempts per page (total 3 tries including the first)
_RETRY_DELAY = 3          # seconds between retries
_DAY_DELAY   = 0.15       # seconds between day-requests to stay under 10 req/s


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def _parse_columnar(body: dict) -> list[dict]:
    """Convert the columnar API response into a list of row dicts.

    Response shape:
        body.data.table.columns[field_name] = [{value: X}, {value: X}, ...]
    All arrays are the same length; row i = zip by index across all fields.
    """
    table = body.get("data", {}).get("table", {})
    cols: dict = table.get("columns", {})
    if not cols:
        return []
    field_names = list(cols.keys())
    n_rows = len(cols[field_names[0]])
    return [
        {field: cols[field][i]["value"] for field in field_names}
        for i in range(n_rows)
    ]


# ---------------------------------------------------------------------------
# Single page fetch (one attempt)
# ---------------------------------------------------------------------------

def _post_page(
    day: date,
    page_num: int,
    api_key: str,
) -> tuple[list[dict], bool]:
    """POST one page for a single calendar day.  Returns (rows, has_more).

    Raises requests.RequestException or RuntimeError on failure.
    """
    payload = {
        "metricName": _METRIC_NAME,
        "columns":    _COLUMNS,
        "startDate":  day.strftime("%Y-%m-%d"),
        "endDate":    day.strftime("%Y-%m-%d"),
        "pageSize":   _PAGE_SIZE,
        "pageNum":    page_num,
    }
    headers = {
        "Content-Type":    "application/json",
        "X-API-KEY-SECRET": api_key,      # never logged
    }

    resp = requests.post(_ENDPOINT, json=payload, headers=headers, timeout=_TIMEOUT)

    if resp.status_code == 429:
        raise RuntimeError("rate_limited")

    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")

    body = resp.json()
    code = body.get("code")
    if code not in (None, 0, 200):
        raise RuntimeError(f"API error {code}: {body.get('message', '')}")

    rows = _parse_columnar(body)
    has_more = len(rows) == _PAGE_SIZE
    return rows, has_more


# ---------------------------------------------------------------------------
# Fetch one day (with retry + pagination)
# ---------------------------------------------------------------------------

def _fetch_day(day: date, api_key: str) -> list[dict]:
    """Fetch all rows for a single calendar day, auto-paginating.

    Retries each page up to _MAX_RETRIES times on transient errors.
    Raises on persistent failure.
    """
    all_rows: list[dict] = []
    page_num = 1

    while True:
        last_exc: Exception | None = None
        for attempt in range(_MAX_RETRIES + 1):
            try:
                rows, has_more = _post_page(day, page_num, api_key)
                break
            except RuntimeError as exc:
                if "rate_limited" in str(exc):
                    time.sleep(2 ** (attempt + 1))
                    last_exc = exc
                    continue
                raise                          # non-transient API error
            except requests.RequestException as exc:
                last_exc = exc
                if attempt < _MAX_RETRIES:
                    time.sleep(_RETRY_DELAY * (attempt + 1))
        else:
            raise RuntimeError(f"page {page_num} failed after retries: {last_exc}")

        all_rows.extend(rows)
        if not has_more:
            break
        page_num += 1
        time.sleep(_DAY_DELAY)

    return all_rows


# ---------------------------------------------------------------------------
# Connectivity probe
# ---------------------------------------------------------------------------

def probe(api_key: str) -> str:
    """Quick connectivity check: fetch page 1 for yesterday with pageSize=1.

    Returns "ok" or an error string — does NOT raise.
    """
    yesterday = date.today() - timedelta(days=1)
    payload = {
        "metricName": _METRIC_NAME,
        "columns":    _COLUMNS,
        "startDate":  yesterday.strftime("%Y-%m-%d"),
        "endDate":    yesterday.strftime("%Y-%m-%d"),
        "pageSize":   1,
        "pageNum":    1,
    }
    headers = {
        "Content-Type":    "application/json",
        "X-API-KEY-SECRET": api_key,
    }
    try:
        resp = requests.post(_ENDPOINT, json=payload, headers=headers, timeout=(10, 20))
        if resp.status_code == 200:
            body = resp.json()
            code = body.get("code")
            if code in (None, 0, 200):
                return "ok"
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
            rows = _fetch_day(day, api_key)
            n = upsert(rows, engine)
            result: DayResult = {"date": day, "status": "ok", "rows": n, "msg": f"{n:,} rows"}
        except Exception as exc:
            result = {"date": day, "status": "error", "rows": 0, "msg": str(exc)}

        results.append(result)
        if day_cb:
            day_cb(day, result["status"], result["rows"], result["msg"])

        time.sleep(_DAY_DELAY)

    return results
