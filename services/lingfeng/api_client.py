"""
LingFeng Open Platform REST API client.

Replaces the old EnosIoT/poseidon-SDK approach with direct REST calls.

Two endpoints (both at https://lingfeng-saas.tradingthink.cn):

  1. /api/open/v1/metrics/data/query   — province-level DA/RT clearing prices
     Doc: 聆风开放平台数据接口文档_V1.1(1).pdf  (2026-07-21)

  2. /api/open/v1/ods/data/query       — nodal RT prices (96 time-slots)
     Doc: 实时节点电价接口使用说明文档_V1.1.pdf  (2026-07-03)

Auth: X-API-KEY-SECRET header (set LINGFENG_API_KEY in config/.env).

Available metrics (endpoint 1):
  quansheng_real_clearing_price     全省统一出清电价-实时
  quansheng_dayahead_clearing_price 全省统一出清电价-日前
"""
from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime
from typing import Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

_BASE_URL = "https://lingfeng-saas.tradingthink.cn"
_METRICS_PATH = "/api/open/v1/metrics/data/query"
_NODAL_PATH = "/api/open/v1/ods/data/query"

# Rate limit: 10 req/sec — we conservatively stay at 8/sec
_MIN_REQUEST_INTERVAL = 0.125  # seconds

# Available province-level metrics
METRICS = {
    "rt":  "quansheng_real_clearing_price",
    "da":  "quansheng_dayahead_clearing_price",
}


class LingFengAPIError(RuntimeError):
    """Raised when the API returns a non-200 business code."""


class LingFengAPIClient:
    """
    Thread-safe client for LingFeng Open Platform API.

    Parameters
    ----------
    api_key : str
        Value for the X-API-KEY-SECRET header.  If omitted, read from
        the LINGFENG_API_KEY environment variable.
    """

    def __init__(self, api_key: str | None = None) -> None:
        self._api_key = api_key or os.environ.get("LINGFENG_API_KEY", "")
        if not self._api_key:
            raise ValueError(
                "LingFeng API key not found. Set LINGFENG_API_KEY in config/.env "
                "or pass api_key= to LingFengAPIClient()."
            )
        self._session = self._build_session()
        self._last_request_time: float = 0.0

    # ------------------------------------------------------------------
    # Session / transport
    # ------------------------------------------------------------------

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["POST"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({
            "X-API-KEY-SECRET": self._api_key,
            "Content-Type": "application/json;charset=utf-8",
        })
        return session

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    def _post(self, path: str, payload: dict, timeout: int = 30) -> dict:
        self._throttle()
        url = _BASE_URL + path
        resp = self._session.post(url, json=payload, timeout=timeout)
        resp.raise_for_status()
        body = resp.json()
        code = body.get("code", 200)
        if code != 200:
            raise LingFengAPIError(
                f"API error {code}: {body.get('message', '')} | path={path} | payload={payload}"
            )
        return body

    # ------------------------------------------------------------------
    # Endpoint 1: province metrics (columnar response)
    # ------------------------------------------------------------------

    def _parse_columnar(self, body: dict, metric_name: str) -> list[dict]:
        """
        Convert columnar response to list of row dicts.

        Response structure:
            data.table.columns.<field>[i].value
        All field arrays are the same length; index i = row i.
        """
        columns = body.get("data", {}).get("table", {}).get("columns", {})
        if not columns:
            return []

        # Build list of field arrays
        fields: dict[str, list] = {}
        for field, arr in columns.items():
            fields[field] = [item.get("value") for item in arr]

        n = len(next(iter(fields.values()), []))
        rows = []
        for i in range(n):
            row = {field: values[i] for field, values in fields.items()}
            rows.append(row)
        return rows

    def fetch_province_clearing(
        self,
        market_name: str,
        metric: str,
        start_date: str | date,
        end_date: str | date,
        page_size: int = 50000,
    ) -> list[dict]:
        """
        Fetch province-level clearing price data.

        Parameters
        ----------
        market_name : str
            Chinese market name, e.g. "蒙西", "山东", "山西".
        metric : str
            "rt" (realtime) or "da" (day-ahead), or the full metric name.
        start_date, end_date : str | date
            Date range, inclusive.
        page_size : int
            Max records per page (API limit: 50000).

        Returns
        -------
        list[dict] with keys: market_name, metric_time, time_order_96, <metric_col>.
        """
        metric_name = METRICS.get(metric, metric)
        sd = start_date.isoformat() if isinstance(start_date, date) else start_date
        ed = end_date.isoformat() if isinstance(end_date, date) else end_date

        all_rows: list[dict] = []
        page_num = 1

        while True:
            payload = {
                "metricName": metric_name,
                "columns": ["market_name", "metric_time", "time_order_96"],
                "startDate": sd,
                "endDate": ed,
                "filters": [f'[\'market_name\'] = "{market_name}"'],
                "pageSize": page_size,
                "pageNum": page_num,
            }
            body = self._post(_METRICS_PATH, payload)
            rows = self._parse_columnar(body, metric_name)
            if not rows:
                break
            all_rows.extend(rows)
            logger.debug(
                "fetch_province_clearing %s %s %s→%s page %d: %d rows",
                market_name, metric_name, sd, ed, page_num, len(rows),
            )
            if len(rows) < page_size:
                break  # last page
            page_num += 1

        return all_rows

    def fetch_province_clearing_as_df(
        self,
        market_name: str,
        metric: str,
        start_date: str | date,
        end_date: str | date,
    ):
        """
        Fetch province clearing price and return as a pandas DataFrame with columns:
          time (TIMESTAMP, shifted to start-of-interval for North China grid)
          price (FLOAT)
        """
        import pandas as pd

        metric_name = METRICS.get(metric, metric)
        rows = self.fetch_province_clearing(market_name, metric, start_date, end_date)
        if not rows:
            return pd.DataFrame(columns=["time", "price"])

        df = pd.DataFrame(rows)
        df["metric_time"] = pd.to_datetime(df["metric_time"], errors="coerce")
        df["time_order_96"] = pd.to_numeric(df["time_order_96"], errors="coerce").astype("Int64")
        df["price"] = pd.to_numeric(df[metric_name], errors="coerce")

        # Convert time_order_96 (1-96) + date → timestamp (start-of-interval)
        # time_order_96=1 → 00:00, time_order_96=96 → 23:45
        df["time"] = df["metric_time"] + pd.to_timedelta((df["time_order_96"] - 1) * 15, unit="min")

        return df[["time", "price"]].dropna().sort_values("time").reset_index(drop=True)

    # ------------------------------------------------------------------
    # Endpoint 2: nodal RT prices (ODS)
    # ------------------------------------------------------------------

    def fetch_nodal_rt_prices(
        self,
        market_name: str,
        query_date: str | date,
    ) -> list[dict]:
        """
        Fetch RT nodal prices for all nodes on a single day.

        Parameters
        ----------
        market_name : str
            Chinese market name, e.g. "蒙西", "山东".
        query_date : str | date
            The date to query.

        Returns
        -------
        list[dict] — each dict:
            nodeName : str
            date     : str  (YYYY-MM-DD)
            price    : dict  {"1": float, "2": float, ..., "96": float}
        """
        qd = query_date.isoformat() if isinstance(query_date, date) else query_date
        payload = {"marketName": market_name, "date": qd}
        body = self._post(_NODAL_PATH, payload)
        return body.get("data", []) or []

    def fetch_nodal_rt_prices_as_df(
        self,
        market_name: str,
        query_date: str | date,
    ):
        """
        Fetch RT nodal prices and return as a pandas DataFrame with columns:
          data_date (DATE)
          datetime  (TIMESTAMP — start-of-interval, shifted -15min for North China grid)
          node_name (TEXT)
          node_price (FLOAT)
        """
        import pandas as pd

        qd = query_date if isinstance(query_date, str) else query_date.isoformat()
        file_date = pd.to_datetime(qd).date()
        records = self.fetch_nodal_rt_prices(market_name, qd)

        if not records:
            return pd.DataFrame(columns=["data_date", "datetime", "node_name", "node_price"])

        rows = []
        for rec in records:
            node = rec.get("nodeName", "")
            prices: dict = rec.get("price") or {}
            for slot_str, price_val in prices.items():
                slot = int(slot_str)
                # Slot 1 → 00:00, slot 96 → 23:45 (start-of-interval, North China -15min shift)
                ts = pd.Timestamp(qd) + pd.Timedelta(minutes=(slot - 1) * 15)
                rows.append({
                    "data_date": file_date,
                    "datetime": ts,
                    "node_name": node,
                    "node_price": float(price_val) if price_val is not None else None,
                })

        df = pd.DataFrame(rows)
        df["node_price"] = pd.to_numeric(df["node_price"], errors="coerce")
        return df.sort_values(["datetime", "node_name"]).reset_index(drop=True)
