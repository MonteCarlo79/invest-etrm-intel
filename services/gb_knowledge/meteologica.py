"""
Meteologica forecast connector for GB renewable energy knowledge.

Behaviour:
  - If METEOLOGICA_API_KEY is set: calls the Meteologica API to retrieve
    7-day wind and solar power forecasts for Great Britain, converts each
    day's forecast to a text document and yields it.
  - If the key is absent: falls back to scraping Meteologica's public blog
    (https://www.meteologica.com/blog/) for market commentary articles.
  - Returns gracefully (zero docs) if neither the key nor public content
    is reachable.

Auth: METEOLOGICA_API_KEY environment variable.

Note: Meteologica is a commercial service.  The API endpoint, request
structure, and response schema below are based on Meteologica's documented
REST interface (JSON, bearer-token auth, /forecasts path).  Adjust
METEOLOGICA_API_BASE and the parsing logic if your subscription exposes a
different endpoint.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterator

import requests
from bs4 import BeautifulSoup

from services.gb_knowledge.base import BaseConnector

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

METEOLOGICA_API_BASE = "https://forecasts.meteologica.com/api"
GB_WIND_ASSET = "GB_WIND_TOTAL"   # adjust to your subscription's asset ID
GB_SOLAR_ASSET = "GB_SOLAR_TOTAL"

# Cornwall Insight — free listing page used as fallback when no Meteologica key.
# Articles are paywalled but titles + URLs are publicly visible on the listing page.
_CI_BLOG_URL = "https://cornwall-insight.com/blog/"
_CI_BASE = "https://www.cornwall-insight.com"

# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------


def _build_headers(api_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }


def _fetch_forecast(
    session: requests.Session,
    api_key: str,
    asset_id: str,
    forecast_type: str,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    """
    Call the Meteologica /forecasts endpoint for one asset.

    Expected JSON shape (Meteologica standard response):
        {
          "forecasts": [
            { "datetime": "2026-05-13T00:00:00Z", "value": 3200.5, "unit": "MW" },
            ...
          ]
        }
    Returns a list of forecast point dicts, or [] on error.
    """
    params = {
        "assetId": asset_id,
        "forecastType": forecast_type,
        "startDate": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "endDate": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "horizon": "7d",
        "granularity": "1h",
    }
    try:
        resp = session.get(
            f"{METEOLOGICA_API_BASE}/forecasts",
            params=params,
            headers=_build_headers(api_key),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("forecasts", data.get("data", []))
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        logger.warning(
            "Meteologica API HTTP %s for asset %s (%s)", status, asset_id, forecast_type
        )
        return []
    except (requests.RequestException, ValueError) as exc:
        logger.warning("Meteologica API request failed for %s: %s", asset_id, exc)
        return []


def _aggregate_daily(points: list[dict[str, Any]]) -> dict[str, float]:
    """
    Average hourly MW values by calendar day (UTC).

    Accepts either ISO-8601 strings or epoch timestamps in the 'datetime' / 'ts'
    field.  Returns { 'YYYY-MM-DD': avg_MW }.
    """
    daily_sum: dict[str, float] = {}
    daily_cnt: dict[str, int] = {}

    for pt in points:
        # Support 'datetime', 'timestamp', or 'ts' keys
        raw_ts = pt.get("datetime") or pt.get("timestamp") or pt.get("ts")
        value = pt.get("value") or pt.get("val") or pt.get("mw")
        if raw_ts is None or value is None:
            continue

        try:
            if isinstance(raw_ts, (int, float)):
                dt = datetime.fromtimestamp(raw_ts, tz=timezone.utc)
            else:
                dt = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
        except ValueError:
            continue

        day = dt.strftime("%Y-%m-%d")
        daily_sum[day] = daily_sum.get(day, 0.0) + float(value)
        daily_cnt[day] = daily_cnt.get(day, 0) + 1

    return {
        day: daily_sum[day] / daily_cnt[day]
        for day in daily_sum
        if daily_cnt[day] > 0
    }


def _build_forecast_doc(
    day_str: str,
    wind_mw: float | None,
    solar_mw: float | None,
) -> dict:
    """Compose a knowledge document for one forecast day."""
    parts: list[str] = []
    if wind_mw is not None:
        parts.append(f"GB wind generation {wind_mw / 1000:.2f} GW")
    if solar_mw is not None:
        parts.append(f"solar {solar_mw / 1000:.2f} GW")

    summary = ", ".join(parts) if parts else "data unavailable"
    content = (
        f"Meteologica forecast for {day_str}: {summary}. "
        f"Source: Meteologica commercial renewable energy forecast service."
    )

    try:
        pub_date = date.fromisoformat(day_str)
    except ValueError:
        pub_date = None

    return {
        "doc_type": "market_data",
        "title": f"Meteologica GB Renewable Forecast — {day_str}",
        "url": f"https://forecasts.meteologica.com/dashboard?date={day_str}&region=GB",
        "published_date": pub_date,
        "content": content,
    }


# ---------------------------------------------------------------------------
# Cornwall Insight listing scraper (fallback when no Meteologica API key)
# ---------------------------------------------------------------------------


def _scrape_cornwall_insight(session: requests.Session) -> Iterator[dict]:
    """Yield knowledge doc stubs from Cornwall Insight's insight-articles listing.

    Cornwall Insight publishes GB energy market analysis. The full articles are
    paywalled but titles and URLs are visible on the listing page.  We store
    title + URL so the agent can cite relevant research topics.
    """
    try:
        resp = session.get(_CI_BLOG_URL, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Cornwall Insight listing fetch failed: %s", exc)
        return

    soup = BeautifulSoup(resp.text, "html.parser")

    for h in soup.find_all(["h2", "h3", "h4"]):
        a = h.find("a", href=True)
        if a is None:
            continue
        href = a.get("href", "").strip()
        if "insight" not in href and "blog" not in href:
            continue
        if not href.startswith("http"):
            href = _CI_BASE + href

        title = h.get_text(strip=True)
        if len(title) < 15:
            continue

        # Try to find a date near the heading
        pub_date: date | None = None
        parent = h.parent
        if parent:
            time_el = parent.find("time")
            if time_el:
                raw = time_el.get("datetime", "") or time_el.get_text(strip=True)
                for fmt in ("%Y-%m-%d", "%B %d, %Y", "%d %B %Y"):
                    try:
                        pub_date = datetime.strptime(raw[:20].strip(), fmt).date()
                        break
                    except ValueError:
                        continue

        content = (
            f"Cornwall Insight GB energy market analysis: {title}. "
            f"Source: Cornwall Insight insight article. URL: {href}"
        )
        yield {
            "doc_type": "article",
            "title": title,
            "url": href,
            "published_date": pub_date,
            "content": content,
        }


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


class MeteologicaConnector(BaseConnector):
    """
    Fetch GB renewable energy forecast knowledge from Meteologica.

    With METEOLOGICA_API_KEY: fetches 7-day wind + solar forecasts via API.
    Without key: scrapes Meteologica's public blog for market commentary.
    """

    source = "meteologica"

    def __init__(self, lookback_days: int = 0, forecast_days: int = 7,
                 request_delay: float = 1.0):
        """
        Args:
            lookback_days: How many past days to include (0 = today onwards only).
            forecast_days: How many days ahead to fetch.
            request_delay: Polite delay between HTTP requests (seconds).
        """
        self._api_key: str | None = os.environ.get("METEOLOGICA_API_KEY")
        self._lookback = lookback_days
        self._forecast_days = forecast_days
        self._delay = request_delay
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "BESS-Platform/1.0"})

    # ------------------------------------------------------------------
    # API path
    # ------------------------------------------------------------------

    def _fetch_api_docs(self) -> Iterator[dict]:
        """Yield forecast docs via Meteologica API."""
        now = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        start = now - timedelta(days=self._lookback)
        end = now + timedelta(days=self._forecast_days)

        logger.info(
            "Fetching Meteologica wind + solar forecasts for GB (%s → %s)…",
            start.date(), end.date(),
        )

        wind_points = _fetch_forecast(
            self._session, self._api_key, GB_WIND_ASSET,
            "wind_power", start, end,
        )
        time.sleep(self._delay)

        solar_points = _fetch_forecast(
            self._session, self._api_key, GB_SOLAR_ASSET,
            "solar_power", start, end,
        )

        wind_by_day = _aggregate_daily(wind_points)
        solar_by_day = _aggregate_daily(solar_points)

        all_days = sorted(set(wind_by_day) | set(solar_by_day))
        if not all_days:
            logger.warning(
                "Meteologica API returned no usable forecast data. "
                "Check asset IDs (%s, %s) and subscription.",
                GB_WIND_ASSET, GB_SOLAR_ASSET,
            )
            return

        for day_str in all_days:
            yield _build_forecast_doc(
                day_str,
                wind_by_day.get(day_str),
                solar_by_day.get(day_str),
            )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fetch(self) -> Iterator[dict]:
        """Yield all documents: API forecasts (if key set) or Cornwall Insight fallback."""
        if self._api_key:
            yield from self._fetch_api_docs()
        else:
            logger.info(
                "METEOLOGICA_API_KEY not set — falling back to Cornwall Insight listing."
            )
            yield from _scrape_cornwall_insight(self._session)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    from pathlib import Path

    # Load .env from repo root
    try:
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).resolve().parents[2] / ".env")
    except ImportError:
        pass

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    from services.gb_knowledge.base import get_db_conn, ensure_table

    conn = get_db_conn()
    ensure_table(conn)

    connector = MeteologicaConnector(lookback_days=0, forecast_days=7)
    n = connector.run(conn)
    print(f"Meteologica: {n} new documents inserted.")
    sys.exit(0)
