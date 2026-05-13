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
METEOLOGICA_BLOG_URL = "https://www.meteologica.com/blog/"
GB_WIND_ASSET = "GB_WIND_TOTAL"   # adjust to your subscription's asset ID
GB_SOLAR_ASSET = "GB_SOLAR_TOTAL"

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
# Public blog scraper (fallback)
# ---------------------------------------------------------------------------


def _scrape_blog(session: requests.Session, request_delay: float) -> Iterator[dict]:
    """Yield knowledge docs from Meteologica's public blog articles."""
    for page in range(1, 3):
        url = METEOLOGICA_BLOG_URL if page == 1 else f"{METEOLOGICA_BLOG_URL}page/{page}/"
        try:
            resp = session.get(url, timeout=20)
            resp.raise_for_status()
            html = resp.text
        except requests.RequestException as exc:
            logger.warning("Failed to fetch Meteologica blog page %d: %s", page, exc)
            break

        time.sleep(request_delay)
        soup = BeautifulSoup(html, "html.parser")

        # Try standard WordPress blog card selectors
        articles = (
            soup.select("article.post")
            or soup.select("article")
            or soup.select("div.blog-post")
            or soup.select("div.entry")
        )

        if not articles:
            # Fallback: any <a> inside a list/card that looks like a blog post
            links = soup.find_all("a", href=lambda h: h and "/blog/" in h and h != METEOLOGICA_BLOG_URL)
            for link in links[:20]:
                title = link.get_text(strip=True)
                href = link.get("href", "")
                if len(title) < 15 or not href:
                    continue
                content = (
                    f"Meteologica market insight: {title}. "
                    f"Source: Meteologica blog. URL: {href}"
                )
                yield {
                    "doc_type": "report",
                    "title": title,
                    "url": href,
                    "published_date": None,
                    "content": content,
                }
            continue

        for article in articles:
            # Title
            title_el = (
                article.find("h1")
                or article.find("h2")
                or article.find("h3")
            )
            title = title_el.get_text(strip=True) if title_el else ""
            if not title:
                continue

            # URL
            link_el = article.find("a", href=True)
            href = link_el["href"] if link_el else ""

            # Date
            date_el = article.find("time") or article.find(
                class_=lambda c: c and "date" in str(c).lower()
            )
            pub_date: date | None = None
            if date_el:
                raw = (
                    date_el.get("datetime")
                    or date_el.get_text(strip=True)
                    or ""
                )
                for fmt in ("%Y-%m-%d", "%B %d, %Y", "%d %B %Y", "%d/%m/%Y"):
                    try:
                        pub_date = datetime.strptime(raw[:20].strip(), fmt).date()
                        break
                    except ValueError:
                        continue

            # Summary
            summary_el = article.find("p") or article.find(
                class_=lambda c: c and "excerpt" in str(c).lower()
            )
            summary = summary_el.get_text(strip=True) if summary_el else ""

            content = f"Meteologica blog: {title}."
            if summary:
                content += f" {summary}"
            if href:
                content += f" Source URL: {href}"

            yield {
                "doc_type": "report",
                "title": title,
                "url": href or None,
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
        """Yield all documents: API forecasts (if key set) or blog fallback."""
        if self._api_key:
            yield from self._fetch_api_docs()
        else:
            logger.info(
                "METEOLOGICA_API_KEY not set — falling back to blog scrape."
            )
            yield from _scrape_blog(self._session, self._delay)


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
