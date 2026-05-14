"""
National Grid ESO connector for GB energy market knowledge.

Replaces the former ENTSO-E connector (public API access removed).
Fetches data from two free, unauthenticated sources:

  1. Carbon Intensity API (api.carbonintensity.org.uk)
       - Intensity data: /intensity/date/{date}  — 30-min actual vs forecast
       - Generation mix: /generation/{from}/{to} — 30-min fuel-type percentages
       Combined into a daily narrative for the last 7 days.

  2. National Grid ESO (NESO) Historic Generation Mix — CKAN datastore
       Base: https://api.neso.energy/api/3/action/datastore_search
       Resource: f93d1835-75bc-43e5-84ad-12472b180a98
       Half-hourly MW + % by fuel type; last 14 days grouped into daily docs.

  3. National Grid ESO publications scrape
       https://www.neso.energy/data-portal (Cloudflare-protected; falls back to
       a curated static list when the live page is blocked).

No API key required for any source.
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Iterator

import requests
from bs4 import BeautifulSoup

from services.gb_knowledge.base import BaseConnector

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CARBON_API_BASE = "https://api.carbonintensity.org.uk"
NESO_CKAN_BASE = "https://api.neso.energy/api/3/action"
NESO_GEN_RESOURCE = "f93d1835-75bc-43e5-84ad-12472b180a98"

NESO_PUBLICATIONS_URL = "https://www.neso.energy/data-portal"
NESO_FES_URL = "https://www.neso.energy/future-energy/future-energy-scenarios"

_USER_AGENT = (
    "Mozilla/5.0 (compatible; BESS-Platform/1.0; "
    "+https://github.com/neso-eso-knowledge-bot)"
)

# Fuel display order for Carbon Intensity API narratives
_CI_FUEL_ORDER = ["wind", "solar", "nuclear", "gas", "biomass", "hydro", "imports", "coal", "other"]

# Fuel columns available in the NESO CKAN generation mix dataset
_NESO_FUEL_COLS = ["GAS", "COAL", "NUCLEAR", "WIND", "WIND_EMB", "HYDRO",
                   "IMPORTS", "BIOMASS", "OTHER", "SOLAR", "STORAGE"]

# Static fallback publications when the live website is Cloudflare-blocked
_STATIC_PUBLICATIONS: list[dict] = [
    {
        "title": "Future Energy Scenarios 2024",
        "url": "https://www.neso.energy/future-energy/future-energy-scenarios",
        "content": (
            "National Grid ESO Future Energy Scenarios (FES) 2024: the annual long-range "
            "outlook for GB energy to 2050. Covers four scenarios — Leading the Way, "
            "Consumer Transformation, System Transformation and Falling Short — with "
            "projections for electricity demand, renewables capacity, storage, hydrogen, "
            "and carbon emissions. Used by policymakers, investors and market participants "
            "to understand plausible GB energy futures. "
            "Source: National Energy System Operator (NESO), formerly National Grid ESO. "
            "URL: https://www.neso.energy/future-energy/future-energy-scenarios"
        ),
        "published_date": date(2024, 7, 10),
    },
    {
        "title": "System Operability Framework 2023",
        "url": "https://www.neso.energy/document/296811/download",
        "content": (
            "National Grid ESO System Operability Framework (SOF) 2023: assessment of "
            "technical operability challenges for the GB transmission system as the energy "
            "mix shifts towards low-carbon generation. Covers inertia, voltage stability, "
            "frequency response, and system strength requirements as coal and gas retire "
            "and inverter-based resources grow. Essential reading for BESS developers and "
            "flexibility market participants. "
            "Source: National Energy System Operator (NESO). "
            "URL: https://www.neso.energy/document/296811/download"
        ),
        "published_date": date(2023, 11, 1),
    },
    {
        "title": "Electricity Market Reform — Capacity Market Reports",
        "url": "https://www.neso.energy/industry-information/balancing-services/capacity-market-cm",
        "content": (
            "National Grid ESO Capacity Market (CM) auction results and reports. "
            "The CM secures reliable electricity supply by contracting capacity providers "
            "— including BESS, gas peakers and demand-side response — through T-4 and T-1 "
            "auctions. Reports contain clearing prices, volume secured, technology mix and "
            "de-rated capacity by fuel type. "
            "Source: National Energy System Operator (NESO). "
            "URL: https://www.neso.energy/industry-information/balancing-services/capacity-market-cm"
        ),
        "published_date": date(2024, 2, 1),
    },
    {
        "title": "Electricity Flexibility Services — Enhanced Frequency Response and Dynamic Containment",
        "url": "https://www.neso.energy/industry-information/balancing-services/frequency-response",
        "content": (
            "National Grid ESO frequency response services including Dynamic Containment (DC), "
            "Dynamic Moderation (DM) and Dynamic Regulation (DR). These are the primary "
            "balancing services procured from BESS in GB. DC pays BESS to hold reserve "
            "for fast-acting frequency correction (±0.5 Hz); DM and DR cover slower "
            "secondary response. Reports include procurement volumes, availability prices "
            "and utilisation data. "
            "Source: National Energy System Operator (NESO). "
            "URL: https://www.neso.energy/industry-information/balancing-services/frequency-response"
        ),
        "published_date": date(2024, 1, 1),
    },
    {
        "title": "GB Electricity Ten Year Statement (ETYS)",
        "url": "https://www.neso.energy/future-energy/network-development-roadmaps",
        "content": (
            "National Grid ESO Electricity Ten Year Statement (ETYS): the forward-looking "
            "view of transmission network development requirements for GB. Identifies network "
            "constraints, planned reinforcements and connection queue data relevant to "
            "generation and storage developers. "
            "Source: National Energy System Operator (NESO). "
            "URL: https://www.neso.energy/future-energy/network-development-roadmaps"
        ),
        "published_date": date(2023, 12, 1),
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(val) -> float | None:
    """Convert a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _avg(values: list[float]) -> float | None:
    """Return the mean of a non-empty list, or None."""
    cleaned = [v for v in values if v is not None]
    if not cleaned:
        return None
    return sum(cleaned) / len(cleaned)


def _iso_to_date(s: str) -> date | None:
    """Parse ISO-8601 datetime string to a UTC date."""
    for suffix in ("+00:00", "Z"):
        s2 = s.replace("Z", "+00:00") if suffix == "Z" else s
        try:
            return datetime.fromisoformat(s2).astimezone(timezone.utc).date()
        except ValueError:
            pass
    # Try plain date
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Carbon Intensity API helpers
# ---------------------------------------------------------------------------

def _format_carbon_intensity_narrative(
    day: str,
    intensity_slots: list[dict],
    genmix_slots: list[dict],
) -> str:
    """Build a human-readable paragraph from Carbon Intensity API slots."""
    actuals = [
        _safe_float(s["intensity"].get("actual"))
        for s in intensity_slots
        if isinstance(s.get("intensity"), dict)
    ]
    forecasts = [
        _safe_float(s["intensity"].get("forecast"))
        for s in intensity_slots
        if isinstance(s.get("intensity"), dict)
    ]
    avg_actual = _avg([v for v in actuals if v is not None])
    avg_forecast = _avg([v for v in forecasts if v is not None])

    # Aggregate generation mix percentages across 30-min slots
    fuel_totals: dict[str, list[float]] = defaultdict(list)
    for slot in genmix_slots:
        for entry in slot.get("generationmix", []):
            fuel = entry.get("fuel", "")
            perc = _safe_float(entry.get("perc"))
            if fuel and perc is not None:
                fuel_totals[fuel].append(perc)

    avg_mix: dict[str, float] = {
        fuel: sum(vals) / len(vals)
        for fuel, vals in fuel_totals.items()
        if vals
    }

    # Build narrative
    lines = []
    actual_str = f"{avg_actual:.0f}" if avg_actual is not None else "n/a"
    forecast_str = f"{avg_forecast:.0f}" if avg_forecast is not None else "n/a"
    lines.append(
        f"On {day}, GB average carbon intensity was {actual_str} gCO2/kWh "
        f"(forecast: {forecast_str} gCO2/kWh)."
    )

    if avg_mix:
        mix_parts = []
        for fuel in _CI_FUEL_ORDER:
            if fuel in avg_mix:
                mix_parts.append(f"{fuel.title()} {avg_mix[fuel]:.1f}%")
        # Any fuels not in the display order
        for fuel, pct in sorted(avg_mix.items(), key=lambda x: -x[1]):
            if fuel not in _CI_FUEL_ORDER:
                mix_parts.append(f"{fuel.title()} {pct:.1f}%")
        lines.append("Generation mix (daily average): " + ", ".join(mix_parts) + ".")

    lines.append(
        "Source: Carbon Intensity API (api.carbonintensity.org.uk), "
        "provided by National Grid ESO, University of Oxford, WWF and others."
    )
    return " ".join(lines)


# ---------------------------------------------------------------------------
# NESO CKAN generation mix helpers
# ---------------------------------------------------------------------------

def _format_neso_generation_narrative(day: str, records: list[dict]) -> str:
    """Build a human-readable paragraph from NESO CKAN generation mix records."""
    if not records:
        return ""

    # Average each fuel column across all half-hourly records for this day
    col_vals: dict[str, list[float]] = defaultdict(list)
    for rec in records:
        for col in _NESO_FUEL_COLS:
            v = _safe_float(rec.get(col))
            if v is not None:
                col_vals[col].append(v)
        ci = _safe_float(rec.get("CARBON_INTENSITY"))
        if ci is not None:
            col_vals["CARBON_INTENSITY"].append(ci)

    def avg_col(col: str) -> float | None:
        return _avg(col_vals.get(col, []))

    total_gen = avg_col("GENERATION") or 0.0
    ci_avg = avg_col("CARBON_INTENSITY")

    lines = [
        f"On {day}, GB electricity generation data from National Grid ESO Historic Mix dataset:"
    ]

    if ci_avg is not None:
        lines.append(f"  Average carbon intensity: {ci_avg:.0f} gCO2/kWh.")

    if total_gen > 0:
        lines.append(f"  Average total generation: {total_gen / 1000:.1f} GW.")

        # Fuel breakdown (MW and %)
        fuel_display = [
            ("Wind (Metered)", "WIND"),
            ("Wind (Embedded)", "WIND_EMB"),
            ("Nuclear", "NUCLEAR"),
            ("Gas (CCGT/OCGT)", "GAS"),
            ("Solar", "SOLAR"),
            ("Biomass", "BIOMASS"),
            ("Imports", "IMPORTS"),
            ("Hydro", "HYDRO"),
            ("Storage (net)", "STORAGE"),
            ("Other", "OTHER"),
            ("Coal", "COAL"),
        ]
        mix_parts = []
        for label, col in fuel_display:
            mw = avg_col(col)
            if mw is not None and mw != 0:
                pct = mw / total_gen * 100
                mix_parts.append(f"{label}: {mw:.0f} MW ({pct:.1f}%)")
        if mix_parts:
            lines.append("  Generation breakdown (average MW, % of total):")
            for part in mix_parts:
                lines.append(f"    {part}")

    lines.append(
        "  Source: National Grid ESO Historic Generation Mix dataset "
        "(NESO Open Data Portal, resource f93d1835-75bc-43e5-84ad-12472b180a98)."
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------

class EntsoEConnector(BaseConnector):
    """
    Fetch GB energy market data from National Grid ESO sources.

    Class name kept as EntsoEConnector so ingest.py requires no changes.
    Source tag changed to 'national_grid_eso'.
    """

    source = "national_grid_eso"

    def __init__(
        self,
        carbon_lookback_days: int = 7,
        neso_lookback_days: int = 14,
        request_delay: float = 0.5,
    ):
        self._carbon_lookback = carbon_lookback_days
        self._neso_lookback = neso_lookback_days
        self._delay = request_delay
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": _USER_AGENT,
            "Accept": "application/json",
        })

    # ------------------------------------------------------------------
    # Internal HTTP helpers
    # ------------------------------------------------------------------

    def _get_json(self, url: str, params: dict | None = None) -> dict | list | None:
        """GET a URL and return parsed JSON, or None on any error."""
        try:
            resp = self._session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            logger.warning("HTTP %s fetching %s", status, url)
            return None
        except (requests.RequestException, ValueError) as exc:
            logger.warning("Request/JSON error for %s: %s", url, exc)
            return None

    def _get_html(self, url: str) -> str | None:
        """GET a public HTML page; return text or None on error."""
        try:
            resp = self._session.get(url, timeout=20)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            logger.debug("Failed to fetch HTML %s: %s", url, exc)
            return None

    # ------------------------------------------------------------------
    # Source 1: Carbon Intensity API
    # ------------------------------------------------------------------

    def _fetch_carbon_intensity(self) -> Iterator[dict]:
        """
        Yield one doc per day for the last `carbon_lookback_days` days.

        Combines intensity data (/intensity/date/{date}) and generation mix
        (/generation/{from}/{to}) into a daily narrative document.
        """
        today = datetime.now(timezone.utc).date()
        for delta in range(self._carbon_lookback, 0, -1):
            target_date = today - timedelta(days=delta)
            day_str = target_date.isoformat()

            # --- Intensity data ---
            intensity_url = f"{CARBON_API_BASE}/intensity/date/{day_str}"
            intensity_data = self._get_json(intensity_url)
            time.sleep(self._delay)

            intensity_slots: list[dict] = []
            if isinstance(intensity_data, dict):
                intensity_slots = intensity_data.get("data") or []

            # --- Generation mix data for the same UTC day ---
            from_ts = f"{day_str}T00:00Z"
            to_ts = f"{day_str}T23:30Z"
            genmix_url = f"{CARBON_API_BASE}/generation/{from_ts}/{to_ts}"
            genmix_data = self._get_json(genmix_url)
            time.sleep(self._delay)

            genmix_slots: list[dict] = []
            if isinstance(genmix_data, dict):
                genmix_slots = genmix_data.get("data") or []

            if not intensity_slots and not genmix_slots:
                logger.debug("No Carbon Intensity data for %s — skipping", day_str)
                continue

            content = _format_carbon_intensity_narrative(
                day_str, intensity_slots, genmix_slots
            )
            if not content:
                continue

            yield {
                "doc_type": "market_data",
                "title": f"GB Carbon Intensity & Generation Mix — {day_str}",
                "url": f"{CARBON_API_BASE}/intensity/date/{day_str}",
                "published_date": target_date,
                "content": content,
            }

    # ------------------------------------------------------------------
    # Source 2: NESO CKAN — Historic Generation Mix
    # ------------------------------------------------------------------

    def _fetch_neso_generation(self) -> Iterator[dict]:
        """
        Yield one doc per day for the last `neso_lookback_days` days using the
        NESO CKAN datastore (Historic Generation Mix dataset).

        Fetches up to 100 records sorted by DATETIME descending, then groups
        by calendar date.
        """
        # The CKAN datastore supports SQL-style filters; use limit + sort.
        # 14 days × 48 half-hourly records = 672 rows; fetch 750 to be safe.
        params = {
            "resource_id": NESO_GEN_RESOURCE,
            "limit": 750,
            "sort": "DATETIME desc",
        }
        url = f"{NESO_CKAN_BASE}/datastore_search"
        data = self._get_json(url, params=params)
        time.sleep(self._delay)

        if not isinstance(data, dict) or not data.get("success"):
            logger.warning("NESO CKAN request failed or returned success=false")
            return

        records: list[dict] = data.get("result", {}).get("records", [])
        if not records:
            logger.warning("NESO CKAN returned no records")
            return

        # Group records by UTC date
        by_day: dict[str, list[dict]] = defaultdict(list)
        cutoff = datetime.now(timezone.utc).date() - timedelta(days=self._neso_lookback)

        for rec in records:
            dt_str = rec.get("DATETIME", "")
            rec_date = _iso_to_date(dt_str) if dt_str else None
            if rec_date is None or rec_date < cutoff:
                continue
            by_day[rec_date.isoformat()].append(rec)

        for day_str in sorted(by_day):
            day_records = by_day[day_str]
            content = _format_neso_generation_narrative(day_str, day_records)
            if not content:
                continue
            try:
                pub_date = date.fromisoformat(day_str)
            except ValueError:
                pub_date = None

            yield {
                "doc_type": "market_data",
                "title": f"NESO Historic Generation Mix — {day_str}",
                "url": (
                    f"https://api.neso.energy/api/3/action/datastore_search"
                    f"?resource_id={NESO_GEN_RESOURCE}&filters={{\"DATETIME\":\"{day_str}\"}}"
                ),
                "published_date": pub_date,
                "content": content,
            }

    # ------------------------------------------------------------------
    # Source 3: NESO publications scrape (with static fallback)
    # ------------------------------------------------------------------

    def _fetch_neso_publications_live(self) -> list[dict]:
        """
        Attempt to scrape the NESO data portal for recent publication titles.
        Returns a (possibly empty) list of doc dicts.
        """
        docs: list[dict] = []
        for page_url in [NESO_PUBLICATIONS_URL, NESO_FES_URL]:
            html = self._get_html(page_url)
            time.sleep(self._delay)
            if not html:
                continue

            soup = BeautifulSoup(html, "html.parser")

            # Skip Cloudflare challenge pages
            if "just a moment" in (soup.title.string or "").lower():
                logger.debug("Cloudflare challenge page for %s — skipping", page_url)
                continue

            # Try common card / article selectors used by NESO / CKAN portals
            items = (
                soup.select("article")
                or soup.select("div.dataset-item")
                or soup.select("div.card")
                or soup.select("li.dataset-item")
                or soup.select("div.report-item")
            )

            for item in items[:20]:
                title_el = (
                    item.find("h2") or item.find("h3")
                    or item.find("h4") or item.find("a")
                )
                title = title_el.get_text(strip=True) if title_el else ""
                if len(title) < 8:
                    continue

                link_el = item.find("a", href=True)
                href = link_el["href"] if link_el else ""
                if href and not href.startswith("http"):
                    href = "https://www.neso.energy" + href

                date_el = item.find("time") or item.find(
                    class_=lambda c: c and "date" in c.lower()
                )
                pub_date: date | None = None
                if date_el:
                    raw = (date_el.get("datetime") or date_el.get_text(strip=True) or "")[:20]
                    for fmt in ("%Y-%m-%d", "%d %B %Y", "%B %d, %Y", "%d/%m/%Y"):
                        try:
                            pub_date = datetime.strptime(raw, fmt).date()
                            break
                        except ValueError:
                            continue

                summary_el = item.find("p")
                summary = summary_el.get_text(strip=True) if summary_el else ""

                content = f"National Grid ESO publication: {title}."
                if summary:
                    content += f" {summary}"
                if href:
                    content += f" Source URL: {href}"

                docs.append({
                    "doc_type": "report",
                    "title": title,
                    "url": href or None,
                    "published_date": pub_date,
                    "content": content,
                })

        return docs

    def _fetch_publications(self) -> Iterator[dict]:
        """
        Yield report docs: first try live scrape; fall back to static list
        if the live scrape returns nothing (e.g. Cloudflare blocked).
        """
        logger.info("Scraping NESO publications…")
        live = self._fetch_neso_publications_live()
        if live:
            logger.info("Scraped %d live publication items", len(live))
            yield from live
        else:
            logger.info(
                "Live scrape returned nothing — yielding %d static publications",
                len(_STATIC_PUBLICATIONS),
            )
            for pub in _STATIC_PUBLICATIONS:
                yield {
                    "doc_type": "report",
                    "title": pub["title"],
                    "url": pub["url"],
                    "published_date": pub["published_date"],
                    "content": pub["content"],
                }

    # ------------------------------------------------------------------
    # Public interface (BaseConnector)
    # ------------------------------------------------------------------

    def fetch(self) -> Iterator[dict]:
        """Yield all documents from all three sources."""
        logger.info(
            "Fetching Carbon Intensity API data (last %d days)…",
            self._carbon_lookback,
        )
        yield from self._fetch_carbon_intensity()

        logger.info(
            "Fetching NESO Historic Generation Mix (last %d days)…",
            self._neso_lookback,
        )
        yield from self._fetch_neso_generation()

        logger.info("Fetching NESO publications…")
        yield from self._fetch_publications()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os
    import sys

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", "config", ".env"))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    from services.gb_knowledge.base import get_db_conn, ensure_table

    conn = get_db_conn()
    ensure_table(conn)
    n = EntsoEConnector().run(conn)
    print(f"Inserted {n} new documents")
