"""Trader-agent tool payloads served from the book (2026-11-23).

The agent's get_dispatch_data read marketdata.ops_bess_dispatch_15min, which
died 2026-04-21 — every later day came back empty. These payloads go through
AssetBook instead, so the agent sees the same nominated / actual / cleared
series as the Book tab, for every full-path station.

All power is MW, positive = discharge, negative = charge, 96 CST slots.
cleared_mw is md_id_cleared_energy.cleared_energy_mwh, which stores interval
average MW despite its name (unit contract 2026-11-05) — no conversion.
"""
from __future__ import annotations

import math
from datetime import date

from libs.trading_book.asset_book import AssetBook
from libs.trading_book.registry import get_station, get_stations


def station_codes(data_path: str = "full") -> list[str]:
    """BESS asset codes the dispatch tools can serve."""
    return [s.asset_code for s in get_stations(asset_type="bess", data_path=data_path)]


def attribution_code(asset_code: str) -> str:
    """Registry code -> reports.bess_asset_daily_attribution code
    (bameng is stored as 'hetao'); unknown codes pass through."""
    try:
        s = get_station(asset_code)
    except KeyError:
        return asset_code
    return s.attribution_code or s.asset_code


def _clean(v):
    return None if v is None or (isinstance(v, float) and math.isnan(v)) else round(float(v), 3)


def dispatch_day_payload(engine, asset_code: str, day: date) -> dict:
    """One station-day of the dispatch chain for the agent.

    Returns {asset_code, plant_name, date, unit, rows: [{time, nominated_mw,
    actual_mw, cleared_mw, cleared_price_cny_mwh}], coverage: {layer: slots}}
    or {error: ...} for an unknown / non-full station. Slots with no data in
    any layer are dropped; a missing layer stays null rather than 0.
    """
    try:
        station = get_station(asset_code)
    except KeyError:
        return {"error": f"unknown asset_code {asset_code!r}",
                "valid_asset_codes": station_codes()}
    if station.data_path != "full":
        return {"error": f"{asset_code} is a {station.data_path} book — "
                         f"no dispatch chain", "valid_asset_codes": station_codes()}
    book = AssetBook(engine, station)
    actual = book.actual(day)
    cleared = book.cleared(day)
    cols = {"nominated_mw": actual.nominated_mw, "actual_mw": actual.actual_mw,
            "cleared_mw": cleared.cleared_mw,
            "cleared_price_cny_mwh": cleared.cleared_price}
    rows = []
    for i in range(96):
        vals = {k: _clean(v[i]) for k, v in cols.items()}
        if all(v is None for v in vals.values()):
            continue
        rows.append({"time": f"{i // 4:02d}:{(i % 4) * 15:02d}", **vals})
    coverage = {k: sum(1 for x in v if _clean(x) is not None) for k, v in cols.items()}
    return {"asset_code": asset_code, "plant_name": station.plant_name,
            "date": day.isoformat(),
            "unit": "MW, positive = discharge; price CNY/MWh; 15-min CST slots",
            "coverage_slots": coverage, "rows": rows}
