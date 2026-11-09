"""Per-layer data freshness for one AssetBook (P0 after the 2026-11-09 review).

Every book layer is fed by a separate pipeline. When one stops, the book keeps
rendering stale or empty layers without saying so — nodal_strategy_daily sat
dead from Oct 12 to Nov 8 and nobody saw it until the Book tab looked empty.
This module answers "is each layer current?" per station, so the tab can say
it up front.

Each layer: latest date present within a bounded lookback, compared to a
tolerance that reflects the layer's arrival cadence. Tolerances are
max_lag_days = today − latest; negative lag is fine (D+1 artifacts).

    forecast     reports.book_forecast_daily       written 16:00 CST for D+1
    strategy     marketdata.nodal_strategy_daily   written 16:00 CST for D+1
    cleared      marketdata.md_id_cleared_energy   exchange file, D-1; Fri/Sat gaps
    actual       marketdata.bess_actual_96         WeCom workbooks, arrive late
    attribution  reports.bess_asset_daily_attribution  17:45 CST for D-1
    settled      reports.book_settled_daily        monthly bills, ~6 weeks behind

Tolerances are first-cut values from those cadences, not yet calibrated
against live lag history.

Frames: every probe reads a DATE column, so no timezone rendering at all.
"today" is the CST calendar date.

Data-path tiers: 'full' books check every layer; 'settlement_only' books
check attribution + settled; 'registry_only' books check nothing.

A query failure (missing table, timeout) yields status 'unavailable' with the
error text, never an exception out of the book — the tab must still render.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pandas as pd
from sqlalchemy import text

from libs.trading_book.models import CST_TZ
from libs.trading_book.registry import StationRef


@dataclass(frozen=True)
class LayerSpec:
    name: str
    sql: str            # must return one column 'latest' (date or timestamp)
    key: str            # which station attribute feeds :k
    max_lag_days: int   # stale when today − latest > this
    lookback_days: int  # bounded scan window


@dataclass(frozen=True)
class LayerFreshness:
    layer: str
    latest: date | None
    lag_days: int | None
    max_lag_days: int
    status: str         # 'ok' | 'stale' | 'missing' | 'unavailable'
    note: str = ""


_LAYERS: tuple[LayerSpec, ...] = (
    LayerSpec("forecast", """
        SELECT MAX(forecast_date) AS latest FROM reports.book_forecast_daily
        WHERE asset_code = :k AND forecast_date >= CAST(:s AS date)""",
        "asset_code", 0, 30),
    LayerSpec("strategy", """
        SELECT MAX(target_date) AS latest FROM marketdata.nodal_strategy_daily
        WHERE plant_name = :k AND model_version LIKE 'nodal_agent%'
          AND target_date >= CAST(:s AS date)""",
        "plant_name", 0, 30),
    # 36M rows; every index leads with data_date or (plant_name, data_date).
    # A datetime filter + leading-wildcard LIKE scans the range and timed out
    # at 120 s in prod (v54). Exact name + data_date hits idx_md_id_plant_date
    # in ~5 ms; all 6 full-path plant names match exactly once (verified
    # 2026-11-09).
    LayerSpec("cleared", """
        SELECT MAX(data_date) AS latest FROM marketdata.md_id_cleared_energy
        WHERE plant_name = :k AND data_date >= CAST(:s AS date)""",
        "plant_name", 3, 30),
    LayerSpec("actual", """
        SELECT MAX(data_date) AS latest FROM marketdata.bess_actual_96
        WHERE asset_code = :k AND data_date >= CAST(:s AS date)""",
        "asset_code", 4, 30),
    LayerSpec("attribution", """
        SELECT MAX(trade_date) AS latest FROM reports.bess_asset_daily_attribution
        WHERE asset_code = :k AND trade_date >= CAST(:s AS date)""",
        "attribution_code", 2, 30),
    LayerSpec("settled", """
        SELECT MAX(data_date) AS latest FROM reports.book_settled_daily
        WHERE asset_code = :k AND data_date >= CAST(:s AS date)""",
        "asset_code", 75, 180),
)

_TIER_LAYERS = {
    "full": {spec.name for spec in _LAYERS},
    "settlement_only": {"attribution", "settled"},
    "registry_only": set(),
}


def cst_today() -> date:
    return datetime.now(CST_TZ).date()


def _key(station: StationRef, key: str) -> str:
    if key == "attribution_code":
        from libs.trading_book.waterfall import _attribution_code
        return _attribution_code(station.asset_code)
    return getattr(station, key)


def _as_date(value) -> date | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).date()


def classify(layer: str, latest: date | None, today: date,
             max_lag_days: int) -> LayerFreshness:
    """Pure status rule — no DB."""
    if latest is None:
        return LayerFreshness(layer, None, None, max_lag_days, "missing",
                              "no rows in lookback window")
    lag = (today - latest).days
    status = "stale" if lag > max_lag_days else "ok"
    return LayerFreshness(layer, latest, lag, max_lag_days, status)


def book_freshness(engine, station: StationRef,
                   today: date | None = None) -> list[LayerFreshness]:
    """Freshness of every layer this station's data-path tier should have."""
    today = today or cst_today()
    wanted = _TIER_LAYERS.get(station.data_path, set())
    out: list[LayerFreshness] = []
    for spec in _LAYERS:
        if spec.name not in wanted:
            continue
        start = (today - timedelta(days=spec.lookback_days)).isoformat()
        try:
            df = pd.read_sql(text(spec.sql), engine,
                             params={"k": _key(station, spec.key), "s": start})
            latest = _as_date(df["latest"].iloc[0]) if not df.empty else None
        except Exception as exc:  # missing table / timeout: report, don't raise
            out.append(LayerFreshness(spec.name, None, None, spec.max_lag_days,
                                      "unavailable", f"{type(exc).__name__}: {exc}"[:200]))
            continue
        out.append(classify(spec.name, latest, today, spec.max_lag_days))
    return out
