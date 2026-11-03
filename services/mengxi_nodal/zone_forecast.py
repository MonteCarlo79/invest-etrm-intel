"""Forecast tomorrow's 96-slot dispatch zones (red/yellow/green) per station.

Model: recency-weighted day-type mode. Zones are operator-assigned daily but
highly structured: ~70-84% per-slot next-day persistence, stronger on the
same day-type (weekday vs weekend). For each slot, the forecast is the zone
that dominated the last N same-day-type days (recency-weighted), with the
vote share as confidence. Validated walk-forward before use.

Used by the D-1 forecast artifact: tomorrow's zone masks feed the strategy LP
(no_charge_slots in red, no_discharge_slots in yellow) with per-slot
confidence attached.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, timedelta

from sqlalchemy import text

MODEL = "recency_daytype_v1"
_LOOKBACK_SAME_TYPE = 10   # same-day-type days to vote over
_LOOKBACK_WINDOW_DAYS = 42  # calendar window to draw them from


def _day_type(d: date) -> str:
    return "weekday" if d.weekday() < 5 else "weekend"


def forecast_zones(engine, asset_code: str, day: date) -> tuple[list[str], list[float]]:
    """Predict the 96-slot zones for `day` from recent same-day-type history.

    Returns (zones, probs): 96 entries each, slots 1-96. Falls back to green
    with 0 confidence when no history exists.
    """
    start = day - timedelta(days=_LOOKBACK_WINDOW_DAYS)
    with engine.connect() as conn:
        rows = list(conn.execute(text(
            "select data_date, slot, zone from marketdata.bess_dispatch_zones "
            "where asset_code = :a and data_date >= :s and data_date < :d "
            "order by data_date"),
            {"a": asset_code, "s": start, "d": day}))
    if not rows:
        return (["green"] * 96, [0.0] * 96)
    by_day: dict[date, dict[int, str]] = defaultdict(dict)
    for d, slot, zone in rows:
        by_day[d][slot] = zone
    target_type = _day_type(day)
    same_type = sorted(d for d in by_day if _day_type(d) == target_type)
    votes: dict[int, Counter] = defaultdict(Counter)
    weights: dict[int, float] = defaultdict(float)
    chosen = same_type[-_LOOKBACK_SAME_TYPE:]
    for k, d in enumerate(reversed(chosen)):
        w = 0.85 ** k
        for slot in range(1, 97):
            z = by_day[d].get(slot, "green")
            votes[slot][z] += w
            weights[slot] += w
    zones, probs = [], []
    for slot in range(1, 97):
        if not votes[slot]:
            zones.append("green")
            probs.append(0.0)
            continue
        zone, share = votes[slot].most_common(1)[0]
        zones.append(zone)
        probs.append(round(share / weights[slot], 3))
    return zones, probs


def persist_forecast(engine, asset_code: str, day: date) -> int:
    """Write tomorrow's forecast into reports.bess_zone_forecast. Returns rows."""
    zones, probs = forecast_zones(engine, asset_code, day)
    with engine.begin() as c:
        c.execute(text("""
            CREATE TABLE IF NOT EXISTS reports.bess_zone_forecast (
                asset_code text NOT NULL,
                forecast_date date NOT NULL,
                slot smallint NOT NULL,
                zone text NOT NULL,
                prob numeric,
                model text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (asset_code, forecast_date, slot, model)
            )"""))
        n = 0
        for i, (z, p) in enumerate(zip(zones, probs), start=1):
            c.execute(text(
                "insert into reports.bess_zone_forecast "
                "(asset_code, forecast_date, slot, zone, prob, model) "
                "values (:a, :d, :s, :z, :p, :m) "
                "on conflict (asset_code, forecast_date, slot, model) "
                "do update set zone = excluded.zone, prob = excluded.prob"),
                {"a": asset_code, "d": day, "s": i, "z": z, "p": p, "m": MODEL})
            n += 1
    return n


def walk_forward_accuracy(engine, asset_code: str, days: int = 30) -> dict:
    """Walk-forward validation: predict each of the last `days` days from
    history before it, compare to actual."""
    with engine.connect() as conn:
        all_days = [r[0] for r in conn.execute(text(
            "select distinct data_date from marketdata.bess_dispatch_zones "
            "where asset_code = :a order by 1 desc limit :n"),
            {"a": asset_code, "n": days})]
    results = []
    for d in sorted(all_days):
        zones, probs = forecast_zones(engine, asset_code, d)
        with engine.connect() as conn:
            actual = {r[0]: r[1] for r in conn.execute(text(
                "select slot, zone from marketdata.bess_dispatch_zones "
                "where asset_code = :a and data_date = :d"),
                {"a": asset_code, "d": d})}
        if len(actual) < 96:
            continue
        same = sum(actual.get(i) == zones[i - 1] for i in range(1, 97))
        results.append((d, same / 96, sum(p for p in probs if p > 0) / 96))
    if not results:
        return {"days": 0}
    acc = [r[1] for r in results]
    conf = [r[2] for r in results]
    return {"days": len(results),
            "median_accuracy": sorted(acc)[len(acc) // 2],
            "mean_accuracy": sum(acc) / len(acc),
            "mean_confidence": sum(conf) / len(conf),
            "worst": min(results, key=lambda r: r[1]),
            "best": max(results, key=lambda r: r[1])}
