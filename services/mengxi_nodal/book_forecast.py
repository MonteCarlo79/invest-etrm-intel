"""D-1 nodal price forecast artifact — 蒙西 station book (plan task 7).

For each station, forecast the next day's 96-slot nodal price on the
station's price node from the last 60 days of
marketdata.md_mengxi_nodal_price_96_filled. The filled table's metric_time /
time_order_96 are CST-NAIVE (v5 frame): window with naive CST literals via
CAST(:s AS timestamp) — no '+08' suffix and no AT TIME ZONE rendering
anywhere.

Model family: same as bess_map's ols_rt_time_v1 — rolling OLS on
[1, sin(2π·slot/96), cos(2π·slot/96), lag7_slot_mean], adapted to 15-min
slots; falls back to the trailing 7-day per-slot mean when history is
short. Fundamentals terms (spot_fundamentals_hourly) are deferred per spec
phasing: net_load_fc_mw stays NULL in v1 and the ForecastSet surfaces it as
None; fleet_response lands in Task 8.

Artifact: reports.book_forecast_daily(asset_code, forecast_date, slot,
price_fc, net_load_fc_mw, created_at) — upserted on (asset_code,
forecast_date, slot), so re-runs are idempotent. Slots are 1-based, the
filled table's time_order_96 convention.
"""
from __future__ import annotations

import re
from datetime import date, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import text

from libs.trading_book.models import ForecastSet
from libs.trading_book.registry import StationRef, get_station

_LOOKBACK_DAYS = 60
_MIN_TRAIN_DAYS = 7
_LAG_DAYS = 7
_MIN_TRAIN_ROWS = _MIN_TRAIN_DAYS * 4  # mirrors ols_rt_time_v1's fit gate

_SCHEMA_DDL = "CREATE SCHEMA IF NOT EXISTS reports"

_DDL = """
CREATE TABLE IF NOT EXISTS reports.book_forecast_daily (
    asset_code     TEXT      NOT NULL,
    forecast_date  DATE      NOT NULL,
    slot           SMALLINT  NOT NULL,
    price_fc       NUMERIC,
    net_load_fc_mw NUMERIC,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (asset_code, forecast_date, slot)
)
"""

_NODE_PROBE_SQL = """
    SELECT node_name
    FROM marketdata.md_mengxi_nodal_price_96_filled
    WHERE node_name = ANY(:names)
      AND metric_time >= CAST(:s AS timestamp)
      AND metric_time <  CAST(:e AS timestamp)
    GROUP BY node_name
"""

_HISTORY_SQL = """
    SELECT metric_time::date AS d, time_order_96, avg_node_price
    FROM marketdata.md_mengxi_nodal_price_96_filled
    WHERE node_name = :node
      AND metric_time >= CAST(:s AS timestamp)
      AND metric_time <  CAST(:e AS timestamp)
    ORDER BY metric_time
"""

_UPSERT_SQL = """
    INSERT INTO reports.book_forecast_daily
        (asset_code, forecast_date, slot, price_fc, net_load_fc_mw)
    VALUES (:ac, :d, :slot, :px, NULL)
    ON CONFLICT (asset_code, forecast_date, slot)
    DO UPDATE SET price_fc = EXCLUDED.price_fc,
                  net_load_fc_mw = EXCLUDED.net_load_fc_mw,
                  created_at = NOW()
"""

_READ_SQL = """
    SELECT slot, price_fc, net_load_fc_mw
    FROM reports.book_forecast_daily
    WHERE asset_code = :ac AND forecast_date = :d
    ORDER BY slot
"""

# Registry price_nodes shorthand (kept as written in the source): range forms
# ('…/220kV.1M–4M', en-dash) and bare fragments ('2M' continuing the previous
# entry's prefix). Expanded here to literal fengxing node_names.
_RANGE_RE = re.compile(r"^(.*\.)(\d+)M[–-](\d+)M$")
_FRAG_RE = re.compile(r"^(\d+)M$")


def _candidate_nodes(station: StationRef) -> list[str]:
    """Registry price_nodes -> literal fengxing node_name candidates in
    priority order (own node first where the station has one)."""
    out: list[str] = []
    prefix = ""
    for entry in station.price_nodes:
        m = _RANGE_RE.match(entry)
        if m:
            prefix = m.group(1)
            out.extend(f"{prefix}{i}M"
                       for i in range(int(m.group(2)), int(m.group(3)) + 1))
            continue
        f = _FRAG_RE.match(entry)
        if f and prefix:
            out.append(f"{prefix}{f.group(1)}M")
            continue
        out.append(entry)
        if "." in entry:
            prefix = entry.rsplit(".", 1)[0] + "."
    seen: set[str] = set()
    return [n for n in out if not (n in seen or seen.add(n))]


def _history_window(day: date,
                    lookback_days: int = _LOOKBACK_DAYS) -> tuple[str, str]:
    """Naive CST bounds [day-lookback, day) — no tz suffix (v5 frame)."""
    return (f"{day - timedelta(days=lookback_days)} 00:00:00",
            f"{day} 00:00:00")


def _resolve_node(conn, candidates: list[str], s: str, e: str) -> str | None:
    """First candidate (registry priority order) with any filled-table rows
    in the window; None when none of them have data."""
    if not candidates:
        return None
    present = {r[0] for r in conn.execute(
        text(_NODE_PROBE_SQL), {"names": candidates, "s": s, "e": e})}
    return next((n for n in candidates if n in present), None)


def _daily_vectors(df: pd.DataFrame) -> list[np.ndarray]:
    """(d, time_order_96, avg_node_price) rows -> per-day 96-slot vectors,
    oldest first. Slots are 1-based; slots with no row stay NaN."""
    out: list[np.ndarray] = []
    if df.empty:
        return out
    for _, g in df.groupby("d"):
        v = np.full(96, np.nan)
        for slot, px in zip(g["time_order_96"], g["avg_node_price"]):
            if pd.notna(px) and 1 <= int(slot) <= 96:
                v[int(slot) - 1] = float(px)
        out.append(v)
    return out


def _slot_nanmean(M: np.ndarray) -> np.ndarray:
    """Per-slot (column) mean ignoring NaN; all-NaN slots -> NaN (no
    RuntimeWarning, unlike np.nanmean)."""
    cnt = np.isfinite(M).sum(axis=0)
    with np.errstate(invalid="ignore"):
        return np.nansum(M, axis=0) / np.where(cnt > 0, cnt, np.nan)


def _fill_nonfinite(v: np.ndarray, *fallbacks) -> np.ndarray:
    """Copy v with non-finite entries replaced from fallbacks in order
    (arrays aligned by slot, or scalars)."""
    out = np.asarray(v, dtype=float).copy()
    for fb in fallbacks:
        mask = ~np.isfinite(out)
        if not mask.any():
            break
        if np.isscalar(fb):
            out[mask] = fb
        else:
            out[mask] = np.asarray(fb, dtype=float)[mask]
    return out


def _ols_time_forecast(history: list[np.ndarray]) -> np.ndarray:
    """96-slot next-day price forecast from per-day 96-slot price history
    (oldest first; NaN-tolerant).

    Same family as bess_map ols_rt_time_v1: rolling OLS on
    [1, sin(2πs/96), cos(2πs/96), lag7_slot_mean] over all (day, slot)
    observations, predicted with the trailing-7-day per-slot mean. Short
    history (< _MIN_TRAIN_DAYS) or an unfittable OLS falls back to that same
    trailing mean. Always returns 96 values; non-finite only when the
    history carries no finite price at all.
    """
    days = [np.asarray(h, dtype=float).ravel() for h in history]
    days = [d for d in days if d.size == 96]
    if not days:
        return np.full(96, np.nan)
    M = np.vstack(days)
    n = M.shape[0]

    finite = M[np.isfinite(M)]
    global_mean = float(finite.mean()) if finite.size else 0.0
    slot_mean = _slot_nanmean(M)
    lag7_pred = _fill_nonfinite(_slot_nanmean(M[-_LAG_DAYS:]),
                                slot_mean, global_mean, 0.0)
    if n < _MIN_TRAIN_DAYS:
        return lag7_pred

    slots = np.arange(96, dtype=float)
    sin_f = np.sin(2.0 * np.pi * slots / 96.0)
    cos_f = np.cos(2.0 * np.pi * slots / 96.0)

    x_rows: list[np.ndarray] = []
    y_rows: list[np.ndarray] = []
    for i in range(1, n):
        prev = _slot_nanmean(M[max(0, i - _LAG_DAYS):i])
        y = M[i]
        ok = np.isfinite(y) & np.isfinite(prev)
        if ok.any():
            x_rows.append(np.column_stack(
                [np.ones(int(ok.sum())), sin_f[ok], cos_f[ok], prev[ok]]))
            y_rows.append(y[ok])
    if sum(len(r) for r in y_rows) < _MIN_TRAIN_ROWS:
        return lag7_pred

    beta, *_ = np.linalg.lstsq(np.vstack(x_rows), np.concatenate(y_rows),
                               rcond=None)
    x_pred = np.column_stack([np.ones(96), sin_f, cos_f, lag7_pred])
    return _fill_nonfinite(x_pred @ beta, lag7_pred, 0.0)


def run_day(engine, asset_code: str, day: date) -> int:
    """Compute and upsert the D-1 forecast for `day` on the station's price
    node; returns rows written (<= 96).

    0 with nothing written when the station has no price-node history in the
    lookback window — a loud zero, the nodal daily writer's contract.
    Unknown asset_code -> KeyError (registry).
    """
    station = get_station(asset_code)
    s, e = _history_window(day)
    candidates = _candidate_nodes(station)
    with engine.begin() as conn:
        conn.execute(text(_SCHEMA_DDL))
        conn.execute(text(_DDL))
        node = _resolve_node(conn, candidates, s, e)
        if node is None:
            return 0
        df = pd.read_sql(text(_HISTORY_SQL), conn,
                         params={"node": node, "s": s, "e": e})

    fc = _ols_time_forecast(_daily_vectors(df))
    rows = [{"ac": asset_code, "d": day, "slot": i + 1, "px": float(fc[i])}
            for i in range(96) if np.isfinite(fc[i])]
    if not rows:
        return 0
    with engine.begin() as conn:
        for r in rows:
            conn.execute(text(_UPSERT_SQL), r)
    return len(rows)


def get_forecast(engine, station: StationRef, day: date) -> ForecastSet:
    """Read the stored D-1 forecast for one station/day -> ForecastSet.

    Contract consumed by libs.trading_book.asset_book.AssetBook.forecast()
    (call-time import — same facade pattern as the Task 4 settlement
    bridge). Slots with no row stay NaN; net_load_fc_mw is None while the
    column is all-NULL (fundamentals deferred in v1); fleet_response is
    Task 8's.
    """
    df = pd.read_sql(text(_READ_SQL), engine,
                     params={"ac": station.asset_code, "d": day})
    price = np.full(96, np.nan)
    net = np.full(96, np.nan)
    for row in df.itertuples():
        i = int(row.slot) - 1
        if not 0 <= i < 96:
            continue
        if row.price_fc is not None and pd.notna(row.price_fc):
            price[i] = float(row.price_fc)
        if row.net_load_fc_mw is not None and pd.notna(row.net_load_fc_mw):
            net[i] = float(row.net_load_fc_mw)
    return ForecastSet(
        day=day,
        nodal_price_fc=price.tolist(),
        net_load_fc_mw=net.tolist() if np.isfinite(net).any() else None,
        fleet_response=None)
