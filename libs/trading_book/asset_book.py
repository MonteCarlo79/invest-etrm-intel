"""AssetBook data readers — 蒙西 cleared/actual/strategy series (spec 2026-10-31).

Source timestamp columns are all CST-naive wall clock
(md_id_cleared_energy.datetime, ops_bess_dispatch_15min.interval_start), so
day windows are naive CST literals ("YYYY-MM-DD 00:00:00") compared via
CAST(:s AS timestamp) — no '+08' suffix and no AT TIME ZONE rendering
anywhere (POSIX-inversion guard; the '+08:00' interval literal renders
UTC-8 walls).

nodal_strategy_daily has no scope column: the nodal-agent scope is the
model_version family ('nodal_agent_v1', …), latest created_at row per
(plant_name, target_date) — the same read pattern as
services/nodal_agents/writer.py::_REGISTER_STRATEGIES_SQL.

Sign convention everywhere: MW, positive = discharge, negative = charge
(matches ops_bess_dispatch_15min and the nodal writer's curve_json).
"""
from __future__ import annotations

import dataclasses
import json
from datetime import date, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import text

from libs.trading_book.models import (
    ActualDispatch,
    ClearedDispatch,
    Day,
    ForecastSet,
    Settlement,
    Strategy,
    TradingBook,
    Waterfall,
)
from libs.trading_book.registry import StationRef


def _cst_naive_window(day: date) -> tuple[str, str]:
    """Naive CST day bounds [day 00:00, day+1 00:00) — no tz suffix."""
    return f"{day} 00:00:00", f"{day + timedelta(days=1)} 00:00:00"


_CLEARED_SQL = """
    SELECT datetime, cleared_price, cleared_energy_mwh
    FROM marketdata.md_id_cleared_energy
    WHERE plant_name LIKE :pn
      AND datetime >= CAST(:s AS timestamp)
      AND datetime <  CAST(:e AS timestamp)
    ORDER BY datetime
"""

_ACTUAL_SQL = """
    SELECT interval_start, nominated_dispatch_mw, actual_dispatch_mw
    FROM marketdata.ops_bess_dispatch_15min
    WHERE asset_code = :ac
      AND interval_start >= CAST(:s AS timestamp)
      AND interval_start <  CAST(:e AS timestamp)
    ORDER BY interval_start
"""

_STRATEGY_SQL = """
    SELECT DISTINCT ON (target_date)
           target_date, curve_json, model_version
    FROM marketdata.nodal_strategy_daily
    WHERE plant_name = :pn
      AND target_date = :day
      AND model_version LIKE 'nodal_agent%'
    ORDER BY target_date, created_at DESC
"""


def _to_96(df: pd.DataFrame, ts_col: str, val_col: str) -> list[float]:
    """Align timestamped values to the day's 96 quarter-hour slots.

    Slot index = hour*4 + minute//15 (0-based). Slots with no source row are
    NaN; on duplicate timestamps the later row wins (source is ORDER BY ts).
    """
    v = np.full(96, np.nan)
    if df.empty:
        return v.tolist()
    ts = pd.to_datetime(df[ts_col])
    slots = ts.dt.hour * 4 + ts.dt.minute // 15
    for slot, val in zip(slots.to_numpy(), df[val_col].to_numpy()):
        if 0 <= int(slot) < 96 and pd.notna(val):
            v[int(slot)] = float(val)
    return v.tolist()


class AssetBook(TradingBook):
    """One 蒙西 BESS station's book: cleared/actual/strategy reads.

    settlement() delegates to the Task 4 bridge; attribution() delegates to
    the Task 5 waterfall assembler; forecast() delegates to the Task 7
    forecast artifact reader.
    """

    def __init__(self, engine, station: StationRef):
        self._engine = engine
        self.station = station

    def cleared(self, day: Day) -> ClearedDispatch:
        """ID-market cleared MW + price, 96 slots (cleared_energy_mwh holds
        per-interval MW — pass through, no unit conversion)."""
        s, e = _cst_naive_window(day)
        df = pd.read_sql(
            text(_CLEARED_SQL), self._engine,
            params={"pn": f"%{self.station.plant_name}%", "s": s, "e": e})
        return ClearedDispatch(
            day=day,
            cleared_mw=_to_96(df, "datetime", "cleared_energy_mwh"),
            cleared_price=_to_96(df, "datetime", "cleared_price"))

    def actual(self, day: Day) -> ActualDispatch:
        """Ops nominated + actual dispatch MW, 96 slots (discharge +)."""
        s, e = _cst_naive_window(day)
        df = pd.read_sql(
            text(_ACTUAL_SQL), self._engine,
            params={"ac": self.station.asset_code, "s": s, "e": e})
        return ActualDispatch(
            day=day,
            nominated_mw=_to_96(df, "interval_start", "nominated_dispatch_mw"),
            actual_mw=_to_96(df, "interval_start", "actual_dispatch_mw"))

    def strategies(self, day: Day) -> list[Strategy]:
        """Nodal-agent strategy curve for the day ([] when the writer has no
        row). curve_json is a 96-slot signed-MW JSON array, discharge +."""
        df = pd.read_sql(
            text(_STRATEGY_SQL), self._engine,
            params={"pn": self.station.plant_name, "day": day})
        out: list[Strategy] = []
        for _, row in df.iterrows():
            try:
                series = [float(x) for x in json.loads(row["curve_json"])]
            except (TypeError, ValueError):
                continue
            if len(series) != 96:
                continue
            out.append(Strategy(scope="nodal_agent", day=day,
                                series=series, primary=True))
        return out

    def settlement(self, period: str) -> Settlement:
        """Delegates to the Task 4 settlement bridge."""
        try:
            from libs.trading_book import settlement_bridge
        except ImportError as exc:
            raise NotImplementedError(
                "settlement bridge lands in Task 4") from exc
        return settlement_bridge.get_settlement(
            self._engine, self.station, period)

    def attribution(self, start: Day, end: Day) -> Waterfall:
        """Delegates to the Task 5 waterfall assembler (call-time import —
        waterfall.py imports AssetBook, so a module-top import would cycle).
        Always passes this book's own engine: a mismatched engine would
        silently mix DBs."""
        try:
            from libs.trading_book import waterfall
        except ImportError as exc:
            raise NotImplementedError(
                "waterfall assembler lands in Task 5") from exc
        return waterfall.build_waterfall(self._engine, self, start, end)

    def forecast(self, day: Day) -> ForecastSet:
        """D-1 forecast artifact (Task 7 reader) + Task 8 fleet-response
        stack, wired end-to-end. Call-time imports, same pattern as
        settlement()/attribution(). The stack is estimated as_of=day, so
        its lookback window ends at the forecast day — no lookahead."""
        try:
            from services.mengxi_nodal import book_forecast
        except ImportError as exc:
            raise NotImplementedError(
                "forecast artifact lands in Task 7") from exc
        fs = book_forecast.get_forecast(self._engine, self.station, day)
        return dataclasses.replace(fs, fleet_response=self._fleet_stack(day))

    def _fleet_stack(self, day: Day) -> dict | None:
        """Task 8 aggregate fleet-response stack; None when there is no
        usable fleet model.

        Never raises from the book: any import/runtime failure in
        estimate_stack degrades to None with a stdout warning (the book
        stays usable without a fleet model). The estimator's loud-empty
        (bands=[]) is also collapsed to None — "no fleet model" (Task 8's
        caller contract).
        """
        try:
            from services.mengxi_nodal import fleet_response
            stack = fleet_response.estimate_stack(self._engine, as_of=day)
        except Exception as exc:
            print(f"WARNING [trading_book.asset_book]: fleet_response "
                  f"estimate_stack failed for {self.station.asset_code} "
                  f"as_of={day}: {exc!r} — ForecastSet.fleet_response=None",
                  flush=True)
            return None
        if not stack.get("bands"):
            return None
        return stack
