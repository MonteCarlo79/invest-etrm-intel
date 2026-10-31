"""Trading book object model — 蒙西 AssetBook v1 (spec 2026-10-31)."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date
from zoneinfo import ZoneInfo

# Hard invariant: named zone only. The interval literal '+08:00' is
# POSIX-inverted (renders UTC−8 walls) — never use it anywhere.
CST_TZ = ZoneInfo("Asia/Shanghai")

Day = date

@dataclass(frozen=True)
class ForecastSet:
    day: Day
    nodal_price_fc: list[float]        # 96 values, station price node
    net_load_fc_mw: list[float] | None # 96 values, provincial
    fleet_response: dict | None        # aggregate stack curves (Task 8)

@dataclass(frozen=True)
class Strategy:
    scope: str                         # 'nodal_agent' | 'dp_optimal' | …
    day: Day
    series: list[float]                # 96 values, signed MW (discharge +)
    primary: bool = False

@dataclass(frozen=True)
class ClearedDispatch:
    day: Day
    cleared_mw: list[float]            # 96
    cleared_price: list[float]         # 96

@dataclass(frozen=True)
class ActualDispatch:
    day: Day
    nominated_mw: list[float]          # 96
    actual_mw: list[float]             # 96

@dataclass(frozen=True)
class Settlement:
    period: str                        # 'YYYY-MM'
    settled_cny: float
    daily: list[tuple[Day, float, str]]  # (day, amount_cny, accrual_grade)

@dataclass(frozen=True)
class WaterfallLine:
    name: str                          # forecast_error|strategy_selection|clearing|
                                       # curtailment_so|equipment_site|settlement_residual
    delta_cny: float
    note: str = ""

@dataclass(frozen=True)
class Waterfall:
    strategy_pnl_cny: float
    lines: tuple[WaterfallLine, ...]
    settled_pnl_cny: float

class TradingBook:
    """Interface. AssetBook is the only family built in v1 (spec decision 1)."""
    def forecast(self, day: Day) -> ForecastSet: raise NotImplementedError
    def strategies(self, day: Day) -> list[Strategy]: raise NotImplementedError
    def cleared(self, day: Day) -> ClearedDispatch: raise NotImplementedError
    def actual(self, day: Day) -> ActualDispatch: raise NotImplementedError
    def settlement(self, period: str) -> Settlement: raise NotImplementedError
    def attribution(self, start: Day, end: Day) -> Waterfall: raise NotImplementedError
