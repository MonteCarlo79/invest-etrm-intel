"""Data model for marketdata.province_fundamentals_monthly (long table)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class FundRow:
    """One long-table row: a single metric value for a province-month.

    month:   month-start date for monthly rows; YYYY-01-01 for annual rows.
    period:  "month" | "year"
    section: capacity | generation | util_hours | demand
    metric:  cap_total / cap_wind / gen_total / hours_solar / demand_yoy / ...
             (different 口径 are different metrics, e.g. cap_total vs cap_total_full)
    value:   None allowed (reported-but-empty cell); unit/source always required.
    source:  filename or hermes-record-fact-YYYYMMDD — part of the DB unique key,
             so a restatement from a new source lands as a separate row.
    """
    province: str
    month: date
    period: str
    section: str
    metric: str
    value: Optional[float]
    unit: str
    source: str

    def __post_init__(self) -> None:
        if self.period not in ("month", "year"):
            raise ValueError(f"period must be 'month' or 'year', got {self.period!r}")
