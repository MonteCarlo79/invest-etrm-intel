# services/hermes/data_patrol.py
"""
Data Patrol Agent
=================
Checks all platform data sources for staleness/gaps and delivers
a tiered Feishu report. Follows the pattern of news_screener.py.

Entry point:
    run_patrol(pg_url, feishu, owner_open_id, api_key) -> PatrolReport
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Literal, Optional

logger = logging.getLogger(__name__)

_BJ = timezone(timedelta(hours=8))
_DAILY_STALE_DAYS = 2       # flag auto/manual daily data if > 2 days behind
_MONTHLY_FLAG_DAY = 10      # flag missing monthly data after 10th of following month


@dataclass
class SourceStatus:
    name: str
    table: str
    last_date: Optional[date]
    days_behind: int
    status: Literal["fresh", "stale", "missing"]
    group: Literal["auto", "manual", "monthly"]
    reminder_text: str = ""   # non-empty → send separate upload reminder
    fill_table: str = ""      # non-empty → show 填入数据 button in detail card
    fill_province: str = ""
    fill_month: str = ""      # YYYY-MM


@dataclass
class KBSummary:
    name: str
    table: str
    last_ingested: Optional[date]
    count_7d: int
    count_30d: int


@dataclass
class PatrolReport:
    sources: list[SourceStatus]
    kb_summaries: list[KBSummary]
    generated_at: datetime = field(default_factory=lambda: datetime.now(_BJ))

    def count_by_status(self, status: str) -> int:
        return sum(1 for s in self.sources if s.status == status)

    def by_group(self, group: str) -> list[SourceStatus]:
        return [s for s in self.sources if s.group == group]

    @property
    def has_alerts(self) -> bool:
        return any(s.status in ("stale", "missing") for s in self.sources)


def _days_behind(last_date: Optional[date]) -> int:
    if last_date is None:
        return 9999
    today = datetime.now(_BJ).date()
    return max(0, (today - last_date).days)


def _classify_daily(days: int) -> Literal["fresh", "stale", "missing"]:
    if days == 9999:
        return "missing"
    if days > _DAILY_STALE_DAYS:
        return "stale"
    return "fresh"
