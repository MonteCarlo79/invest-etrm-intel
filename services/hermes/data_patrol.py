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
_MISSING_SENTINEL = 9999   # sentinel value for "no data at all"


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


def _days_behind(last_date: Optional[date], today: Optional[date] = None) -> int:
    if last_date is None:
        return _MISSING_SENTINEL
    if today is None:
        today = datetime.now(_BJ).date()
    return max(0, (today - last_date).days)


def _classify_daily(days: int) -> Literal["fresh", "stale", "missing"]:
    if days == _MISSING_SENTINEL:
        return "missing"
    if days > _DAILY_STALE_DAYS:
        return "stale"
    return "fresh"


# ── Group A: Auto pipeline freshness checks ───────────────────────────────────

_AUTO_SOURCES = [
    # (display_name, table, date_col, extra_where)
    ("LingFeng 基本面 (29省)", "marketdata.spot_fundamentals_hourly", "datetime::date", ""),
    ("LingFeng 现货价格 (29省)", "marketdata.spot_prices_hourly", "datetime::date", ""),
    ("Canon 日内出清", "marketdata.md_id_cleared_energy", "data_date", ""),
    ("Canon 日前出清", "marketdata.md_da_cleared_energy", "data_date", ""),
    ("Canon RT节点电价", "marketdata.md_rt_nodal_price", "data_date", ""),
    ("BESS捕获率日数据", "marketdata.bess_capture_daily", "trade_date", ""),
    ("GB Elexon结算", "intl_market.gb_elexon_sp", "settlement_date", ""),
    ("GB风电预测", "intl_market.gb_wind_forecast", "start_time::date", ""),
]

_MENGXI_HIST_TABLES = [
    "hist_mengxi_provincerealtimeclearprice_15min",
    "hist_mengxi_newenergyreal_15min",
    "hist_mengxi_windpowerreal_15min",
    "hist_mengxi_solarpowerreal_15min",
    "hist_mengxi_loadregulationreal_15min",
    "hist_mengxi_biddingspacereal_15min",
]

_FENGXING_TABLES = [
    ("marketdata.md_shanxi_nodal_price_96", "data_date"),
]


def check_auto_pipelines(pg_url: str) -> list[SourceStatus]:
    """Query max date for each auto-scheduled data source."""
    import psycopg2
    results: list[SourceStatus] = []

    def _query_max(cur, table: str, date_col: str, extra: str = "") -> Optional[date]:
        try:
            where = f"WHERE {extra}" if extra else ""
            cur.execute(f"SELECT MAX({date_col}) FROM {table} {where}")
            row = cur.fetchall()
            val = row[0][0] if row else None
            if val is None:
                return None
            return val.date() if hasattr(val, "date") else val
        except Exception as exc:
            logger.warning("patrol query failed for %s: %s", table, exc)
            return None

    try:
        conn = psycopg2.connect(pg_url)
        with conn:
            with conn.cursor() as cur:
                # Standard auto sources
                for name, table, date_col, extra in _AUTO_SOURCES:
                    last = _query_max(cur, table, date_col, extra)
                    days = _days_behind(last)
                    results.append(SourceStatus(
                        name=name, table=table, last_date=last,
                        days_behind=days, status=_classify_daily(days), group="auto",
                    ))

                # Mengxi hist tables (report as group — show worst)
                mengxi_dates = []
                for tbl in _MENGXI_HIST_TABLES:
                    d = _query_max(cur, f"public.{tbl}", "time::date")
                    mengxi_dates.append(d)
                mengxi_last = max((d for d in mengxi_dates if d), default=None)
                days = _days_behind(mengxi_last)
                results.append(SourceStatus(
                    name="蒙西 hist_* 实时数据", table="public.hist_mengxi_*",
                    last_date=mengxi_last, days_behind=days,
                    status=_classify_daily(days), group="auto",
                ))

                # Fengxing nodal tables
                for table, date_col in _FENGXING_TABLES:
                    last = _query_max(cur, table, date_col)
                    days = _days_behind(last)
                    results.append(SourceStatus(
                        name=f"丰行节点电价 ({table.split('.')[-1]})",
                        table=table, last_date=last, days_behind=days,
                        status=_classify_daily(days), group="auto",
                    ))
        conn.close()
    except Exception as exc:
        logger.error("check_auto_pipelines: DB unavailable: %s", exc)
        # Return all missing on total DB failure
        for name, table, _, _ in _AUTO_SOURCES:
            results.append(SourceStatus(
                name=name, table=table, last_date=None,
                days_behind=_MISSING_SENTINEL, status="missing", group="auto",
            ))
    return results
