# services/exchange_reports/fundamentals_bridge.py
"""Bridge: staging.exchange_monthly_metrics → marketdata.province_fundamentals_monthly.

The hermes exchange watcher (daily 06:20 UTC) LLM-extracts monthly disclosure
reports into staging.exchange_monthly_metrics. This bridge maps those rows
into the Market Fundamentals tab's long-table vocabulary, so newly ingested
monthly reports appear in S1 (capacity), S2 (generation) and S6 (interprov)
without a manual backfill.

Rules:
- capacity GW → 万千瓦 (×100); generation / interprov GWh → 亿千瓦时 (÷100)
- demand section is SKIPPED: total_volume_gwh is 总成交/用电量 caliber-ambiguous
  and must not contaminate the 全社会用电量 series
- files win on overlap: a (province, month, section, metric) cell that already
  has a confirmed row (any source) is left untouched — the bridge fills holes
  only, never re-states file-sourced data with exchange-extraction caliber
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

import psycopg2

from services.province_fundamentals.models import FundRow
from services.province_fundamentals.store import write_rows

logger = logging.getLogger(__name__)

SOURCE = "exchange_watcher"

# metrics_extractor column → (section, metric, multiplier)
_MAPPING = {
    "installed_capacity_gw": ("capacity", "cap_total", 100.0),
    "thermal_capacity_gw":   ("capacity", "cap_thermal", 100.0),
    "wind_capacity_gw":      ("capacity", "cap_wind", 100.0),
    "solar_capacity_gw":     ("capacity", "cap_solar", 100.0),
    "bess_capacity_gw":      ("capacity", "cap_storage", 100.0),
    "nuclear_capacity_gw":   ("capacity", "cap_nuclear", 100.0),
    "thermal_volume_gwh":    ("generation", "gen_thermal", 0.01),
    "wind_volume_gwh":       ("generation", "gen_wind", 0.01),
    "solar_volume_gwh":      ("generation", "gen_solar", 0.01),
    "hydro_volume_gwh":      ("generation", "gen_hydro", 0.01),
    "nuclear_volume_gwh":    ("generation", "gen_nuclear", 0.01),
    "incoming_volume_gwh":   ("interprov", "import_gwh", 0.01),
    "outgoing_volume_gwh":   ("interprov", "export_gwh", 0.01),
}

_UNITS = {"capacity": "万千瓦", "generation": "亿千瓦时", "interprov": "亿千瓦时"}


def build_rows(metrics: dict, province: str, month: date) -> list[FundRow]:
    """Map one exchange_monthly_metrics row (as a dict) to FundRows."""
    rows: list[FundRow] = []
    for col, (section, metric, factor) in _MAPPING.items():
        v = metrics.get(col)
        if v is None:
            continue
        rows.append(FundRow(
            province=province, month=month, period="month",
            section=section, metric=metric,
            value=round(float(v) * factor, 2),
            unit=_UNITS[section], source=SOURCE,
        ))
    return rows


def _filter_existing(rows: list[FundRow], cur) -> list[FundRow]:
    """Drop rows whose (province, month, section, metric) already has a
    confirmed row from any source — file-sourced data wins on overlap."""
    out = []
    for r in rows:
        cur.execute(
            """SELECT 1 FROM marketdata.province_fundamentals_monthly
               WHERE province=%s AND month=%s AND period='month'
                 AND section=%s AND metric=%s AND status='confirmed' LIMIT 1""",
            (r.province, r.month, r.section, r.metric),
        )
        if cur.fetchone() is None:
            out.append(r)
    return out


def bridge_report(province: str, report_month: date, pg_url: str) -> dict:
    """Bridge one province-month. Returns write_rows stats + cells considered."""
    conn = psycopg2.connect(pg_url, options="-c statement_timeout=30000")
    try:
        cur = conn.cursor()
        cols = ", ".join(_MAPPING)
        cur.execute(
            f"SELECT {cols} FROM staging.exchange_monthly_metrics"
            " WHERE province=%s AND report_month=%s",
            (province, report_month),
        )
        got = cur.fetchone()
        if got is None:
            return {"cells": 0, "written": 0, "skipped": 0, "reason": "no_metrics_row"}
        metrics = dict(zip(_MAPPING.keys(), got))
        rows = build_rows(metrics, province, report_month)
        rows = _filter_existing(rows, cur)
        stats = write_rows(rows, pg_url=pg_url)
        stats["cells"] = len(rows)
        return stats
    finally:
        conn.close()


def bridge_pending(pg_url: str, limit: int = 200) -> dict:
    """Bridge all metric rows whose (province, report_month) has no bridged cell
    yet. Idempotent catch-up for watcher runs that predate the bridge."""
    conn = psycopg2.connect(pg_url, options="-c statement_timeout=60000")
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT DISTINCT m.province, m.report_month
               FROM staging.exchange_monthly_metrics m
               WHERE NOT EXISTS (
                   SELECT 1 FROM marketdata.province_fundamentals_monthly f
                   WHERE f.province = m.province AND f.month = m.report_month
                     AND f.period = 'month' AND f.source = %s)
               ORDER BY m.report_month DESC LIMIT %s""",
            (SOURCE, limit),
        )
        pending = cur.fetchall()
    finally:
        conn.close()

    total = {"months": 0, "cells": 0, "written": 0}
    for province, report_month in pending:
        stats = bridge_report(province, report_month, pg_url)
        total["months"] += 1
        total["cells"] += stats.get("cells", 0)
        total["written"] += stats.get("written", 0)
        logger.info("bridge %s %s → %s", province, report_month, stats)
    return total
