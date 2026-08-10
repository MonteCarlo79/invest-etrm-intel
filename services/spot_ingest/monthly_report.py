"""Recognise and ingest 电力现货市场价格与运行月报 (national spot monthly report) PDFs.

Separate from the daily pipeline (spot_ingest_bridge / is_spot_pdf), which only
handles 日报. Monthly files previously fell through to is_exchange_report and were
misrouted into staging.exchange_monthly_reports — this module takes precedence.
"""
from __future__ import annotations

import datetime as dt
import logging
import re
from typing import Optional

from services.spot_ingest.provinces import PROVINCES_MAP

logger = logging.getLogger(__name__)

# Filename patterns that identify the national spot monthly report (case-insensitive)
SPOT_MONTHLY_PATTERNS = ["电力现货市场价格与运行月报"]


def is_spot_monthly_pdf(filename: str) -> bool:
    name_lower = filename.lower()
    return name_lower.endswith(".pdf") and any(
        p.lower() in name_lower for p in SPOT_MONTHLY_PATTERNS
    )


def infer_report_month(filename: str) -> Optional[dt.date]:
    """Infer report month (first of month) from filename, e.g. （2026年6月）.

    Returns None if no explicit year+month — never stamps the current year
    (same rule as settlement ingest, commit 1064925).
    """
    m = re.search(r"(\d{4})年(\d{1,2})月", filename)
    if not m:
        return None
    try:
        return dt.date(int(m.group(1)), int(m.group(2)), 1)
    except ValueError:
        return None
