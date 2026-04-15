"""
PDF parsing helpers for China spot daily reports.

We parse:
  - Day-ahead provincial prices (均价/最高价/最低价)
  - Real-time provincial prices (均价/最高价/最低价)
  - Narrative highlight text block (for LLM province filtering)
"""

from __future__ import annotations

import re
from typing import Dict, List

import pdfplumber
import datetime as dt


def _infer_page_date(text: str, year: int) -> dt.date | None:
    m = re.search(r"(\d{1,2})\s*月\s*(\d{1,2})\s*日", text)
    if not m:
        return None
    month = int(m.group(1))
    day = int(m.group(2))
    try:
        return dt.date(year, month, day)
    except Exception:
        return None


def _extract_reason_sentences(text: str, provinces_cn: List[str]) -> str:
    """
    Pull narrative '...均价/价格...原因为...' lines even if tables exist.

    We keep it short so the LLM has a clean input.
    """
    t = re.sub(r"\s+", " ", text)
    hits = []
    for p in provinces_cn:
        # province + optional 实时/日前 + some words + 均价/价格 ... 原因为 ... (； or 。)
        pattern = rf"{p}.{{0,12}}(?:实时|日前)?.{{0,10}}(?:均价|价格).*?原因为.{0,60}?(?:；|。)"
        for m in re.finditer(pattern, t):
            hits.append(m.group(0))

    return " ".join(hits).strip()


def _clean_num(s: str | None):
    if s is None:
        return None
    s = s.strip()
    if not s:
        return None
    s = (
        s.replace(",", "")
        .replace("%", "")
        .replace("％", "")
        .replace("—", "")
        .replace("－", "")
    )
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _pick_triplet_from_tail(tail: List[str]):
    """
    Many reports use: 均价/环比/最高价/环比/最低价/环比
    Prefer index 0/2/4 when available.
    Otherwise fall back to first three numeric values found.
    """
    if len(tail) >= 5:
        a = _clean_num(tail[0])
        b = _clean_num(tail[2])
        c = _clean_num(tail[4])
        if any(x is not None for x in (a, b, c)):
            return a, b, c

    nums = []
    for x in tail:
        v = _clean_num(x)
        if v is not None:
            nums.append(v)
        if len(nums) >= 3:
            break

    if len(nums) >= 3:
        return nums[0], nums[1], nums[2]

    return None, None, None


def _parse_table_rows(page, provinces_cn: List[str], mode: str) -> List[Dict]:
    out: List[Dict] = []

    tables = page.extract_tables() or []
    for tbl in tables:
        if not tbl:
            continue

        for raw_row in tbl:
            if not raw_row:
                continue
            row = [(c or "").strip() for c in raw_row]

            # Skip header-ish rows
            header_markers = {"省份", "均价", "最高价", "最低价", "出清均价", "环比"}
            if any(cell in header_markers for cell in row):
                continue

            # Find province cell
            prov_idx = None
            prov_cn = None
            for i, cell in enumerate(row):
                for p in provinces_cn:
                    if cell == p:
                        prov_idx = i
                        prov_cn = p
                        break
                if prov_idx is not None:
                    break

            if prov_idx is None or prov_cn is None:
                continue

            tail = row[prov_idx + 1 :]

            avg, mx, mn = _pick_triplet_from_tail(tail)

            if mode == "RT":
                out.append(
                    {
                        "province_cn": prov_cn,
                        "rt_avg": avg,
                        "rt_max": mx,
                        "rt_min": mn,
                    }
                )
            else:  # "DA"
                out.append(
                    {
                        "province_cn": prov_cn,
                        "da_avg": avg,
                        "da_max": mx,
                        "da_min": mn,
                    }
                )

    return out


def parse_daily_report_multi(pdf_path: str, cfg: Dict):
    """
    Parse a PDF that may contain multiple days.

    Returns:
        da_by_date: {date: [ {province_cn, da_*} ... ]}
        rt_by_date: {date: [ {province_cn, rt_*} ... ]}
        hi_by_date: {date: "raw highlight text"}
    """
    year = int(cfg.get("year", 2025))
    provinces_cn = list(cfg.get("provinces", {}).keys())

    da_by_date: Dict[dt.date, List[Dict]] = {}
    rt_by_date: Dict[dt.date, List[Dict]] = {}
    hi_by_date: Dict[dt.date, str] = {}

    mode: str | None = None  # "RT" or "DA"

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            page_date = _infer_page_date(text, year)

            # Update section mode when we see headers.
            if ("现货实时市场运行情况" in text) or ("实时市场出清均价" in text):
                mode = "RT"
            if ("现货日前市场运行情况" in text) or ("日前市场出清均价" in text):
                mode = "DA"

            # --- Highlights: extract reasons on any dated page ---
            if page_date:
                reasons = _extract_reason_sentences(text, provinces_cn)
                if reasons:
                    hi_by_date[page_date] = (
                        hi_by_date.get(page_date, "") + " " + reasons
                    ).strip()

            # --- Tables: parse according to current mode ---
            if mode and page_date:
                rows = _parse_table_rows(page, provinces_cn, mode)
                if rows:
                    if mode == "RT":
                        rt_by_date.setdefault(page_date, []).extend(rows)
                    else:
                        da_by_date.setdefault(page_date, []).extend(rows)

    return da_by_date, rt_by_date, hi_by_date
