"""
Parser for 省间现货交易情况 (inter-provincial spot market trading) tables
in China electricity market daily PDF reports.

Table title pattern: 二、X月XX日省间现货交易情况

Structure (9 data rows, two direction groups):
  Columns:  发/受端 | 条目 | 省份/地区 | 价格(元/千瓦时) | 环比 | 主要时段 | 总电量(亿千瓦时)

  Direction groups (送端省 / 受端省), each with 4 metric rows:
      最高均价  highest average price province
      最低均价  lowest average price province
      最高电量  highest volume province
      最高价    highest spot price province

  total_vol_100gwh (亿千瓦时) appears in the last column of the first row of
  each direction group only (i.e. the '最高均价' row).

Returns a flat list of dicts ready for DB upsert into staging.spot_interprov_flow.
"""
from __future__ import annotations

import datetime as dt
import re
from pathlib import Path
from typing import List, Optional

import pdfplumber


_TABLE_TITLE_RE = re.compile(r"省间现货交易")

_DIRECTION_MAP = {
    "送端": "送端",
    "受端": "受端",
}

_METRIC_TYPES = {"最高均价", "最低均价", "最高电量", "最高价"}


# ── helpers ───────────────────────────────────────────────────────────────────

def _clean_num(s: str | None) -> Optional[float]:
    if not s:
        return None
    s = (
        str(s).strip()
        .replace(",", "").replace("，", "")
        .replace("%", "").replace("％", "")
        .replace("亿", "")
        .replace("—", "").replace("－", "").replace("\u2014", "")
    )
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _province_and_share(cell: str) -> tuple[Optional[str], Optional[float]]:
    """Parse '浙江(35%)' → ('浙江', 35.0),  '广东' → ('广东', None)."""
    if not cell:
        return None, None
    m = re.search(r"[\(（](\d+(?:\.\d+)?)[%％][\)）]", cell)
    share = float(m.group(1)) if m else None
    province = re.sub(r"[\(（]\d+(?:\.\d+)?[%％][\)）]", "", cell).strip()
    return (province or None), share


def _infer_date(text: str, year: int) -> Optional[dt.date]:
    m = re.search(r"(\d{1,2})\s*月\s*(\d{1,2})\s*日", text)
    if not m:
        return None
    try:
        return dt.date(year, int(m.group(1)), int(m.group(2)))
    except ValueError:
        return None


# ── table parsing ─────────────────────────────────────────────────────────────

def _parse_interprov_table(
    rows: List[List[str]],
    report_date: dt.date,
    source_pdf: str,
) -> List[dict]:
    """
    Convert one extracted table into a list of interprov flow dicts.

    The table has merged cells in the first column (送端省 / 受端省) that
    pdfplumber renders as None for continuation rows.  We carry the last
    seen direction forward and reset total_vol when direction changes.
    """
    out: List[dict] = []
    current_direction: Optional[str] = None
    direction_total: Optional[float] = None

    for row in rows:
        if len(row) < 4:
            continue

        # Normalise cells
        cells = [(c or "").strip() for c in row]

        # ── Detect direction change ───────────────────────────────────────────
        col0 = cells[0]
        for kw, mapped in _DIRECTION_MAP.items():
            if kw in col0:
                if mapped != current_direction:
                    current_direction = mapped
                    direction_total = None
                    # The total volume is usually in the last numeric cell of
                    # the direction header row (same row as 送端省 / 受端省 label)
                    for cell in reversed(cells):
                        v = _clean_num(cell)
                        if v is not None and v > 0.01:
                            direction_total = v
                            break
                break

        if current_direction is None:
            continue

        # ── Detect metric type ────────────────────────────────────────────────
        metric_type = cells[1] if len(cells) > 1 else ""
        if metric_type not in _METRIC_TYPES:
            continue

        province_cn, province_share = _province_and_share(
            cells[2] if len(cells) > 2 else ""
        )
        price = _clean_num(cells[3] if len(cells) > 3 else None)
        price_chg = _clean_num(cells[4] if len(cells) > 4 else None)
        time_period = (cells[5] if len(cells) > 5 else "").replace("\n", " ").strip() or None

        # Total volume only for '最高均价' row (first row of each direction group)
        total_vol = direction_total if metric_type == "最高均价" else None

        out.append({
            "report_date":      report_date,
            "direction":        current_direction,
            "metric_type":      metric_type,
            "province_cn":      province_cn,
            "province_share":   province_share,
            "price_yuan_kwh":   price,
            "price_chg_pct":    price_chg,
            "time_period":      time_period,
            "total_vol_100gwh": total_vol,
            "source_pdf":       source_pdf,
        })

    return out


# ── public API ────────────────────────────────────────────────────────────────

def parse_interprov(pdf_path: str | Path, year: int) -> List[dict]:
    """
    Parse all 省间现货交易情况 tables from a PDF.

    Returns a flat list of dicts matching staging.spot_interprov_flow columns.
    Returns [] if the table is absent (not all PDFs contain 省间 data).
    """
    results: List[dict] = []
    pdf_name = Path(pdf_path).name

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""

            if not _TABLE_TITLE_RE.search(text):
                continue

            # Infer report date — use the first date found on this page
            report_date = _infer_date(text, year)
            if report_date is None:
                continue

            for tbl in (page.extract_tables() or []):
                if not tbl:
                    continue
                # Quick filter: table must contain at least one direction keyword
                flat = " ".join(str(c or "") for row in tbl for c in row)
                if "送端" not in flat and "受端" not in flat:
                    continue
                rows = [[(c or "").strip() for c in row] for row in tbl]
                results.extend(_parse_interprov_table(rows, report_date, pdf_name))

    return results
