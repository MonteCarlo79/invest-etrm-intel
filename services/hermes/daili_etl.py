"""
Province 代理购电 (Proxy Purchase) ETL
======================================
Parses annual 代理购电价格汇总 Excel files (one per province per year) and
upserts the 系统运行费用折价 row into province_sysopfee_monthly.

File layout (common to 国网 + 南方电网 provinces):
  Row 0: title  (e.g. "2025年广东省电网企业代理购电价格信息汇总")
  Row 1: unit   (e.g. "单位：亿千瓦时，元/千瓦时" or "...，分/千瓦时（含税）")
  Row 2: header (名称 | 序号 | 明细 | 计算关系 | 1月份 | 2月份 | … | 12月份)
  Row 3+: data

Unit detection:
  If any proxy-purchase-price (seq 4) value > 1.0  →  values are in 分/千瓦时
  → multiply by 0.01 to get yuan/kWh.
  Otherwise values are already in yuan/kWh.

Entry points:
  parse_daili_file(filepath)                — single file → list of row dicts
  upsert_daili_file(filepath, pg_url)       — parse + upsert
  backfill_daili_dir(root_dir, pg_url)      — walk directory, upsert all files
  is_daili_file(filename)                   — detect by filename
"""
from __future__ import annotations

import logging
import os
import re
from datetime import date
from pathlib import Path
from typing import Optional

import openpyxl

from services.hermes.sysopfee_etl import upsert_sysopfee_rows

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

# Keywords that signal a 系统运行费 data row
_SYSOP_KEYWORDS = ["系统运行费", "运行费用折", "系统运行费用"]

# Keywords that signal a sub-component row (skip these)
_SUBCOMP_PREFIXES = ["其中", "           ", "                ", "含税", "备注"]

# Rows to skip when searching for seq-4 proxy purchase price
_PROXY_KEYWORDS = ["代理购电价格", "工商业代理购电价", "代理购电交易价格"]

# Province names that must NOT be confused with other cells
_PROVINCE_FOLDER_RE = re.compile(r'各省电网购电信息[/\\]([^/\\]+)[/\\]')

_YEAR_RE = re.compile(r'(20\d{2})')


# ── Province extraction ────────────────────────────────────────────────────────

def _extract_province(filepath: str) -> Optional[str]:
    """Extract province name from directory path."""
    m = _PROVINCE_FOLDER_RE.search(filepath.replace("\\", "/"))
    if m:
        return m.group(1)
    # Fall back: grandparent folder of the file
    parts = Path(filepath).parts
    for i, part in enumerate(parts):
        if "各省电网购电信息" in part and i + 1 < len(parts):
            return parts[i + 1]
    return None


def _extract_year(rows: list) -> Optional[int]:
    """Extract 4-digit year from title row (row 0)."""
    if not rows:
        return None
    for cell in rows[0]:
        if cell is None:
            continue
        m = _YEAR_RE.search(str(cell))
        if m:
            yr = int(m.group(1))
            if 2020 <= yr <= 2030:
                return yr
    return None


def _extract_year_from_path(filepath: str) -> Optional[int]:
    """Fall-back: extract year from path (e.g., '2025年' folder)."""
    m = _YEAR_RE.search(filepath)
    if m:
        yr = int(m.group(1))
        if 2020 <= yr <= 2030:
            return yr
    return None


# ── Header + column mapping ────────────────────────────────────────────────────

def _find_header_row_idx(rows: list) -> Optional[int]:
    """Find the FIRST row index whose columns 4+ contain >=2 'X月份' patterns."""
    month_pat = re.compile(r'^(\d{1,2})月份?$')
    for idx, row in enumerate(rows):
        count = sum(
            1 for c in (row[4:] if len(row) > 4 else [])
            if isinstance(c, str) and month_pat.match(c.strip())
        )
        if count >= 2:
            return idx
    return None


def _map_columns(header_row: tuple | list, year: int) -> dict[int, int]:
    """Return {col_index: month_number} from the header row."""
    month_pat = re.compile(r'^(\d{1,2})月份?$')
    col_to_month: dict[int, int] = {}
    for col_idx, cell in enumerate(header_row):
        if isinstance(cell, str):
            m = month_pat.match(cell.strip())
            if m:
                col_to_month[col_idx] = int(m.group(1))
    return col_to_month


# ── Scale detection ────────────────────────────────────────────────────────────

def _detect_scale(rows: list, header_idx: int, col_to_month: dict[int, int]) -> float:
    """
    Return 0.01 if price values are in 分/千瓦时 (fen/kWh), else 1.0.
    Detection: look at proxy purchase price row (代理购电价格); if median > 1 → fen.
    """
    if not col_to_month:
        return 1.0
    for row in rows[header_idx + 1:]:
        if len(row) < 4:
            continue
        desc = str(row[3]) if row[3] is not None else ""
        if not any(kw in desc for kw in _PROXY_KEYWORDS):
            continue
        vals = []
        for col_idx in col_to_month:
            if col_idx < len(row) and row[col_idx] is not None:
                try:
                    vals.append(float(row[col_idx]))
                except (TypeError, ValueError):
                    pass
        if vals:
            # If the majority of values are > 1, it's fen/kWh
            if sum(1 for v in vals if v > 1.0) > len(vals) / 2:
                logger.debug("Detected fen/kWh unit (proxy price median=%.2f)", sum(vals) / len(vals))
                return 0.01
            return 1.0
    return 1.0


# ── 系统运行费 row finder ──────────────────────────────────────────────────────

def _find_sysop_row(
    rows: list, header_idx: int, col_to_month: dict[int, int]
) -> Optional[tuple | list]:
    """
    Return the FIRST data row (after header_idx) that:
    - contains a 系统运行费 keyword in col[3]
    - is NOT a sub-component row (description doesn't start with 其中/indent)
    - has at least one numeric value in month columns
    - does NOT appear to be in the summary section (col[1] is None and col[2] is an int)
    """
    for row in rows[header_idx + 1:]:
        if len(row) < 4:
            continue
        desc = str(row[3]) if row[3] is not None else ""
        if not any(kw in desc for kw in _SYSOP_KEYWORDS):
            continue
        # Skip sub-component rows
        if any(desc.lstrip().startswith(p) for p in _SUBCOMP_PREFIXES):
            continue
        # Skip empty summary rows (all month columns are None)
        has_value = any(
            col_idx < len(row) and row[col_idx] is not None
            for col_idx in col_to_month
        )
        if not has_value:
            continue
        return row
    return None


# ── Main parse function ────────────────────────────────────────────────────────

def parse_daili_file(filepath: str) -> list[dict]:
    """
    Parse a single annual 代理购电 Excel file.
    Returns list of {province, year_month: date, fee_yuan_kwh: float}.
    """
    try:
        wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        wb.close()
    except Exception as exc:
        logger.warning("Cannot open %s: %s", filepath, exc)
        return []

    # Province
    province = _extract_province(filepath)
    if not province:
        logger.warning("Cannot determine province for %s", filepath)
        return []

    # Year
    year = _extract_year(rows) or _extract_year_from_path(filepath)
    if not year:
        logger.warning("Cannot determine year for %s", filepath)
        return []

    # Header row
    header_idx = _find_header_row_idx(rows)
    if header_idx is None:
        logger.warning("No header row found in %s", filepath)
        return []

    col_to_month = _map_columns(rows[header_idx], year)
    if not col_to_month:
        logger.warning("No month columns found in %s", filepath)
        return []

    # Scale (yuan vs fen)
    scale = _detect_scale(rows, header_idx, col_to_month)

    # 系统运行费 row
    sysop_row = _find_sysop_row(rows, header_idx, col_to_month)
    if sysop_row is None:
        logger.warning("No 系统运行费 row found in %s", filepath)
        return []

    # Extract monthly values
    results: list[dict] = []
    for col_idx, month in col_to_month.items():
        if col_idx >= len(sysop_row):
            continue
        val = sysop_row[col_idx]
        if val is None:
            continue
        try:
            fee = float(val) * scale
        except (TypeError, ValueError):
            continue
        if fee == 0.0:
            continue  # skip zero entries (usually missing data months)
        results.append({
            "province": province,
            "year_month": date(year, month, 1),
            "fee_yuan_kwh": round(fee, 8),
        })

    logger.info(
        "Parsed %d month-rows from %s (province=%s year=%d scale=%s)",
        len(results), os.path.basename(filepath), province, year, scale,
    )
    return results


# ── DB upsert ─────────────────────────────────────────────────────────────────

def upsert_daili_file(filepath: str, pg_url: str) -> dict:
    """Parse one file and upsert to province_sysopfee_monthly."""
    rows = parse_daili_file(filepath)
    if not rows:
        return {"upserted": 0, "rows": [], "errors": [f"No data from {filepath}"]}
    return upsert_sysopfee_rows(rows, pg_url, os.path.basename(filepath))


def backfill_daili_dir(root_dir: str, pg_url: str) -> dict:
    """
    Walk root_dir recursively, parse every 代理购电 Excel, upsert all rows.
    Returns summary dict.
    """
    total_upserted = 0
    total_errors: list[str] = []
    files_processed = 0

    for dirpath, _dirs, filenames in os.walk(root_dir):
        for fn in filenames:
            if fn.startswith("~$"):
                continue
            if not fn.lower().endswith((".xlsx", ".xls")):
                continue
            if not is_daili_file(fn):
                continue
            filepath = os.path.join(dirpath, fn)
            result = upsert_daili_file(filepath, pg_url)
            files_processed += 1
            total_upserted += result["upserted"]
            total_errors.extend(result["errors"])
            logger.info(
                "daili backfill: %s → %d rows", fn, result["upserted"]
            )

    return {
        "files_processed": files_processed,
        "total_upserted": total_upserted,
        "errors": total_errors,
    }


# ── File detection ────────────────────────────────────────────────────────────

def is_daili_file(filename: str) -> bool:
    """Return True if filename looks like a 代理购电 annual summary Excel."""
    name = filename
    # Positive keywords
    pos = ["代理购电", "购电价格", "购电月度", "购电信息", "代理采购", "daili"]
    # Negative keywords (avoid sysopfee standalone files, comparison files)
    neg = ["系统运行费", "sysopfee", "vs", "对比", "比较", "占比"]
    has_pos = any(kw in name for kw in pos)
    has_neg = any(kw in name for kw in neg)
    return has_pos and not has_neg
