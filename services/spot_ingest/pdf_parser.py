"""
Improved PDF parser for China spot market daily reports.

Key fixes:
  1. Mode is reset to None whenever page_date changes — prevents day-N's last
     section mode from bleeding into day-N+1's tables in multi-day PDFs.
  2. Secondary table-header detection: scans column headers for 日前/实时
     keywords as a per-table fallback when page-level section headers are absent.
  3. Pages whose page-level mode is unknown are still processed via table-level
     detection — fixes per-day sub-tables on later pages of multi-day PDFs.
  4. Combined multi-date tables (e.g. "3月6日-3月8日各地现货实时市场运行情况")
     are detected by finding date labels in the header rows; each date group is
     extracted independently so all dates are captured.
  5. DA prices are stored on the delivery date shown in the table header
     (no D+1 shift — the date above the table IS the delivery date).

Returns:
    parse_pdf(pdf_path, year, provinces_cn)
      -> Dict[date, Dict[province_cn, {da_avg, da_max, da_min, rt_avg, rt_max, rt_min}]]
"""
from __future__ import annotations

import datetime as dt
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pdfplumber


# ── Low-level helpers ─────────────────────────────────────────────────────────

def _clean_num(s: str | None) -> Optional[float]:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    s = (
        s.replace(",", "").replace("，", "")
        .replace("%", "").replace("％", "")
        .replace("—", "").replace("－", "")
        .replace("\u2014", "")  # em-dash
    )
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _pick_triplet_from_tail(tail: List[str]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Many reports: 均价 / 环比 / 最高价 / 环比 / 最低价 / 环比
    Prefer index 0/2/4. Fall back to first three numerics.
    """
    if len(tail) >= 5:
        a = _clean_num(tail[0])
        b = _clean_num(tail[2])
        c = _clean_num(tail[4])
        if any(x is not None for x in (a, b, c)):
            return a, b, c

    nums: List[float] = []
    for x in tail:
        v = _clean_num(x)
        if v is not None:
            nums.append(v)
        if len(nums) >= 3:
            break

    if len(nums) >= 3:
        return nums[0], nums[1], nums[2]
    return None, None, None


def _infer_page_date(text: str, year: int) -> Optional[dt.date]:
    m = re.search(r"(\d{1,2})\s*月\s*(\d{1,2})\s*日", text)
    if not m:
        return None
    try:
        return dt.date(year, int(m.group(1)), int(m.group(2)))
    except ValueError:
        return None


def _detect_section_mode(text: str) -> Optional[str]:
    """Return 'RT', 'DA', or None based on section headers in page text.

    Only match SECTION TITLE keywords (运行情况 variants).  Column-header
    keywords like 出清均价 are intentionally excluded here because they also
    appear verbatim in page footnotes (e.g. "现货价差=日前市场出清均价−实时市场
    出清均价"), which would falsely override the inherited mode on RT continuation
    pages.  Column-header detection is handled by _detect_table_mode instead.
    """
    has_rt = (
        "现货实时市场运行情况" in text
        or "实时市场运行情况" in text
    )
    has_da = (
        "现货日前市场运行情况" in text
        or "日前市场运行情况" in text
    )
    if has_da and has_rt:
        # Both on same page: pick whichever appears first in the text
        idx_da = min(
            (text.find(k) for k in (
                "现货日前市场运行情况", "日前市场出清均价", "日前出清均价",
                "日前市场运行情况",
            ) if k in text),
            default=len(text),
        )
        idx_rt = min(
            (text.find(k) for k in (
                "现货实时市场运行情况", "实时市场出清均价", "实时出清均价",
                "实时市场运行情况",
            ) if k in text),
            default=len(text),
        )
        return "DA" if idx_da < idx_rt else "RT"
    if has_da:
        return "DA"
    if has_rt:
        return "RT"
    return None


def _detect_table_mode(header_row: List[str]) -> Optional[str]:
    """Check table column header row for 日前/实时 keywords.

    When both keywords are present (RT table with a companion DA column, or
    vice versa), use occurrence counts to identify the primary mode: the
    primary section type will have many more keyword instances than the
    companion column.  Returns None only when counts are equal (true mixed
    table) so the page-level mode can take precedence.
    """
    combined = " ".join((c or "") for c in header_row)
    has_da = "日前" in combined
    has_rt = "实时" in combined
    if has_da and has_rt:
        rt_count = combined.count("实时")
        da_count = combined.count("日前")
        if rt_count != da_count:
            return "RT" if rt_count > da_count else "DA"
        return None   # equal counts — let page_mode win
    if has_da:
        return "DA"
    if has_rt:
        return "RT"
    return None


# ── Combined multi-date table helpers ─────────────────────────────────────────

def _find_date_groups(header_rows: List[list], year: int) -> List[Tuple[int, dt.date]]:
    """
    Scan the first few rows of a table for cells containing date patterns
    (e.g. "3月6日实时市场").

    Returns a sorted list of (col_index, date) for each unique date found,
    or [] if fewer than 2 distinct dates are found (not a combined table).
    """
    seen: dict[dt.date, int] = {}  # date → first col_index where it appears
    for row in header_rows:
        for col_i, cell in enumerate(row or []):
            m = re.search(r"(\d{1,2})\s*月\s*(\d{1,2})\s*日", str(cell or ""))
            if m:
                try:
                    d = dt.date(year, int(m.group(1)), int(m.group(2)))
                    if d not in seen:
                        seen[d] = col_i
                except ValueError:
                    pass
    if len(seen) < 2:
        return []
    # Return as (col_index, date) sorted by col_index
    return sorted([(col, d) for d, col in seen.items()], key=lambda x: x[0])


def _parse_combined_table(
    tbl: List[list],
    provinces_cn: List[str],
    year: int,
    mode: str,
    date_groups: List[Tuple[int, dt.date]],
) -> Tuple[List[dict], List[dict]]:
    """
    Extract per-date triplets from a combined multi-date table.

    date_groups: [(col_index, date), ...] sorted by col_index — from _find_date_groups.
    Each date group occupies `group_size` columns (avg, chg, max, chg, min, chg = 6).
    """
    da_rows: List[dict] = []
    rt_rows: List[dict] = []

    # Infer group size from column spacing between first two date labels
    first_date_col = date_groups[0][0]   # col index in raw row
    group_size = date_groups[1][0] - first_date_col if len(date_groups) >= 2 else 6

    header_markers = {"省份", "均价", "最高价", "最低价", "出清均价", "环比", "地区"}

    for raw_row in tbl:
        if not raw_row:
            continue
        row = [(c or "").strip() for c in raw_row]

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

        tail = row[prov_idx + 1:]
        # Offset within tail where the first date group starts
        first_group_offset = first_date_col - prov_idx - 1

        for group_idx, (date_col, date) in enumerate(date_groups):
            offset = first_group_offset + group_idx * group_size
            if offset < 0 or offset >= len(tail):
                continue
            group_tail = tail[offset: offset + group_size]
            avg, mx, mn = _pick_triplet_from_tail(group_tail)

            if mode == "DA":
                da_rows.append({
                    "province_cn": prov_cn,
                    "date": date,
                    "da_avg": avg,
                    "da_max": mx,
                    "da_min": mn,
                })
            else:
                rt_rows.append({
                    "province_cn": prov_cn,
                    "date": date,
                    "rt_avg": avg,
                    "rt_max": mx,
                    "rt_min": mn,
                })

    return da_rows, rt_rows


# ── Page-level table parsing ──────────────────────────────────────────────────

def _parse_tables_from_page(
    page,
    provinces_cn: List[str],
    page_mode: Optional[str],
    year: int,
) -> Tuple[List[dict], List[dict]]:
    """
    Parse DA and RT rows from all tables on a page.

    For each table:
      1. Check if it is a combined multi-date table (2+ date labels in header).
         If so, extract per-date triplets using _parse_combined_table.
      2. Otherwise use standard single-date extraction.
         a. Determine mode from table header keywords (日前/实时).
         b. Fall back to page_mode.

    Returns (da_rows, rt_rows). Each row dict may include an optional 'date'
    key (set only for combined-table rows); callers fall back to page_date
    when the key is absent.
    """
    da_rows: List[dict] = []
    rt_rows: List[dict] = []

    tables = page.extract_tables() or []
    header_markers = {"省份", "均价", "最高价", "最低价", "出清均价", "环比", "地区"}

    for tbl in tables:
        if not tbl:
            continue

        # ── Check for combined multi-date table ───────────────────────────────
        date_groups = _find_date_groups(tbl[:3], year)
        if date_groups:
            # Determine mode for this combined table
            combined_mode = page_mode
            for candidate_row in tbl[:3]:
                if not candidate_row:
                    continue
                detected = _detect_table_mode([(c or "") for c in candidate_row])
                if detected:
                    combined_mode = detected
                    break
            if combined_mode is None:
                continue  # can't determine mode even with table-level detection
            c_da, c_rt = _parse_combined_table(tbl, provinces_cn, year, combined_mode, date_groups)
            da_rows.extend(c_da)
            rt_rows.extend(c_rt)
            continue

        # ── Standard single-date table ────────────────────────────────────────
        table_mode = page_mode

        # Scan first two rows for header keywords that reveal DA/RT
        for candidate_row in tbl[:3]:
            if not candidate_row:
                continue
            detected = _detect_table_mode([(c or "") for c in candidate_row])
            if detected:
                table_mode = detected
                break

        if table_mode is None:
            # Cannot determine DA vs RT for this table; skip
            continue

        for raw_row in tbl:
            if not raw_row:
                continue
            row = [(c or "").strip() for c in raw_row]

            # Skip header rows
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

            tail = row[prov_idx + 1:]
            # Skip leading empty cells — continuation pages from a prior page's
            # merged-cell column produce a spurious empty cell at the start of
            # each data row, which would cause _pick_triplet_from_tail to use
            # change-% values instead of prices.
            while tail and not tail[0]:
                tail = tail[1:]
            avg, mx, mn = _pick_triplet_from_tail(tail)

            if table_mode == "DA":
                da_rows.append({
                    "province_cn": prov_cn,
                    "da_avg": avg,
                    "da_max": mx,
                    "da_min": mn,
                })
            else:  # RT
                rt_rows.append({
                    "province_cn": prov_cn,
                    "rt_avg": avg,
                    "rt_max": mx,
                    "rt_min": mn,
                })

    return da_rows, rt_rows


# ── Public API ────────────────────────────────────────────────────────────────

def parse_pdf(
    pdf_path: str | Path,
    year: int,
    provinces_cn: List[str],
) -> Dict[dt.date, Dict[str, dict]]:
    """
    Parse a spot market daily report PDF (single or multi-day).

    Returns:
        {
          date: {
            province_cn: {
              da_avg, da_max, da_min,   # None if not found
              rt_avg, rt_max, rt_min,
            },
            ...
          },
          ...
        }

    Date semantics: the date shown above a table IS the delivery date for both
    DA and RT prices.  No D+1 shift is applied.
    """
    result: Dict[dt.date, Dict[str, dict]] = {}

    mode: Optional[str] = None
    last_date: Optional[dt.date] = None

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            page_date = _infer_page_date(text, year)

            # ── Mode reset at each new date ──────────────────────────────────
            if page_date is not None and page_date != last_date:
                mode = None
                last_date = page_date

            # ── Update mode from page-level section headers ──────────────────
            detected = _detect_section_mode(text)
            if detected:
                mode = detected

            # Use last known date for pages with no explicit date.
            # Continuation pages (table overflows to the next page) have no
            # date header; carry forward last_date so their rows are stored
            # under the same date as the table's first page.
            effective_date = page_date if page_date is not None else last_date
            if effective_date is None:
                continue  # no date known yet — skip until first date seen

            # Note: mode may be None here — _parse_tables_from_page handles
            # that via table-level keyword detection, so we proceed regardless.

            # ── Parse tables ─────────────────────────────────────────────────
            da_rows, rt_rows = _parse_tables_from_page(page, provinces_cn, mode, year)

            for r in da_rows:
                pcn = r["province_cn"]
                # Combined tables carry their own date; single-date tables use effective_date
                da_date = r.get("date", effective_date)
                prov = result.setdefault(da_date, {}).setdefault(pcn, {
                    "da_avg": None, "da_max": None, "da_min": None,
                    "rt_avg": None, "rt_max": None, "rt_min": None,
                })
                for k in ("da_avg", "da_max", "da_min"):
                    if r[k] is not None:
                        prov[k] = r[k]

            for r in rt_rows:
                pcn = r["province_cn"]
                rt_date = r.get("date", effective_date)
                prov = result.setdefault(rt_date, {}).setdefault(pcn, {
                    "da_avg": None, "da_max": None, "da_min": None,
                    "rt_avg": None, "rt_max": None, "rt_min": None,
                })
                for k in ("rt_avg", "rt_max", "rt_min"):
                    if r[k] is not None:
                        prov[k] = r[k]

    # Year-boundary correction: a PDF published in early year (e.g. Jan) may
    # contain Dec dates from the PREVIOUS year (e.g. Dec 31 RT in a Jan 1-4
    # PDF).  Detect by: early-month dates dominate AND some Oct-Dec dates exist.
    if result:
        from collections import Counter
        month_cnt = Counter(d.month for d in result)
        early = sum(v for m, v in month_cnt.items() if 1 <= m <= 4)
        late  = sum(v for m, v in month_cnt.items() if 10 <= m <= 12)
        if early > 0 and late > 0 and early >= late:
            corrected: Dict[dt.date, Dict[str, dict]] = {}
            for d, provs in result.items():
                corrected[dt.date(d.year - 1, d.month, d.day) if d.month >= 10 else d] = provs
            result = corrected

    return result


def parse_pdf_flat(
    pdf_path: str | Path,
    year: int,
    provinces_cn: List[str],
) -> List[dict]:
    """
    Convenience wrapper: returns a flat list of dicts ready for DB upsert.

        [{"report_date": date, "province_cn": str, "da_avg": ..., ...}, ...]
    """
    nested = parse_pdf(pdf_path, year, provinces_cn)
    rows = []
    for d, provinces in nested.items():
        for pcn, vals in provinces.items():
            rows.append({
                "report_date": d,
                "province_cn": pcn,
                **vals,
            })
    return rows
