"""Parser for the monthly 蒙西 BESS 复盘 (internal review) PowerPoint decks.

Pure domain code. Output is long rows `asset_code, month, metric, value, unit` plus notes.
Revenue in 万元 is stored as ¥ (x1e4); energy MWh; prices ¥/MWh. Blank cells are absent, not 0.
"""
from __future__ import annotations

import io
import re
from datetime import date

import pandas as pd

from libs.asset_risk.review.aliases import asset_code_for

COLUMNS = ["asset_code", "month", "metric", "value", "unit"]

# overview header keyword -> (metric, unit, scale); first matching keyword wins
_OVERVIEW = [
    ("月度总收益", "total_revenue_cny", "CNY", 1e4),
    ("额定容量收益", "unit_capacity_revenue", "CNY/MWh", 1.0),
    ("日均充电次数", "daily_cycles", "cycles/day", 1.0),
    ("现货价差", "spot_spread", "CNY/MWh", 1.0),
    ("系统运行费单价", "sysop_fee_unit", "CNY/MWh", 1.0),
]
_CAPCOMP_RE = re.compile(r"容量补[偿贴].*单价")
_BREAKDOWN_ROWS = [("策略申报", "strategy"), ("实时出清", "cleared"), ("实际执行", "executed")]


def deck_month(texts: list[str], file_name: str = "") -> date | None:
    """Month from a title "（N月）" + date line "YYYY.M"; the date line is the publication
    month, so a label later in the year than the date month belongs to the previous year.
    Falls back to "N月" in the file name (year from the date line or a 20xx in the file name)."""
    blob = "\n".join(texts)
    m = re.search(r"[（(]\s*(\d{1,2})\s*月\s*[）)]", blob)
    d = re.search(r"(20\d{2})\s*[.\-/年]\s*(\d{1,2})", blob)
    n = int(m.group(1)) if m else None
    year = int(d.group(1)) if d else None
    if n is None:
        fm = re.search(r"(\d{1,2})月", file_name or "")
        n = int(fm.group(1)) if fm else None
        if year is None:
            fy = re.search(r"(20\d{2})", file_name or "")
            year = int(fy.group(1)) if fy else None
    elif d and n > int(d.group(2)):
        year -= 1
    if n is None or year is None or not 1 <= n <= 12:
        return None
    return date(year, n, 1)


def _num(s) -> float | None:
    t = str(s or "").replace(",", "").replace("\xa0", "").strip()
    if not t:
        return None
    try:
        return float(t)
    except ValueError:
        return None


def _cells(table) -> list[list[str]]:
    return [[(c.text or "").strip() for c in r.cells] for r in table.rows]


def _ffill(row: list[str]) -> list[str]:
    out, cur = [], ""
    for v in row:
        cur = v or cur
        out.append(cur)
    return out


def _overview_rows(grid: list[list[str]], month: date, notes: list[str]):
    hdr_i = next(i for i, r in enumerate(grid) if r and r[0].replace(" ", "") == "场站名称")
    hdr = grid[hdr_i]
    month_row = grid[hdr_i + 1] if hdr_i + 1 < len(grid) and grid[hdr_i + 1][:1] == ["月份"] else None
    data_start = hdr_i + (2 if month_row else 1)
    labels = _ffill(hdr) if month_row else hdr
    col_for: dict[int, tuple[str, str, float]] = {}
    for ci, lab in enumerate(labels):
        if ci == 0 or not lab:
            continue
        spec = None
        for kw, metric, unit, scale in _OVERVIEW:
            if kw in lab:
                spec = (metric, unit, scale)
                break
        if spec is None and _CAPCOMP_RE.search(lab):
            spec = ("capcomp_unit_price", "CNY/MWh", 1.0)
        if spec is None:
            continue
        if month_row is not None:
            mm = re.match(r"(\d{1,2})月", month_row[ci])
            if not mm or int(mm.group(1)) != month.month:
                continue
        col_for[ci] = spec
    if month_row is not None and not col_for:
        notes.append(f"overview table: no column for {month.month}月 in two-month header")
    for r in grid[data_start:]:
        name = r[0]
        if not name:
            continue
        code = asset_code_for(name)
        if code is None:
            notes.append(f"overview: unknown station '{name}' skipped")
            continue
        for ci, (metric, unit, scale) in col_for.items():
            v = _num(r[ci]) if ci < len(r) else None
            if v is not None:
                yield code, metric, v * scale, unit


def _breakdown_rows(grid: list[list[str]], notes: list[str]):
    if len(grid) < 3:
        return
    names = _ffill(grid[0])
    for ri in range(2, len(grid)):
        row = grid[ri]
        prefix = next((p for kw, p in _BREAKDOWN_ROWS if kw in row[0]), None)
        if prefix is None:
            continue
        for start in range(1, len(row) - 2, 3):
            name = names[start]
            code = asset_code_for(name)
            if code is None:
                notes.append(f"breakdown: unknown station '{name}' skipped")
                continue
            for off, metric, unit, scale in (
                (0, f"{prefix}_revenue_cny", "CNY", 1e4),
                (1, f"{prefix}_charge_mwh", "MWh", 1.0),
                (2, f"{prefix}_discharge_mwh", "MWh", 1.0),
            ):
                v = _num(row[start + off])
                if v is not None:
                    yield code, metric, v * scale, unit


def parse_bess_deck(path_or_buffer, file_name: str) -> tuple[pd.DataFrame, list[str]]:
    from pptx import Presentation

    if isinstance(path_or_buffer, (bytes, bytearray)):
        path_or_buffer = io.BytesIO(path_or_buffer)
    prs = Presentation(path_or_buffer)
    notes: list[str] = []
    slides = list(prs.slides)
    first_texts = [sh.text_frame.text for sh in (slides[0].shapes if slides else [])
                   if sh.has_text_frame]
    month = deck_month(first_texts, file_name)
    if month is None:
        return pd.DataFrame(columns=COLUMNS), [f"{file_name}: month not detected; nothing parsed"]

    rows: dict[tuple[str, str], tuple[float, str]] = {}   # first occurrence wins
    found = 0
    for slide in slides:
        for sh in slide.shapes:
            if not sh.has_table:
                continue
            grid = _cells(sh.table)
            if not grid or not grid[0]:
                continue
            flat = " ".join(c for r in grid[:3] for c in r)
            if any(r and r[0].replace(" ", "") == "场站名称" for r in grid[:3]) and "月度总收益" in flat:
                it = _overview_rows(grid, month, notes)
            elif grid[0][0] == "曲线":
                it = _breakdown_rows(grid, notes)
            else:
                continue
            found += 1
            for code, metric, v, unit in it:
                rows.setdefault((code, metric), (v, unit))
    if not found:
        notes.append(f"{file_name}: no overview/breakdown tables found")
    df = pd.DataFrame(
        [(c, month, m, v, u) for (c, m), (v, u) in rows.items()], columns=COLUMNS)
    return df, list(dict.fromkeys(notes))
