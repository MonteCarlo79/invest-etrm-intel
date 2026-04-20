"""
services/ops_ingestion/inner_mongolia/date_parser.py

Parse the report date from an Inner Mongolia BESS operations Excel filename.

Filename pattern (canonical): 【X月Y日】内蒙储能电站运营统计.xlsx
                or with date:  【2026-02-10】内蒙储能电站运营统计.xlsx

Year inference priority (high → low):
  1. ISO date embedded in filename: YYYY-MM-DD → year comes from there
  2. Year folder in file path: looks for /YYYY/ component (e.g. .../2026/...)
  3. Explicit year_hint argument (from --year CLI flag)
  4. Workbook internal date hint: first 5 rows of sheet 0 for datetime objects
  5. Current calendar year — last resort; emits a warnings.warn()
"""
from __future__ import annotations

import os
import re
import warnings
from datetime import date
from typing import Optional


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_date(path: str, year_hint: Optional[int] = None) -> date:
    """
    Return the report date for the given file path.

    Parameters
    ----------
    path : str
        Full or relative path to the Excel file.
    year_hint : int | None
        Explicit year override (e.g. from --year CLI flag).  Takes precedence
        over the folder-inferred year but NOT over an ISO date in the filename.

    Raises
    ------
    ValueError
        If neither an ISO date nor a 月/日 pattern can be found in the filename.
    """
    basename = os.path.basename(path)

    # 1. ISO date embedded in filename  (e.g. 2026-02-10)
    m_iso = re.search(r'(\d{4}-\d{2}-\d{2})', basename)
    if m_iso:
        return date.fromisoformat(m_iso.group(1))

    # Determine year from sources 2–5 (used only when ISO date is absent)
    year = _extract_year_from_path(path)   # step 2

    if year_hint is not None:              # step 3 — CLI flag overrides folder
        year = year_hint

    if year is None:                       # step 4 — workbook internal hint
        year = _infer_year_from_workbook(path)

    if year is None:                       # step 5 — current year, with warning
        warnings.warn(
            f"Could not infer year for {basename!r}; defaulting to current calendar year. "
            "Pass --year or place the file under a /YYYY/ directory to resolve this.",
            stacklevel=2,
        )
        year = date.today().year

    # Parse month/day from 【X月Y日】 or bare X月Y日
    return _parse_month_day(basename, year)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_year_from_path(path: str) -> Optional[int]:
    """
    Look for a /YYYY/ component in the file path.

    Examples
    --------
    >>> _extract_year_from_path('/data/inner-mongolia/2026/【2月10日】.xlsx')
    2026
    >>> _extract_year_from_path('/data/inner-mongolia/【2月10日】.xlsx') is None
    True
    """
    # Normalise separators to forward-slash for regex
    normalised = path.replace('\\', '/')
    m = re.search(r'/(\d{4})/', normalised)
    if m:
        year = int(m.group(1))
        if 2000 <= year <= 2100:   # sanity guard
            return year
    return None


def _infer_year_from_workbook(path: str) -> Optional[int]:
    """
    Open the workbook and search the first 5 rows of the first sheet for a
    datetime.datetime cell value.  Returns the year of the first one found.

    This is a best-effort fallback and may return None if the file cannot be
    opened or contains no datetime values in the header area.
    """
    try:
        import openpyxl
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        ws = wb.worksheets[0]
        import datetime as _dt
        for i, row in enumerate(ws.iter_rows(max_row=5)):
            for cell in row:
                if isinstance(cell.value, _dt.datetime):
                    year = cell.value.year
                    wb.close()
                    if 2000 <= year <= 2100:
                        return year
        wb.close()
    except Exception:
        pass
    return None


def _parse_month_day(basename: str, year: int) -> date:
    """
    Extract month and day from the filename and combine with *year*.

    Accepts:
      - 【X月Y日】  (full-width brackets)
      - X月Y日      (bare)

    Raises ValueError if no month/day pattern is found.
    """
    # Full-width brackets first (more specific)
    m = re.search(r'【(\d+)月(\d+)日】', basename)
    if m:
        return date(year, int(m.group(1)), int(m.group(2)))

    # Bare pattern
    m = re.search(r'(\d+)月(\d+)日', basename)
    if m:
        return date(year, int(m.group(1)), int(m.group(2)))

    raise ValueError(
        f"Cannot parse month/day from filename: {basename!r}. "
        "Expected pattern like 【2月10日】 or 2月10日."
    )
