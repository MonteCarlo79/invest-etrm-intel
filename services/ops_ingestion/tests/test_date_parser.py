"""
tests/test_date_parser.py

Unit tests for services/ops_ingestion/inner_mongolia/date_parser.py

Covers:
  - Canonical filename pattern 【X月Y日】 + year from /YYYY/ path
  - ISO date override in filename
  - --year CLI hint override
  - Bare X月Y日 without brackets
  - ValueError when no month/day pattern
  - Year-folder beats current-year fallback
"""
from __future__ import annotations

import sys
import os
import warnings
from datetime import date

import pytest

# Add service root to path
sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')))

from inner_mongolia.date_parser import (
    parse_date,
    _extract_year_from_path,
    _parse_month_day,
)


# ---------------------------------------------------------------------------
# _extract_year_from_path
# ---------------------------------------------------------------------------

class TestExtractYearFromPath:
    def test_unix_style_path(self):
        assert _extract_year_from_path('/data/inner-mongolia/2026/【2月10日】.xlsx') == 2026

    def test_windows_style_path(self):
        assert _extract_year_from_path(r'C:\data\inner-mongolia\2026\【2月10日】.xlsx') == 2026

    def test_no_year_folder(self):
        assert _extract_year_from_path('/data/inner-mongolia/【2月10日】.xlsx') is None

    def test_year_out_of_range(self):
        # Year 1999 and 2101 are outside the sanity guard
        assert _extract_year_from_path('/data/1999/file.xlsx') is None
        assert _extract_year_from_path('/data/2101/file.xlsx') is None

    def test_year_2025(self):
        assert _extract_year_from_path('/ops/2025/file.xlsx') == 2025


# ---------------------------------------------------------------------------
# parse_date — ISO date in filename
# ---------------------------------------------------------------------------

class TestParseDateISO:
    def test_iso_date_in_filename(self):
        result = parse_date('/data/2026/【2026-02-10】内蒙储能电站运营统计.xlsx')
        assert result == date(2026, 2, 10)

    def test_iso_date_ignores_year_folder(self):
        # ISO date in filename takes priority over folder year
        result = parse_date('/data/2025/【2026-02-10】file.xlsx')
        assert result == date(2026, 2, 10)

    def test_iso_date_ignores_year_hint(self):
        # ISO date in filename takes priority over year_hint argument
        result = parse_date('/data/【2026-02-10】file.xlsx', year_hint=2025)
        assert result == date(2026, 2, 10)


# ---------------------------------------------------------------------------
# parse_date — year from folder + month/day from filename
# ---------------------------------------------------------------------------

class TestParseDateFolderYear:
    def test_canonical_pattern(self):
        path = '/data/inner-mongolia/2026/【2月10日】内蒙储能电站运营统计.xlsx'
        assert parse_date(path) == date(2026, 2, 10)

    def test_year_folder_beats_current_year(self):
        # Even if today is a different year, folder year wins
        path = '/archive/2024/【3月15日】file.xlsx'
        assert parse_date(path) == date(2024, 3, 15)

    def test_single_digit_month_day(self):
        path = '/data/2026/【1月5日】file.xlsx'
        assert parse_date(path) == date(2026, 1, 5)

    def test_double_digit_month_day(self):
        path = '/data/2026/【12月31日】file.xlsx'
        assert parse_date(path) == date(2026, 12, 31)


# ---------------------------------------------------------------------------
# parse_date — year_hint (CLI --year flag)
# ---------------------------------------------------------------------------

class TestParseDateYearHint:
    def test_year_hint_overrides_folder(self):
        # year_hint takes priority over folder year
        path = '/data/inner-mongolia/2025/【2月10日】file.xlsx'
        assert parse_date(path, year_hint=2026) == date(2026, 2, 10)

    def test_year_hint_used_when_no_folder(self):
        path = '/data/inner-mongolia/【2月10日】file.xlsx'
        assert parse_date(path, year_hint=2026) == date(2026, 2, 10)


# ---------------------------------------------------------------------------
# parse_date — bare X月Y日 (no brackets)
# ---------------------------------------------------------------------------

class TestParseDateBarePattern:
    def test_bare_month_day(self):
        path = '/data/2026/2月10日file.xlsx'
        assert parse_date(path) == date(2026, 2, 10)


# ---------------------------------------------------------------------------
# parse_date — ValueError when no month/day pattern
# ---------------------------------------------------------------------------

class TestParseDateErrors:
    def test_no_month_day_raises(self):
        with pytest.raises(ValueError, match="Cannot parse month/day"):
            parse_date('/data/2026/report_no_date.xlsx')

    def test_completely_wrong_filename(self):
        with pytest.raises(ValueError):
            parse_date('/data/2026/random_file_abc123.xlsx')


# ---------------------------------------------------------------------------
# parse_date — current-year fallback with warning
# ---------------------------------------------------------------------------

class TestParseDateCurrentYearFallback:
    def test_current_year_fallback_warns(self):
        # No ISO date, no folder year, no year_hint
        path = '/data/【2月10日】file.xlsx'
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = parse_date(path)
            assert len(w) == 1
            assert "current calendar year" in str(w[0].message)
        from datetime import date as _date
        assert result.month == 2
        assert result.day == 10
        assert result.year == _date.today().year
