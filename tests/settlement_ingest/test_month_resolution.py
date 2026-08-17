"""Tests for the settlement month-resolution ladder in scanner.py.

Covers the three skip buckets found in the 2026-08 invoice survey:
- dot-separated dates (乌海 "2025.01电费清单")
- no year in filename or folder (民勤 flat folder) → PDF-content fallback
- no date anywhere (射阳) → still skipped (returns None)
"""
from pathlib import Path
from unittest.mock import patch

from services.settlement_ingest.scanner import (
    extract_month_from_filename,
    resolve_settlement_month,
)

_BILLING = "services.settlement_ingest.parser_charge.extract_billing_period"


class TestFilenamePatterns:
    def test_dash_date(self):
        assert extract_month_from_filename("2026-01电费结算单.pdf") == "2026-01-01"

    def test_dot_date(self):
        assert extract_month_from_filename("富景五虎山储能电站2025.01电费清单(1)(1).pdf") == "2025-01-01"

    def test_cn_date(self):
        assert extract_month_from_filename("2026年1月上网电费结算单.pdf") == "2026-01-01"

    def test_dash_cn_mixed(self):
        assert extract_month_from_filename("B-11四子王旗2026-03月上网结算单.pdf") == "2026-03-01"

    def test_month_only_needs_year(self):
        assert extract_month_from_filename("杭锦旗1月份电费清单.pdf") == "NEED_YEAR-01-01"

    def test_no_month(self):
        assert extract_month_from_filename("B-4下-射阳远汇智慧能源有限公司.pdf") is None


class TestResolveLadder:
    def test_folder_year_completes_month(self):
        p = Path("/root/2026年结算单/杭锦旗1月份电费清单.pdf")
        assert resolve_settlement_month(p) == "2026-01-01"

    def test_content_fallback_when_no_year_anywhere(self):
        p = Path("/root/B-12甘肃民勤/B-12-下1月民勤.pdf")
        with patch(_BILLING, return_value="2026-01-01"):
            assert resolve_settlement_month(p) == "2026-01-01"

    def test_content_fallback_not_called_when_filename_suffices(self):
        p = Path("/root/2026年结算单/2026年1月上网电费结算单.pdf")
        with patch(_BILLING, side_effect=AssertionError("must not be called")):
            assert resolve_settlement_month(p) == "2026-01-01"

    def test_returns_none_when_nothing_works(self):
        p = Path("/root/B-4 江苏射阳/2026年结算单/B-4下-射阳远汇智慧能源有限公司.pdf")
        with patch(_BILLING, return_value=None):
            assert resolve_settlement_month(p) is None

    def test_content_fallback_exception_returns_none(self):
        p = Path("/root/B-12甘肃民勤/B-12-下1月民勤.pdf")
        with patch(_BILLING, side_effect=RuntimeError("boom")):
            assert resolve_settlement_month(p) is None
