# tests/ancillary_revenue/test_extract_ancillary.py
"""新疆 monthly 调频补偿 extraction tests (real PDFs under
data/exchange-monthly-reports/新疆月报/ — skipped if data dir absent)."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "services" / "ancillary_revenue"))
from extract_ancillary import _month_from_name, extract_xinjiang_monthly  # noqa: E402

_DATA = Path(__file__).resolve().parents[2] / "data" / "exchange-monthly-reports" / "新疆月报"


def test_month_from_name():
    assert _month_from_name("2026年6月省内辅助服务市场运行情况.pdf") == "2026-06-01"
    assert _month_from_name("新疆-2026-07-report.pdf") == "2026-07-01"
    assert _month_from_name("no-date.pdf") is None


@pytest.mark.skipif(not _DATA.is_dir(), reason="exchange reports data dir not present")
def test_june_2026_extraction():
    r = extract_xinjiang_monthly(_DATA / "2026年6月省内辅助服务市场运行情况.pdf")
    assert r is not None
    assert r.province == "新疆" and r.month == "2026-06-01"
    assert r.metric == "调频补偿费用_独立储能"
    assert r.amount_yuan == pytest.approx(534.99e4)   # 534.99 万元


@pytest.mark.skipif(not _DATA.is_dir(), reason="exchange reports data dir not present")
def test_jan_2026_no_bess_figure():
    assert extract_xinjiang_monthly(_DATA / "2026年1月省内辅助服务市场运行情况.pdf") is None
