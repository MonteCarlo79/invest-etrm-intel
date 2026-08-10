import datetime as dt

import pytest

from services.spot_ingest.monthly_report import is_spot_monthly_pdf, infer_report_month


def test_matches_monthly_report():
    assert is_spot_monthly_pdf("电力现货市场价格与运行月报（2026年6月）.pdf") is True


def test_rejects_daily_report():
    # 日报 must NOT match — it has its own pipeline (is_spot_pdf)
    assert is_spot_monthly_pdf("电力现货市场价格与运行日报2026-06-01.pdf") is False


def test_rejects_exchange_monthly():
    # provincial exchange 月报 must NOT match — handled by is_exchange_report
    assert is_spot_monthly_pdf("山东电力交易中心2026年6月月报.pdf") is False


def test_requires_pdf_extension():
    assert is_spot_monthly_pdf("电力现货市场价格与运行月报（2026年6月）.xlsx") is False


def test_infer_month_full_width_parens():
    assert infer_report_month("电力现货市场价格与运行月报（2026年6月）.pdf") == dt.date(2026, 6, 1)


def test_infer_month_zero_padded():
    assert infer_report_month("电力现货市场价格与运行月报（2026年06月）.pdf") == dt.date(2026, 6, 1)


def test_infer_month_yearless_returns_none():
    assert infer_report_month("电力现货市场价格与运行月报（6月）.pdf") is None
