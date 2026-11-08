# tests/exchange_reports/test_watcher_month_parse.py
"""infer_report_month bare-YYYYMM pattern + watcher candidate filter.

Regression for the aggregate-batch gap (2026-11-08): files named
省-2026MM-披露-月度信息披露报告.pdf fell through every pattern → "Cannot infer
report_month" → KB-rerouted, and the KB∪monthly skip set then kept them out of
the metrics path forever.
"""
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from services.exchange_reports.ingestor import infer_report_month  # noqa: E402
from services.exchange_reports.watcher import _is_candidate  # noqa: E402


def test_bare_yyyymm():
    assert infer_report_month("安徽-202608-披露-月度信息披露报告.pdf") == date(2026, 8, 1)
    assert infer_report_month("山西-202609-披露-月度信息披露报告.pdf") == date(2026, 9, 1)
    assert infer_report_month("甘肃-202608-电力市场信息披露报告.pdf") == date(2026, 8, 1)


def test_bare_yyyymm_rejects_timestamps_and_bad_months():
    # 蒙西 timestamp suffix — month comes from the 2026年7月 part instead
    assert infer_report_month("蒙西电力市场2026年7月信息披露报告20260814155020.pdf") == date(2026, 7, 1)
    assert infer_report_month("某省-202613-披露.pdf") is None        # month 13
    assert infer_report_month("某省-202600-披露.pdf") is None        # month 00


def test_existing_patterns_untouched():
    assert infer_report_month("2026年9月安徽电力市场信息披露月报.pdf") == date(2026, 9, 1)
    assert infer_report_month("安徽-2026-07-电力市场信息披露月报.pdf") == date(2026, 7, 1)
    assert infer_report_month("安徽电力市场2026年一季度市场交易信息公报.pdf") == date(2026, 1, 1)
    assert infer_report_month("安徽-2026-09-结算情况截图.png") is not None  # month from name


def test_candidate_filter():
    monthly = {"already-ingested.pdf"}
    kb = {"rerouted-monthly.pdf", "annual-report.pdf"}
    assert not _is_candidate("already-ingested.pdf", monthly, kb)
    # KB-known but month-parseable → candidate again (reprocess path)
    assert _is_candidate("安徽-202608-披露-月度信息披露报告.pdf", monthly, kb)
    # KB-known and NOT month-parseable → stays skipped
    assert not _is_candidate("annual-report.pdf", monthly, kb)
    # brand new file
    assert _is_candidate("2026年10月山东电力市场信息披露月报.pdf", monthly, kb)
