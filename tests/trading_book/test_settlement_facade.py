"""DB-free unit tests for the settlement facade — aggregation shape, month
bounds, and the contract seam AssetBook.settlement() lazy-imports. No RDS in
unit tests: get_settlement itself runs against the live DB via callers.
"""
import inspect
from datetime import date

from libs.trading_book import settlement_bridge
from libs.trading_book.settlement_bridge import _SQL, _aggregate, month_bounds


def test_facade_contract_signature():
    # libs.trading_book.asset_book.AssetBook.settlement() lazy-imports exactly
    # this callable — keep the seam stable.
    sig = inspect.signature(settlement_bridge.get_settlement)
    assert list(sig.parameters) == ["engine", "station", "period"]


def test_month_bounds():
    assert month_bounds("2026-10") == (date(2026, 10, 1), date(2026, 10, 31))
    assert month_bounds("2026-02") == (date(2026, 2, 1), date(2026, 2, 28))
    assert month_bounds("2028-02") == (date(2028, 2, 1), date(2028, 2, 29))  # leap


def test_aggregate_merges_same_day_same_grade():
    rows = [
        (date(2026, 10, 5), 100.0, "bill"),
        (date(2026, 10, 5), -40.0, "bill"),     # second charge stream, same day+grade
        (date(2026, 10, 6), 25.0, "bill"),
    ]
    s = _aggregate(rows, "2026-10")
    assert s.period == "2026-10"
    assert s.daily == [(date(2026, 10, 5), 60.0, "bill"),
                       (date(2026, 10, 6), 25.0, "bill")]
    assert s.settled_cny == 85.0


def test_aggregate_keeps_grades_separate_and_ordered():
    rows = [
        (date(2026, 10, 2), 7.0, "accrual"),
        (date(2026, 10, 1), 5.0, "bill"),
        (date(2026, 10, 1), 3.0, "accrual"),
    ]
    s = _aggregate(rows, "2026-10")
    assert s.daily == [(date(2026, 10, 1), 3.0, "accrual"),
                       (date(2026, 10, 1), 5.0, "bill"),
                       (date(2026, 10, 2), 7.0, "accrual")]
    assert s.settled_cny == 15.0


def test_aggregate_empty_month():
    s = _aggregate([], "2026-10")
    assert s.settled_cny == 0.0 and s.daily == []


def test_sql_targets_book_settled_daily():
    assert "book_settled_daily" in _SQL and "asset_code" in _SQL
    assert "data_date" in _SQL and "accrual_grade" in _SQL
