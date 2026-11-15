"""DB-free unit tests for the settlement bridge — DDL shape, pro-rata spread,
book-name map. No RDS in unit tests: refresh_settled_daily itself is exercised
against the live DB by callers, not here.
"""
from datetime import date

from services.mengxi_nodal.settlement_bridge import _DDL, _spread_lump


def test_ddl_has_accrual_grade():
    assert "accrual_grade" in _DDL and "book_settled_daily" in _DDL


def test_ddl_pk_and_columns():
    for col in ("asset_code", "data_date", "amount_cny", "source"):
        assert col in _DDL
    assert "PRIMARY KEY" in _DDL and "reports" in _DDL


def test_spread_lump_prorata():
    rows = _spread_lump(3100.0, date(2026, 10, 1), date(2026, 10, 31))
    assert len(rows) == 31
    assert abs(sum(r[1] for r in rows) - 3100.0) < 0.01
    assert all(r[2] == "accrual" for r in rows)


def test_spread_lump_residual_lands_on_last_day():
    # 1000 over 28 days does not divide evenly — even split + last-day residual
    rows = _spread_lump(1000.0, date(2026, 2, 1), date(2026, 2, 28))
    assert len(rows) == 28
    assert abs(sum(r[1] for r in rows) - 1000.0) < 1e-9
    per = 1000.0 / 28
    assert all(abs(r[1] - per) < 1e-9 for r in rows[:-1])
    assert rows[0][0] == date(2026, 2, 1) and rows[-1][0] == date(2026, 2, 28)


def test_spread_lump_single_day_and_negative():
    rows = _spread_lump(-500.0, date(2026, 3, 31), date(2026, 3, 31))
    assert rows == [(date(2026, 3, 31), -500.0, "accrual")]


def test_registry_only_stations_skip_loudly(capsys):
    # Skip happens before any engine use, so engine=None proves it.
    from services.mengxi_nodal.settlement_bridge import refresh_settled_daily
    # wuchuan/alashan left registry_only 2026-11-11 (exchange accounts mapped).
    for code in ("alashanzuoqi", "erlian"):
        assert refresh_settled_daily(None, code, "2026-10") == 0
    out = capsys.readouterr().out
    assert out.count("WARNING") == 2 and out.count("registry_only") == 2
    assert "erlian" in out


def test_unknown_code_raises_before_engine_use():
    import pytest
    from services.mengxi_nodal.settlement_bridge import refresh_settled_daily
    with pytest.raises(KeyError):
        refresh_settled_daily(None, "nope", "2026-10")
