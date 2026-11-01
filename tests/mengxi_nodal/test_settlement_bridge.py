"""DB-free unit tests for the settlement bridge — DDL shape, pro-rata spread,
book-name map. No RDS in unit tests: refresh_settled_daily itself is exercised
against the live DB by callers, not here.
"""
from datetime import date

from services.mengxi_nodal.settlement_bridge import _DDL, _RM_ASSET_NAME, _spread_lump


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


def test_book_name_map_covers_settlement_capable_stations():
    # Every station with a settlement read path (full / settlement_only) must
    # resolve to an rm_assets.name; registry_only stations are pre-mapped only
    # when an rm entry already exists (wuchuan/alashan) — the refresh writer
    # skips them loudly either way.
    from libs.trading_book.registry import get_stations
    for s in get_stations():
        if s.data_path != "registry_only":
            assert _RM_ASSET_NAME[s.asset_code]


def test_task11_rm_names():
    # Verified against the rm_assets probe (task brief).
    assert _RM_ASSET_NAME["wuhai"] == "乌海康富"
    assert _RM_ASSET_NAME["wuchuan"] == "武川"
    assert _RM_ASSET_NAME["alashan"] == "阿拉善储能"
    assert _RM_ASSET_NAME["lingt46"] == "零碳46风电"


def test_wind_without_rm_book_absent_from_map():
    # W-3/W-4 have no rm_assets entry — deliberately unmapped.
    assert "alashanzuoqi" not in _RM_ASSET_NAME
    assert "erlian" not in _RM_ASSET_NAME


def test_registry_only_stations_skip_loudly(capsys):
    # Skip happens before any engine use, so engine=None proves it.
    from services.mengxi_nodal.settlement_bridge import refresh_settled_daily
    for code in ("wuchuan", "alashan", "alashanzuoqi", "erlian"):
        assert refresh_settled_daily(None, code, "2026-10") == 0
    out = capsys.readouterr().out
    assert out.count("WARNING") == 4 and out.count("registry_only") == 4
    assert "erlian" in out


def test_unknown_code_raises_before_engine_use():
    import pytest
    from services.mengxi_nodal.settlement_bridge import refresh_settled_daily
    with pytest.raises(KeyError):
        refresh_settled_daily(None, "nope", "2026-10")
