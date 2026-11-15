from datetime import date

import pytest
from sqlalchemy import create_engine, event, text

from libs.asset_risk.legs_store import ensure_table, get_or_compute, is_month_complete


@pytest.fixture()
def sqlite_engine(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'main.db'}")

    @event.listens_for(eng, "connect")
    def _attach(dbapi_conn, _):
        dbapi_conn.execute(f"ATTACH DATABASE '{tmp_path / 'md.db'}' AS marketdata")

    ensure_table(eng)
    return eng


def _row_count(eng):
    with eng.connect() as c:
        return c.execute(text("SELECT COUNT(*) FROM marketdata.rm_waterfall_legs")).scalar()


def _stored(eng, asset_id, month, leg):
    with eng.connect() as c:
        return dict(c.execute(text(
            "SELECT * FROM marketdata.rm_waterfall_legs WHERE asset_id=:a AND month=:m AND leg=:l"),
            {"a": asset_id, "m": month, "l": leg}).mappings().one())


def test_incomplete_month_is_computed_but_not_stored(sqlite_engine):
    calls = []
    out = get_or_compute(sqlite_engine, "benchmark", 1, "2026-11", lambda: calls.append(1) or {"arb_cny": 1.0, "dis_mwh": 2.0, "chg_mwh": 2.1, "cap_cny": 0.7, "days_solved": 10, "days_skipped": 20, "age": 1, "note": ""},
                         today=date(2026, 11, 14))
    assert out["arb_cny"] == 1.0 and calls == [1]
    assert _row_count(sqlite_engine) == 0


def test_complete_month_is_stored_then_served_without_recompute(sqlite_engine):
    calls = []
    f = lambda: calls.append(1) or {"arb_cny": 5.0, "dis_mwh": 1, "chg_mwh": 1, "cap_cny": 1, "days_solved": 30, "days_skipped": 0, "age": 0, "note": "节点填充电价"}
    get_or_compute(sqlite_engine, "benchmark", 1, "2026-09", f, today=date(2026, 11, 14))
    out = get_or_compute(sqlite_engine, "benchmark", 1, "2026-09", f, today=date(2026, 11, 14))
    assert calls == [1] and out["note"] == "节点填充电价" and _row_count(sqlite_engine) == 1
    assert _stored(sqlite_engine, 1, "2026-09", "benchmark")["price_source"] == "节点填充电价"


def test_force_recomputes_and_overwrites(sqlite_engine):
    get_or_compute(sqlite_engine, "forecast", 1, "2026-09", lambda: {"arb_cny": 1.0}, today=date(2026, 11, 14))
    out = get_or_compute(sqlite_engine, "forecast", 1, "2026-09", lambda: {"arb_cny": 9.0}, force=True, today=date(2026, 11, 14))
    assert out["arb_cny"] == 9.0 and _stored(sqlite_engine, 1, "2026-09", "forecast")["arb_cny"] == 9.0


def test_forecast_partial_dict_roundtrips_with_none(sqlite_engine):
    get_or_compute(sqlite_engine, "forecast", 2, "2026-09", lambda: {"arb_cny": 3.0, "note": ""}, today=date(2026, 11, 14))
    out = get_or_compute(sqlite_engine, "forecast", 2, "2026-09", lambda: 1 / 0, today=date(2026, 11, 14))
    assert out["arb_cny"] == 3.0 and out.get("dis_mwh") is None


def test_is_month_complete_boundary():
    assert not is_month_complete("2026-10", date(2026, 11, 3))
    assert is_month_complete("2026-10", date(2026, 11, 4))
