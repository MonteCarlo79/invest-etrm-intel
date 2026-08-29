"""Tests for province/asset-class book filtering in tab_pnl."""
import datetime as dt

import pytest
from sqlalchemy import create_engine, text

from apps.asset_risk.tab_pnl import book_matches_classes, ASSET_CLASS_OPTIONS


def test_single_classes():
    assert book_matches_classes("wind", "asset", ["wind"])
    assert book_matches_classes("bess", "asset", ["bess"])
    assert not book_matches_classes("wind", "asset", ["bess"])
    assert book_matches_classes("solar", "asset", ["solar"])
    assert book_matches_classes("thermal", "asset", ["thermal"])


def test_load_class_matches_book_type_not_asset_type():
    assert book_matches_classes(None, "load", ["load"])
    assert book_matches_classes("wind", "load", ["load"])  # load book with linked asset still load
    assert not book_matches_classes("wind", "asset", ["load"])
    assert not book_matches_classes("bess", "asset", ["load"])


def test_wind_plus_bess():
    assert book_matches_classes("wind", "asset", ["wind+bess"])
    assert book_matches_classes("bess", "asset", ["wind+bess"])
    assert not book_matches_classes("solar", "asset", ["wind+bess"])
    assert not book_matches_classes(None, "load", ["wind+bess"])


def test_wind_plus_bess_plus_load():
    assert book_matches_classes("wind", "asset", ["wind+bess+load"])
    assert book_matches_classes("bess", "asset", ["wind+bess+load"])
    assert book_matches_classes(None, "load", ["wind+bess+load"])
    assert not book_matches_classes("solar", "asset", ["wind+bess+load"])
    assert not book_matches_classes("thermal", "asset", ["wind+bess+load"])


def test_solar_plus_bess_plus_load():
    assert book_matches_classes("solar", "asset", ["solar+bess+load"])
    assert book_matches_classes("bess", "asset", ["solar+bess+load"])
    assert book_matches_classes(None, "load", ["solar+bess+load"])
    assert not book_matches_classes("wind", "asset", ["solar+bess+load"])


# --- Date-range filtering of the single-book settlement query ---

from apps.asset_risk.tab_pnl import _load_settlement_items  # noqa: E402


@pytest.fixture()
def sqlite_engine():
    """In-memory sqlite with marketdata schema attached (mirrors Postgres naming)."""
    eng = create_engine("sqlite:///:memory:")
    with eng.connect() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS marketdata"))
        conn.execute(text("CREATE TABLE marketdata.rm_settlements "
                          "(id INTEGER PRIMARY KEY, book_id INTEGER, settlement_month TEXT)"))
        conn.execute(text("CREATE TABLE marketdata.rm_settlement_items "
                          "(id INTEGER PRIMARY KEY, settlement_id INTEGER, category TEXT, "
                          "amount_cny REAL, volume_mwh REAL)"))
        for sid, month in [(1, "2026-01-01"), (2, "2026-02-01"), (3, "2026-03-01")]:
            conn.execute(text("INSERT INTO marketdata.rm_settlements (id, book_id, settlement_month) "
                              "VALUES (:sid, 7, :m)"), {"sid": sid, "m": month})
            conn.execute(text("INSERT INTO marketdata.rm_settlement_items "
                              "(settlement_id, category, amount_cny, volume_mwh) "
                              "VALUES (:sid, 'discharge_energy', :amt, 10)"),
                         {"sid": sid, "amt": sid * 1000.0})
        conn.commit()
    yield eng
    eng.dispose()


def test_no_date_range_returns_all_months(sqlite_engine):
    df = _load_settlement_items(sqlite_engine, 7, ())
    assert df["total"].iloc[0] == pytest.approx(6000.0)


def test_date_range_filters_to_selected_months(sqlite_engine):
    df = _load_settlement_items(sqlite_engine, 7, (dt.date(2026, 2, 1), dt.date(2026, 2, 28)))
    assert df["total"].iloc[0] == pytest.approx(2000.0)


def test_reversed_range_is_normalized(sqlite_engine):
    df = _load_settlement_items(sqlite_engine, 7, (dt.date(2026, 3, 15), dt.date(2026, 1, 1)))
    assert df["total"].iloc[0] == pytest.approx(6000.0)


def test_multiple_selected_classes_is_union():
    sel = ["wind", "load"]
    assert book_matches_classes("wind", "asset", sel)
    assert book_matches_classes(None, "load", sel)
    assert not book_matches_classes("bess", "asset", sel)


def test_empty_selection_matches_nothing():
    assert not book_matches_classes("bess", "asset", [])


def test_options_constant():
    assert "wind+bess" in ASSET_CLASS_OPTIONS
    assert "wind+bess+load" in ASSET_CLASS_OPTIONS
    assert "solar+bess+load" in ASSET_CLASS_OPTIONS
