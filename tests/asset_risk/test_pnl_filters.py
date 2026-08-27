"""Tests for province/asset-class book filtering in tab_pnl."""
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
