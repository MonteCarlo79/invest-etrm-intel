import json

from academy.coverage import (build_matrix, gaps, render_coverage_md,
                              run_coverage, tag_source)
from academy.tracks import TRACKS
from tests.fakes import FakeClient

OUT = {"concepts": [{"name": "Spark spread", "scope": "s"}, {"name": "Weird", "scope": "w"},
                    {"name": "Hedge ratio", "scope": "h"}]}


def test_tracks_are_the_eight_expected():
    assert [t["id"] for t in TRACKS] == [
        "market_fundamentals", "price_curve_modelling", "asset_valuation",
        "optimisation_dispatch", "storage_flexibility", "hedging_trading",
        "risk_management", "modern_markets"]


def test_tag_source_coerces_invalid_track_ids():
    reply = json.dumps({"mapping": {"Spark spread": "asset_valuation",
                                    "Weird": "not_a_track", "Hedge ratio": None}})
    m = tag_source(FakeClient([reply]), "m", "src", OUT)
    assert m == {"Spark spread": "asset_valuation", "Weird": None, "Hedge ratio": None}


def test_matrix_gaps_and_render():
    maps = {"a": {"x": "asset_valuation", "y": "asset_valuation"},
            "b": {"z": "asset_valuation", "w": None}}
    matrix = build_matrix(maps)
    assert matrix["asset_valuation"] == {"a": 2, "b": 1}
    g = gaps(matrix, min_sources=2)
    assert "asset_valuation" not in g and "modern_markets" in g and len(g) == 7
    md = render_coverage_md(matrix, {"a": "Paper A", "b": "Paper B"}, g)
    assert "Paper A" in md and "GAP" in md and "modern_markets" in md


def test_run_coverage_isolates_failures(tmp_path):
    outlines = {"a": OUT, "b": OUT}
    good = json.dumps({"mapping": {"Spark spread": "asset_valuation"}})
    c = FakeClient([good, "garbage", "garbage"])
    rep = run_coverage(outlines, c, "m", tmp_path, {"a": "A", "b": "B"})
    assert list(rep["failed"]) == ["b"]
    assert (tmp_path / "coverage_map.md").exists()
    assert set(json.loads((tmp_path / "_mappings.json").read_text())) == {"a"}
