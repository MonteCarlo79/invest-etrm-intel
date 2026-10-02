import json

from academy.concepts import parse_concept, validate_concept
from academy.syllabus import (draft_track, render_review, run_syllabus,
                              stub_front_matter, write_syllabus)
from academy.tracks import TRACKS
from tests.fakes import FakeClient

AV = next(t for t in TRACKS if t["id"] == "asset_valuation")
REPLY = json.dumps({"concepts": [
    {"id": "option_basics", "scope": "calls and puts", "level": "foundation",
     "prerequisites": [], "markets": ["EU"], "sources": ["s1", "ghost"]},
    {"id": "spark_spread_option", "scope": "spread option", "level": "wizard",
     "prerequisites": ["option_basics"], "markets": ["MARS"], "sources": []},
]})


def test_draft_track_normalises_stubs():
    stubs = draft_track(FakeClient([REPLY]), "m", AV,
                        [{"source_id": "s1", "name": "x", "scope": "y"}], ["s1"])
    a, b = stubs
    assert a["sources"] == [{"id": "s1", "use": "background"}] and a["originality"] == "synthesized"
    assert b["level"] == "intermediate" and b["markets"] == ["EU"]
    assert b["sources"] == [] and b["originality"] == "original"
    assert a["track"] == "asset_valuation"
    assert validate_concept(stub_front_matter(a)) == []


def test_write_syllabus_files_and_errors(tmp_path):
    stubs = draft_track(FakeClient([REPLY]), "m", AV, [], ["s1"])
    errs = write_syllabus({"asset_valuation": stubs}, tmp_path)
    assert errs == []
    assert (tmp_path / "syllabus" / "tracks.yaml").exists()
    graph = (tmp_path / "syllabus" / "graph.yaml").read_text()
    assert "option_basics" in graph and "spark_spread_option" in graph
    fm, body = parse_concept(tmp_path / "concepts" / "asset_valuation" / "option_basics.md")
    assert fm["status"] == "stub" and "## Learning objectives" in body


def test_review_has_sizing_warning_pilot_and_gaps():
    md = render_review({"asset_valuation": [{"id": "a"}]}, ["x: bad"], ["modern_markets"])
    assert "asset_valuation: 1" in md and "x: bad" in md and "modern_markets" in md
    assert "fewer than 50" in md.lower() and "hedging_trading" in md


def test_run_syllabus_end_to_end(tmp_path):
    outlines = {"s1": {"concepts": [{"name": "Spark spread", "scope": "s"}]}}
    mappings = {"s1": {"Spark spread": "asset_valuation"}}
    replies = [json.dumps({"concepts": []}) for _ in TRACKS]
    replies[2] = REPLY
    rep = run_syllabus(outlines, mappings, FakeClient(replies), "m", tmp_path)
    assert rep["counts"]["asset_valuation"] == 2 and rep["errors"] == []
    assert (tmp_path / "review" / "syllabus_review.md").exists()
