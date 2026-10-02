from academy.graph import validate_graph


def c(id_, prereq=(), level="foundation", sources=("s",), orig="synthesized"):
    return {"id": id_, "level": level, "prerequisites": list(prereq),
            "sources": list(sources), "originality": orig}


def test_clean_graph_has_no_errors():
    g = [c("a"), c("b", ["a"], "intermediate"), c("d", ["b"], "advanced")]
    assert validate_graph(g) == []


def test_unknown_prerequisite_reported():
    assert validate_graph([c("a", ["zzz"])]) == ["a: unknown prerequisite 'zzz'"]


def test_cycle_reported():
    errs = validate_graph([c("a", ["b"]), c("b", ["a"])])
    assert any("cycle" in e for e in errs)


def test_orphan_root_must_be_foundation():
    errs = validate_graph([c("a", level="advanced")])
    assert errs == ["a: chain ends at non-foundation root 'a'"]


def test_synthesized_without_sources_reported_original_ok():
    assert validate_graph([c("a", sources=[])]) == ["a: synthesized concept has no sources"]
    assert validate_graph([c("a", sources=[], orig="original")]) == []
