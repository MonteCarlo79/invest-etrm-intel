from academy.authoring import build_pack, find_concept
from academy.concepts import render_concept


def _setup(root):
    (root / "concepts" / "asset_valuation").mkdir(parents=True)
    fm = {"id": "spark_dark_spread_fundamentals", "track": "asset_valuation",
          "level": "foundation", "prerequisites": [], "markets": ["EU"],
          "status": "stub",
          "sources": [{"id": "clewlow", "use": "background"},
                      {"id": "power_european_toll", "use": "practice_example"}],
          "originality": "synthesized",
          "translations": {"zh": {"status": "none", "en_hash": None}}}
    (root / "concepts" / "asset_valuation" / "spark_dark_spread_fundamentals.md"
     ).write_text(render_concept(fm, "## Learning objectives\n"), encoding="utf-8")
    (root / "inventory").mkdir()
    (root / "inventory" / "clewlow.md").write_text("# Clewlow\n\n- **MRJDx** — scope\n",
                                                  encoding="utf-8")
    (root / "cache").mkdir()
    (root / "cache" / "clewlow.txt").write_text("x" * 9000, encoding="utf-8")
    return root


def test_find_concept_locates_file(tmp_path):
    _setup(tmp_path)
    assert find_concept(tmp_path, "spark_dark_spread_fundamentals").name == \
        "spark_dark_spread_fundamentals.md"
    import pytest
    with pytest.raises(FileNotFoundError):
        find_concept(tmp_path, "nope")


def test_pack_contains_stub_outlines_and_bounded_excerpts(tmp_path):
    pack = build_pack(_setup(tmp_path), "spark_dark_spread_fundamentals")
    assert "spark_dark_spread_fundamentals" in pack and "MRJDx" in pack
    assert "clewlow" in pack and "power_european_toll" in pack
    # 9 000-char cache file must be bounded to the 4 000-char excerpt
    assert "x" * 5000 not in pack and ("x" * 4000) in pack
    assert "(no cached text)" in pack          # power_european_toll has no cache file
