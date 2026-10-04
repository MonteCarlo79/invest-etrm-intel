from academy.registry import (load_index, models_catalogue, register_library,
                              register_practice, slugify, write_index)


def _touch(p, data=b"x"):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)


def test_slugify_cjk_and_symbols():
    assert slugify("火花价差 notes (v2).pdf") == "火花价差_notes_v2_pdf"
    assert slugify("???") == "source"
    assert len(slugify("a" * 200)) == 60


def test_library_dedupes_same_stem_and_skips_noise(tmp_path):
    lib = tmp_path / "lib"
    _touch(lib / "Hedging" / "Plan.pdf")
    _touch(lib / "Valuation" / "Plan.pdf")          # same stem, other folder
    _touch(lib / "Swing" / "deck.ppt")
    _touch(lib / "Swing" / "pic.png")               # not a document
    _touch(lib / "workspace" / "docs" / "readme.txt")  # skipped top-level
    _touch(lib / "Hedging" / ".git" / "x.pdf")      # skipped dir
    entries = register_library(lib, set())
    ids = [e.id for e in entries]
    assert len(ids) == len(set(ids)) == 3
    assert sorted(e.type for e in entries) == ["pdf", "pdf", "ppt"]
    assert all(e.source_class == "library" and not e.cleared and e.license_risk == "high"
               for e in entries)


def test_practice_one_entry_per_folder_and_cross_root_unique(tmp_path):
    a, b = tmp_path / "power", tmp_path / "dipeng"
    (a / "Battery").mkdir(parents=True)
    (b / "battery").mkdir(parents=True)
    (b / ".hidden").mkdir()
    seen = set()
    ea = register_practice(a, seen)
    eb = register_practice(b, seen)
    assert [e.id for e in ea] == ["power_battery"]
    assert [e.id for e in eb] == ["dipeng_battery"]
    assert ea[0].type == "folder" and ea[0].cleared and ea[0].license_risk == "low"


def test_models_catalogue_and_index_roundtrip(tmp_path):
    _touch(tmp_path / "m" / "a.xlsx")
    _touch(tmp_path / "m" / "b.py")
    _touch(tmp_path / "m" / "c.pdf")
    cat = models_catalogue(tmp_path)
    assert sorted(c["ext"] for c in cat) == [".py", ".xlsx"]
    lib = tmp_path / "m"
    entries = register_library(lib, set())
    write_index(entries, tmp_path / "index.yaml")
    assert load_index(tmp_path / "index.yaml") == entries
