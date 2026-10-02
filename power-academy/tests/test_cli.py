from academy.concepts import body_hash, render_concept
from academy.cli import validate_repo


def _fm(id_, prereq=(), level="foundation", zh=None):
    return {"id": id_, "track": "asset_valuation", "level": level,
            "prerequisites": list(prereq), "markets": ["EU"], "status": "stub",
            "sources": [{"id": "s", "use": "background"}], "originality": "synthesized",
            "translations": {"zh": zh or {"status": "none", "en_hash": None}}}


def _write(root, name, fm, body):
    p = root / "concepts" / "asset_valuation" / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(render_concept(fm, body), encoding="utf-8")


def _glossary(root):
    g = root / "glossary"
    g.mkdir()
    (g / "terms.yaml").write_text("terms:\n  - {en: spark spread, zh: 火花价差}\n", encoding="utf-8")


def test_validate_repo_clean(tmp_path):
    _glossary(tmp_path)
    en = "The spark spread.\n"
    _write(tmp_path, "a.md", _fm("a", zh={"status": "drafted", "en_hash": body_hash(en)}), en)
    _write(tmp_path, "a.zh.md", {"id": "a"}, "火花价差。\n")
    assert validate_repo(tmp_path) == []


def test_validate_repo_reports_stale_glossary_and_graph(tmp_path):
    _glossary(tmp_path)
    en = "The spark spread.\n"
    _write(tmp_path, "a.md", _fm("a", zh={"status": "drafted", "en_hash": "stale"}), en)
    _write(tmp_path, "a.zh.md", {"id": "a"}, "发电价差。\n")
    _write(tmp_path, "b.md", _fm("b", ["missing"], "intermediate"), "x\n")
    errs = " | ".join(validate_repo(tmp_path))
    assert "a: zh translation is stale" in errs
    assert "spark spread" in errs
    assert "unknown prerequisite 'missing'" in errs


def test_zh_file_without_en_reported(tmp_path):
    _glossary(tmp_path)
    _write(tmp_path, "orphan.zh.md", {"id": "orphan"}, "x\n")
    assert any("orphan.zh.md" in e and "no English" in e for e in validate_repo(tmp_path))
