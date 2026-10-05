import pytest

from academy.authoring import set_status
from academy.concepts import parse_concept, render_concept


def _concept(root, cid="c1", status="stub", with_lab=False):
    d = root / "concepts" / "asset_valuation"
    d.mkdir(parents=True, exist_ok=True)
    fm = {"id": cid, "track": "asset_valuation", "level": "intermediate",
          "prerequisites": [], "markets": ["EU"], "status": status,
          "sources": [{"id": "s", "use": "background"}], "originality": "synthesized",
          "translations": {"zh": {"status": "none", "en_hash": None}}}
    (d / f"{cid}.md").write_text(render_concept(fm, "body\n"), encoding="utf-8")
    if with_lab:
        lab = root / "labs" / cid
        lab.mkdir(parents=True)
        (lab / "test_lab.py").write_text("def test_ok():\n    assert True\n")
    return root


def test_reviewed_requires_passing_lab(tmp_path):
    root = _concept(tmp_path)
    with pytest.raises(ValueError, match="lab"):
        set_status(root, "c1", "reviewed", labs_root=tmp_path / "labs")
    root = _concept(tmp_path, with_lab=True)
    set_status(root, "c1", "reviewed", labs_root=tmp_path / "labs")
    fm, _ = parse_concept(tmp_path / "concepts" / "asset_valuation" / "c1.md")
    assert fm["status"] == "reviewed"


def test_reviewed_refused_when_lab_test_fails(tmp_path):
    root = _concept(tmp_path, with_lab=True)
    (tmp_path / "labs" / "c1" / "test_lab.py").write_text(
        "def test_bad():\n    assert False\n")
    with pytest.raises(ValueError, match="lab tests failing"):
        set_status(root, "c1", "reviewed", labs_root=tmp_path / "labs")


def test_published_requires_signoff_en(tmp_path):
    root = _concept(tmp_path, with_lab=True)
    set_status(root, "c1", "reviewed", labs_root=tmp_path / "labs")
    with pytest.raises(ValueError, match="signoff.en"):
        set_status(root, "c1", "published", labs_root=tmp_path / "labs")


def test_backward_status_allowed_without_gates(tmp_path):
    root = _concept(tmp_path)
    set_status(root, "c1", "drafted", labs_root=tmp_path / "labs")
    fm, _ = parse_concept(tmp_path / "concepts" / "asset_valuation" / "c1.md")
    assert fm["status"] == "drafted"


def test_published_also_requires_lab(tmp_path):
    root = _concept(tmp_path)          # no lab
    p = root / "concepts" / "asset_valuation" / "c1.md"
    from academy.concepts import parse_concept, render_concept
    fm, body = parse_concept(p)
    fm["signoff"] = {"en": "2026-10-05"}
    p.write_text(render_concept(fm, body), encoding="utf-8")
    with pytest.raises(ValueError, match="lab"):
        set_status(root, "c1", "published", labs_root=tmp_path / "labs")
