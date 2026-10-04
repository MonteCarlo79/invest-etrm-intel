import pytest

from academy.column.schema import new_article, validate_brief


def _fm(**kw):
    fm = {"id": "003-test", "byline": "pen_name", "status": "idea",
          "thesis": "t", "western": {"concept_ids": ["merit_order"], "markets": ["GB"]},
          "china": {"provinces": ["山东"], "topics": ["现货"]},
          "derivatives_angle": "d", "questions": ["q"], "readers": ["A", "C"]}
    fm.update(kw)
    return fm


def test_valid_brief_passes():
    assert validate_brief(_fm()) == []


def test_enum_and_required_errors():
    errs = " | ".join(validate_brief(_fm(status="ready", byline="anon", readers=["B"])))
    assert "status" in errs and "byline" in errs and "readers" in errs
    assert validate_brief({"id": "x"})


def test_new_article_creates_skeleton(tmp_path):
    d = new_article(tmp_path, 3, "merit-order-and-shandong-spot")
    assert d.name == "003-merit-order-and-shandong-spot"
    assert (d / "brief.md").exists() and (d / "evidence" / "data").is_dir()
    assert (d / "evidence" / "charts").is_dir() and (d / "gates.md").exists()
    fm_text = (d / "brief.md").read_text(encoding="utf-8")
    assert "003-merit-order-and-shandong-spot" in fm_text and "status: idea" in fm_text
    with pytest.raises(FileExistsError):
        new_article(tmp_path, 3, "merit-order-and-shandong-spot")
