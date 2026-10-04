from academy.concepts import (body_hash, parse_concept, render_concept,
                              validate_concept, zh_is_stale)


def _fm(**kw):
    fm = {"id": "spark_spread_option", "track": "asset_valuation", "level": "intermediate",
          "prerequisites": [], "markets": ["EU", "GB"], "status": "stub",
          "sources": [], "originality": "original",
          "translations": {"zh": {"status": "none", "en_hash": None}}}
    fm.update(kw)
    return fm


def test_valid_stub_passes_and_roundtrips(tmp_path):
    assert validate_concept(_fm()) == []
    p = tmp_path / "c.md"
    p.write_text(render_concept(_fm(), "## Intuition\nx\n"), encoding="utf-8")
    fm, body = parse_concept(p)
    assert fm["id"] == "spark_spread_option" and body.startswith("## Intuition")


def test_enum_and_required_errors():
    errs = validate_concept(_fm(level="expert", originality="derived", markets=["MARS"]))
    joined = " | ".join(errs)
    assert "level" in joined and "originality" in joined and "markets" in joined
    assert validate_concept({"id": "x"})  # missing fields reported


def test_reviewed_needs_lab_or_reason(tmp_path):
    labs = tmp_path / "labs"
    assert validate_concept(_fm(status="reviewed"), labs)  # no lab, no reason
    assert validate_concept(_fm(status="reviewed", no_lab_reason="pure theory"), labs) == []
    (labs / "spark_spread_option").mkdir(parents=True)
    (labs / "spark_spread_option" / "run.py").write_text("x")
    assert validate_concept(_fm(status="reviewed"), labs) == []


def test_published_needs_signoff_and_zh_signoff_needs_reviewed_zh():
    base = dict(status="published", no_lab_reason="n/a")
    assert validate_concept(_fm(**base))  # no signoff
    ok = _fm(**base, signoff={"en": "2026-10-02"})
    assert validate_concept(ok) == []
    bad = _fm(**base, signoff={"en": "2026-10-02", "zh": "2026-10-03"})
    assert any("zh" in e for e in validate_concept(bad))


def test_zh_staleness():
    en = "## Intuition\nforward curve\n"
    tr = {"status": "drafted", "en_hash": body_hash(en)}
    fm = _fm(translations={"zh": tr})
    assert zh_is_stale(fm, en, tr) is False
    assert zh_is_stale(fm, en + "changed", tr) is True
    tr_nohash = {"status": "drafted", "en_hash": None}
    assert zh_is_stale(fm, en, tr_nohash) is True      # drafted without a hash
    assert zh_is_stale(fm, en, {"status": "none", "en_hash": None}) is False
