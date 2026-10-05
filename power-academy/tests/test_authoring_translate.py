import json

from academy.authoring import translate_concept
from academy.concepts import body_hash, parse_concept, render_concept
from academy.io import dump_yaml
from tests.fakes import FakeClient


def _setup(root):
    d = root / "concepts" / "asset_valuation"
    d.mkdir(parents=True, exist_ok=True)
    fm = {"id": "c1", "track": "asset_valuation", "level": "intermediate",
          "prerequisites": [], "markets": ["EU"], "status": "reviewed",
          "sources": [], "originality": "original",
          "translations": {"zh": {"status": "none", "en_hash": None}}}
    body = "## Intuition\nThe spark spread matters.\n"
    (d / "c1.md").write_text(render_concept(fm, body), encoding="utf-8")
    dump_yaml({"terms": [{"en": "spark spread", "zh": "火花价差"}]},
              root / "glossary" / "terms.yaml")
    return root, body


def test_translate_writes_zh_and_stamps_hash(tmp_path):
    root, body = _setup(tmp_path)
    c = FakeClient([json.dumps({"zh_body": "## 直觉\n火花价差很重要。\n"}, ensure_ascii=False)])
    out = translate_concept(root, "c1", c, "m")
    assert out.name == "c1.zh.md"
    _, zh_body = parse_concept(out)
    assert "火花价差" in zh_body
    fm, _ = parse_concept(root / "concepts" / "asset_valuation" / "c1.md")
    assert fm["translations"]["zh"] == {"status": "drafted", "en_hash": body_hash(body)}
    # glossary was injected into the prompt
    assert "火花价差" in c.calls[0]["system"]
