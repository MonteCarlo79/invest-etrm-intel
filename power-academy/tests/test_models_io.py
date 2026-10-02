from academy.io import dump_yaml, load_yaml
from academy.models import SourceEntry


def test_yaml_roundtrip_keeps_chinese(tmp_path):
    p = tmp_path / "x" / "a.yaml"
    dump_yaml({"term": "火花价差", "n": 1}, p)
    assert load_yaml(p) == {"term": "火花价差", "n": 1}
    assert "火花价差" in p.read_text(encoding="utf-8")


def test_source_entry_class_key_roundtrip():
    e = SourceEntry(id="a", path="/p", title="t", type="pdf",
                    source_class="library", cleared=False, license_risk="high")
    d = e.to_dict()
    assert d["class"] == "library" and "source_class" not in d
    assert SourceEntry.from_dict(d) == e
