import pytest
import sqlalchemy as sa

from academy.column.evidence import build_pack, verify_pack
from academy.column.schema import new_article
from academy.io import dump_yaml


def _kb_engine():
    e = sa.create_engine("sqlite://")
    with e.begin() as c:
        c.execute(sa.text("create table docs (id int, title text, active bool, created_at text)"))
        c.execute(sa.text("create table chunks (id int, doc_id int, page_no int, chunk_index int, chunk_text text)"))
        c.execute(sa.text("insert into docs values (1,'山东电力现货市场规则(试行)',1,'2026-09-01')"))
        c.execute(sa.text("insert into chunks values (1,1,3,0,:t)"),
                  {"t": "第三十二条 现货电能量市场采用节点边际电价机制 " * 20})
    return e


def test_kb_excerpt_capped_with_citation(tmp_path, monkeypatch):
    import academy.column.evidence as ev
    monkeypatch.setattr(ev, "KB_DOCS_TABLE", "docs")
    monkeypatch.setattr(ev, "KB_CHUNKS_TABLE", "chunks")
    d = new_article(tmp_path, 1, "demo")
    dump_yaml({"queries": [{"id": "ex_rule32", "kind": "kb_excerpt",
                            "source": "staging.spot_knowledge_docs",
                            "title_like": "山东电力现货市场规则", "max_chars": 120}]},
              d / "evidence" / "queries.yaml")
    entries = build_pack(d, _kb_engine(), {"staging.spot_knowledge_docs": "public"},
                         today="2026-10-04")
    e = entries[0]
    assert e["kind"] == "policy_excerpt" and e["license"] == "public"
    md = (d / "evidence" / "excerpts" / "ex_rule32.md").read_text(encoding="utf-8")
    assert "山东电力现货市场规则(试行)" in md and "第三十二条" in md and len(md) < 500


def test_western_fact_is_citation_only(tmp_path):
    d = new_article(tmp_path, 2, "demo2")
    dump_yaml({"queries": [{"id": "wf_merit", "kind": "western_fact", "concept_id": "merit_order",
                            "source_id": "clewlow", "note": "stack ordering logic"}]},
              d / "evidence" / "queries.yaml")
    entries = build_pack(d, None, {}, today="2026-10-04")
    e = entries[0]
    assert e["kind"] == "western_fact" and e["license"] == "own"
    assert e["concept_id"] == "merit_order" and "sha256" not in e


def test_chart_entry_runs_script_and_hash(tmp_path):
    d = new_article(tmp_path, 3, "demo3")
    (d / "evidence" / "data" / "e_x.csv").write_text("a\n1\n2\n", encoding="utf-8")
    script = d / "evidence" / "charts" / "e_c.py"
    script.write_text(
        "import matplotlib; matplotlib.use('Agg')\n"
        "import matplotlib.pyplot as plt, sys\n"
        "plt.figure(); plt.plot([1,2],[1,2])\n"
        "plt.savefig(sys.argv[1])\n", encoding="utf-8")
    dump_yaml({"queries": [{"id": "e_c", "kind": "chart", "script": "e_c.py",
                            "inputs": ["e_x"], "public": True}]},
              d / "evidence" / "queries.yaml")
    entries = build_pack(d, None, {}, today="2026-10-04")
    e = entries[0]
    assert e["kind"] == "chart" and e["public"] is True and len(e["sha256"]) == 64
    assert (d / "evidence" / "charts" / "e_c.png").exists()
    # license leak: restricted input on a public chart must be caught at build time
    dump_yaml({"entries": [{"id": "e_x", "kind": "data", "license": "licensed_restricted"}]},
              d / "evidence" / "manifest.yaml")
    with pytest.raises(ValueError, match="licensed_restricted"):
        build_pack(d, None, {}, today="2026-10-04")


def test_verify_pack_reports_changes(tmp_path):
    d = new_article(tmp_path, 4, "demo4")
    e = sa.create_engine("sqlite://")
    with e.begin() as c:
        c.execute(sa.text("create table p (a int)"))
        c.execute(sa.text("insert into p values (1)"))
    dump_yaml({"queries": [{"id": "e_d", "kind": "sql", "source": "t", "sql": "select * from p"}]},
              d / "evidence" / "queries.yaml")
    build_pack(d, e, {}, today="2026-10-04")
    assert verify_pack(d, e) == []
    with e.begin() as c:
        c.execute(sa.text("insert into p values (2)"))
    assert verify_pack(d, e) == ["e_d: data changed since retrieval"]
