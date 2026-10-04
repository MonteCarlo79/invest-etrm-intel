from academy.column.gates import (check_blocklist, check_compliance,
                                  check_license, check_terms, run_gates)
from academy.column.schema import new_article
from academy.io import dump_yaml


def test_license_blocks_nonpublic_chart_reference():
    entries = [{"id": "c1", "kind": "chart", "public": False},
               {"id": "d1", "kind": "data", "license": "licensed_restricted"}]
    assert check_license("见下图。\n[[C:c1]]", entries)
    entries_pub = [{"id": "c1", "kind": "chart", "public": True}]
    assert check_license("[[C:c1]]", entries_pub) == []


def test_blocklist_case_and_cjk():
    texts = {"draft": "Shell Energy曾…", "excerpt": "与Counterparty签署"}
    hits = check_blocklist(texts, ["shell energy", "counterparty"])
    assert len(hits) == 2 and "draft" in hits[0]
    assert check_blocklist(texts, ["其他公司"]) == []


def test_compliance_flags_with_line_numbers():
    hits = check_compliance("价格将涨到0.8元。\n机制值得借鉴。", ["将涨到", "稳赚"])
    assert hits == ["line 1: risky pattern '将涨到'"]


def test_terms_variant_flagged():
    hits = check_terms("现货市场价格波动", [{"canonical": "现货市场", "variants": ["即期市场"]}])
    assert hits == []
    hits = check_terms("即期市场价格波动", [{"canonical": "现货市场", "variants": ["即期市场"]}])
    assert hits == ["use '现货市场' instead of '即期市场'"]


def test_run_gates_writes_auto_section_preserving_owner_text(tmp_path):
    root = tmp_path
    (root / "style").mkdir()
    for name, obj in [("blocklist.yaml", {"names": ["ACME"]}),
                      ("compliance_patterns.yaml", {"patterns": ["将涨到"]}),
                      ("zh_terms.yaml", {"terms": [{"canonical": "现货", "variants": ["即期"]}]})]:
        dump_yaml(obj, root / "style" / name)
    d = new_article(root, 1, "demo")
    (d / "draft.zh.md").write_text("价格将涨到1元/kWh。[[E:e1]]\n", encoding="utf-8")
    dump_yaml({"entries": [{"id": "e1", "kind": "data", "license": "own"}]},
              d / "evidence" / "manifest.yaml")
    (d / "gates.md").write_text(
        "# G\n\n<!-- auto:begin -->\nold\n<!-- auto:end -->\n\nowner_signoff: 张三 2026-10-05\n",
        encoding="utf-8")
    res = run_gates(d, root)
    assert res["fact_trace"] == "pass" and res["compliance"] == "flagged:1"
    body = (d / "gates.md").read_text(encoding="utf-8")
    assert "old" not in body and "张三 2026-10-05" in body and "fact_trace" in body
