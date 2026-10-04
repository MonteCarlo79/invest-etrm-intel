import pytest

from academy.column.render import render_article
from academy.column.schema import check_approval_ready, new_article
from academy.io import dump_yaml


def _article(tmp_path):
    root = tmp_path
    (root / "style").mkdir()
    (root / "style" / "disclaimer.md").write_text("免责声明：本文仅为研究交流。", encoding="utf-8")
    d = new_article(root, 1, "demo")
    png = d / "evidence" / "charts" / "c1.png"
    png.write_bytes(b"\x89PNG\r\n\x1a\n" + b"x" * 100)
    dump_yaml({"entries": [
        {"id": "e1", "kind": "data", "source": "marketdata.spot_prices_hourly",
         "retrieved_at": "2026-10-04", "license": "own"},
        {"id": "c1", "kind": "chart", "public": True, "retrieved_at": "2026-10-04",
         "license": "own"}]}, d / "evidence" / "manifest.yaml")
    (d / "draft.zh.md").write_text(
        "# 测试标题\n\n均价0.42元/kWh。[[E:e1]]\n\n[[C:c1]]\n\n<script>alert(1)</script>\n",
        encoding="utf-8")
    (d / "gates.md").write_text(
        "<!-- auto:begin -->\n- fact_trace: pass\n- license: pass\n"
        "- confidentiality: pass\n- compliance: pass\n- terminology: pass\n"
        "<!-- auto:end -->\n\nowner_signoff: 张三 2026-10-05\n", encoding="utf-8")
    return root, d


def test_render_embeds_chart_refs_and_strips_bad_tags(tmp_path):
    root, d = _article(tmp_path)
    out = render_article(d, root)
    html = out.read_text(encoding="utf-8")
    assert "[[E:" not in html and "<script" not in html
    assert "data:image/png;base64," in html
    assert "数据来源" in html
    assert "免责声明" in html and "spot_prices_hourly" in html
    assert "[1]" in html  # evidence reference numbering


def test_render_refuses_unknown_tag(tmp_path):
    root, d = _article(tmp_path)
    (d / "draft.zh.md").write_text("幽灵引用。[[E:ghost]]\n", encoding="utf-8")
    with pytest.raises(ValueError, match="unknown tag"):
        render_article(d, root)


def test_approval_needs_signoff_and_green_gates(tmp_path):
    root, d = _article(tmp_path)
    (d / "gates.md").write_text("<!-- auto:begin -->\n(not run yet)\n<!-- auto:end -->\n",
                                encoding="utf-8")
    assert check_approval_ready(d)            # no signoff, gates not run
    (d / "gates.md").write_text(
        "<!-- auto:begin -->\n- fact_trace: pass\n- license: pass\n"
        "- confidentiality: pass\n- compliance: pass\n- terminology: pass\n"
        "<!-- auto:end -->\n", encoding="utf-8")
    assert check_approval_ready(d)            # still no signoff
    with open(d / "gates.md", "a", encoding="utf-8") as f:
        f.write("\nowner_signoff: 张三 2026-10-05\n")
    assert check_approval_ready(d) == []


def test_render_refuses_without_approval(tmp_path):
    root = tmp_path
    (root / "style").mkdir()
    (root / "style" / "disclaimer.md").write_text("免责声明", encoding="utf-8")
    d = new_article(root, 2, "demo2")
    with pytest.raises(ValueError, match="not approved"):
        render_article(d, root)


def test_render_refuses_blocklisted_name(tmp_path):
    root, d = _article(tmp_path)
    dump_yaml({"names": ["ACME能源"]}, root / "style" / "blocklist.yaml")
    (d / "draft.zh.md").write_text("ACME能源 的报价。[[E:e1]]\n", encoding="utf-8")
    with pytest.raises(ValueError, match="blocklisted"):
        render_article(d, root)


def test_render_refuses_malformed_tag(tmp_path):
    root, d = _article(tmp_path)
    (d / "draft.zh.md").write_text("引用。[[E:my id]]\n", encoding="utf-8")
    with pytest.raises(ValueError, match="malformed"):
        render_article(d, root)
