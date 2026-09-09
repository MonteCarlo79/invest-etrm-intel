"""Structurer tab render test (AppTest, no LLM calls)."""
import sys

from streamlit.testing.v1 import AppTest

HARNESS = "/tmp/structurer_tab_harness.py"


def _write_harness():
    root = "/Users/chenzhuqi/Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform"
    with open(HARNESS, "w") as f:
        f.write(
            "import sys\n"
            f"sys.path.insert(0, {root!r})\n"
            "import streamlit as st\n"
            "import services.deal_structurer.structurer_agent as _sa\n"
            "_sa._note_revision(1, '谷山梁二期 (rev 2)')\n"
            "from apps.deal_structurer import strategist\n"
            "strategist.render()\n"
        )


def test_tab_renders_uploader_chat_and_revision_panel():
    _write_harness()
    at = AppTest.from_file(HARNESS, default_timeout=60)
    at.run()
    assert not at.exception
    assert any("Structurer" in h.value for h in at.header)
    assert len(at.file_uploader) == 1
    assert at.chat_input is not None
    # Revision panel: expander label carries "修订记录"; its rows are markdown.
    assert any("修订记录" in e.label for e in at.expander)
    assert any("result `#1`" in m.value for m in at.markdown)


def test_nav_label_is_structurer():
    src = open(
        "/Users/chenzhuqi/Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform"
        "/apps/deal_structurer/app.py").read()
    assert "💬 Structurer" in src
    assert "💬 Strategist" not in src
