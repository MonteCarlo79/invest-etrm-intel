"""delete_result unit test + 历史 DAF delete UI (AppTest, no DB)."""
import sys
from pathlib import Path
from unittest.mock import MagicMock

from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from services.deal_committee.library import delete_result  # noqa: E402

HARNESS = "/tmp/daf_delete_harness.py"


class TestDeleteResult:
    def test_deletes_result_and_linked_pdf(self):
        conn = MagicMock()
        engine = MagicMock()
        engine.begin.return_value.__enter__.return_value = conn
        conn.execute.return_value.fetchone.return_value = (55,)
        assert delete_result(engine, 12) is True
        sqls = [c.args[0].text for c in conn.execute.call_args_list]
        assert any("deal_daf_results" in s and "DELETE" in s for s in sqls)
        assert any("deal_daf_library" in s and "DELETE" in s for s in sqls)

    def test_no_linked_pdf(self):
        conn = MagicMock()
        engine = MagicMock()
        engine.begin.return_value.__enter__.return_value = conn
        conn.execute.return_value.fetchone.return_value = (None,)
        assert delete_result(engine, 12) is False
        sqls = [c.args[0].text for c in conn.execute.call_args_list]
        assert not any("deal_daf_library" in s for s in sqls)


def _write_harness():
    rows = [
        {"id": 21, "deal_name": "谷山梁二期 (rev 4)", "province": "蒙西",
         "asset_type": "bess", "recommendation": "NO-GO",
         "created_at": "2026-09-09 04:10", "daf_id": 55, "filename": "DAF_x.pdf"},
        {"id": 20, "deal_name": "谷山梁二期", "province": "蒙西",
         "asset_type": "bess", "recommendation": "NO-GO",
         "created_at": "2026-09-07 05:56", "daf_id": None, "filename": None},
    ]
    with open(HARNESS, "w") as f:
        f.write(
            "import sys\n"
            f"sys.path.insert(0, {str(ROOT)!r})\n"
            "from unittest.mock import MagicMock\n"
            "import streamlit as st\n"
            "import services.common.db_utils as dbu\n"
            "import services.deal_committee.library as lib\n"
            f"ROWS = {rows!r}\n"
            "lib.list_briefs = lambda engine, limit=20: []\n"
            "lib.list_results = lambda engine, limit=20: ROWS\n"
            "lib.load_daf = lambda engine, i: (b'PDF', 'DAF_x.pdf')\n"
            "lib.load_result = lambda engine, i: {}\n"
            "st.session_state.setdefault('_deleted_result', None)\n"
            "def _del(engine, rid):\n"
            "    st.session_state['_deleted_result'] = rid\n"
            "    return True\n"
            "lib.delete_result = _del\n"
            "dbu.get_engine = lambda: MagicMock()\n"
            "from apps.deal_structurer import committee_tab\n"
            "committee_tab._history_sections()\n"
        )


def test_rows_render_delete_buttons():
    _write_harness()
    at = AppTest.from_file(HARNESS, default_timeout=60)
    at.run()
    assert not at.exception
    labels = [b.label for b in at.button]
    assert labels.count("🗑 删除") == 2, labels
    assert labels.count("查看") == 2


def test_delete_confirm_and_cancel():
    _write_harness()
    at = AppTest.from_file(HARNESS, default_timeout=60)
    at.run()
    dels = [b for b in at.button if b.label == "🗑 删除"]
    dels[0].click().run()  # first row = rev 4 (has PDF)
    warn = [w.value for w in at.warning]
    assert any("谷山梁二期 (rev 4)" in w and "DAF PDF" in w for w in warn), warn
    # confirm → delete_result called with id 21
    [b for b in at.button if b.label == "🗑 确认删除"][0].click().run()
    assert at.session_state["_deleted_result"] == 21
    # cancel path: banner shown, cancel clears it
    at2 = AppTest.from_file(HARNESS, default_timeout=60)
    at2.run()
    [b for b in at2.button if b.label == "🗑 删除"][1].click().run()
    [b for b in at2.button if b.label == "取消"][0].click().run()
    assert at2.session_state["_deleted_result"] is None
    assert not any("确认删除" in w.value for w in at2.warning)
