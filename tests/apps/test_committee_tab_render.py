"""Regression: tab 6 history browsers must render without a confirmed brief in session.

Bug (2026-09-06): after a hard browser refresh the Streamlit session is empty,
committee_tab.render() early-returned on missing deal_brief, and the
历史交易要素 / 历史 DAF browsers below the return were unreachable — users
could not retrieve saved deals.
"""
import sys
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

REPO = str(Path(__file__).resolve().parents[2])
HARNESS = """
import sys
sys.path.insert(0, %r)

import services.common.db_utils as dbu
import services.deal_committee.library as lib

dbu.get_engine = lambda: object()
lib.list_briefs = lambda engine: [{
    "id": 1, "deal_name": "谷山梁二期", "created_at": "2026-09-06T00:31:00",
    "brief": {"deal_name": "谷山梁二期", "asset_type": "bess", "province": "蒙西",
              "capacity_mw": 500.0, "capacity_mwh": 2000.0,
              "capex_total_yuan": 13e8, "confirmed": True},
    "recommendation": None, "result_id": None, "daf_id": None,
}]
lib.list_results = lambda engine: []

from apps.deal_structurer import committee_tab
committee_tab.render()
""" % REPO


def _at():
    import tempfile
    f = tempfile.NamedTemporaryFile("w", suffix=".py", delete=False)
    f.write(HARNESS)
    f.close()
    at = AppTest.from_file(f.name, default_timeout=60)
    at.run()
    return at


def test_history_visible_without_confirmed_brief():
    at = _at()
    assert not at.exception, f"render exception: {at.exception}"
    subs = [s.value for s in at.subheader]
    assert any("历史交易要素" in s for s in subs), f"history browser missing: {subs}"
    assert any("历史 DAF" in s for s in subs), f"DAF history missing: {subs}"


def test_load_brief_restores_session():
    at = _at()
    load = [b for b in at.button if b.label == "载入要素"]
    assert len(load) == 1, f"载入要素 button missing: {[b.label for b in at.button]}"
    load[0].click().run()
    assert not at.exception, f"exception after 载入要素: {at.exception}"
    brief = at.session_state["deal_brief"]
    assert brief.deal_name == "谷山梁二期" and brief.province == "蒙西"
