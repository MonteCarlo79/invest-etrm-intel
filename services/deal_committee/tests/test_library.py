# services/deal_committee/tests/test_library.py
from unittest.mock import MagicMock

from services.deal_committee.brief import DealBrief
from services.deal_committee.library import list_dafs, load_daf, save_brief, save_daf


def _engine_with(fetch_one=None, fetch_all=None):
    conn = MagicMock()
    if fetch_one is not None:
        conn.execute.return_value.fetchone.return_value = fetch_one
    if fetch_all is not None:
        conn.execute.return_value.fetchall.return_value = fetch_all
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    engine.connect.return_value.__enter__.return_value = conn
    return engine, conn


def test_save_brief_inserts_jsonb_and_returns_id():
    engine, conn = _engine_with(fetch_one=(42,))
    brief = DealBrief(deal_name="蒙西储能一期", province="蒙西", confirmed=True)
    brief_id = save_brief(engine, brief)
    assert brief_id == 42
    sql, params = conn.execute.call_args[0][0], conn.execute.call_args[0][1]
    assert "marketdata.deal_briefs" in str(sql)
    assert "蒙西储能一期" in params["brief"]


def test_save_daf_stores_bytes_and_size():
    engine, conn = _engine_with(fetch_one=(7,))
    daf_id = save_daf(engine, 42, DealBrief(deal_name="x"), b"%PDF-fake", "DAF_x.pdf", "GO")
    assert daf_id == 7
    params = conn.execute.call_args[0][1]
    assert params["pdf"] == b"%PDF-fake"
    assert params["recommendation"] == "GO"


def test_list_dafs_returns_dicts():
    engine, conn = _engine_with(fetch_all=[(7, "蒙西储能一期", "DAF_a.pdf", 512, "GO", "2026-09-04")])
    rows = list_dafs(engine)
    assert rows[0]["deal_name"] == "蒙西储能一期"
    assert rows[0]["recommendation"] == "GO"


def test_load_daf_returns_bytes_and_filename():
    engine, conn = _engine_with(fetch_one=(b"%PDF-data", "DAF_a.pdf"))
    pdf, name = load_daf(engine, 7)
    assert pdf == b"%PDF-data" and name == "DAF_a.pdf"
