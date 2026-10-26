# tests/option_engine/test_scout_store.py
import os, pytest
from services.option_engine.scout import store

def test_signature_stable_and_scoped():
    c = {"kind": "trading", "province": "山东", "asset_type": "bess", "scan_key": "da_rt_basis"}
    assert store.build_signature(c) == "trading|山东|bess|da_rt_basis"
    assert store.build_signature(dict(c, thesis="不同的文字", evidence_json={"a": 1})) == store.build_signature(c)

def test_review_action_sql_shape():
    class Rec:
        def __init__(self): self.calls = []
        def execute(self, sql, params=None): self.calls.append((str(sql), params))
        def __enter__(self): return self
        def __exit__(self, *a): return False
    class Eng:
        def __init__(self): self.c = Rec()
        def connect(self): return self.c
        def begin(self): return self.c
    eng = Eng()
    store.review_action(eng, 7, "approved", "理由")
    sql, params = eng.c.calls[0]
    assert "UPDATE marketdata.option_opportunity_cards" in sql
    assert params == {"cid": 7, "status": "approved", "note": "理由"}
    with pytest.raises(ValueError):
        store.review_action(eng, 7, "promoted", "")     # promote goes through the brief flow, not review_action

@pytest.mark.skipif(not os.environ.get("PGURL"), reason="integration: needs PGURL")
def test_card_roundtrip_live():
    from sqlalchemy import create_engine
    eng = create_engine(os.environ["PGURL"])
    store.ensure_table(eng)
    cid = store.save_card(eng, {"kind": "trading", "province": "测试省", "asset_type": "bess",
                                "thesis": "测试卡", "evidence_json": {"x": 1}, "validation": "pending",
                                "validation_detail": {}, "scan_params": {}, "scan_date": "2026-10-26",
                                "signature": "trading|测试省|bess|test"})
    cards = store.list_cards(eng, status="new")
    assert any(c["id"] == cid for c in cards)
    store.review_action(eng, cid, "rejected", "测试清理")
    assert not any(c["id"] == cid for c in store.list_cards(eng, status="new"))
