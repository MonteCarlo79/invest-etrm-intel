# tests/option_engine/test_scout_validate.py
from services.option_engine.scout import validate

def test_confirmed_when_realized_matches(monkeypatch):
    monkeypatch.setattr(validate.readers, "read_capture_rate", lambda *a, **k: 0.50)
    v, d = validate.validate_candidate(None, {"kind": "permit", "province": "山东", "asset_type": "bess",
                                              "evidence": {"capture_adj": 192.0}, "_capture_used": 0.515})
    assert v == "confirmed" and d["realized_capture"] == 0.50

def test_weakened_when_realized_far_below():
    class R:
        @staticmethod
        def read_capture_rate(*a, **k): return 0.30
    validate.readers = R()
    v, d = validate.validate_candidate(None, {"kind": "permit", "province": "山东", "asset_type": "bess",
                                              "evidence": {}, "_capture_used": 0.515})
    assert v == "weakened" and abs(d["gap_pp"] - (-0.215)) < 1e-3

def test_no_evidence_never_fabricates():
    class R:
        @staticmethod
        def read_capture_rate(*a, **k): return None
    validate.readers = R()
    v, d = validate.validate_candidate(None, {"kind": "trading", "province": "海南", "asset_type": "solar",
                                              "evidence": {}})
    assert v == "no_evidence" and "reason" in d
