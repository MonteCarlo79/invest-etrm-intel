# tests/deal_structurer/test_review_queue.py
import json

from apps.deal_structurer.screen_tab import (
    build_brief_payload,
    promote_card,
    record_review_decision,
    verdict_badge,
)
from services.deal_committee.brief import parse_brief_json


def test_promoted_brief_is_valid_and_unconfirmed():
    card = {"id": 3, "kind": "permit", "province": "山西", "asset_type": "bess",
            "thesis": "内在价值不足，期权价值补足…", "evidence_json": {"irr_uplift": 0.04},
            "scan_date": "2026-10-26"}
    payload = build_brief_payload(card)
    brief = parse_brief_json(payload, source_files=["scout:card:3"])
    assert brief.province == "山西" and brief.asset_type == "bess"
    assert brief.confirmed is False                                 # committee flow unbroken: human confirms in Tab 0
    assert "irr_uplift" in brief.structure_notes


def test_build_brief_payload_fields_and_asset_fallback():
    card = {"id": 7, "kind": "trading", "province": "山东", "asset_type": "spread",
            "thesis": None, "evidence_json": {"basis_mean": 12.5},
            "scan_date": "2026-10-26"}
    payload = build_brief_payload(card)
    assert payload["deal_name"] == "trading:山东:spread:2026-10-26"
    assert payload["asset_type"] == "bess"                          # trading/non-Literal → bess
    assert "默认 bess" in payload["structure_notes"]                # fallback note recorded
    assert payload["confirmed"] is False
    assert payload["field_confidence"] == {"source": 0.9}
    # thesis=None (LLM-absent run): structure_notes still carries the evidence
    # digest — never an empty string.
    assert "basis_mean" in payload["structure_notes"]


def test_verdict_badge_colors_as_is():
    assert verdict_badge("confirmed") == "🟢 confirmed"
    assert verdict_badge("weakened") == "🟡 weakened"
    assert verdict_badge("no_evidence") == "⬜ no_evidence"
    assert verdict_badge("pending") == "⚪ pending"
    assert verdict_badge(None) == "⚪ pending"                      # absent → pending, never blank
    assert verdict_badge("bogus") == "⚪ bogus"                     # unknown words shown as-is


class _Rec:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def execute(self, sql, params=None):
        if self.fail:
            raise RuntimeError("db down")
        self.calls.append((str(sql), params))

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Eng:
    def __init__(self, fail=False):
        self.c = _Rec(fail=fail)

    def connect(self):
        return self.c

    def begin(self):
        return self.c


def test_record_review_decision_sql_shape():
    eng = _Eng()
    card = {"id": 11, "kind": "permit", "province": "山西", "asset_type": "bess"}
    assert record_review_decision(eng, card, "approved", "理由") is True
    sql, params = eng.c.calls[0]
    assert "INSERT INTO marketdata.agent_memory" in sql
    assert "'deal_scout'" in sql                                    # app + source pinned in SQL
    assert params["subject"] == "11"
    content = json.loads(params["content"])
    assert content == {"decision": "approved", "note": "理由",
                       "kind": "permit", "province": "山西", "asset_type": "bess"}


def test_record_review_decision_failure_never_breaks_review():
    eng = _Eng(fail=True)
    assert record_review_decision(eng, {"id": 1}, "rejected", "") is False  # logged, swallowed


def test_promote_card_saves_unconfirmed_brief_and_sets_promoted(monkeypatch):
    import services.deal_committee.library as library

    saved = {}

    def fake_save_brief(eng, brief):
        saved["brief"] = brief
        return 42

    monkeypatch.setattr(library, "save_brief", fake_save_brief)
    eng = _Eng()
    card = {"id": 3, "kind": "permit", "province": "山西", "asset_type": "bess",
            "thesis": "内在价值不足，期权价值补足…", "evidence_json": {"irr_uplift": 0.04},
            "scan_date": "2026-10-26"}
    brief_id = promote_card(eng, card, note="批准晋升")
    assert brief_id == 42
    brief = saved["brief"]
    assert brief.confirmed is False
    assert brief.source_files == ["scout:card:3"]
    assert brief.province == "山西"
    # terminal status via set_status — never review_action's guard
    sql, params = eng.c.calls[0]
    assert "UPDATE marketdata.option_opportunity_cards" in sql
    assert params == {"cid": 3, "status": "promoted", "note": "批准晋升"}
