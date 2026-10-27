# tests/option_engine/test_scout.py
from unittest.mock import MagicMock
from services.option_engine.scout import scout

def test_sweep_pipeline_new_candidate(monkeypatch):
    monkeypatch.setattr(scout.scans_trading, "scan_da_rt_basis", lambda eng, **k: [
        {"kind": "trading", "province": "山东", "asset_type": "bess", "scan_key": "da_rt_basis",
         "evidence": {"z": 2.4}}])
    monkeypatch.setattr(scout.scans_trading, "scan_nodal_pf_outliers", lambda eng, **k: [])
    monkeypatch.setattr(scout.scans_trading, "scan_interconnector_windows", lambda eng, **k: [])
    monkeypatch.setattr(scout.store, "recent_same_signature", lambda *a, **k: None)
    saved = {}
    monkeypatch.setattr(scout.store, "save_card", lambda eng, card: saved.update(card) or 1)
    monkeypatch.setattr(scout, "validate_candidate", lambda eng, c: ("confirmed", {"realized_capture": 0.5}))
    client = MagicMock(); client.messages.create.return_value.content = [MagicMock(text="测试卡文字")]
    out = scout.run_sweep(None, "trading", client=client)
    assert out["saved"] == 1 and saved["thesis"] == "测试卡文字" and saved["validation"] == "confirmed"

def test_sweep_survives_scan_error(monkeypatch):
    def boom(eng, **k): raise RuntimeError("db down")
    monkeypatch.setattr(scout.scans_trading, "scan_da_rt_basis", boom)
    monkeypatch.setattr(scout.scans_trading, "scan_nodal_pf_outliers", boom)
    monkeypatch.setattr(scout.scans_trading, "scan_interconnector_windows", boom)
    out = scout.run_sweep(None, "trading", client=None)
    assert out["saved"] == 0 and len(out["errors"]) == 3     # all three recorded, no raise

def test_sweep_llm_absent_still_saves(monkeypatch):
    monkeypatch.setattr(scout.scans_trading, "scan_da_rt_basis", lambda eng, **k: [
        {"kind": "trading", "province": "广东", "asset_type": "bess", "scan_key": "da_rt_basis", "evidence": {"z": 2.1}}])
    monkeypatch.setattr(scout.scans_trading, "scan_nodal_pf_outliers", lambda eng, **k: [])
    monkeypatch.setattr(scout.scans_trading, "scan_interconnector_windows", lambda eng, **k: [])
    monkeypatch.setattr(scout.store, "recent_same_signature", lambda *a, **k: None)
    monkeypatch.setattr(scout.store, "save_card", lambda eng, card: 1)
    monkeypatch.setattr(scout, "validate_candidate", lambda eng, c: ("no_evidence", {"reason": "test"}))
    out = scout.run_sweep(None, "trading", client=None)      # no LLM
    assert out["saved"] == 1                                  # card saved without thesis

# ── Task-6 additions: delta-gate carry, seam rulings, sweep validation ──────

def test_sweep_carried_when_evidence_unchanged(monkeypatch):
    """Delta gate (Review Focus #1 — the cost lever): a same-signature card
    whose numeric evidence is within tolerance is marked carried — no LLM
    call, no new card, no re-validation."""
    monkeypatch.setattr(scout.scans_trading, "scan_da_rt_basis", lambda eng, **k: [
        {"kind": "trading", "province": "山东", "asset_type": "bess", "scan_key": "da_rt_basis",
         "evidence": {"z": 2.4}}])
    monkeypatch.setattr(scout.scans_trading, "scan_nodal_pf_outliers", lambda eng, **k: [])
    monkeypatch.setattr(scout.scans_trading, "scan_interconnector_windows", lambda eng, **k: [])
    monkeypatch.setattr(scout.store, "recent_same_signature",
                        lambda *a, **k: {"id": 42, "evidence_json": {"z": 2.4}})
    calls = {"carried": [], "saved": [], "validated": []}
    monkeypatch.setattr(scout.store, "mark_carried",
                        lambda eng, cid: calls["carried"].append(cid))
    monkeypatch.setattr(scout.store, "save_card",
                        lambda eng, card: calls["saved"].append(card) or 1)
    monkeypatch.setattr(scout, "validate_candidate",
                        lambda eng, c: calls["validated"].append(c) or ("x", {}))
    client = MagicMock()
    out = scout.run_sweep(None, "trading", client=client)
    assert out["carried"] == 1 and out["saved"] == 0
    assert calls["carried"] == [42]
    assert calls["saved"] == [] and calls["validated"] == []
    assert not client.messages.create.called               # zero LLM spend

def test_sweep_card_translation_seams(monkeypatch):
    """Seam rulings: candidate `evidence` → stored `evidence_json`; scan_date
    = run date injected (delta-gate visibility); scan_params carries
    sweep/scan/provinces/thresholds (+ llm_unavailable note when client is
    None); signature built when the candidate lacks one."""
    from datetime import date
    monkeypatch.setattr(scout.scans_trading, "scan_da_rt_basis", lambda eng, **k: [
        {"kind": "trading", "province": "山西", "asset_type": "bess", "scan_key": "da_rt_basis",
         "evidence": {"z": -2.2}}])
    monkeypatch.setattr(scout.scans_trading, "scan_nodal_pf_outliers", lambda eng, **k: [])
    monkeypatch.setattr(scout.scans_trading, "scan_interconnector_windows", lambda eng, **k: [])
    monkeypatch.setattr(scout.store, "recent_same_signature", lambda *a, **k: None)
    saved = {}
    monkeypatch.setattr(scout.store, "save_card", lambda eng, card: saved.update(card) or 7)
    monkeypatch.setattr(scout, "validate_candidate", lambda eng, c: ("no_evidence", {"reason": "t"}))
    out = scout.run_sweep(None, "trading", client=None, provinces=["山西"])
    assert out["saved"] == 1
    assert saved["evidence_json"] == {"z": -2.2} and "evidence" not in saved
    assert saved["scan_date"] == date.today()
    assert saved["scan_params"]["sweep"] == "trading"
    assert saved["scan_params"]["scan"] == "da_rt_basis"
    assert saved["scan_params"]["provinces"] == ["山西"]
    assert saved["scan_params"]["thresholds"] == {"z_threshold": 2.0}
    assert saved["scan_params"]["note"] == "llm_unavailable"
    assert saved["thesis"] is None
    assert saved["signature"] == "trading|山西|bess|da_rt_basis"

def test_run_sweep_rejects_unknown_sweep():
    import pytest
    with pytest.raises(ValueError):
        scout.run_sweep(None, "weekly", client=None)

def test_sweep_candidate_exception_is_contained(monkeypatch):
    """Headline invariant (final review): one candidate's exception never
    kills the sweep — the failure is recorded and the next candidate still
    saves."""
    monkeypatch.setattr(scout.scans_trading, "scan_da_rt_basis", lambda eng, **k: [
        {"kind": "trading", "province": "山东", "asset_type": "bess", "scan_key": "da_rt_basis",
         "evidence": {"z": 2.4}},
        {"kind": "trading", "province": "山西", "asset_type": "bess", "scan_key": "da_rt_basis",
         "evidence": {"z": -2.2}}])
    monkeypatch.setattr(scout.scans_trading, "scan_nodal_pf_outliers", lambda eng, **k: [])
    monkeypatch.setattr(scout.scans_trading, "scan_interconnector_windows", lambda eng, **k: [])
    monkeypatch.setattr(scout.store, "recent_same_signature", lambda *a, **k: None)
    saved = []
    monkeypatch.setattr(scout.store, "save_card", lambda eng, card: saved.append(card) or 1)
    def boom(eng, c):
        if c["province"] == "山东":
            raise RuntimeError("validator blew up")
        return ("no_evidence", {"reason": "t"})
    monkeypatch.setattr(scout, "validate_candidate", boom)
    out = scout.run_sweep(None, "trading", client=None)
    assert out["saved"] == 1 and out["skipped"] == 1 and len(out["errors"]) == 1
    assert saved[0]["province"] == "山西"
