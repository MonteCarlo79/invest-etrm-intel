# services/option_engine/scout/scout.py
"""Orchestrator for the generative investment scout (Task 6).

One ``run_sweep`` pass chains the pipeline: deterministic scans (Tasks 2/3)
→ per candidate the delta gate ``needs_draft`` (Task 4) →

- gate True  → grounded ``draft_thesis`` (the only LLM spend) → validate →
  save card;
- gate False → ``store.mark_carried`` on the prior card (no LLM spend, no
  new card — Review Focus #1, the cost lever).

Seam rulings honored here (controller, Task-6 dispatch):

- candidate dicts carry ``evidence``; the store column is ``evidence_json``
  — translated at save time in ``_to_card``;
- every card gets ``scan_date`` = the run's date (a NULL scan_date is
  invisible to the delta gate's recency window) and ``scan_params`` =
  {sweep, scan, provinces, thresholds} — the orchestrator pins scan kwargs
  in ``_scan_plan`` so the thresholds recorded are the ones actually run,
  and ``provinces`` comes from the scan's own kwargs (None for scans that
  take no provinces filter);
- one scan's or one candidate's exception NEVER kills the sweep: record in
  ``errors`` and continue.

Validation ordering (pinned by tests/option_engine/test_scout.py::
test_sweep_pipeline_new_candidate — the saved card must already carry the
final verdict): the candidate is validated BEFORE save; the card starts at
``validation='pending'`` and ``validate_candidate``'s verdict overwrites it
pre-save, so the queue never shows an unvalidated row and the row is
written exactly once. Re-validation of an already-saved card (without
touching review status) goes through ``store.set_validation``.

LLM-absent path: ``client is None`` → cards save with ``thesis=None`` and
``scan_params["note"] = "llm_unavailable"`` (the queue still works — the
deterministic scans carry the value). ``run_sweep`` never builds a client
itself; the CLI decides via ``is_llm_available`` and passes one in — this
keeps unit tests free of real LLM calls (Global Constraints).

Write allowlist: marketdata.option_opportunity_cards only (via store).
"""
from __future__ import annotations

import logging
from datetime import date

from services.option_engine.scout import scans_investment, scans_trading, store
from services.option_engine.scout.thesis import draft_thesis, needs_draft
from services.option_engine.scout.validate import validate_candidate

log = logging.getLogger(__name__)

SWEEPS = ("investment", "trading")


def _scan_plan(sweep: str, provinces: list[str] | None):
    """(name, callable, kwargs) per scan — resolved at CALL time (inside
    run_sweep) so test monkeypatches of the scan modules are picked up.

    Thresholds are pinned HERE rather than left to scan defaults so the
    scan_params recorded on each card are exactly what ran (seam ruling).
    Values mirror the Task-2/3 brief defaults; the interconnector and
    investment scans take no provinces filter (channel pairs span
    provinces; snapshot/strategy_experiments scans are nation-wide).
    """
    if sweep == "trading":
        return [
            ("da_rt_basis", scans_trading.scan_da_rt_basis,
             {"provinces": provinces, "z_threshold": 2.0}),
            ("nodal_pf_outliers", scans_trading.scan_nodal_pf_outliers,
             {"provinces": provinces, "top_n": 5, "days": 30}),
            ("interconnector_windows",
             scans_trading.scan_interconnector_windows,
             {"z_threshold": 1.0}),
        ]
    if sweep == "investment":
        return [
            ("permit_hunt", scans_investment.scan_permit_hunt,
             {"capex_yuan_kwh_bess": 600.0, "capex_yuan_kw_renewable": 5500.0,
              "hurdle": 0.08, "life_years": 15}),
            ("underperformance", scans_investment.scan_underperformance,
             {"threshold_pp": -0.15}),
        ]
    raise ValueError(f"unknown sweep {sweep!r} — expected one of {SWEEPS}")


def _to_card(cand: dict, thesis_text, scan_date, scan_params: dict) -> dict:
    """Candidate → store-schema card (seam rulings: ``evidence`` →
    ``evidence_json``; ``scan_date``/``scan_params`` injected). Starts at
    ``validation='pending'``; the caller overwrites with the verdict before
    save (module docstring on ordering)."""
    return {
        "kind": cand.get("kind"),
        "province": cand.get("province"),
        "asset_type": cand.get("asset_type"),
        "thesis": thesis_text,
        "evidence_json": cand.get("evidence") or {},
        "validation": "pending",
        "validation_detail": {},
        "scan_params": scan_params,
        "scan_date": scan_date,
        "signature": cand["signature"],
    }


def run_sweep(eng, sweep: str, client=None, provinces: list[str] | None = None,
              scan_date=None) -> dict:
    """Run one sweep end to end.

    Returns ``{saved, carried, skipped, errors, scans_ran}`` — one scan's or
    one candidate's exception is recorded in ``errors`` and never kills the
    sweep. ``scans_ran`` counts scans that completed without raising (the
    CLI exits 1 when zero ran — loud failure, nodal-writer pattern).
    ``scan_date`` defaults to today; pass explicitly for reproducible runs.
    """
    out = {"saved": 0, "carried": 0, "skipped": 0, "errors": [],
           "scans_ran": 0}
    run_date = scan_date or date.today()

    for name, scan, kwargs in _scan_plan(sweep, provinces):
        print(f"[{sweep}] scan {name} ...", flush=True)
        try:
            candidates = scan(eng, **kwargs)
        except Exception as exc:
            log.warning("%s scan failed: %s", name, exc)
            out["errors"].append({"scan": name, "error": str(exc)})
            print(f"[{sweep}] scan {name} FAILED: {exc}", flush=True)
            continue
        out["scans_ran"] += 1
        print(f"[{sweep}] scan {name} -> {len(candidates)} candidate(s)",
              flush=True)
        for cand in candidates:
            _process(eng, cand, client, sweep, name, kwargs, run_date, out)
    return out


def _process(eng, cand: dict, client, sweep: str, scan_name: str,
             scan_kwargs: dict, run_date, out: dict) -> None:
    """One candidate through gate → thesis → validate → save (or carry).

    Any exception is recorded (candidate never kills the sweep) — the
    signature derivation sits INSIDE the guard, so a non-dict candidate
    fails safe and is recorded via repr. A missing scan_key surfaces as a
    KeyError here — loud-by-intent (Task-1 ruling), but non-fatal.
    """
    sig = None
    try:
        if not cand.get("signature"):
            # Scans attach it via _card; build it for candidates that lack
            # one so the delta gate queries the real key (KeyError on a
            # missing scan_key is loud-by-intent).
            cand["signature"] = store.build_signature(cand)
        sig = cand["signature"]
        draft, prior = needs_draft(eng, cand)
        if not draft:
            store.mark_carried(eng, prior["id"])
            out["carried"] += 1
            return
        # scan_params records EXACTLY what ran (final-review ruling):
        # provinces provenance comes from the scan's own kwargs — scans
        # that take no provinces filter record None, not the CLI value.
        params = {"sweep": sweep, "scan": scan_name,
                  "provinces": scan_kwargs.get("provinces"),
                  "thresholds": {k: v for k, v in scan_kwargs.items()
                                 if k != "provinces"}}
        if client is not None:
            thesis_text = draft_thesis(client, cand)
        else:
            thesis_text = None
            params["note"] = "llm_unavailable"
        card = _to_card(cand, thesis_text, run_date, params)
        verdict, detail = validate_candidate(eng, cand)
        card["validation"] = verdict
        card["validation_detail"] = detail
        store.save_card(eng, card)
        out["saved"] += 1
    except Exception as exc:
        where = sig if sig is not None else repr(cand)[:80]
        log.warning("candidate %s failed: %s", where, exc)
        out["errors"].append({"candidate": where, "error": str(exc)})
        out["skipped"] += 1
