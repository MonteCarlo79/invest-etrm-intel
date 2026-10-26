# services/option_engine/scout/scans_investment.py
"""Deterministic investment scans (generative scout, Task 3).

Two SELECT-only scans producing opportunity-card candidates. No LLM in this
module — candidates come only from threshold statistics with the driving
numbers attached as ``evidence`` (spec §5.9: deterministic generate, LLM
explain; thesis drafting is Task 4).

1. ``scan_permit_hunt`` — the spec's headline hunt list: snapshot cells
   (latest per province × asset_type × duration_h, read via
   ``apps.deal_structurer.screen_tab.read_latest_cells`` — import-safe,
   proven) where the STATIC revenue stack does not carry the build but the
   optionality case closes the gap:

   - static test (ruling R1): ``annuitize(intrinsic + policy_net) < capex``
     — intrinsic-plus-policy alone doesn't clear capex;
   - full case: ``annuitize(v) >= capex × 1.05`` (5% margin of safety,
     brief-pinned), ``v = capture_adj if not None else total`` (ruling 3:
     wind/solar cells carry capture_adj = None → total is used and the
     evidence carries a screen-grade note).

   Capex basis per asset class (brief's parameters): renewable cells
   (duration_h = 0) use ``capex_yuan_kw_renewable`` flat per kW; BESS cells
   use ``capex_yuan_kwh_bess × duration_h × 2`` per kW — the factor pinned
   by the controller-corrected test block (2h @ 600 ¥/kWh → 2400 ¥/kW capex
   annuity threshold; every test comment computes against 2400). NOTE: this
   doubles screen_tab's display conversion (1 kW = duration_h kWh → 1200
   ¥/kW for the same cell) — flagged to the controller for a confirming
   ruling; the test assertions only pass with the 2400 basis.

2. ``scan_underperformance`` — owned-asset sell-or-fix signals from
   ``marketdata.strategy_experiments`` (ruling 2: latest window_end per
   scope only — stale windows don't re-fire; champion-flip/retirement
   detection uses the status transitions visible in the latest window's
   rows):

   (a) scope='bess_capture': a province whose champion row's
       ``mean_capture_rate < 0.55`` (weak monetization environment), or a
       fresh retirement (status 'retired-candidate' in the latest window);
   (b) scope='nodal_agent' (our IM fleet — the province column carries the
       plant name): rows with ``delta_vs_champion < threshold_pp`` (our
       plant materially below the champion strategy).

Structure mirrors scans_trading.py: thin SQL readers + pure compute cores;
the tests pin the cores, the readers are thin enough to audit by eye.

Candidate contract (``store.build_signature`` / ``store.save_card``
compatible)::

    {"kind": "permit"|"mna", "province": <label — plant name for
      nodal_agent>, "asset_type": <row's asset_type; "bess" for
      underperformance>, "scan_key": "permit_hunt"|"underperformance",
     "evidence": {...numbers...}, "signature": store.build_signature(card)}

Seam ruling (Task 5 validator): a permit candidate derived from a
capture-adjusted snapshot cell carries ``candidate["_capture_used"]`` — the
snapshot's capture rate, read from the row's ``mc_params["capture_rate"]``
(the key ``services/option_engine/batch.py`` writes; None for wind/solar and
for BESS without capture data — then the key is omitted and the validator
correctly returns no_evidence). mc_params may arrive as a dict or a JSON
string; both are handled. Underperformance candidates never carry it.
"""
from __future__ import annotations

import json
import logging
import math

from sqlalchemy import text

from apps.deal_structurer.screen_tab import implied_irr, read_latest_cells
from libs.option_value.value_stack import annuitize
from services.option_engine.scout import store

log = logging.getLogger(__name__)

# Brief-pinned thresholds.
_MARGIN_OF_SAFETY = 1.05          # full case must clear capex × 1.05
_WEAK_CHAMPION_CAPTURE = 0.55     # champion capture below this = weak monetization
_STRAT_EXP_LOOKBACK_DAYS = 45     # reader window; the core filters to the per-scope latest

# Dedup priority when several rows of one entity fire in one window (the
# signature is per-entity, so they collapse to one card): a fresh retirement
# outranks a weak champion, which outranks a below-champion plant.
_SIGNAL_PRIO = {"fresh_retirement": 0, "weak_monetization": 1, "below_champion": 2}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _f(v) -> float | None:
    """float(v), with None and non-finite (NaN/Inf) mapped to None — the
    scans never propagate NaN into thresholds or evidence (screen_tab._f
    pattern)."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _card(kind: str, province: str, asset_type: str, scan_key: str,
          evidence: dict) -> dict:
    card = {"kind": kind, "province": province, "asset_type": asset_type,
            "scan_key": scan_key, "evidence": evidence}
    card["signature"] = store.build_signature(card)
    return card


def _capture_from_mc(mc_params) -> float | None:
    """The snapshot's capture rate from mc_params["capture_rate"] (the
    batch's key), tolerating a JSON-string or missing/unparseable mc_params
    (→ None — never fabricated)."""
    mc = mc_params
    if isinstance(mc, str):
        try:
            mc = json.loads(mc)
        except Exception:
            return None
    if not isinstance(mc, dict):
        return None
    return _f(mc.get("capture_rate"))


# ---------------------------------------------------------------------------
# 1. Permit hunt
# ---------------------------------------------------------------------------

def compute_permit_hunt(rows: list[dict], capex_yuan_kwh_bess: float = 600.0,
                        capex_yuan_kw_renewable: float = 5500.0,
                        hurdle: float = 0.08, life_years: int = 15,
                        margin_of_safety: float = _MARGIN_OF_SAFETY) -> list[dict]:
    """Pure core: latest-per-cell snapshot rows → permit candidates.

    A cell fires iff the static stack (intrinsic + policy_net, ruling R1)
    annuitizes BELOW capex and the optionality case (capture_adj, else
    total) annuitizes to at least capex × margin_of_safety. Cells missing
    the inputs needed to classify (intrinsic/policy/value, or a BESS
    duration) are skipped with a log line — never classified on partial
    data. Candidate order follows input row order (deterministic).
    """
    out: list[dict] = []
    for row in rows or []:
        cand = _permit_candidate(row, float(capex_yuan_kwh_bess),
                                 float(capex_yuan_kw_renewable),
                                 float(hurdle), int(life_years),
                                 float(margin_of_safety))
        if cand is not None:
            out.append(cand)
    return out


def _permit_candidate(row: dict, capex_kwh_bess: float, capex_kw_ren: float,
                      hurdle: float, life_years: int,
                      margin_of_safety: float) -> dict | None:
    province = row.get("province")
    asset_type = row.get("asset_type")
    duration_h = _f(row.get("duration_h")) or 0.0
    if asset_type == "bess":
        if duration_h <= 0:
            log.info("permit_hunt %s skipped: BESS cell with duration_h <= 0",
                     province)
            return None
        capex_kw = capex_kwh_bess * duration_h * 2.0  # test-pinned basis (module docstring)
    else:
        capex_kw = capex_kw_ren

    intr = _f(row.get("intrinsic_yuan_kw"))
    pol = _f(row.get("policy_net_yuan_kw"))
    capture_adj = _f(row.get("capture_adj_yuan_kw"))
    total = _f(row.get("total_yuan_kw"))
    if intr is None or pol is None:
        log.info("permit_hunt %s skipped: intrinsic/policy missing", province)
        return None
    v = capture_adj if capture_adj is not None else total
    if v is None or v <= 0:
        log.info("permit_hunt %s skipped: no positive value case", province)
        return None

    static = intr + pol
    if annuitize(static, hurdle, life_years) >= capex_kw:
        return None  # static clears capex — no optionality gap, not a permit play
    if annuitize(v, hurdle, life_years) < capex_kw * margin_of_safety:
        return None  # optionality does not close the gap with the safety margin

    irr_static = implied_irr(static, capex_kw, life_years)
    irr_full = implied_irr(v, capex_kw, life_years)
    uplift = (
        irr_full - irr_static
        if (irr_full is not None and irr_static is not None)
        else None
    )
    extr = _f(row.get("extrinsic_yuan_kw"))
    evidence = {
        "intrinsic": intr,
        "extrinsic": extr,
        "extrinsic_share": (extr / v) if (extr is not None and v) else None,
        "policy_net": pol,
        "capture_adj": capture_adj,
        "irr_static": irr_static,
        "irr_full": irr_full,
        "irr_uplift": uplift,
        "asof": (None if row.get("asof_date") is None
                 else str(row.get("asof_date"))),
    }
    if capture_adj is None:
        # Ruling 3: wind/solar (and uncalibrated BESS) — total stands in for
        # the capture-adjusted case; the card is screen-grade, not bid-grade.
        evidence["note"] = "screen-grade: capture_adj unavailable — total used"
    card = _card("permit", province, asset_type, "permit_hunt", evidence)
    capture_used = _capture_from_mc(row.get("mc_params"))
    if capture_used is not None:
        card["_capture_used"] = capture_used  # seam ruling: Task 5 validator input
    return card


def scan_permit_hunt(eng, capex_yuan_kwh_bess: float = 600.0,
                     capex_yuan_kw_renewable: float = 5500.0,
                     hurdle: float = 0.08, life_years: int = 15) -> list[dict]:
    """Permit-hunt scan over the latest option-value snapshot (SELECT-only).

    Reads the latest row per (province, asset_type, duration_h) cell via
    screen_tab.read_latest_cells and hands the rows to the pure core.
    Raises when the snapshot table is absent — same contract as
    read_latest_cells (the orchestrator's degradation ladder catches it).
    """
    rows = read_latest_cells(eng)
    return compute_permit_hunt(rows, capex_yuan_kwh_bess=capex_yuan_kwh_bess,
                               capex_yuan_kw_renewable=capex_yuan_kw_renewable,
                               hurdle=hurdle, life_years=life_years)


# ---------------------------------------------------------------------------
# 2. Underperformance (owned-asset sell-or-fix)
# ---------------------------------------------------------------------------

def compute_underperformance(rows: list[dict],
                             threshold_pp: float = -0.15) -> list[dict]:
    """Pure core: strategy_experiments rows → mna candidates.

    Ruling 2: only each scope's latest ``window_end`` is evaluated — stale
    windows don't re-fire. Signals:

    - scope='bess_capture', latest window: a champion row with
      mean_capture_rate < 0.55 (weak monetization environment), or any
      'retired-candidate' row (fresh retirement — the transition signal is
      the status visible in the latest window's rows);
    - scope='nodal_agent', latest window: delta_vs_champion < threshold_pp
      (fraction units, brief-pinned -0.15).

    ``champion_capture`` = capture − delta when the row carries a delta,
    else the same-(scope, province) champion row's capture when visible.
    ``delta_pp`` is stored in percentage points (delta × 100) — display
    unit; ``threshold_pp`` stays a fraction per the brief's signature.

    One card per entity: rows colliding on signature dedup to the
    highest-priority signal (fresh_retirement > weak_monetization >
    below_champion).
    """
    latest: dict[str, object] = {}
    for r in rows or []:
        scope = r.get("scope")
        we = r.get("window_end")
        if scope is None or we is None:
            continue
        if scope not in latest or we > latest[scope]:
            latest[scope] = we

    champions: dict[tuple, float] = {}
    for r in rows or []:
        if (r.get("status") == "champion"
                and latest.get(r.get("scope")) == r.get("window_end")):
            cap = _f(r.get("mean_capture_rate"))
            if cap is not None:
                champions[(r.get("scope"), r.get("province"))] = cap

    out: list[dict] = []
    for r in rows or []:
        scope = r.get("scope")
        if scope is None or r.get("window_end") != latest.get(scope):
            continue  # stale window (ruling 2) or row without scope/window
        province = r.get("province")
        capture = _f(r.get("mean_capture_rate"))
        delta = _f(r.get("delta_vs_champion"))
        champion_capture = (
            capture - delta if (capture is not None and delta is not None)
            else champions.get((scope, province))
        )
        signal = None
        if scope == "bess_capture":
            if r.get("status") == "retired-candidate":
                signal = "fresh_retirement"
            elif (r.get("status") == "champion" and capture is not None
                  and capture < _WEAK_CHAMPION_CAPTURE):
                signal = "weak_monetization"
        elif scope == "nodal_agent":
            if delta is not None and delta < threshold_pp:
                signal = "below_champion"
        if signal is None:
            continue
        evidence = {
            "scope": scope,
            "model": r.get("model"),
            "province_or_plant": province,
            "capture": capture,
            "champion_capture": champion_capture,
            "delta_pp": (delta * 100.0) if delta is not None else None,
            "window_end": str(r.get("window_end")),
            "signal": signal,
        }
        out.append(_card("mna", province, "bess", "underperformance", evidence))

    best: dict[str, dict] = {}
    for cand in out:
        sig = cand["signature"]
        cur = best.get(sig)
        if (cur is None or _SIGNAL_PRIO[cand["evidence"]["signal"]]
                < _SIGNAL_PRIO[cur["evidence"]["signal"]]):
            best[sig] = cand
    return list(best.values())


def scan_underperformance(eng, threshold_pp: float = -0.15) -> list[dict]:
    """Underperformance scan over marketdata.strategy_experiments (SELECT-only).

    Reads the trailing 45 days of bess_capture / nodal_agent rows (several
    evaluation windows; the pure core keeps each scope's latest), and hands
    them to ``compute_underperformance``.
    """
    sql = text("""
        SELECT scope, model, province, duration_h, mean_capture_rate,
               delta_vs_champion, status, window_end
        FROM marketdata.strategy_experiments
        WHERE scope IN ('bess_capture', 'nodal_agent')
          AND window_end >= CURRENT_DATE - CAST(:days AS integer)
    """)
    with eng.connect() as conn:
        rows = [dict(r) for r in conn.execute(
            sql, {"days": _STRAT_EXP_LOOKBACK_DAYS}).mappings().fetchall()]
    return compute_underperformance(rows, threshold_pp=threshold_pp)
