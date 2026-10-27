# services/option_engine/scout/validate.py
"""Realized-monetization validator (generative scout, Task 5).

Checks each opportunity candidate against what operating assets ACTUALLY
banked. SELECT-only; writes nothing.

Circularity ruling (ratified, plan-3 final review): the capture verdict is
a FRESHNESS/DRIFT check, not independent statistical validation — it
compares CURRENT realized capture (trailing 90d) against the card's
snapshot-time assumption, and both trace to the same underlying price and
dispatch data. The independent evidence in v1 is the register/retail
context (empirical comp rate, frequency revenue, retail net margin); it is
attached to detail only and never moves the verdict.

Hard rule: NEVER fabricate evidence. No comparable data →
``validation='no_evidence'`` with ``detail["reason"]``; a register-unsupported
province is not an error.

Verdict logic (brief-pinned): BESS permit/trading with capture data →
realized trailing-90d aggregate capture (``readers.read_capture_rate``,
champion model) vs the card's assumed capture (``_capture_used``, else the
latest snapshot's ``mc_params["capture_rate"]`` — ruling on ambiguity, never
guessed): ``realized < 0.8 × assumed`` → ``weakened``; otherwise
``confirmed`` (the ±20% band and above-band upside both confirm — evidence
exceeding the assumption is not a weakening signal). No capture rows →
``no_evidence``.

Context enrichments (detail only, never verdict-moving in v1):
- register provinces (``_REGISTER_PROVINCE_MAP``: 蒙西/蒙东/甘肃/广西) —
  empirical capacity-comp rate (¥/MWh discharged) + frequency revenue over
  the trailing 12 months from the risk register;
- trading cards in load-book provinces (``景融售电-<province>``) — latest
  month's retail net margin from ``retail_risk.pnl_bridge.bridge_month``
  (import guarded: ImportError → the enrichment is skipped; if it was the
  card's only evidence path the verdict stays ``no_evidence`` — never crash).
"""
from __future__ import annotations

import json
import logging
import math

from sqlalchemy import text

from services.option_engine import readers

# pnl_bridge is the one cross-service dependency; guard it so a retail_risk
# refactor degrades the enrichment instead of breaking the validator.
try:
    from services.retail_risk.pnl_bridge import bridge_month as _bridge_month
except ImportError:  # pragma: no cover - exercised only when retail_risk moves
    _bridge_month = None

log = logging.getLogger(__name__)

# Brief-pinned: trailing window for the realized capture read, and the
# confirmed/weakened boundary (within ±20% relative → confirmed).
_WINDOW_DAYS = 90
_CONFIRM_TOLERANCE = 0.20

# Platform default BESS duration when the candidate does not carry one —
# same default as readers.read_policy_levers; the capture table's canonical
# comparison duration.
_DEFAULT_DURATION_H = 2.0

# brief.province (中文) → rm_assets.province code in the risk register.
# Replicated from services/deal_committee/economics.py (its
# _REGISTER_PROVINCE_MAP) — private function, not imported across services.
_REGISTER_PROVINCE_MAP = {
    "蒙西": "inner_mongolia_mengxi",
    "蒙东": "inner_mongolia_mengdong",
    "甘肃": "gansu",
    "广西": "guangxi",
}


def _f(v) -> float | None:
    """float(v); None and non-finite → None (scans_investment._f pattern —
    the validator never propagates NaN into verdicts or detail)."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _capture_from_mc(mc_params) -> float | None:
    """capture_rate out of a snapshot's mc_params (dict or JSON string).

    Replicated from scans_investment._capture_from_mc so the validator reads
    the batch's capture key with exactly the scan's tolerance rules.
    """
    mc = mc_params
    if isinstance(mc, str):
        try:
            mc = json.loads(mc)
        except Exception:
            return None
    if not isinstance(mc, dict):
        return None
    return _f(mc.get("capture_rate"))


def _assumed_capture(eng, candidate: dict, duration_h: float) -> float | None:
    """The card's assumed capture: ``_capture_used`` first (seam ruling —
    permit candidates carry it from the snapshot row they derived from),
    else the latest snapshot's mc_params capture_rate for the same
    (province, asset_type, duration_h) — ruling on ambiguity. Never guessed:
    no readable assumption → None. Snapshot lookup needs a real engine and
    is failure-tolerant (missing table / DB error → None → no_evidence)."""
    assumed = _f(candidate.get("_capture_used"))
    if assumed is not None or eng is None:
        return assumed
    try:
        from services.option_engine import snapshot
        row = snapshot.read_latest(eng, candidate.get("province"),
                                   candidate.get("asset_type"), duration_h)
    except Exception:
        return None
    if not row:
        return None
    return _capture_from_mc(row.get("mc_params"))


def _register_context(eng, province: str) -> dict | None:
    """Empirical comp rate + frequency revenue from the risk register.

    SQL pattern replicated from
    services/deal_committee/economics.py::_register_comp_rate (capacity_
    compensation ÷ discharge_energy over the trailing 12 months, province
    mapped via _REGISTER_PROVINCE_MAP), extended with the frequency-revenue
    sum in the same pass. Context only — never verdict-moving in v1.
    None when the province has no register mapping, no engine, or the read
    fails (unsupported province is not an error).
    """
    code = _REGISTER_PROVINCE_MAP.get(province)
    if not code or eng is None:
        return None
    sql = text("""
        SELECT
          SUM(CASE WHEN si.category = 'capacity_compensation'
                   THEN si.amount_cny END)
          / NULLIF(SUM(CASE WHEN si.category = 'discharge_energy'
                            THEN si.volume_mwh END), 0),
          SUM(CASE WHEN si.category = 'frequency' THEN si.amount_cny END)
        FROM marketdata.rm_settlement_items si
        JOIN marketdata.rm_settlements s ON s.id = si.settlement_id
        JOIN marketdata.rm_books b ON b.id = s.book_id
        JOIN marketdata.rm_assets a ON a.id = b.asset_id
        WHERE a.province = :code
          AND s.settlement_month >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
    """)
    try:
        with eng.connect() as conn:
            row = conn.execute(sql, {"code": code}).fetchone()
    except Exception:
        log.info("register context read failed for %s", province)
        return None
    if row is None:
        return None
    return {"comp_rate_yuan_mwh": _f(row[0]),
            "frequency_revenue_cny_12m": _f(row[1])}


def _retail_context(eng, province: str) -> dict | None:
    """Latest month's retail net margin for the 景融售电-<province> load book.

    Uses pnl_bridge.bridge_month on a real connection. Skipped (None, never
    a crash) when the import was guarded away, no engine, no book, or no
    settled month. Context only — never verdict-moving in v1.
    """
    if _bridge_month is None or eng is None:
        return None
    try:
        with eng.connect() as conn:
            book_id = conn.execute(text(
                "SELECT id FROM marketdata.rm_books "
                "WHERE book_type = 'load' AND name = :name LIMIT 1"
            ), {"name": f"景融售电-{province}"}).scalar()
            if book_id is None:
                return None
            month = conn.execute(text(
                "SELECT MAX(settlement_month) FROM marketdata.rm_settlements "
                "WHERE book_id = :b"
            ), {"b": book_id}).scalar()
            if month is None:
                return None
            res = _bridge_month(conn, int(book_id), month)
        return {"book_id": int(book_id), "month": str(month),
                "net_cny": _f(res.net)}
    except Exception:
        log.info("retail margin read failed for %s", province)
        return None


def validate_candidate(eng, candidate: dict) -> tuple[str, dict]:
    """Validate one opportunity candidate against realized monetization.

    Returns ``(verdict, detail)`` with verdict in
    {confirmed, weakened, no_evidence}. The capture comparison is the only
    verdict mover; register/retail reads attach context to detail.
    """
    kind = candidate.get("kind")
    province = candidate.get("province")
    asset_type = candidate.get("asset_type")
    evidence = candidate.get("evidence") or {}
    detail: dict = {"kind": kind, "province": province,
                    "asset_type": asset_type}

    verdict = "no_evidence"
    duration_h = (_f(candidate.get("duration_h"))
                  or _f(evidence.get("duration_h"))
                  or _DEFAULT_DURATION_H)

    if kind in ("permit", "trading") and asset_type == "bess":
        assumed = _assumed_capture(eng, candidate, duration_h)
        if assumed is None or assumed <= 0:
            detail["reason"] = (
                "no assumed capture rate: card carries no _capture_used and "
                "no snapshot mc_params capture_rate — never guessed"
            )
        else:
            try:
                realized = readers.read_capture_rate(
                    eng, province, duration_h, days=_WINDOW_DAYS)
            except Exception:
                realized = None  # unreadable → honest no_evidence
            realized = _f(realized)
            if realized is None:
                detail["reason"] = (
                    f"no bess_capture_daily rows for {province} "
                    f"duration_h={duration_h} in trailing {_WINDOW_DAYS}d "
                    f"(champion model)"
                )
            else:
                detail["realized_capture"] = realized
                detail["assumed_capture"] = assumed
                detail["gap_pp"] = realized - assumed
                detail["window_days"] = _WINDOW_DAYS
                if realized < (1.0 - _CONFIRM_TOLERANCE) * assumed:
                    verdict = "weakened"
                else:
                    # ±20% band and upside-surprise both confirm (module docstring).
                    verdict = "confirmed"
    elif kind in ("permit", "trading"):
        detail["reason"] = (
            f"{asset_type}: no realized-capture benchmark for non-BESS "
            f"assets in v1"
        )
    else:
        detail["reason"] = (
            f"{kind}: no realized-capture verdict path for this card kind "
            f"in v1"
        )

    # Context enrichments — detail only, never move the verdict.
    reg = _register_context(eng, province)
    if reg is not None:
        detail["register"] = reg
    if kind == "trading":
        ret = _retail_context(eng, province)
        if ret is not None:
            detail["retail"] = ret

    return verdict, detail
