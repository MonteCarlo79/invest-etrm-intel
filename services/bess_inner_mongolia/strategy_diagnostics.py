# -*- coding: utf-8 -*-
"""
services/bess_inner_mongolia/strategy_diagnostics.py
─────────────────────────────────────────────────────
Heuristic strategy inference for Inner Mongolia BESS assets.

IMPORTANT DISCLAIMER
─────────────────────
All strategy labels produced here are **heuristic inferences** derived
from observed performance metrics (cycles/day, efficiency, profit/MWh).
Competitors' actual dispatch algorithms, contracts, or operational
strategies are NOT known and MUST NOT be claimed as known.

Evidence levels
───────────────
  observed            – metric taken directly from pipeline output
  proxy-based         – computed from observed data via a model step
  heuristic inference – label or classification by rule-based logic
                        applied to observed/proxy metrics
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# ── Evidence level constants (import these in the page for consistency) ────────

EVIDENCE_OBSERVED  = "observed"
EVIDENCE_PROXY     = "proxy-based"
EVIDENCE_HEURISTIC = "heuristic inference"

DISCLAIMER = (
    "⚠️ Strategy style labels below are **heuristic inferences** only, "
    "derived from observed performance metrics (cycles/day, efficiency, "
    "profit/MWh). Competitors' exact operational strategies, dispatch "
    "algorithms, or contractual arrangements are **not known**."
)


# ── Single-asset inference ────────────────────────────────────────────────────

def infer_strategy_style(row: pd.Series) -> dict:
    """
    Classify the probable strategy style for one BESS asset.

    Heuristic rules (all labeled 'heuristic inference'):
      cycles/day ≥ 1.5 AND eff ≥ 0.85  → High-frequency arbitrage (efficient)
      cycles/day ≥ 1.5 AND eff < 0.85  → High-frequency arbitrage (volume-led)
      0.8 ≤ cycles/day < 1.5 AND profit/MWh above threshold → Selective peak-spread
      0.8 ≤ cycles/day < 1.5 otherwise → Standard daily cycling
      cycles/day < 0.8                  → Conservative / event-driven

    Returns dict with keys: style, evidence_level, rationale, disclaimer
    """
    cycles = _safe(row, "estimated_cycles_per_day")
    eff    = _safe(row, "efficiency")
    p_mwh  = _safe(row, "total_profit_per_discharge_mwh") \
              or _safe(row, "arbitrage_profit_per_discharge_mwh")

    if cycles is None:
        return {
            "style":          "Undetermined",
            "evidence_level": EVIDENCE_HEURISTIC,
            "rationale":      "cycles/day metric not available for this asset.",
            "disclaimer":     DISCLAIMER,
        }

    HIGH_PROFIT_THRESHOLD = 150.0   # ¥/MWh — proxy threshold for peak-spread label

    if cycles >= 1.5:
        if eff is not None and eff >= 0.85:
            style = "High-frequency arbitrage — efficient"
            rationale = (
                f"≥1.5 cycles/day ({cycles:.2f}) with round-trip efficiency "
                f"{eff:.1%} ≥ 85%. Consistent with tightly-optimised, frequent "
                "charge-discharge exploiting intra-day price spreads. "
                "[Evidence: heuristic inference from observed cycle rate + efficiency]"
            )
        else:
            eff_str = f"{eff:.1%}" if eff is not None else "unavailable"
            style = "High-frequency arbitrage — volume-led"
            rationale = (
                f"≥1.5 cycles/day ({cycles:.2f}), round-trip efficiency {eff_str} "
                "< 85%. Prioritises throughput volume over per-cycle margin; may "
                "reflect co-located renewable must-take charging or aggressive "
                "charge/discharge timing. "
                "[Evidence: heuristic inference]"
            )

    elif cycles >= 0.8:
        if p_mwh is not None and p_mwh >= HIGH_PROFIT_THRESHOLD:
            style = "Selective peak-spread arbitrage"
            rationale = (
                f"~1 cycle/day ({cycles:.2f}) with above-threshold profit/MWh "
                f"({p_mwh:,.0f} ¥/MWh ≥ {HIGH_PROFIT_THRESHOLD:.0f}). Consistent "
                "with targeting only the widest intra-day price spreads. "
                "[Evidence: heuristic inference]"
            )
        else:
            p_str = f"{p_mwh:,.0f} ¥/MWh" if p_mwh is not None else "unavailable"
            style = "Standard daily cycling"
            rationale = (
                f"~1 cycle/day ({cycles:.2f}), profit/MWh {p_str}. Consistent "
                "with rule-based once-daily dispatch following scheduled charge/"
                "discharge windows. "
                "[Evidence: heuristic inference]"
            )

    else:
        style = "Conservative / event-driven"
        rationale = (
            f"< 0.8 cycles/day ({cycles:.2f}). Consistent with selective "
            "activation on extreme price events only, or reduced availability "
            "due to maintenance / SoC management constraints. "
            "[Evidence: heuristic inference]"
        )

    return {
        "style":          style,
        "evidence_level": EVIDENCE_HEURISTIC,
        "rationale":      rationale,
        "disclaimer":     DISCLAIMER,
    }


def _nodal_context(plant_name: str, clusters: pd.DataFrame) -> str:
    """
    Return a short description of the nodal peer environment.
    Evidence: proxy-based (cluster membership from price-signature similarity).
    """
    if clusters.empty or "plant_name" not in clusters.columns:
        return "Nodal data unavailable for this period."

    mask = clusters["plant_name"] == plant_name
    if not mask.any():
        return "Plant not found in cluster data."

    row = clusters[mask].iloc[0]
    cluster_id   = row.get("cluster_id",   "?")
    cluster_size = row.get("cluster_size", "?")
    asset_type   = row.get("asset_type",   "?")

    peers = clusters[
        (clusters["cluster_id"] == cluster_id) &
        (clusters["plant_name"] != plant_name)
    ]
    type_counts  = peers["asset_type"].value_counts().to_dict() if not peers.empty else {}
    peer_summary = (
        ", ".join(f"{v} {k}" for k, v in type_counts.items())
        if type_counts else "none identified"
    )

    return (
        f"Cluster {cluster_id} (size {cluster_size}); "
        f"this asset type: {asset_type}; "
        f"co-located peer types: {peer_summary}. "
        "[Evidence: proxy-based — cluster inferred from price-signature similarity, "
        "not official grid topology]"
    )


# ── Table builder ─────────────────────────────────────────────────────────────

def build_strategy_table(
    df: pd.DataFrame,
    clusters: pd.DataFrame | None = None,
    envision_tag: str = "远景",
) -> pd.DataFrame:
    """
    Build a per-asset strategy inference table.

    Columns returned:
      plant_name, owner, is_envision,
      cycles_per_day    (observed),
      efficiency        (observed),
      profit_per_mwh    (observed),
      strategy_style    (heuristic inference),
      evidence_level,
      rationale,
      nodal_context     (proxy-based),
      disclaimer
    """
    if df.empty:
        return pd.DataFrame()

    if clusters is None:
        clusters = pd.DataFrame()

    records = []
    for _, row in df.iterrows():
        inferred = infer_strategy_style(row)
        nodal    = _nodal_context(str(row.get("plant_name", "")), clusters)

        records.append({
            "plant_name":     row.get("plant_name", ""),
            "owner":          row.get("owner",      ""),
            "is_envision":    envision_tag in str(row.get("owner", "")),
            "cycles_per_day": _safe(row, "estimated_cycles_per_day"),
            "efficiency":     _safe(row, "efficiency"),
            "profit_per_mwh": _safe(row, "total_profit_per_discharge_mwh"),
            "strategy_style": inferred["style"],
            "evidence_level": inferred["evidence_level"],
            "rationale":      inferred["rationale"],
            "nodal_context":  nodal,
            "disclaimer":     DISCLAIMER,
        })

    return pd.DataFrame(records)


# ── Private helpers ───────────────────────────────────────────────────────────

def _safe(row: pd.Series, col: str) -> float | None:
    v = row.get(col, None)
    if v is None:
        return None
    try:
        f = float(v)
        return None if np.isnan(f) else f
    except (TypeError, ValueError):
        return None
