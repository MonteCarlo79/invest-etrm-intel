# services/nodal_agents/recursion.py
"""Recursive convergence for the L3 agents (spec SS2 feedback loop).

Each iteration: curves_fn regenerates price curves from the aggregate BESS
dispatch prediction (bidding-space adjustment lives inside curves_fn) ->
every agent re-optimizes -> compare total dispatch energy change against the
previous iteration. Stop when the change falls below tol_mwh_pct (% of the
previous iteration's total |dispatch| energy) or max_iter is reached. The
converged strategy is the "recursive optimal dispatch" — downstream
evaluation never uses the first-pass one.

curves_fn contract: curves_fn(iter_no, dispatch) where dispatch is the dict
{plant_name: curve (96,)} from the previous iteration (None on the first
call); returns either a single (96,) ndarray shared by all assets or a dict
{plant_name: (96,)}.

Cap coordination: each asset's `others` = its others_baseline (zone forecast
of non-agent BESS, default 0) + the curves of the OTHER agents at the SAME
substation (assets carry an optional "substation" key; assets without one
form singleton groups). Updates are Gauss-Seidel: within an iteration each
asset sees the CURRENT-iteration curves of peers already solved and the
previous-iteration curves of the rest — simultaneous updates can oscillate
when a shared cap binds. Fixed-point priority follows list order (v1;
spec SS5.2's proportional split is future work). Per-asset LP enforces the
cap via agent.optimize_asset's headroom scaling.
"""
from __future__ import annotations

import numpy as np

from services.nodal_agents import agent as _agent

DT_H = 0.25  # 15-min intervals
_EPS_MWH = 1e-9


def _curve_for(curves, plant: str) -> np.ndarray:
    c = curves.get(plant) if isinstance(curves, dict) else curves
    if c is None:
        raise ValueError(f"curves_fn returned no curve for {plant!r}")
    c = np.nan_to_num(np.asarray(c, dtype=float), nan=0.0)
    if c.shape != (96,):
        raise ValueError(f"curve for {plant!r} must be (96,), got {c.shape}")
    return c


def _energy_mwh(curves: dict) -> float:
    return float(sum(np.abs(c).sum() * DT_H for c in curves.values()))


def converge(assets, curves_fn, max_iter: int = 3, tol_mwh_pct: float = 2.0) -> dict:
    if max_iter < 2:
        raise ValueError("max_iter must be >= 2 — convergence needs an initial "
                         "pass plus at least one regenerated pass")
    assets = list(assets)
    if not assets:
        return {"strategies": {}, "iterations": 0, "convergence_delta_mwh": 0.0}

    prev = None
    strategies = {}
    delta_mwh = 0.0
    iterations = 0
    for it in range(max_iter):
        curves = curves_fn(it, prev)
        substation_peers = {}
        for a in assets:
            substation_peers.setdefault(a.get("substation"), []).append(a["plant_name"])

        dispatch = {}
        for a in assets:
            plant = a["plant_name"]
            others = np.array(a.get("others_baseline", np.zeros(96)), dtype=float)
            peers = substation_peers.get(a.get("substation"), [])
            for peer in peers:
                if peer == plant:
                    continue
                # Gauss-Seidel: prefer current-iteration peer curves
                peer_curve = dispatch.get(peer, prev.get(peer) if prev else None)
                if peer_curve is not None:
                    others = others + np.maximum(peer_curve, 0.0)
            zones = a.get("zones", np.zeros(96, dtype=int))
            res = _agent.optimize_asset(a, _curve_for(curves, plant), others,
                                        cap_mw=a.get("cap_mw"), zones=zones)
            strategies[plant] = res
            dispatch[plant] = res["curve"]

        iterations = it + 1
        if prev is not None:
            moved = sum(np.abs(dispatch[p] - prev[p]).sum() * DT_H for p in dispatch)
            delta_mwh = float(moved)
            pct = 100.0 * moved / max(_energy_mwh(prev), _EPS_MWH)
            if pct < tol_mwh_pct:
                break
        prev = dispatch

    return {"strategies": strategies,
            "iterations": iterations,
            "convergence_delta_mwh": delta_mwh}
