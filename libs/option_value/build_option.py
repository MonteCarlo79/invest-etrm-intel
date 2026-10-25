"""Permit & M&A acquisition-option valuation (spec §5.6).

The engine's per-kW asset value is the *underlying*; a development permit
(开发指标) or an M&A ask is an option *on* it. Two entry points:

- ``value_permit`` — scenario-weighted decision tree over the permit's
  validity window. Nodes = sorted unique scenario effective dates + expiry;
  states = subsets of activated scenarios (each scenario an independent
  Bernoulli reveal at its own effective date; once active it stays active).
  At each node the holder builds iff NPV(tau, state) > 0, where

      NPV(tau, state) = annuitize((energy_value + policy_net(state))
                                  * power_mw * 1000, hurdle, life_years)
                        - capex(tau)
      capex(tau)      = capex_yuan_kwh_today * (1 - decline)^tau
                        * power_mw * duration_h * 1000

  ``energy_value_yuan_kw`` is the capture-adjusted ENERGY part of V_cap
  (intrinsic + extrinsic after haircut), passed in by the caller and held
  constant across states; only the policy stack flips with the state.
  Root value - permit_cost = v_permit_yuan; breakeven_permit_yuan = root
  value before cost. Cross-check: Dixit-Pindyck perpetual build option.

- ``value_acquisition`` — distressed-asset purchase: fair NPV vs ask, with
  an actuals anchor that REPLACES the model energy value when available.

Simplifications (documented, spec §5.6):
- volume for volume-based policy levers is held at the caller-provided base
  volume (the engine's MC-mean) in every state; it does not re-simulate per
  scenario. Both value functions therefore take ``volume_mwh_per_kw``
  (default 0.0, exact whenever all volume-based levers are zero).
- scenario effective dates are converted to tau in years from
  ``date.today()`` (the valuation date), clamped at 0 — a scenario already
  in effect resolves at the root.
- scenarios sharing one effective_date are mutually exclusive reveals:
  their probabilities must sum to <= 1 (else ValueError). Scenarios on
  different dates are independent — no constraint.
- when two active scenarios override the same lever, the one LATER in
  (effective_date, name) order wins.

Pure computation only: no DB, no file I/O, no streamlit, no RNG.
Units: energy_value ¥/kW/yr in; NPV / values ¥ out; capex ¥/kWh x kWh.
"""

from __future__ import annotations

import dataclasses
import math
from datetime import date

from .contracts import (
    AcquisitionResult,
    PermitInputs,
    PermitResult,
    PolicyLevers,
    PolicyScenario,
)
from .value_stack import annuitize, apply_policy

_DAYS_PER_YEAR = 365.25


def dp_perpetual_build(
    v: float, cost: float, sigma: float, r: float, rn_drift: float
) -> float:
    """Dixit-Pindyck perpetual build option, closed form.

    Reimplemented verbatim from
    ``power-academy/labs/real_options_framework_for_generation_assets/compute.py:17-36``
    (power-academy is not an importable dependency of libs):

        a       = 1/2 - rn_drift / sigma^2
        beta1   = a + sqrt(a^2 + 2r / sigma^2)
        trigger = beta1 / (beta1 - 1) * cost          (wait multiplier x cost)
        value   = v - cost                            if v >= trigger
                = (trigger - cost) * (v / trigger)^beta1   otherwise

    Degenerate inputs fall back to the immediate-exercise value
    ``max(v - cost, 0)``: no uncertainty (sigma <= 0), non-positive project
    value, or beta1 <= 1 (the r <= 0 singularity where the perpetual wait
    multiplier diverges).
    """
    v = float(v)
    cost = float(cost)
    if v <= 0.0 or sigma <= 0.0:
        return max(v - cost, 0.0)
    a = 0.5 - float(rn_drift) / float(sigma) ** 2
    beta1 = a + math.sqrt(a**2 + 2.0 * float(r) / float(sigma) ** 2)
    if beta1 <= 1.0:
        return max(v - cost, 0.0)
    trigger = beta1 / (beta1 - 1.0) * cost
    if v >= trigger:
        return v - cost
    return (trigger - cost) * (v / trigger) ** beta1


def _capex(inputs: PermitInputs, tau: float) -> float:
    """Build cost at tau years from today, ¥."""
    return (
        inputs.capex_yuan_kwh_today
        * (1.0 - inputs.capex_decline_rate) ** tau
        * inputs.power_mw
        * inputs.duration_h
        * 1000.0
    )


def _state_levers(
    base_levers: PolicyLevers,
    scenarios: list[PolicyScenario],
    active: frozenset,
) -> PolicyLevers:
    """Base levers with the active scenarios' overrides merged in order."""
    merged: dict = {}
    for i in sorted(active):
        merged.update(scenarios[i].lever_overrides)
    return dataclasses.replace(base_levers, **merged) if merged else base_levers


def _npv(
    tau: float,
    state: frozenset,
    scenarios: list[PolicyScenario],
    base_levers: PolicyLevers,
    energy_value_yuan_kw: float,
    asset_type: str,
    rte: float,
    volume_mwh_per_kw: float,
    inputs: PermitInputs,
) -> float:
    """NPV of building at ``tau`` under ``state``, ¥ (time-tau quantity)."""
    levers = _state_levers(base_levers, scenarios, state)
    policy_net, _ = apply_policy(levers, asset_type, volume_mwh_per_kw, rte)
    annual_yuan = (energy_value_yuan_kw + policy_net) * inputs.power_mw * 1000.0
    return annuitize(annual_yuan, inputs.hurdle, inputs.life_years) - _capex(
        inputs, tau
    )


def _validate_probabilities(scenarios: list[PolicyScenario]) -> None:
    """Same-date scenarios are mutually exclusive reveals: p must sum <= 1."""
    for sc in scenarios:
        if not 0.0 <= sc.probability <= 1.0:
            raise ValueError(
                f"scenario {sc.name!r} probability {sc.probability} outside [0, 1]"
            )
    by_date: dict[date, float] = {}
    for sc in scenarios:
        by_date[sc.effective_date] = by_date.get(sc.effective_date, 0.0) + sc.probability
    for d, total in by_date.items():
        if total > 1.0 + 1e-9:
            raise ValueError(
                f"scenarios effective {d} have probabilities summing to "
                f"{total:.6g} > 1 — same-date reveals are mutually exclusive"
            )


def _tree_root_value(
    scenarios: list[PolicyScenario],
    taus: list[float],
    base_levers: PolicyLevers,
    energy_value_yuan_kw: float,
    asset_type: str,
    rte: float,
    volume_mwh_per_kw: float,
    inputs: PermitInputs,
) -> float:
    """Backward-induction value of the permit before its cost, ¥.

    Nodes = [0] + sorted unique scenario taus in (0, validity) + [validity].
    At each node the newly-effective scenarios reveal first (independent
    Bernoulli), then the holder exercises iff NPV > continuation value.
    Continuation values discount at the hurdle over the node gap.
    """
    interior = sorted({t for t in taus if 0.0 < t < inputs.validity_years})
    nodes = [0.0] + interior + [float(inputs.validity_years)]
    newly = [
        tuple(i for i, t in enumerate(taus) if t == node_tau) for node_tau in nodes
    ]
    last = len(nodes) - 1

    memo: dict = {}

    def node_value(k: int, active: frozenset) -> float:
        key = (k, active)
        if key in memo:
            return memo[key]
        # Reveal distribution over states after this node's Bernoulli draws.
        dist = [(active, 1.0)]
        for i in newly[k]:
            p = scenarios[i].probability
            dist = [(s | {i}, q * p) for s, q in dist] + [
                (s, q * (1.0 - p)) for s, q in dist
            ]
        value = 0.0
        for state, prob in dist:
            npv = _npv(
                nodes[k],
                state,
                scenarios,
                base_levers,
                energy_value_yuan_kw,
                asset_type,
                rte,
                volume_mwh_per_kw,
                inputs,
            )
            if k == last:
                value += prob * max(npv, 0.0)
            else:
                gap = nodes[k + 1] - nodes[k]
                cont = (1.0 + inputs.hurdle) ** (-gap) * node_value(k + 1, state)
                value += prob * max(npv, cont)
        memo[key] = value
        return value

    return node_value(0, frozenset())


def value_permit(
    scenarios: list[PolicyScenario],
    base_levers: PolicyLevers,
    energy_value_yuan_kw: float,
    asset_type: str,
    rte: float,
    inputs: PermitInputs,
    volume_mwh_per_kw: float = 0.0,
) -> PermitResult:
    """Value a greenfield development permit as an option to build.

    ``scenarios`` is designed for n <= 4 (<= 16 states per node). Root value
    minus ``inputs.permit_cost_yuan`` is ``v_permit_yuan``; GO iff positive.
    ``decomposition`` maps each scenario name to its leave-one-out marginal
    contribution to the root value, plus a ``notes`` list.
    """
    _validate_probabilities(scenarios)
    scenarios = sorted(scenarios, key=lambda sc: (sc.effective_date, sc.name))
    today = date.today()
    taus = [
        max(0.0, (sc.effective_date - today).days / _DAYS_PER_YEAR)
        for sc in scenarios
    ]

    def root_value(sc_set: list[PolicyScenario], tau_set: list[float]) -> float:
        return _tree_root_value(
            sc_set,
            tau_set,
            base_levers,
            energy_value_yuan_kw,
            asset_type,
            rte,
            volume_mwh_per_kw,
            inputs,
        )

    root = root_value(scenarios, taus)

    # Per-scenario marginal contribution: leave-one-out.
    per_scenario: dict[str, float] = {}
    for i, sc in enumerate(scenarios):
        reduced = root_value(
            scenarios[:i] + scenarios[i + 1 :], taus[:i] + taus[i + 1 :]
        )
        per_scenario[sc.name] = root - reduced

    # Dixit-Pindyck cross-check on the tau=0 lattice: v = base-state NPV,
    # cost = capex(0), sigma = spread of the scenario-state NPVs.
    notes: list[str] = []
    base_npv0 = _npv(
        0.0,
        frozenset(),
        scenarios,
        base_levers,
        energy_value_yuan_kw,
        asset_type,
        rte,
        volume_mwh_per_kw,
        inputs,
    )
    state_npvs = [
        _npv(
            0.0,
            frozenset(j for j in range(len(scenarios)) if mask >> j & 1),
            scenarios,
            base_levers,
            energy_value_yuan_kw,
            asset_type,
            rte,
            volume_mwh_per_kw,
            inputs,
        )
        for mask in range(1 << len(scenarios))
    ]
    if len(state_npvs) >= 2:
        mean = sum(state_npvs) / len(state_npvs)
        sigma = math.sqrt(sum((x - mean) ** 2 for x in state_npvs) / len(state_npvs))
    else:
        sigma = 0.0
    if sigma > 0.0:
        dp_check = dp_perpetual_build(
            base_npv0, _capex(inputs, 0.0), sigma, inputs.hurdle, 0.0
        )
        if base_npv0 <= 0.0 or inputs.hurdle <= 0.0:
            notes.append(
                "dp_check_value is a degenerate Dixit-Pindyck fallback "
                "(max(v - cost, 0)): base-state NPV <= 0 or non-positive "
                "hurdle makes the perpetual closed form inapplicable"
            )
    else:
        dp_check = max(base_npv0, 0.0)
        notes.append(
            "dp_check_value is the immediate-exercise value max(base NPV, 0): "
            "fewer than 2 scenario states (or zero NPV spread) leaves sigma "
            "undefined for the Dixit-Pindyck cross-check"
        )

    v_permit = root - inputs.permit_cost_yuan
    return PermitResult(
        v_permit_yuan=v_permit,
        breakeven_permit_yuan=root,
        go=v_permit > 0.0,
        decomposition={"per_scenario": per_scenario, "notes": notes},
        dp_check_value=dp_check,
    )


def value_acquisition(
    ask_yuan: float,
    energy_value_yuan_kw: float,
    levers: PolicyLevers,
    power_mw: float,
    duration_h: float,
    asset_type: str,
    rte: float,
    uplift_rows: list[tuple[str, float]],
    actuals_anchor_yuan_kw: float | None,
    hurdle: float = 0.08,
    life_years: int = 15,
    volume_mwh_per_kw: float = 0.0,
) -> AcquisitionResult:
    """Value a distressed-asset purchase: fair NPV vs ask.

    When ``actuals_anchor_yuan_kw`` is not None it REPLACES the model
    ``energy_value_yuan_kw`` (realized monetization beats simulation).
    ``uplift_rows`` are (name, ¥/kW/yr) turnaround levers added on top.
    ``rows`` decomposes each uplift's NPV contribution; ``margin`` = fair
    NPV - ask; GO iff margin > 0.
    """
    energy = (
        float(actuals_anchor_yuan_kw)
        if actuals_anchor_yuan_kw is not None
        else float(energy_value_yuan_kw)
    )
    policy_net, _ = apply_policy(levers, asset_type, volume_mwh_per_kw, rte)
    kw_scale = float(power_mw) * 1000.0

    rows = []
    uplift_total = 0.0
    for name, uplift_yuan_kw in uplift_rows:
        uplift_total += float(uplift_yuan_kw)
        rows.append(
            {
                "name": name,
                "annual_yuan_kw": float(uplift_yuan_kw),
                "npv_yuan": annuitize(
                    float(uplift_yuan_kw) * kw_scale, hurdle, life_years
                ),
            }
        )

    annual_yuan = (energy + policy_net + uplift_total) * kw_scale
    fair_npv = annuitize(annual_yuan, hurdle, life_years)
    margin = fair_npv - float(ask_yuan)
    return AcquisitionResult(
        fair_npv_yuan=fair_npv,
        margin_yuan=margin,
        go=margin > 0.0,
        rows=rows,
    )
