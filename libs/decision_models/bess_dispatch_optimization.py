"""
libs/decision_models/bess_dispatch_optimization.py

Reusable model asset: BESS perfect-foresight dispatch optimisation — SINGLE DAY.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SCOPE: ONE DAY, 24 HOURLY INTERVALS, NO CROSS-DAY SOC CARRYOVER
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

This model solves a single-day LP for one set of 24 hourly prices.
It does NOT handle multi-day continuity or cross-day SOC carryover.
For multi-day batch simulation use: bess_dispatch_simulation_multiday.

Source of truth: services/bess_map/optimisation_engine.optimise_day()
Production pipeline that uses this engine: services/bess_map/run_capture_pipeline.py

This module is a thin registry wrapper. All LP logic stays in the engine.
Self-registers into the module-level registry on import.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
REQUIREMENTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    - bess-platform repo root must be on PYTHONPATH so that
      `from services.bess_map.optimisation_engine import ...` resolves.
      This is the project-wide convention used throughout the repo.
    - pulp and its bundled CBC solver must be installed.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
USAGE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Via registry runner (recommended for apps and agents):

    import libs.decision_models.bess_dispatch_optimization   # register
    from libs.decision_models.runners.local import run

    result = run("bess_dispatch_optimization", {
        "prices_24": [30.0]*8 + [120.0]*4 + [30.0]*4 + [120.0]*4 + [30.0]*4,
        "power_mw": 100.0,
        "duration_h": 2.0,
        "roundtrip_eff": 0.85,
    })
    # Keys: charge_mw (24), discharge_mw (24), dispatch_grid_mw (24),
    #       soc_mwh (24), profit, solver_status, energy_capacity_mwh

Direct engine call (no registry, for pipeline code):

    from services.bess_map.optimisation_engine import optimise_day
    import numpy as np
    res = optimise_day(np.array([...24 prices...]), power_mw=100, duration_h=2, roundtrip_eff=0.85)
    # res.charge_mw, res.discharge_mw, res.soc_mwh, res.profit, res.status
"""
from __future__ import annotations

import dataclasses
from typing import Any, Dict

import numpy as np

from libs.decision_models.model_spec import ModelSpec
from libs.decision_models.registry import registry
from libs.decision_models.schemas.bess_dispatch_optimization import (
    DispatchOptInput,
    DispatchOptOutput,
)

# ---------------------------------------------------------------------------
# Model assumptions and known limitations (machine-readable + human-readable)
# ---------------------------------------------------------------------------

MODEL_ASSUMPTIONS = {
    # Optimisation scope
    "horizon": "single_day",
    "granularity": "hourly",
    "intervals_per_day": 24,
    "price_vector_length": 24,

    # SOC boundary conditions
    "initial_soc": "zero",                      # SOC is forced to 0 at t=0
    "terminal_soc": "unconstrained",            # SOC at end of day is not constrained
    "cross_day_soc_carryover": False,           # each day is solved independently

    # Physics model
    "simultaneous_charge_discharge": False,     # binary variable enforces this
    "efficiency_model": "symmetric_sqrt",       # eta_c = eta_d = sqrt(roundtrip_eff)
    "efficiency_note": (
        "Split symmetrically from round-trip efficiency. "
        "Different from legacy StorageOpt which used separate charge_eff/disch_eff multipliers."
    ),

    # Solver
    "solver": "pulp_cbc",
    "solver_type": "milp",                      # mixed-integer LP (binary y variable)
    "problem_type": "arbitrage_maximisation",

    # Known limitations
    "limitations": [
        "24-hour horizon only — cannot model intra-day rolling dispatch",
        "No cross-day SOC carryover — each day resets to SOC=0",
        "No terminal SOC constraint — battery may end day at any level",
        "Hourly granularity only — does not support 15-min or sub-hourly dispatch",
        "Perfect foresight on prices — not a real-time or forecast-based model",
        "No degradation model beyond optional throughput/cycle caps",
        "No ramp rate constraints",
        "No minimum charge/discharge duration constraints",
        "Assumes lossless grid connection (no transmission charges modelled)",
    ],
}


# ---------------------------------------------------------------------------
# run_fn
# ---------------------------------------------------------------------------

def _run(
    prices_24: list,
    power_mw: float,
    duration_h: float,
    roundtrip_eff: float = 0.85,
    max_throughput_mwh: float = None,
    max_cycles_per_day: float = None,
) -> Dict[str, Any]:
    """
    Validate inputs then delegate to the production LP engine.

    The engine import is deferred (inside the function) so that pulp is not
    loaded at model registration time — only when the model is actually run.
    """
    from services.bess_map.optimisation_engine import optimise_day

    # --- Input validation ---
    if len(prices_24) != 24:
        raise ValueError(
            f"prices_24 must have exactly 24 elements (one per hour). "
            f"Got {len(prices_24)}. "
            "This model is single-day only. "
            "For multi-day use bess_dispatch_simulation_multiday."
        )
    if power_mw <= 0:
        raise ValueError(f"power_mw must be positive, got {power_mw}")
    if duration_h <= 0:
        raise ValueError(f"duration_h must be positive, got {duration_h}")
    if not (0 < roundtrip_eff <= 1):
        raise ValueError(
            f"roundtrip_eff must be in (0, 1], got {roundtrip_eff}. "
            "This is the round-trip efficiency (e.g. 0.85), not a split factor."
        )

    prices_arr = np.array(prices_24, dtype=float)
    if not np.isfinite(prices_arr).all():
        raise ValueError("prices_24 contains NaN or inf values")

    res = optimise_day(
        prices=prices_arr,
        power_mw=float(power_mw),
        duration_h=float(duration_h),
        roundtrip_eff=float(roundtrip_eff),
        max_throughput_mwh=float(max_throughput_mwh) if max_throughput_mwh is not None else None,
        max_cycles_per_day=float(max_cycles_per_day) if max_cycles_per_day is not None else None,
    )

    output = DispatchOptOutput(
        charge_mw=res.charge_mw.tolist(),
        discharge_mw=res.discharge_mw.tolist(),
        dispatch_grid_mw=(res.discharge_mw - res.charge_mw).tolist(),
        soc_mwh=res.soc_mwh.tolist(),
        profit=res.profit,
        solver_status=res.status,
        energy_capacity_mwh=float(power_mw) * float(duration_h),
    )
    return dataclasses.asdict(output)


# ---------------------------------------------------------------------------
# ModelSpec — registered on import
# ---------------------------------------------------------------------------

_SPEC = ModelSpec(
    name="bess_dispatch_optimization",
    version="1.0.0",
    description=(
        "SINGLE-DAY perfect-foresight BESS arbitrage dispatch optimisation. "
        "Input: 24 hourly prices + battery parameters. "
        "Output: hour-by-hour charge/discharge schedule, SOC trajectory, daily profit. "
        "SOC resets to 0 each day; no cross-day carryover. "
        "Solver: PuLP/CBC MILP. "
        "For multi-day batch use bess_dispatch_simulation_multiday."
    ),
    input_schema=DispatchOptInput,
    output_schema=DispatchOptOutput,
    run_fn=_run,
    tags=["bess", "dispatch", "optimization", "perfect_foresight", "lp", "single_day", "arbitrage"],
    metadata={
        # Standard metadata contract keys
        "category": "optimization",
        "scope": "single_day",
        "market": None,
        "asset_type": "bess",
        "granularity": "hourly",
        "horizon": "single_day",
        "deterministic": True,
        "model_family": "lp_milp",
        "source_of_truth_module": "services/bess_map/optimisation_engine.py",
        "source_of_truth_functions": ["optimise_day"],
        "assumptions": MODEL_ASSUMPTIONS,
        "limitations": MODEL_ASSUMPTIONS["limitations"],
        "fallback_behavior": None,
        "status": "production",
        "owner": "bess-platform",

        # Domain-specific extras
        "production_pipeline": "services/bess_map/run_capture_pipeline.py",
        "intervals_per_day": 24,
        "price_vector_length_required": 24,
        "initial_soc": "zero",
        "terminal_soc": "unconstrained",
        "cross_day_soc_carryover": False,
        "solver": "pulp_cbc",
        "solver_type": "milp",
        "multiday_model": "bess_dispatch_simulation_multiday",
    },
)

registry.register(_SPEC)
