"""
libs/decision_models/dispatch_pnl_attribution.py

Registered decision model: BESS dispatch P&L attribution.

Takes pre-computed per-scenario PnL values and returns the attribution
ladder — losses at each step of the dispatch chain from perfect foresight
down to cleared actual.

Self-registers into the module-level registry on import.

Usage:
    import libs.decision_models.dispatch_pnl_attribution
    from libs.decision_models.runners.local import run

    result = run("dispatch_pnl_attribution", {
        "asset_code": "suyou",
        "trade_date": date(2026, 4, 1),
        "pf_unrestricted_pnl": 85000.0,
        "pf_grid_feasible_pnl": 78000.0,
        "tt_forecast_optimal_pnl": 71000.0,
        "tt_strategy_pnl": 68000.0,
        "nominated_pnl": 63000.0,
        "cleared_actual_pnl": 60000.0,
    })
"""
from __future__ import annotations

import dataclasses
from datetime import date
from typing import Any, Dict, List, Optional

from libs.decision_models.model_spec import ModelSpec
from libs.decision_models.registry import registry
from libs.decision_models.schemas.dispatch_pnl_attribution import (
    AttributionInput,
    AttributionOutput,
)

_SCENARIO_FIELDS = [
    "pf_unrestricted_pnl",
    "pf_grid_feasible_pnl",
    "tt_forecast_optimal_pnl",
    "tt_strategy_pnl",
    "nominated_pnl",
    "cleared_actual_pnl",
]

MODEL_ASSUMPTIONS = {
    "attribution_ladder": [
        "pf_unrestricted_pnl",
        "pf_grid_feasible_pnl",
        "tt_forecast_optimal_pnl",
        "tt_strategy_pnl",
        "nominated_pnl",
        "cleared_actual_pnl",
    ],
    "loss_definitions": {
        "grid_restriction_loss": "pf_unrestricted_pnl - pf_grid_feasible_pnl",
        "forecast_error_loss": "pf_grid_feasible_pnl - tt_forecast_optimal_pnl",
        "strategy_error_loss": "tt_forecast_optimal_pnl - tt_strategy_pnl",
        "nomination_loss": "tt_strategy_pnl - nominated_pnl",
        "execution_clearing_loss": "nominated_pnl - cleared_actual_pnl",
    },
    "loss_sign_convention": "positive = value lost at this step (upstream > downstream)",
    "partial_ladder": "Loss is None if either endpoint scenario is None",
    "realisation_gap_vs_pf": "pf_unrestricted_pnl - cleared_actual_pnl",
    "realisation_gap_vs_pf_grid": "pf_grid_feasible_pnl - cleared_actual_pnl",
    "limitations": [
        "Requires pre-computed scenario PnL — does not fetch raw prices or dispatch",
        "No time-of-day granularity — daily totals only",
        "Attribution is additive waterfall only — not causal decomposition",
        "Losses can be negative (downstream scenario outperforms upstream) — not clamped",
        "Single-asset, single-day scope per call",
    ],
}


def _diff(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """Return a - b if both are not None, else None."""
    return (a - b) if (a is not None and b is not None) else None


def _run(
    asset_code: str,
    trade_date: date,
    pf_unrestricted_pnl: Optional[float] = None,
    pf_grid_feasible_pnl: Optional[float] = None,
    tt_forecast_optimal_pnl: Optional[float] = None,
    tt_strategy_pnl: Optional[float] = None,
    nominated_pnl: Optional[float] = None,
    cleared_actual_pnl: Optional[float] = None,
) -> Dict[str, Any]:
    scenarios_available = [
        f
        for f, v in {
            "pf_unrestricted_pnl": pf_unrestricted_pnl,
            "pf_grid_feasible_pnl": pf_grid_feasible_pnl,
            "tt_forecast_optimal_pnl": tt_forecast_optimal_pnl,
            "tt_strategy_pnl": tt_strategy_pnl,
            "nominated_pnl": nominated_pnl,
            "cleared_actual_pnl": cleared_actual_pnl,
        }.items()
        if v is not None
    ]

    output = AttributionOutput(
        asset_code=asset_code,
        trade_date=trade_date,
        pf_unrestricted_pnl=pf_unrestricted_pnl,
        pf_grid_feasible_pnl=pf_grid_feasible_pnl,
        tt_forecast_optimal_pnl=tt_forecast_optimal_pnl,
        tt_strategy_pnl=tt_strategy_pnl,
        nominated_pnl=nominated_pnl,
        cleared_actual_pnl=cleared_actual_pnl,
        grid_restriction_loss=_diff(pf_unrestricted_pnl, pf_grid_feasible_pnl),
        forecast_error_loss=_diff(pf_grid_feasible_pnl, tt_forecast_optimal_pnl),
        strategy_error_loss=_diff(tt_forecast_optimal_pnl, tt_strategy_pnl),
        nomination_loss=_diff(tt_strategy_pnl, nominated_pnl),
        execution_clearing_loss=_diff(nominated_pnl, cleared_actual_pnl),
        realisation_gap_vs_pf=_diff(pf_unrestricted_pnl, cleared_actual_pnl),
        realisation_gap_vs_pf_grid=_diff(pf_grid_feasible_pnl, cleared_actual_pnl),
        scenarios_available=scenarios_available,
    )
    return dataclasses.asdict(output)


_SPEC = ModelSpec(
    name="dispatch_pnl_attribution",
    version="1.0.0",
    description=(
        "BESS daily dispatch P&L attribution. "
        "Accepts pre-computed per-scenario PnL values and returns the attribution ladder: "
        "grid_restriction_loss, forecast_error_loss, strategy_error_loss, "
        "nomination_loss, execution_clearing_loss, plus realisation gaps vs PF benchmarks. "
        "Losses are None when either endpoint scenario was not provided."
    ),
    input_schema=AttributionInput,
    output_schema=AttributionOutput,
    run_fn=_run,
    tags=["bess", "pnl", "attribution", "monitoring", "mengxi", "daily"],
    metadata={
        "category": "analytics",
        "scope": "asset_level",
        "market": "mengxi",
        "asset_type": "bess",
        "granularity": "daily",
        "horizon": "historical",
        "deterministic": True,
        "model_family": "rule_based",
        "source_of_truth_module": "libs/decision_models/dispatch_pnl_attribution.py",
        "source_of_truth_functions": ["_run", "_diff"],
        "assumptions": MODEL_ASSUMPTIONS,
        "limitations": MODEL_ASSUMPTIONS["limitations"],
        "fallback_behavior": None,
        "status": "production",
        "owner": "bess-platform",
        "attribution_ladder_scenarios": MODEL_ASSUMPTIONS["attribution_ladder"],
        "persisted_table": "reports.bess_asset_daily_attribution",
        "batch_job": "services/monitoring/run_daily_attribution.py",
    },
)

registry.register(_SPEC)
