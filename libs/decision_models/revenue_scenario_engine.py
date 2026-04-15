"""
libs/decision_models/revenue_scenario_engine.py

Reusable model asset: BESS daily revenue scenario engine.

Wraps apps/trading/bess/mengxi/pnl_attribution/calc.py scenario P&L logic.
Self-registers into the module-level registry on import.

Usage:
    import libs.decision_models.revenue_scenario_engine
    from libs.decision_models.runners.local import run

    result = run("revenue_scenario_engine", {
        "asset_code": "suyou",
        "trade_date": date(2026, 4, 1),
        "actual_price": [...],            # 96 floats
        "scenario_dispatch": {
            "perfect_foresight_unrestricted": [...],   # 96 floats
            "cleared_actual": [...],
        },
        "compensation_yuan_per_mwh": 350.0,
    })
"""
from __future__ import annotations

import dataclasses
from datetime import date
from typing import Any, Dict, List

from libs.decision_models.model_spec import ModelSpec
from libs.decision_models.registry import registry
from libs.decision_models.schemas.revenue_scenario_engine import (
    ScenarioEngineInput,
    ScenarioEngineOutput,
    ScenarioResult,
)

_INTERVAL_HRS = 0.25  # 15-min intervals

MODEL_ASSUMPTIONS = {
    "granularity": "15min",
    "intervals_per_day": 96,
    "price_vector_length": 96,
    "dispatch_vector_length": 96,
    "interval_hours": 0.25,

    "revenue_formula": "sum(price[t] * dispatch_mw[t] * interval_h for t in 0..95)",
    "compensation_formula": "sum(max(dispatch_mw[t], 0) * interval_h for t in 0..95) * rate",
    "dispatch_sign_convention": "positive = discharge, negative = charge",

    "attribution_ladder": [
        "perfect_foresight_unrestricted",
        "perfect_foresight_grid_feasible",
        "tt_forecast_optimal",
        "tt_strategy",
        "nominated_dispatch",
        "cleared_actual",
    ],
    "attribution_losses": {
        "grid_restriction_loss": "perfect_foresight_unrestricted - perfect_foresight_grid_feasible",
        "forecast_error_loss": "perfect_foresight_grid_feasible - tt_forecast_optimal",
        "strategy_error_loss": "tt_forecast_optimal - tt_strategy",
        "nomination_loss": "tt_strategy - nominated_dispatch",
        "execution_clearing_loss": "nominated_dispatch - cleared_actual",
    },
    "partial_ladder": "Loss is None if either endpoint scenario is absent from scenario_dispatch",

    "limitations": [
        "15-min granularity only — does not handle hourly or sub-15-min dispatch",
        "Single-day scope only — one trade_date per call",
        "Attribution ladder losses are None when either endpoint scenario is missing",
        "No position sizing validation — dispatch values are taken as-is",
        "Compensation is a flat rate per MWh discharged — no time-of-day variation",
        "No network or settlement fees modelled",
        "Asset-level (nodal) pricing — uses actual_price as given",
    ],
}


def _calc_scenario_pnl(
    prices: List[float],
    dispatch_mw: List[float],
    compensation_yuan_per_mwh: float,
) -> ScenarioResult:
    """Compute revenue for one scenario given actual prices and dispatch profile."""
    market_revenue = sum(
        p * d * _INTERVAL_HRS for p, d in zip(prices, dispatch_mw)
    )
    discharge_mwh = sum(max(d, 0) * _INTERVAL_HRS for d in dispatch_mw)
    charge_mwh = sum(abs(min(d, 0)) * _INTERVAL_HRS for d in dispatch_mw)
    comp_revenue = discharge_mwh * compensation_yuan_per_mwh
    return ScenarioResult(
        scenario_name="",  # caller fills this in
        market_revenue_yuan=market_revenue,
        compensation_revenue_yuan=comp_revenue,
        total_revenue_yuan=market_revenue + comp_revenue,
        discharge_mwh=discharge_mwh,
        charge_mwh=charge_mwh,
    )


def _run(
    asset_code: str,
    trade_date: date,
    actual_price: List[float],
    scenario_dispatch: Dict[str, List[float]],
    compensation_yuan_per_mwh: float = 350.0,
) -> Dict[str, Any]:
    scenario_results: List[ScenarioResult] = []
    pnl_by_scenario: Dict[str, float] = {}

    for scenario_name, dispatch in scenario_dispatch.items():
        result = _calc_scenario_pnl(actual_price, dispatch, compensation_yuan_per_mwh)
        result.scenario_name = scenario_name
        scenario_results.append(result)
        pnl_by_scenario[scenario_name] = result.total_revenue_yuan

    # Attribution ladder — only compute when both endpoints are present
    def _loss(from_scenario: str, to_scenario: str):
        a = pnl_by_scenario.get(from_scenario)
        b = pnl_by_scenario.get(to_scenario)
        return (a - b) if (a is not None and b is not None) else None

    output = ScenarioEngineOutput(
        asset_code=asset_code,
        trade_date=trade_date,
        scenarios=scenario_results,
        grid_restriction_loss=_loss(
            "perfect_foresight_unrestricted", "perfect_foresight_grid_feasible"
        ),
        forecast_error_loss=_loss(
            "perfect_foresight_grid_feasible", "tt_forecast_optimal"
        ),
        strategy_error_loss=_loss("tt_forecast_optimal", "tt_strategy"),
        nomination_loss=_loss("tt_strategy", "nominated_dispatch"),
        execution_clearing_loss=_loss("nominated_dispatch", "cleared_actual"),
    )
    return dataclasses.asdict(output)


_SPEC = ModelSpec(
    name="revenue_scenario_engine",
    version="1.0.0",
    description=(
        "Daily BESS P&L attribution engine. "
        "Accepts actual RT prices and per-scenario dispatch profiles (96 × 15-min MW values). "
        "Returns per-scenario revenues (market + compensation) and the attribution ladder "
        "(grid_restriction_loss, forecast_error_loss, strategy_error_loss, "
        "nomination_loss, execution_clearing_loss). "
        "Logic is inlined — does not import from apps/trading."
    ),
    input_schema=ScenarioEngineInput,
    output_schema=ScenarioEngineOutput,
    run_fn=_run,
    tags=["bess", "pnl", "scenario", "attribution", "mengxi", "15min"],
    metadata={
        # Standard metadata contract keys
        "category": "analytics",
        "scope": "asset_level",
        "market": "mengxi",
        "asset_type": "bess",
        "granularity": "15min",
        "horizon": "historical",
        "deterministic": True,
        "model_family": "rule_based",
        "source_of_truth_module": "libs/decision_models/revenue_scenario_engine.py",
        "source_of_truth_functions": ["_calc_scenario_pnl"],
        "assumptions": MODEL_ASSUMPTIONS,
        "limitations": MODEL_ASSUMPTIONS["limitations"],
        "fallback_behavior": None,
        "status": "production",
        "owner": "bess-platform",

        # Domain-specific extras
        "intervals_per_day": 96,
        "interval_hours": 0.25,
        "attribution_ladder_scenarios": MODEL_ASSUMPTIONS["attribution_ladder"],
    },
)

registry.register(_SPEC)
