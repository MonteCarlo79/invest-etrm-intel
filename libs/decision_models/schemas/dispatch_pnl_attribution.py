"""
libs/decision_models/schemas/dispatch_pnl_attribution.py

Input/output contracts for the dispatch_pnl_attribution model.

The model takes pre-computed per-scenario PnL values and returns the
attribution ladder: losses at each step of the dispatch chain.

Scenario ladder order (highest to lowest potential value):
  1. perfect_foresight_unrestricted
  2. perfect_foresight_grid_feasible  → grid_restriction_loss
  3. tt_forecast_optimal              → forecast_error_loss
  4. tt_strategy                      → strategy_error_loss
  5. nominated                        → nomination_loss
  6. cleared_actual                   → execution_clearing_loss
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import List, Optional


@dataclass
class AttributionInput:
    """
    Per-scenario daily PnL values for a single BESS asset.

    Provide whichever scenarios are available; losses for missing scenario
    pairs are returned as None. All PnL values are in Yuan.
    """
    asset_code: str
    trade_date: date
    pf_unrestricted_pnl: Optional[float] = None
    pf_grid_feasible_pnl: Optional[float] = None
    tt_forecast_optimal_pnl: Optional[float] = None
    tt_strategy_pnl: Optional[float] = None
    nominated_pnl: Optional[float] = None
    cleared_actual_pnl: Optional[float] = None


@dataclass
class AttributionOutput:
    """
    Attribution ladder output.

    Loss fields are positive when the upstream scenario outperforms the
    downstream (i.e. value was lost moving down the ladder). None when
    either endpoint scenario was not provided.
    """
    asset_code: str
    trade_date: date

    # Input scenario PnL — echoed back for auditability
    pf_unrestricted_pnl: Optional[float]
    pf_grid_feasible_pnl: Optional[float]
    tt_forecast_optimal_pnl: Optional[float]
    tt_strategy_pnl: Optional[float]
    nominated_pnl: Optional[float]
    cleared_actual_pnl: Optional[float]

    # Attribution ladder
    grid_restriction_loss: Optional[float]
    forecast_error_loss: Optional[float]
    strategy_error_loss: Optional[float]
    nomination_loss: Optional[float]
    execution_clearing_loss: Optional[float]

    # Aggregate
    realisation_gap_vs_pf: Optional[float]       # pf_unrestricted - cleared_actual
    realisation_gap_vs_pf_grid: Optional[float]  # pf_grid_feasible - cleared_actual

    # Scenarios that were provided (non-None) in input
    scenarios_available: List[str] = field(default_factory=list)
