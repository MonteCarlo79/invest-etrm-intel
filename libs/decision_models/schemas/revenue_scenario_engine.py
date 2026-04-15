"""
libs/decision_models/schemas/revenue_scenario_engine.py

Input/output contracts for the revenue_scenario_engine model.
Wraps the scenario P&L logic in apps/trading/bess/mengxi/pnl_attribution/calc.py.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional


@dataclass
class ScenarioEngineInput:
    """
    Input for daily scenario revenue calculation for a single BESS asset.

    asset_code:        stable internal asset code (e.g. "suyou", "wulate")
    trade_date:        the business date
    actual_price:      list of 96 15-min real-time nodal prices for the day
    scenario_dispatch: mapping of scenario_name -> list of 96 dispatch MW values
                       Only include scenarios that are available for this asset/date.
    compensation_yuan_per_mwh: asset/month compensation rate (from core.asset_monthly_compensation)
    """
    asset_code: str
    trade_date: date
    actual_price: List[float]          # 96 values (15-min RT price)
    scenario_dispatch: Dict[str, List[float]]  # scenario_name -> [96 MW values]
    compensation_yuan_per_mwh: float = 350.0


@dataclass
class ScenarioResult:
    scenario_name: str
    market_revenue_yuan: float
    compensation_revenue_yuan: float
    total_revenue_yuan: float
    discharge_mwh: float
    charge_mwh: float


@dataclass
class ScenarioEngineOutput:
    asset_code: str
    trade_date: date
    scenarios: List[ScenarioResult]
    # Attribution ladder — None if a scenario is not available
    grid_restriction_loss: Optional[float] = None
    forecast_error_loss: Optional[float] = None
    strategy_error_loss: Optional[float] = None
    nomination_loss: Optional[float] = None
    execution_clearing_loss: Optional[float] = None
