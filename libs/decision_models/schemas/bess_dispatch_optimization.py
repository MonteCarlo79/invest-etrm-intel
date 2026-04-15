"""
libs/decision_models/schemas/bess_dispatch_optimization.py

Input/output contracts for the bess_dispatch_optimization model.

SCOPE: single-day, 24 hourly intervals, no cross-day SOC carryover.
Maps directly to optimise_day() in services/bess_map/optimisation_engine.py.

For multi-day batch usage see: schemas/bess_dispatch_simulation_multiday.py
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class DispatchOptInput:
    """
    Input for ONE day of BESS perfect-foresight dispatch optimisation.

    SCOPE CONSTRAINTS — read before using:
      - Exactly 24 hourly prices required (hourly granularity only).
      - SOC starts at 0 at the beginning of each day (no carryover from prior days).
      - No terminal SOC constraint; battery may end the day at any SOC level.
      - This model solves a SINGLE day in isolation.
        For multi-day continuity use bess_dispatch_simulation_multiday.

    Fields:
        prices_24:
            List of exactly 24 floats, ordered hour 0 (midnight) → hour 23.
            Unit: Yuan/MWh (or the market price unit in use).
            Must not contain NaN or inf.

        power_mw:
            Inverter / power rating (MW). Hard ceiling on both charge and
            discharge power in any single hour. Must be > 0.

        duration_h:
            Battery duration in hours. Determines energy capacity:
            energy_capacity_mwh = power_mw × duration_h. Must be > 0.

        roundtrip_eff:
            Round-trip efficiency in (0, 1]. e.g. 0.85 = 85%.
            Applied symmetrically: eta_charge = eta_discharge = sqrt(roundtrip_eff).
            NOTE: this differs from the old StorageOpt convention which used
            separate charge_eff / disch_eff multipliers.

        max_throughput_mwh:
            Optional. Cap on total discharge energy per day (MWh).
            Simple degradation / throughput proxy. No cap applied if None.

        max_cycles_per_day:
            Optional. Cap on equivalent full cycles per day.
            Implemented as: total_discharge_mwh ≤ cycles × energy_capacity_mwh.
            No cap applied if None.
    """
    prices_24: List[float]
    power_mw: float
    duration_h: float
    roundtrip_eff: float = 0.85
    max_throughput_mwh: Optional[float] = None
    max_cycles_per_day: Optional[float] = None


@dataclass
class DispatchOptOutput:
    """
    Output for ONE day of BESS dispatch optimisation.

    All arrays have exactly 24 elements (one per hour).
    Indices correspond to the same hour ordering as the input prices_24.

    Fields:
        charge_mw:            Charging power (MW, ≥ 0) per hour.
        discharge_mw:         Discharging power (MW, ≥ 0) per hour.
        dispatch_grid_mw:     Net grid dispatch = discharge_mw − charge_mw per hour.
                              Positive = net export (discharge), negative = net import (charge).
        soc_mwh:              End-of-interval state of charge (MWh) per hour.
                              soc_mwh[0] is SOC after hour 0.
                              soc_mwh[-1] is final SOC at end of day (unconstrained).
        profit:               Total day profit = sum(price[t] × dispatch_grid_mw[t])
                              in price-unit × MWh (e.g. Yuan).
        solver_status:        PuLP solver status string.
                              "Optimal"    — solution found and is globally optimal.
                              "Infeasible" — no feasible solution (check constraints).
                              Other values indicate solver issues.
        energy_capacity_mwh:  power_mw × duration_h (informational; not a decision variable).
    """
    charge_mw: List[float]
    discharge_mw: List[float]
    dispatch_grid_mw: List[float]
    soc_mwh: List[float]
    profit: float
    solver_status: str
    energy_capacity_mwh: float
