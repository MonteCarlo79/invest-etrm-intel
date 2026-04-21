"""
libs/decision_models/schemas/bess_spread_call_strip.py

Input/output contracts for the bess_spread_call_strip model.

The model treats a BESS asset as a strip of N daily spread call options:
  - Underlying 1: daily peak price F_pk
  - Underlying 2: daily offpeak price F_off
  - Payoff per MWh discharged: max(F_pk - F_off/η - K, 0)
    where η = roundtrip_eff, K = om_cost_yuan_per_mwh
  - Daily discharge capacity: q_max = η × power_mw × duration_h MWh
  - Strip value: q_max × Σᵢ C_Kirk(F_pk, F_off/η + K, T_i)

Pricing uses the Kirk/Margrabe spread call approximation (closed-form,
no external dependencies).
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class BESSSpreadCallInput:
    """
    Inputs for BESS spread call strip valuation.

    Forward prices and vols are provided externally (e.g. from historical
    price data or broker quotes). The model does not fetch market data.
    """
    asset_code: str
    as_of_date: str                          # ISO date string, e.g. '2026-04-21'
    n_days_remaining: int                    # number of daily options in strip (e.g. 252 for ~1yr)

    # Market inputs
    peak_forward_yuan: float                 # avg daily peak clearing price (¥/MWh)
    offpeak_forward_yuan: float              # avg daily offpeak clearing price (¥/MWh)
    peak_vol: float                          # annualised peak price vol, e.g. 0.30
    offpeak_vol: float                       # annualised offpeak price vol, e.g. 0.25

    # Correlation between peak and offpeak prices
    peak_offpeak_corr: float = 0.85

    # Asset physical parameters
    roundtrip_eff: float = 0.85              # roundtrip efficiency η ∈ (0, 1]
    power_mw: float = 100.0                  # inverter / power rating (MW)
    duration_h: float = 2.0                  # storage duration (hours)
    om_cost_yuan_per_mwh: float = 0.0        # O&M cost per MWh discharged (effective strike K)
    risk_free_rate: float = 0.0              # annualised risk-free rate (CNY, typically ~0)


@dataclass
class BESSSpreadCallOutput:
    """
    BESS spread call strip valuation output.

    All ¥ values are for the full strip (all n_days_remaining options combined),
    scaled by q_max_mwh_per_day (daily discharge capacity).
    """
    asset_code: str
    as_of_date: str

    # Strip value decomposition
    strip_value_yuan: float                  # total option value: Σᵢ C_Kirk(T_i) × q_max
    per_day_value_yuan: float               # strip_value / n_days_remaining
    effective_strike_yuan_per_mwh: float    # K = om_cost (O&M per MWh)
    intrinsic_value_yuan: float             # max(net_spread_forward - K, 0) × q_max × n_days
    time_value_yuan: float                  # strip_value - intrinsic_value
    net_spread_forward: float               # F_pk - F_off/η (adjusted spread before K)

    # Moneyness
    moneyness_pct: float                    # (net_spread_forward - K) / max(|K|, 1) × 100
                                            # positive = in the money

    # Greeks (strip-level, numerical finite differences)
    delta_yuan_per_yuan: float              # dV/dF_peak × q_max × n_days [¥ per ¥/MWh peak move]
    vega_yuan_per_vol_point: float          # dV/dσ_spread per 1 vol point (i.e. 1% = 0.01)
    theta_yuan_per_day: float               # -dV/dT per calendar day (always ≤ 0 for long option)

    # Asset / strip parameters
    n_days_remaining: int
    q_max_mwh_per_day: float               # η × power_mw × duration_h
    spread_vol_used: float                  # σ_spread = √(σ1² − 2ρσ1σ2 + σ2²) used in pricing
