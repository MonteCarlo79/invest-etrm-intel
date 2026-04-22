"""
libs/decision_models/bess_spread_call_strip.py

Registered decision model: BESS spread call strip valuation.

Treats a BESS asset as a strip of N daily spread call options priced via the
Kirk/Margrabe approximation (closed-form, no external dependencies).

Model:
  - Payoff per MWh discharged: max(F_pk - F_off/η - K, 0)
    where η = roundtrip_eff, K = om_cost_yuan_per_mwh
  - Second forward adjusted: F2_eff = F_off/η + K  (Kirk substitution)
  - Spread vol (Margrabe, K=0 form): σ_s = √(σ1² − 2ρσ1σ2 + σ2²)
  - Per-day call (Margrabe): C(T) = e^{-rT}[F_pk·N(d1) − F2_eff·N(d2)]
    d1 = (ln(F_pk/F2_eff) + 0.5·σ_s²·T) / (σ_s·√T)
    d2 = d1 − σ_s·√T
  - Strip: q_max × Σᵢ C(T_i)  for T_i = i/252, i=1..N
  - Greeks: numerical finite differences

Self-registers into the module-level registry on import.

Usage:
    import libs.decision_models.bess_spread_call_strip
    from libs.decision_models.runners.local import run

    result = run("bess_spread_call_strip", {
        "asset_code": "suyou",
        "as_of_date": "2026-04-21",
        "n_days_remaining": 252,
        "peak_forward_yuan": 350.0,
        "offpeak_forward_yuan": 200.0,
        "peak_vol": 0.30,
        "offpeak_vol": 0.25,
    })
"""
from __future__ import annotations

import dataclasses
import math
from typing import Any, Dict

from libs.decision_models.model_spec import ModelSpec
from libs.decision_models.registry import registry
from libs.decision_models.schemas.bess_spread_call_strip import (
    BESSSpreadCallInput,
    BESSSpreadCallOutput,
)

# ---------------------------------------------------------------------------
# Kirk / Margrabe closed-form pricer
# ---------------------------------------------------------------------------

_TRADING_DAYS_PER_YEAR = 252.0


def _norm_cdf(x: float) -> float:
    """Standard normal CDF via math.erfc — no scipy dependency."""
    return math.erfc(-x / math.sqrt(2)) / 2.0


def _margrabe_call(F1: float, F2_eff: float, sigma_s: float, T: float, r: float = 0.0) -> float:
    """
    Margrabe exchange option price: C = max(F1 - F2_eff, 0) with zero strike.
    Kirk absorbs K into F2_eff so this is the correct form for our spread call.

    Returns intrinsic value (max(F1-F2_eff,0)·e^{-rT}) when T≤0 or sigma_s≤0.
    """
    disc = math.exp(-r * T)
    if T <= 0.0 or sigma_s <= 0.0 or F2_eff <= 0.0:
        return disc * max(F1 - F2_eff, 0.0)
    sq_T = math.sqrt(T)
    d1 = (math.log(F1 / F2_eff) + 0.5 * sigma_s ** 2 * T) / (sigma_s * sq_T)
    d2 = d1 - sigma_s * sq_T
    return disc * (F1 * _norm_cdf(d1) - F2_eff * _norm_cdf(d2))


def _spread_vol(sigma1: float, sigma2: float, rho: float) -> float:
    """
    Margrabe/Kirk spread vol (Margrabe K=0 form):
      σ_s = √(σ1² − 2ρσ1σ2 + σ2²)
    Clamped to a small positive floor to avoid degenerate zero-vol state.
    """
    var = sigma1 ** 2 - 2.0 * rho * sigma1 * sigma2 + sigma2 ** 2
    return math.sqrt(max(var, 1e-8))


def _strip_value(
    F_pk: float,
    F2_eff: float,
    sigma_s: float,
    q_max: float,
    n_days: int,
    r: float,
) -> float:
    """Sum of N daily Margrabe calls, each at T_i = i / 252 years."""
    total = 0.0
    for i in range(1, n_days + 1):
        T_i = i / _TRADING_DAYS_PER_YEAR
        total += _margrabe_call(F_pk, F2_eff, sigma_s, T_i, r)
    return q_max * total


# ---------------------------------------------------------------------------
# Model entry-point
# ---------------------------------------------------------------------------

MODEL_ASSUMPTIONS = {
    "pricing_model": "Kirk/Margrabe spread call approximation",
    "formula": {
        "adjusted_second_forward": "F2_eff = F_off / η + K",
        "spread_vol": "σ_s = sqrt(σ1² - 2ρσ1σ2 + σ2²)  [Margrabe K=0 form]",
        "per_day_call": "C(T) = e^{-rT} [F_pk·N(d1) - F2_eff·N(d2)]",
        "strip_value": "q_max × Σᵢ C(T_i),  T_i = i/252,  i = 1..N",
        "q_max": "η × power_mw × duration_h  [MWh/day]",
    },
    "greek_method": "numerical finite differences",
    "greek_bump_sizes": {
        "delta": "ΔF_pk = +1.0 ¥/MWh",
        "vega": "Δσ_peak = +0.01 (1 vol point)",
        "theta": "ΔT = -1 calendar day (1/365 year)",
    },
    "cdf_implementation": "math.erfc(-x/sqrt(2))/2  — no scipy",
    "time_convention": "trading days: T_i = i/252",
    "vol_surface": "flat per leg — no smile, no term structure",
    "limitations": [
        "Flat vol per leg — no vol smile or term structure",
        "Constant correlation — no correlation regime switching",
        "Daily granularity — no intraday optionality",
        "Single-asset, single-valuation-date per call",
        "Forward prices are caller-supplied — no term structure interpolation",
        "Kirk approximation breaks down for very short maturities (T < 1/252) or very low F2_eff",
    ],
}


def _run(
    asset_code: str,
    as_of_date: str,
    n_days_remaining: int,
    peak_forward_yuan: float,
    offpeak_forward_yuan: float,
    peak_vol: float,
    offpeak_vol: float,
    peak_offpeak_corr: float = 0.85,
    roundtrip_eff: float = 0.85,
    power_mw: float = 100.0,
    duration_h: float = 2.0,
    om_cost_yuan_per_mwh: float = 0.0,
    risk_free_rate: float = 0.0,
) -> Dict[str, Any]:
    # Derived quantities
    q_max = roundtrip_eff * power_mw * duration_h           # MWh/day
    F2_eff = offpeak_forward_yuan / roundtrip_eff + om_cost_yuan_per_mwh
    net_spread_fwd = peak_forward_yuan - offpeak_forward_yuan / roundtrip_eff

    sigma_s = _spread_vol(peak_vol, offpeak_vol, peak_offpeak_corr)

    # Strip value
    sv = _strip_value(peak_forward_yuan, F2_eff, sigma_s, q_max, n_days_remaining, risk_free_rate)

    # Intrinsic / time value
    intr = max(net_spread_fwd - om_cost_yuan_per_mwh, 0.0) * q_max * n_days_remaining
    time_val = sv - intr

    # Moneyness: (F_pk - F2_eff) / F2_eff × 100
    # Normalised against the effective second forward (always positive) so the
    # percentage is meaningful regardless of whether om_cost is zero or not.
    # Positive = ITM (peak price exceeds efficiency-adjusted offpeak + costs).
    moneyness = (peak_forward_yuan - F2_eff) / max(F2_eff, 1.0) * 100.0

    # Greeks — numerical finite differences
    # Delta: bump F_pk by +1 ¥/MWh
    sv_up = _strip_value(
        peak_forward_yuan + 1.0, F2_eff, sigma_s, q_max, n_days_remaining, risk_free_rate
    )
    delta = sv_up - sv  # ¥ per ¥/MWh peak move (strip-level)

    # Vega: bump σ_peak by +0.01 (1 vol point)
    sigma_s_up = _spread_vol(peak_vol + 0.01, offpeak_vol, peak_offpeak_corr)
    sv_vega = _strip_value(
        peak_forward_yuan, F2_eff, sigma_s_up, q_max, n_days_remaining, risk_free_rate
    )
    vega = sv_vega - sv  # ¥ per 1 vol point

    # Theta: shift all T_i by −1 calendar day (−1/365 year per option)
    # Implemented by reducing n_days by 1 (one less option in strip) and shifting
    # remaining options one day closer.  Approximation: use n_days−1 with T_i shifted.
    if n_days_remaining > 1:
        sv_theta = _strip_value(
            peak_forward_yuan, F2_eff, sigma_s, q_max, n_days_remaining - 1, risk_free_rate
        )
        theta = sv_theta - sv  # ¥ per calendar day (≤ 0 for long option)
    else:
        theta = -sv  # last day: value drops to zero after expiry

    output = BESSSpreadCallOutput(
        asset_code=asset_code,
        as_of_date=as_of_date,
        strip_value_yuan=sv,
        per_day_value_yuan=sv / n_days_remaining if n_days_remaining > 0 else 0.0,
        effective_strike_yuan_per_mwh=om_cost_yuan_per_mwh,
        intrinsic_value_yuan=intr,
        time_value_yuan=time_val,
        net_spread_forward=net_spread_fwd,
        moneyness_pct=moneyness,
        delta_yuan_per_yuan=delta,
        vega_yuan_per_vol_point=vega,
        theta_yuan_per_day=theta,
        n_days_remaining=n_days_remaining,
        q_max_mwh_per_day=q_max,
        spread_vol_used=sigma_s,
    )
    return dataclasses.asdict(output)


_SPEC = ModelSpec(
    name="bess_spread_call_strip",
    version="1.0.0",
    description=(
        "BESS spread call strip valuation using Kirk/Margrabe approximation. "
        "Treats each day of remaining asset life as a spread call option on the "
        "peak/offpeak price spread, adjusted for roundtrip efficiency and O&M costs. "
        "Returns strip value, intrinsic/time value decomposition, moneyness, and Greeks "
        "(delta, vega, theta) via numerical finite differences. "
        "No external dependencies — CDF via math.erfc."
    ),
    input_schema=BESSSpreadCallInput,
    output_schema=BESSSpreadCallOutput,
    run_fn=_run,
    tags=["bess", "options", "spread-call", "kirk", "margrabe", "valuation", "mengxi", "analytics"],
    metadata={
        "category": "analytics",
        "scope": "asset_level",
        "market": "mengxi",
        "asset_type": "bess",
        "granularity": "daily",
        "horizon": "forward",
        "deterministic": True,
        "model_family": "analytical",
        "source_of_truth_module": "libs/decision_models/bess_spread_call_strip.py",
        "source_of_truth_functions": ["_run", "_margrabe_call", "_spread_vol", "_strip_value", "_norm_cdf"],
        "assumptions": MODEL_ASSUMPTIONS,
        "limitations": MODEL_ASSUMPTIONS["limitations"],
        "fallback_behavior": None,
        "status": "experimental",
        "owner": "bess-platform",
        "pricing_model": "Kirk/Margrabe spread call approximation",
        "cdf_implementation": "math.erfc (stdlib only)",
        "greeks_method": "numerical finite differences",
    },
)

registry.register(_SPEC)
