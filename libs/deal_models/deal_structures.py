"""libs/deal_models/deal_structures.py — Deal payoff functions + pricing. Self-registers on import."""
from __future__ import annotations
from dataclasses import dataclass
import numpy as np
from pydantic import BaseModel, Field
from libs.deal_models.registry import DealStructureSpec, register


# ── Params schemas ────────────────────────────────────────────────────────────

class FloorParams(BaseModel):
    floor_yuan: float = Field(..., gt=0)

class CapParams(BaseModel):
    cap_yuan: float = Field(..., gt=0)

class CollarParams(BaseModel):
    floor_yuan: float = Field(..., gt=0)
    cap_yuan: float = Field(..., gt=0)

class SwapParams(BaseModel):
    fixed_revenue_yuan: float = Field(..., gt=0)

class TollingParams(BaseModel):
    toll_yuan: float = Field(..., gt=0)

class PPAParams(BaseModel):
    fixed_price_yuan_mwh: float = Field(..., gt=0)
    annual_volume_mwh: float = Field(..., gt=0)


# ── Pricing result ────────────────────────────────────────────────────────────

@dataclass
class DealPricingResult:
    expected_cost: float
    p95_cost: float
    min_premium: float
    suggested_premium: float
    payout_paths: np.ndarray


# ── Universal pricing function ────────────────────────────────────────────────

def price_structure(
    name: str,
    revenue_paths: np.ndarray,
    params: BaseModel,
    cost_of_capital: float = 0.10,
) -> DealPricingResult:
    """Price any registered deal structure against simulated revenue paths."""
    from libs.deal_models.registry import get
    spec = get(name)
    raw_payouts = spec.payoff_fn(revenue_paths, params)
    payouts = np.maximum(raw_payouts, 0.0).astype(float)

    expected_cost = float(payouts.mean())
    p95_cost = float(np.percentile(payouts, 95))
    threshold = float(np.percentile(payouts, 95))
    tail = payouts[payouts >= threshold]
    cvar = float(tail.mean()) if len(tail) else p95_cost
    risk_charge = 0.30 * max(cvar - expected_cost, 0.0)

    return DealPricingResult(
        expected_cost=expected_cost,
        p95_cost=p95_cost,
        min_premium=expected_cost,
        suggested_premium=expected_cost + risk_charge,
        payout_paths=payouts,
    )


# ── Register the 6 standard structures ───────────────────────────────────────

register(DealStructureSpec(
    name="revenue_floor",
    description="Guarantees minimum annual revenue. Payout = max(floor − R, 0)",
    payoff_fn=lambda rev, p: np.maximum(p.floor_yuan - rev, 0.0),
    params_schema=FloorParams,
))
register(DealStructureSpec(
    name="revenue_cap",
    description="Upside sharing above cap. Cost = max(R − cap, 0)",
    payoff_fn=lambda rev, p: np.maximum(rev - p.cap_yuan, 0.0),
    params_schema=CapParams,
))
register(DealStructureSpec(
    name="collar",
    description="Revenue band: floor protection minus cap income.",
    payoff_fn=lambda rev, p: np.maximum(p.floor_yuan - rev, 0.0) - np.maximum(rev - p.cap_yuan, 0.0),
    params_schema=CollarParams,
))
register(DealStructureSpec(
    name="fixed_revenue_swap",
    description="Owner exchanges variable revenue for fixed amount.",
    payoff_fn=lambda rev, p: p.fixed_revenue_yuan - rev,
    params_schema=SwapParams,
))
register(DealStructureSpec(
    name="tolling",
    description="Operator pays toll, keeps all upside. Payout = max(toll − R, 0)",
    payoff_fn=lambda rev, p: np.maximum(p.toll_yuan - rev, 0.0),
    params_schema=TollingParams,
))
register(DealStructureSpec(
    name="ppa_fixed_price",
    description="Fixed-price PPA. Payout = max(fixed_price × vol − R, 0)",
    payoff_fn=lambda rev, p: np.maximum(p.fixed_price_yuan_mwh * p.annual_volume_mwh - rev, 0.0),
    params_schema=PPAParams,
))
