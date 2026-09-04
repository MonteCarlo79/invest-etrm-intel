"""libs/deal_models/adapters/agent_tools.py — Claude API tool definitions + dispatch."""
from __future__ import annotations
import json
import numpy as np
from libs.deal_models.contracts import (
    OUParams, PriceSimRequest, DispatchRequest,
    ProjectFinancials, MCRequest,
)
from libs.deal_models.price_simulator import simulate_prices
from libs.deal_models.dispatch_valuation import dispatch_annual
from libs.deal_models.project_cashflow import compute_cashflow
from libs.deal_models.monte_carlo import run_monte_carlo
import libs.deal_models.deal_structures as _ds_mod  # triggers registration

# Cached last MC result so Strategist can reference it across tool calls
_last_mc_result = None
_last_price_paths = None


def _j(obj) -> str:
    """JSON serialize; convert numpy scalars."""
    def _default(o):
        if isinstance(o, (np.integer, np.floating)):
            return float(o)
        if isinstance(o, np.ndarray):
            return o.tolist()
        raise TypeError(f"Not serializable: {type(o)}")
    return json.dumps(obj, default=_default)


AGENT_TOOLS = [
    {
        "name": "run_price_simulation",
        "description": "Simulate forward price paths for a province using OU or PCA model. Returns P10/P50/P90 price levels and stores paths for downstream dispatch valuation.",
        "input_schema": {
            "type": "object",
            "properties": {
                "province": {"type": "string", "description": "Province name e.g. 蒙西"},
                "model": {"type": "string", "enum": ["ou", "pca"], "description": "Price model"},
                "kappa": {"type": "number", "description": "OU mean-reversion speed (default 2.0)"},
                "mu": {"type": "number", "description": "OU long-run mean yuan/MWh (default 300)"},
                "sigma": {"type": "number", "description": "OU annualised vol yuan/MWh (default 80)"},
                "n_simulations": {"type": "integer", "description": "Number of paths (default 1000)"},
            },
            "required": ["province"],
        },
    },
    {
        "name": "run_dispatch_valuation",
        "description": "Estimate annual BESS or wind revenue using the last simulated price paths.",
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_type": {"type": "string", "enum": ["bess", "wind", "wind_bess"]},
                "capacity_mwh": {"type": "number"},
                "power_mw": {"type": "number"},
                "roundtrip_eff": {"type": "number"},
                "installed_mw": {"type": "number", "description": "Wind installed capacity MW"},
            },
            "required": ["asset_type"],
        },
    },
    {
        "name": "run_project_cashflow",
        "description": "Compute project IRR, equity IRR, DSCR, and NPV for given financial assumptions.",
        "input_schema": {
            "type": "object",
            "properties": {
                "capex_total_yuan": {"type": "number"},
                "annual_revenue_yuan": {"type": "number", "description": "Single annual revenue figure (repeated over project life)"},
                "annual_om_yuan": {"type": "number"},
                "debt_ratio": {"type": "number"},
                "interest_rate": {"type": "number"},
                "loan_term_years": {"type": "integer"},
                "project_life_years": {"type": "integer"},
                "hurdle_rate": {"type": "number"},
            },
            "required": ["capex_total_yuan", "annual_revenue_yuan", "annual_om_yuan"],
        },
    },
    {
        "name": "run_monte_carlo",
        "description": "Full MC simulation: price paths → dispatch revenue → project cashflow. Returns IRR/NPV distributions and tornado chart.",
        "input_schema": {
            "type": "object",
            "properties": {
                "province": {"type": "string"},
                "asset_type": {"type": "string", "enum": ["bess", "wind", "wind_bess"]},
                "capacity_mwh": {"type": "number"},
                "power_mw": {"type": "number"},
                "installed_mw": {"type": "number"},
                "capex_total_yuan": {"type": "number"},
                "annual_om_yuan": {"type": "number"},
                "n_simulations": {"type": "integer"},
                "mu": {"type": "number", "description": "OU long-run mean price yuan/MWh"},
                "annual_revenue_yuan": {"type": "number", "description": "Base annual revenue yuan for cashflow model"},
            },
            "required": ["province", "asset_type", "capex_total_yuan", "annual_om_yuan"],
        },
    },
    {
        "name": "price_deal_structure",
        "description": "Price a deal structure (floor, cap, collar, swap, tolling, PPA) against the last MC revenue paths.",
        "input_schema": {
            "type": "object",
            "properties": {
                "structure_type": {"type": "string", "enum": ["revenue_floor", "revenue_cap", "collar", "fixed_revenue_swap", "tolling", "ppa_fixed_price"]},
                "floor_yuan": {"type": "number"},
                "cap_yuan": {"type": "number"},
                "fixed_revenue_yuan": {"type": "number"},
                "toll_yuan": {"type": "number"},
                "fixed_price_yuan_mwh": {"type": "number"},
                "annual_volume_mwh": {"type": "number"},
            },
            "required": ["structure_type"],
        },
    },
]


def dispatch_tool(name: str, inputs: dict) -> str:
    """Route a tool call from the Claude API to the appropriate function."""
    global _last_mc_result, _last_price_paths
    try:
        if name == "run_price_simulation":
            ou = OUParams(
                kappa=inputs.get("kappa", 2.0),
                mu=inputs.get("mu", 300.0),
                sigma=inputs.get("sigma", 80.0),
            )
            req = PriceSimRequest(
                province=inputs["province"],
                n_simulations=inputs.get("n_simulations", 500),
                model=inputs.get("model", "ou"),
                ou_params=ou,
            )
            paths = simulate_prices(req)
            _last_price_paths = paths
            return _j({
                "province": req.province,
                "n_simulations": req.n_simulations,
                "p10_yuan_mwh": float(np.percentile(paths, 10)),
                "p50_yuan_mwh": float(np.percentile(paths, 50)),
                "p90_yuan_mwh": float(np.percentile(paths, 90)),
            })

        if name == "run_dispatch_valuation":
            if _last_price_paths is None:
                return _j({"error": "Run run_price_simulation first"})
            req = DispatchRequest(
                asset_type=inputs["asset_type"],
                capacity_mwh=inputs.get("capacity_mwh", 0.0),
                power_mw=inputs.get("power_mw", 0.0),
                installed_mw=inputs.get("installed_mw", 0.0),
                roundtrip_eff=inputs.get("roundtrip_eff", 0.85),
            )
            result = dispatch_annual(_last_price_paths, req)
            return _j({"p10": result.p10, "p50": result.p50, "p90": result.p90, "mean": result.mean})

        if name == "run_project_cashflow":
            n = inputs.get("project_life_years", 20)
            rev = inputs["annual_revenue_yuan"]
            fin = ProjectFinancials(
                capex_total_yuan=inputs["capex_total_yuan"],
                annual_revenue_yuan=[rev] * n,
                annual_om_yuan=inputs["annual_om_yuan"],
                debt_ratio=inputs.get("debt_ratio", 0.7),
                interest_rate=inputs.get("interest_rate", 0.05),
                loan_term_years=inputs.get("loan_term_years", 10),
                project_life_years=n,
                hurdle_rate=inputs.get("hurdle_rate", 0.08),
            )
            cf = compute_cashflow(fin)
            return _j({
                "project_irr": cf.project_irr,
                "equity_irr": cf.equity_irr,
                "roace": cf.roace,
                "dscr_min": cf.dscr_min,
                "dscr_avg": cf.dscr_avg,
                "npv": cf.npv,
                "payback_years": cf.payback_years,
            })

        if name == "run_monte_carlo":
            n_sim = inputs.get("n_simulations", 500)
            n = inputs.get("project_life_years", 20)
            base_rev = inputs.get("annual_revenue_yuan", inputs.get("mu", 300.0) * 50.0 * 8760 * 0.3)
            req = MCRequest(
                price_sim=PriceSimRequest(
                    province=inputs["province"],
                    n_simulations=n_sim,
                    model="ou",
                    ou_params=OUParams(kappa=2.0, mu=inputs.get("mu", 300.0), sigma=80.0),
                ),
                dispatch=DispatchRequest(
                    asset_type=inputs["asset_type"],
                    capacity_mwh=inputs.get("capacity_mwh", 0.0),
                    power_mw=inputs.get("power_mw", 0.0),
                    installed_mw=inputs.get("installed_mw", 0.0),
                ),
                financials=ProjectFinancials(
                    capex_total_yuan=inputs["capex_total_yuan"],
                    annual_revenue_yuan=[base_rev] * n,
                    annual_om_yuan=inputs["annual_om_yuan"],
                ),
                n_simulations=n_sim,
            )
            _last_mc_result = run_monte_carlo(req)
            return _j({
                "revenue_p10": _last_mc_result.revenue_p10,
                "revenue_p50": _last_mc_result.revenue_p50,
                "revenue_p90": _last_mc_result.revenue_p90,
                "equity_irr_p10": _last_mc_result.equity_irr_p10,
                "equity_irr_p50": _last_mc_result.equity_irr_p50,
                "equity_irr_p90": _last_mc_result.equity_irr_p90,
                "irr_prob_below_hurdle": _last_mc_result.irr_prob_below_hurdle,
                "tornado_top3": _last_mc_result.tornado[:3],
            })

        if name == "price_deal_structure":
            if _last_mc_result is None:
                return _j({"error": "Run run_monte_carlo first to get revenue paths"})
            struct = inputs["structure_type"]
            from libs.deal_models.registry import get
            spec = get(struct)
            param_fields = {k: v for k, v in inputs.items() if k != "structure_type" and v is not None}
            params = spec.params_schema(**param_fields)
            result = _ds_mod.price_structure(struct, _last_mc_result.revenue_paths, params)
            return _j({
                "expected_cost": result.expected_cost,
                "p95_cost": result.p95_cost,
                "min_premium": result.min_premium,
                "suggested_premium": result.suggested_premium,
            })

        return _j({"error": f"Unknown tool: {name}"})
    except Exception as e:
        return _j({"error": str(e)})
