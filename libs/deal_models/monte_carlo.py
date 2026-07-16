"""libs/deal_models/monte_carlo.py — MC orchestrator: price → dispatch → cashflow."""
from __future__ import annotations

import numpy as np
from libs.deal_models.contracts import MCRequest, MCResult, ProjectFinancials
from libs.deal_models.price_simulator import simulate_prices
from libs.deal_models.dispatch_valuation import dispatch_annual
from libs.deal_models.project_cashflow import compute_cashflow



def _tornado(financials: ProjectFinancials, base_rev: float) -> list[dict]:
    """Sensitivity: ±10% perturbation of key financial parameters → IRR swing."""
    params_to_perturb = [
        ("capex", "capex_total_yuan", 0.10),
        ("interest_rate", "interest_rate", 0.10),
        ("om_cost", "annual_om_yuan", 0.10),
        ("degradation", "annual_degradation_rate", 0.10),
        ("revenue_scale", None, 0.10),      # special: perturb base_rev itself
    ]
    n = financials.project_life_years
    results = []
    for label, field, pct in params_to_perturb:
        row_irrs = []
        for direction in (+1, -1):
            if field is None:
                rev = base_rev * (1.0 + direction * pct)
                fin = financials.model_copy(update={"annual_revenue_yuan": [rev] * n})
            else:
                base_val = getattr(financials, field)
                new_val = base_val * (1.0 + direction * pct)
                fin = financials.model_copy(
                    update={field: new_val, "annual_revenue_yuan": [base_rev] * n}
                )
            try:
                row_irrs.append(compute_cashflow(fin).equity_irr)
            except (OverflowError, ZeroDivisionError, ValueError):
                row_irrs.append(-0.9999)
        irr_high, irr_low = row_irrs
        results.append({
            "param": label,
            "irr_high": irr_high,
            "irr_low": irr_low,
            "swing": abs(irr_high - irr_low),
        })
    return sorted(results, key=lambda x: x["swing"], reverse=True)


def run_monte_carlo(req: MCRequest) -> MCResult:
    """Full MC: simulate prices → dispatch revenue → project cashflow for each path."""
    # Override price_sim.n_simulations with req.n_simulations (MCRequest is authoritative)
    price_sim = req.price_sim.model_copy(update={"n_simulations": req.n_simulations})

    # 1. Price paths
    price_paths = simulate_prices(price_sim, seed=req.random_seed)  # (n_sim, 8760)

    # 2. Annual dispatch revenue per path — direct from dispatch_annual, no fallback
    revenue_paths = dispatch_annual(price_paths, req.dispatch).revenue_paths  # (n_sim,)

    # 3. Project cashflow per path
    n = req.financials.project_life_years
    equity_irr_paths = np.empty(req.n_simulations)
    npv_paths = np.empty(req.n_simulations)

    for i in range(req.n_simulations):
        fin = req.financials.model_copy(
            update={"annual_revenue_yuan": [float(revenue_paths[i])] * n}
        )
        try:
            cf = compute_cashflow(fin)
            equity_irr_paths[i] = cf.equity_irr
            npv_paths[i] = cf.npv
        except (OverflowError, ZeroDivisionError, ValueError):
            # IRR solver diverged (e.g. deep loss scenario, multiple sign changes)
            equity_irr_paths[i] = -0.9999
            npv_paths[i] = float(np.sum(
                [float(revenue_paths[i]) - req.financials.annual_om_yuan] * n
            ) - req.financials.capex_total_yuan)

    # 4. Revenue risk statistics
    rv5 = float(np.percentile(revenue_paths, 5))
    cvar_mask = revenue_paths <= rv5
    revenue_cvar = float(revenue_paths[cvar_mask].mean()) if cvar_mask.any() else rv5

    # 5. Tornado at P50 revenue
    p50_rev = float(np.median(revenue_paths))
    tornado = _tornado(req.financials, p50_rev)

    hurdle = req.financials.hurdle_rate
    return MCResult(
        revenue_p10=float(np.percentile(revenue_paths, 10)),
        revenue_p50=float(np.percentile(revenue_paths, 50)),
        revenue_p90=float(np.percentile(revenue_paths, 90)),
        revenue_var_5pct=rv5,
        revenue_cvar_5pct=revenue_cvar,
        equity_irr_p10=float(np.percentile(equity_irr_paths, 10)),
        equity_irr_p50=float(np.percentile(equity_irr_paths, 50)),
        equity_irr_p90=float(np.percentile(equity_irr_paths, 90)),
        irr_prob_below_hurdle=float((equity_irr_paths < hurdle).mean()),
        npv_p10=float(np.percentile(npv_paths, 10)),
        npv_p50=float(np.percentile(npv_paths, 50)),
        npv_p90=float(np.percentile(npv_paths, 90)),
        tornado=tornado,
        revenue_paths=revenue_paths,
        equity_irr_paths=equity_irr_paths,
        npv_paths=npv_paths,
    )
