"""services/deal_engine/batch_runner.py — Run MC with Streamlit progress tracking."""
from __future__ import annotations
from typing import Optional, Callable
import numpy as np
from libs.deal_models.contracts import MCRequest, MCResult, ProjectFinancials
from libs.deal_models.price_simulator import simulate_prices
from libs.deal_models.dispatch_valuation import dispatch_annual
from libs.deal_models.project_cashflow import compute_cashflow
from libs.deal_models.monte_carlo import _tornado


def run_batch(
    req: MCRequest,
    progress_callback: Optional[Callable[[float], None]] = None,
) -> MCResult:
    """
    Execute MC run with optional progress updates.

    progress_callback(fraction: float) is called every 5% of sims.
    Compatible with Streamlit's st.progress() via: run_batch(req, progress_callback=bar.progress)
    """
    price_paths = simulate_prices(req.price_sim, seed=req.random_seed)
    dispatch_result = dispatch_annual(price_paths, req.dispatch)
    revenue_paths = dispatch_result.revenue_paths

    n = req.financials.project_life_years
    n_sim = req.n_simulations
    equity_irr_paths = np.empty(n_sim)
    npv_paths = np.empty(n_sim)

    report_every = max(1, n_sim // 20)
    for i in range(n_sim):
        fin = req.financials.model_copy(
            update={"annual_revenue_yuan": [float(revenue_paths[i])] * n}
        )
        try:
            cf = compute_cashflow(fin)
            equity_irr_paths[i] = cf.equity_irr
            npv_paths[i] = cf.npv
        except (OverflowError, ZeroDivisionError, ValueError):
            equity_irr_paths[i] = -0.9999
            npv_paths[i] = float(np.sum(
                [float(revenue_paths[i]) - req.financials.annual_om_yuan] * n
            ) - req.financials.capex_total_yuan)
        if progress_callback and i % report_every == 0:
            progress_callback(i / n_sim)

    if progress_callback:
        progress_callback(1.0)

    rv5 = float(np.percentile(revenue_paths, 5))
    cvar_mask = revenue_paths <= rv5
    revenue_cvar = float(revenue_paths[cvar_mask].mean()) if cvar_mask.any() else rv5
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
