"""libs/deal_models/project_cashflow.py — Full project financial model."""
from __future__ import annotations
from libs.deal_models.contracts import AnnualRow, CashFlowResult, ProjectFinancials


def _irr(cashflows: list[float], guess: float = 0.10, max_iter: int = 500, tol: float = 1e-7) -> float:
    """Newton-Raphson IRR solver. cashflows[0] is the initial investment (negative).

    Returns a large negative value when there is no sign change in the cashflow
    stream (i.e. the project never recovers its investment).
    """
    # Require at least one positive inflow after the initial outflow; if none,
    # the IRR is deeply negative — return a sentinel below -100% proxy.
    if not any(cf > 0 for cf in cashflows[1:]):
        return -0.9999

    # If NPV at rate=0 (undiscounted sum) is non-positive, no real positive IRR exists.
    if sum(cashflows) <= 0.0:
        return -0.9999

    rate = guess
    for _ in range(max_iter):
        try:
            npv = sum(cf / (1.0 + rate) ** t for t, cf in enumerate(cashflows))
            d_npv = sum(-t * cf / (1.0 + rate) ** (t + 1) for t, cf in enumerate(cashflows))
        except (OverflowError, ZeroDivisionError):
            return -0.9999
        if abs(d_npv) < 1e-12:
            break
        new_rate = rate - npv / d_npv
        if abs(new_rate - rate) < tol:
            return float(new_rate)
        # Guard against divergence in both directions
        rate = max(min(new_rate, 100.0), -0.9999)
    return float(rate)


def compute_cashflow(financials: ProjectFinancials) -> CashFlowResult:
    """Compute full project P&L, KPIs, and IRR."""
    n = financials.project_life_years
    total_capex = financials.capex_total_yuan
    debt = total_capex * financials.debt_ratio
    equity = total_capex * (1.0 - financials.debt_ratio)

    # Straight-line depreciation
    depn_per_yr = (total_capex * (1.0 - financials.residual_value_ratio)) / financials.depreciation_years

    # Level principal repayment (after grace period)
    active_loan_yrs = max(financials.loan_term_years - financials.grace_years, 1)
    annual_principal = debt / active_loan_yrs

    rows: list[AnnualRow] = []
    equity_cfs = [-equity]        # t=0 equity outflow
    project_cfs = [-total_capex]  # t=0 total outflow (unlevered)
    remaining_debt = debt

    for yr in range(1, n + 1):
        # Revenue degraded from base year-1 figure
        base_rev = (
            financials.annual_revenue_yuan[yr - 1]
            if yr <= len(financials.annual_revenue_yuan)
            else financials.annual_revenue_yuan[-1]
        )
        rev = base_rev * (1.0 - financials.annual_degradation_rate) ** (yr - 1)

        opex = financials.annual_om_yuan
        ebitda = rev - opex
        depn = depn_per_yr if yr <= financials.depreciation_years else 0.0
        ebit = ebitda - depn

        # Debt service
        interest = remaining_debt * financials.interest_rate
        if yr <= financials.grace_years or yr > financials.loan_term_years:
            principal = 0.0
        else:
            principal = min(annual_principal, remaining_debt)
        remaining_debt = max(remaining_debt - principal, 0.0)
        debt_service = principal + interest

        ebt = ebit - interest
        tax = max(ebt * financials.corporate_tax_rate, 0.0)
        net_income = ebt - tax
        equity_fcf = net_income + depn - principal

        rows.append(AnnualRow(
            year=yr, revenue=rev, opex=opex, ebitda=ebitda,
            depreciation=depn, ebit=ebit, interest=interest,
            ebt=ebt, tax=tax, net_income=net_income,
            debt_service=debt_service, equity_fcf=equity_fcf,
        ))
        equity_cfs.append(equity_fcf)
        # Unlevered project CF = EBITDA - tax_on_ebit
        project_cfs.append(ebitda - max(ebit * financials.corporate_tax_rate, 0.0))

    # KPIs
    equity_irr = _irr(equity_cfs)
    project_irr = _irr(project_cfs)

    avg_net_income = sum(r.net_income for r in rows) / n
    roace = avg_net_income / total_capex

    dscr_vals = [r.ebitda / r.debt_service for r in rows if r.debt_service > 1e-6]
    dscr_min = min(dscr_vals) if dscr_vals else float("nan")
    dscr_avg = sum(dscr_vals) / len(dscr_vals) if dscr_vals else float("nan")

    npv = sum(cf / (1.0 + financials.hurdle_rate) ** t for t, cf in enumerate(project_cfs))

    payback = float("nan")
    cumulative = 0.0
    for row in rows:
        cumulative += row.equity_fcf
        if cumulative >= equity:
            payback = float(row.year)
            break

    return CashFlowResult(
        annual=rows,
        project_irr=project_irr,
        equity_irr=equity_irr,
        roace=roace,
        dscr_min=dscr_min,
        dscr_avg=dscr_avg,
        npv=npv,
        payback_years=payback,
    )
