"""libs/deal_models/tests/test_project_cashflow.py — TDD tests for project_cashflow."""
from __future__ import annotations
import math
import pytest
from libs.deal_models.contracts import ProjectFinancials


def _base_financials(**overrides) -> ProjectFinancials:
    defaults = dict(
        capex_total_yuan=1e8,
        project_life_years=20,
        debt_ratio=0.7,
        loan_term_years=10,
        interest_rate=0.05,
        grace_years=1,
        annual_revenue_yuan=[2e7] * 20,
        annual_om_yuan=3e6,
        annual_degradation_rate=0.01,
    )
    defaults.update(overrides)
    return ProjectFinancials(**defaults)


def test_annual_rows_count():
    from libs.deal_models.project_cashflow import compute_cashflow
    result = compute_cashflow(_base_financials(project_life_years=15, annual_revenue_yuan=[2e7] * 15))
    assert len(result.annual) == 15


def test_positive_equity_irr_for_profitable_project():
    from libs.deal_models.project_cashflow import compute_cashflow
    result = compute_cashflow(_base_financials())
    assert result.equity_irr > 0.05


def test_negative_equity_irr_for_zero_revenue():
    from libs.deal_models.project_cashflow import compute_cashflow
    result = compute_cashflow(_base_financials(annual_revenue_yuan=[0.0] * 20))
    assert result.equity_irr < 0.0


def test_ebitda_equals_revenue_minus_opex():
    from libs.deal_models.project_cashflow import compute_cashflow
    result = compute_cashflow(_base_financials())
    row = result.annual[0]
    assert abs(row.ebitda - (row.revenue - row.opex)) < 1.0


def test_dscr_positive_during_debt_period():
    from libs.deal_models.project_cashflow import compute_cashflow
    result = compute_cashflow(_base_financials())
    # Rows 2-10 have active debt service (grace_years=1, loan_term=10)
    for row in result.annual[1:10]:
        assert row.debt_service > 0
    assert result.dscr_min > 0.0


def test_no_principal_after_loan_term():
    from libs.deal_models.project_cashflow import compute_cashflow
    result = compute_cashflow(_base_financials(loan_term_years=5, project_life_years=20, annual_revenue_yuan=[2e7]*20))
    # Years 6-20: no principal repayment (loan fully paid by year 5)
    for row in result.annual[5:]:
        # debt_service after loan term = only interest on 0 remaining debt = 0
        assert row.debt_service == pytest.approx(0.0, abs=1.0)


def test_npv_positive_above_hurdle():
    from libs.deal_models.project_cashflow import compute_cashflow
    result = compute_cashflow(_base_financials())
    # equity_irr > hurdle_rate (0.08) → NPV > 0
    assert result.npv > 0


def test_irr_helper_known_value():
    from libs.deal_models.project_cashflow import _irr
    # -100 now, +110 in year 1 → IRR = 10%
    assert abs(_irr([-100.0, 110.0]) - 0.10) < 1e-5
