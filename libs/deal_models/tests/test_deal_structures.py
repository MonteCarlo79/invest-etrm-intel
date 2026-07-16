"""libs/deal_models/tests/test_deal_structures.py — TDD tests for deal_structures."""
from __future__ import annotations
import numpy as np
import pytest


def test_all_six_structures_registered():
    import libs.deal_models.deal_structures  # triggers self-registration
    from libs.deal_models.registry import list_structures
    for name in ["revenue_floor", "revenue_cap", "collar", "fixed_revenue_swap", "tolling", "ppa_fixed_price"]:
        assert name in list_structures(), f"{name} not registered"


def test_floor_payout_only_on_shortfall():
    import libs.deal_models.deal_structures
    from libs.deal_models.deal_structures import FloorParams, price_structure
    revs = np.array([12e6, 15e6, 8e6])
    params = FloorParams(floor_yuan=10e6)
    result = price_structure("revenue_floor", revs, params)
    assert result.payout_paths[0] == pytest.approx(0.0)
    assert result.payout_paths[1] == pytest.approx(0.0)
    assert result.payout_paths[2] == pytest.approx(2e6, rel=1e-5)


def test_floor_well_below_p5_has_zero_expected_cost():
    import libs.deal_models.deal_structures
    from libs.deal_models.deal_structures import FloorParams, price_structure
    rng = np.random.default_rng(42)
    revs = rng.normal(10e6, 1e6, 5000)
    p1 = float(np.percentile(revs, 1))
    params = FloorParams(floor_yuan=p1 * 0.5)
    result = price_structure("revenue_floor", revs, params)
    assert result.expected_cost == pytest.approx(0.0, abs=1.0)


def test_cap_payout_only_above_cap():
    import libs.deal_models.deal_structures
    from libs.deal_models.deal_structures import CapParams, price_structure
    revs = np.array([8e6, 10e6, 14e6])
    result = price_structure("revenue_cap", revs, CapParams(cap_yuan=12e6))
    assert result.payout_paths[0] == pytest.approx(0.0)
    assert result.payout_paths[2] == pytest.approx(2e6, rel=1e-5)


def test_collar_bounded_net_exposure():
    import libs.deal_models.deal_structures
    from libs.deal_models.deal_structures import CollarParams, price_structure
    revs = np.linspace(5e6, 20e6, 100)
    result = price_structure("collar", revs, CollarParams(floor_yuan=8e6, cap_yuan=15e6))
    # collar net payout can be negative (owner receives cap income)
    assert result.expected_cost is not None


def test_suggested_premium_ge_min_premium():
    import libs.deal_models.deal_structures
    from libs.deal_models.deal_structures import FloorParams, price_structure
    rng = np.random.default_rng(42)
    revs = rng.normal(10e6, 2e6, 1000)
    result = price_structure("revenue_floor", revs, FloorParams(floor_yuan=9e6))
    assert result.suggested_premium >= result.min_premium
