import pytest

from . import compute as c


def test_heat_rate_curve_endpoints_and_midpoint():
    assert c.heat_rate_at(c.MEL) == c.HR_FULL
    assert c.heat_rate_at(c.SEL) == c.HR_MIN
    assert round(c.heat_rate_at(300), 3) == 2.267


def test_heat_rate_curve_rejects_unreachable_output():
    with pytest.raises(ValueError):
        c.heat_rate_at(100)      # below SEL, above 0: not reachable
    with pytest.raises(ValueError):
        c.heat_rate_at(600)      # above MEL


def test_start_cost_tiers_and_boundaries():
    assert c.start_cost(5.9) == c.C_HOT
    assert c.start_cost(6.0) == c.C_WARM     # boundary inclusive on the cold-er side
    assert c.start_cost(20) == c.C_WARM
    assert c.start_cost(48.0) == c.C_COLD
    assert c.start_cost(56) == c.C_COLD


def test_worked_example_numbers():
    assert round(c.marginal_cost(500), 1) == 60.0
    assert round(c.marginal_cost(300), 1) == 68.0
    assert c.start_cost(20) == 60_000.0
    assert round(c.start_amortised_per_mwh(20), 1) == 15.0
