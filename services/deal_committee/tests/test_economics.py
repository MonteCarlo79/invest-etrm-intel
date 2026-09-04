# services/deal_committee/tests/test_economics.py
import math

import numpy as np
import pytest

from services.deal_committee.brief import DealBrief
from services.deal_committee.economics import economics_section_markdown, run_economics

BRIEF = DealBrief(deal_name="蒙西储能一期", asset_type="bess", province="蒙西",
                  capacity_mw=100, capacity_mwh=200, capex_total_yuan=1.2e9,
                  commissioning_year=2027, tenor_years=15)


def _fake_prices(province, start, end):
    rng = np.random.default_rng(7)
    hours = 370 * 24
    return (300 + 60 * np.sin(np.arange(hours) / 24 / 15) + rng.normal(0, 40, hours)).tolist()


def _fake_monthly(engine, province):
    return [(f"2026-{m:02d}", 280.0 + m * 5) for m in range(1, 13)]


def test_run_economics_returns_mc_result():
    res = run_economics(BRIEF, n_simulations=50, fetch_fn=_fake_prices, monthly_fn=_fake_monthly)
    assert res.n_simulations == 50
    assert res.n_price_hours == 370 * 24
    assert len(res.monthly_price) == 12
    assert math.isfinite(res.mc.revenue_p50)
    assert res.mc.revenue_p10 < res.mc.revenue_p50 < res.mc.revenue_p90
    assert 0.0 <= res.mc.irr_prob_below_hurdle <= 1.0


def test_run_economics_requires_capex():
    bad = BRIEF.model_copy(update={"capex_total_yuan": None})
    with pytest.raises(ValueError, match="总投资"):
        run_economics(bad, fetch_fn=_fake_prices, monthly_fn=_fake_monthly)


def test_run_economics_requires_province():
    bad = BRIEF.model_copy(update={"province": ""})
    with pytest.raises(ValueError, match="省份"):
        run_economics(bad, fetch_fn=_fake_prices, monthly_fn=_fake_monthly)


def test_markdown_contains_kpis():
    res = run_economics(BRIEF, n_simulations=50, fetch_fn=_fake_prices, monthly_fn=_fake_monthly)
    md = economics_section_markdown(res, BRIEF)
    assert "P50" in md and "股权 IRR" in md
    assert "蒙西" in md


def test_solar_maps_to_wind_dispatch():
    solar = BRIEF.model_copy(update={"asset_type": "solar", "installed_mw": 200.0,
                                     "capacity_mw": 0.0, "capacity_mwh": 0.0})
    res = run_economics(solar, n_simulations=50, fetch_fn=_fake_prices, monthly_fn=_fake_monthly)
    assert math.isfinite(res.mc.revenue_p50)
