"""Capacity-compensation component tests (deterministic ¥/MWh stream)."""
from unittest.mock import MagicMock

import numpy as np

from libs.deal_models.contracts import DispatchRequest
from libs.deal_models.dispatch_valuation import dispatch_annual
from services.deal_committee import economics as econ_mod
from services.deal_committee.brief import DealBrief
from services.deal_committee.economics import (
    EconomicsResult, _register_comp_rate, economics_section_markdown,
)


def _req(comp_rate=0.0):
    return DispatchRequest(asset_type="bess", capacity_mwh=2000.0, power_mw=500.0,
                           roundtrip_eff=0.85, cycles_per_day=1.0,
                           comp_rate_yuan_mwh=comp_rate)


class TestDispatchComp:
    def test_comp_adds_exact_constant(self):
        rng = np.random.default_rng(0)
        paths = rng.uniform(200, 500, size=(16, 8760))  # 16 sims × 1y hourly
        base = dispatch_annual(paths, _req(0.0))
        with_comp = dispatch_annual(paths, _req(365.0))
        # rate × capacity_mwh × eff × cycles × days (full-cycle user convention,
        # matches register empiricals — NOT the model's 1h-slot volume)
        expected = 365.0 * 2000.0 * 0.85 * 1 * 365
        assert with_comp.comp_annual_yuan == expected
        np.testing.assert_allclose(
            with_comp.revenue_paths - base.revenue_paths, expected, rtol=1e-9)
        # percentiles shift by the same constant (deterministic stream)
        assert with_comp.p50 - base.p50 == expected

    def test_zero_rate_is_backward_compatible(self):
        paths = np.full((4, 8760), 300.0)
        res = dispatch_annual(paths, _req(0.0))
        assert res.comp_annual_yuan == 0.0


class TestRegisterCompRate:
    def test_computes_ratio(self):
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (364.7,)
        assert _register_comp_rate("蒙西", engine) == 364.7

    def test_unknown_province_returns_zero(self):
        assert _register_comp_rate("海南", MagicMock()) == 0.0

    def test_db_failure_returns_zero(self):
        engine = MagicMock()
        engine.connect.side_effect = RuntimeError("db down")
        assert _register_comp_rate("蒙西", engine) == 0.0


class TestCompMarkdown:
    def _res(self, rate, comp):
        from libs.deal_models.monte_carlo import MCResult
        mc = MCResult(
            revenue_p10=6e7, revenue_p50=3e8, revenue_p90=5e8,
            revenue_var_5pct=5e7, revenue_cvar_5pct=4e7,
            equity_irr_p10=0.05, equity_irr_p50=0.12, equity_irr_p90=0.2,
            irr_prob_below_hurdle=0.4,
            npv_p10=-1e8, npv_p50=2e8, npv_p90=5e8,
            tornado=[], revenue_paths=[], equity_irr_paths=[], npv_paths=[],
        )
        return EconomicsResult(mc=mc, monthly_price=[], n_price_hours=8760,
                               n_simulations=100, model="ou",
                               comp_rate_yuan_mwh=rate, comp_annual_yuan=comp)

    def test_shows_breakdown_when_comp_present(self):
        md = economics_section_markdown(self._res(365.0, 2.4e8),
                                        DealBrief(province="蒙西"))
        assert "容量补偿 365 ¥/MWh" in md
        assert "收入构成" in md and "容量补偿 ¥240.0M/年" in md
        assert "现货套利 ¥60.0M" in md

    def test_no_comp_line_when_absent(self):
        md = economics_section_markdown(self._res(0.0, 0.0),
                                        DealBrief(province="山东"))
        assert "容量补偿" not in md and "收入构成" not in md
