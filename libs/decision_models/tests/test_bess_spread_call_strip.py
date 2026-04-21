"""
libs/decision_models/tests/test_bess_spread_call_strip.py

Unit tests for the bess_spread_call_strip decision model.

Tests cover:
  - _norm_cdf: standard normal CDF correctness
  - _margrabe_call: boundary conditions and monotonicity
  - _spread_vol: formula correctness
  - _strip_value: additive structure, q_max scaling
  - _run (full model): output schema, Greek signs, ITM/OTM, arithmetic identities
  - Registry: model registered, metadata contract satisfied

No DB, no external dependencies. Pure-Python only.

Run:
    pytest libs/decision_models/tests/test_bess_spread_call_strip.py -v
"""
from __future__ import annotations

import dataclasses
import math

import pytest

import libs.decision_models.bess_spread_call_strip as mod
from libs.decision_models.bess_spread_call_strip import (
    _margrabe_call,
    _norm_cdf,
    _run,
    _spread_vol,
    _strip_value,
)
from libs.decision_models.registry import registry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _default_inputs(**overrides):
    base = dict(
        asset_code="suyou",
        as_of_date="2026-04-21",
        n_days_remaining=252,
        peak_forward_yuan=350.0,
        offpeak_forward_yuan=200.0,
        peak_vol=0.30,
        offpeak_vol=0.25,
        peak_offpeak_corr=0.85,
        roundtrip_eff=0.85,
        power_mw=100.0,
        duration_h=2.0,
        om_cost_yuan_per_mwh=0.0,
        risk_free_rate=0.0,
    )
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# TestNormCDF
# ---------------------------------------------------------------------------

class TestNormCDF:
    def test_at_zero_is_half(self):
        assert abs(_norm_cdf(0.0) - 0.5) < 1e-10

    def test_large_positive_approaches_one(self):
        assert _norm_cdf(10.0) > 0.9999

    def test_large_negative_approaches_zero(self):
        assert _norm_cdf(-10.0) < 1e-4

    def test_symmetry(self):
        for x in [0.5, 1.0, 1.96, 2.58]:
            assert abs(_norm_cdf(x) + _norm_cdf(-x) - 1.0) < 1e-12

    def test_known_values(self):
        # N(1.645) ≈ 0.95, N(-1.645) ≈ 0.05
        assert abs(_norm_cdf(1.645) - 0.95) < 0.001
        assert abs(_norm_cdf(-1.645) - 0.05) < 0.001


# ---------------------------------------------------------------------------
# TestSpreadVol
# ---------------------------------------------------------------------------

class TestSpreadVol:
    def test_zero_correlation_is_pythagorean(self):
        s = _spread_vol(0.30, 0.25, 0.0)
        expected = math.sqrt(0.30 ** 2 + 0.25 ** 2)
        assert abs(s - expected) < 1e-10

    def test_perfect_correlation_is_difference(self):
        # ρ=1 → σ_s = |σ1 - σ2|
        s = _spread_vol(0.30, 0.25, 1.0)
        assert abs(s - abs(0.30 - 0.25)) < 1e-10

    def test_higher_correlation_lowers_spread_vol(self):
        s_low = _spread_vol(0.30, 0.25, 0.50)
        s_high = _spread_vol(0.30, 0.25, 0.95)
        assert s_high < s_low

    def test_positive_result(self):
        for rho in [0.0, 0.5, 0.85, 0.99]:
            assert _spread_vol(0.30, 0.25, rho) > 0.0

    def test_symmetric_vols_zero_corr(self):
        s = _spread_vol(0.20, 0.20, 0.0)
        assert abs(s - 0.20 * math.sqrt(2)) < 1e-10


# ---------------------------------------------------------------------------
# TestMargrabeCall
# ---------------------------------------------------------------------------

class TestMargrabeCall:
    def test_zero_time_returns_intrinsic(self):
        c = _margrabe_call(350.0, 230.0, 0.15, T=0.0)
        assert abs(c - 120.0) < 1e-8

    def test_zero_time_otm_returns_zero(self):
        c = _margrabe_call(200.0, 350.0, 0.15, T=0.0)
        assert c == 0.0

    def test_zero_vol_returns_intrinsic(self):
        c = _margrabe_call(350.0, 230.0, 0.0, T=1.0)
        assert abs(c - 120.0) < 1e-8

    def test_positive_for_itm(self):
        c = _margrabe_call(350.0, 230.0, 0.15, T=1.0)
        assert c > 120.0  # must exceed intrinsic (has time value)

    def test_value_increases_with_maturity(self):
        c1 = _margrabe_call(350.0, 230.0, 0.20, T=0.1)
        c2 = _margrabe_call(350.0, 230.0, 0.20, T=1.0)
        assert c2 > c1

    def test_value_increases_with_vol(self):
        c_low = _margrabe_call(280.0, 230.0, 0.10, T=0.5)
        c_high = _margrabe_call(280.0, 230.0, 0.40, T=0.5)
        assert c_high > c_low

    def test_deep_otm_near_zero(self):
        # F1 << F2_eff → option nearly worthless
        c = _margrabe_call(100.0, 500.0, 0.20, T=0.5)
        assert c < 1.0

    def test_deep_itm_approaches_intrinsic(self):
        # Very low vol → approaches intrinsic
        intrinsic = 200.0
        c = _margrabe_call(500.0, 300.0, 0.001, T=0.01)
        assert abs(c - intrinsic) < 1.0


# ---------------------------------------------------------------------------
# TestStripValue
# ---------------------------------------------------------------------------

class TestStripValue:
    def test_single_day_matches_margrabe(self):
        F_pk, F2_eff, sigma_s, q_max, r = 350.0, 230.0, 0.15, 170.0, 0.0
        sv = _strip_value(F_pk, F2_eff, sigma_s, q_max, n_days=1, r=r)
        T1 = 1 / 252
        expected = q_max * _margrabe_call(F_pk, F2_eff, sigma_s, T1, r)
        assert abs(sv - expected) < 1e-8

    def test_strip_increases_with_n_days(self):
        sv30 = _strip_value(350.0, 230.0, 0.15, 170.0, 30, 0.0)
        sv252 = _strip_value(350.0, 230.0, 0.15, 170.0, 252, 0.0)
        assert sv252 > sv30

    def test_q_max_scales_linearly(self):
        sv1 = _strip_value(350.0, 230.0, 0.15, 100.0, 30, 0.0)
        sv2 = _strip_value(350.0, 230.0, 0.15, 200.0, 30, 0.0)
        assert abs(sv2 / sv1 - 2.0) < 1e-10

    def test_zero_q_max_returns_zero(self):
        sv = _strip_value(350.0, 230.0, 0.15, 0.0, 252, 0.0)
        assert sv == 0.0

    def test_otm_strip_still_positive(self):
        # OTM has time value
        sv = _strip_value(200.0, 350.0, 0.30, 170.0, 252, 0.0)
        assert sv >= 0.0


# ---------------------------------------------------------------------------
# TestBESSSpreadCallRun — full model
# ---------------------------------------------------------------------------

class TestBESSSpreadCallRun:
    def test_output_is_dict(self):
        result = _run(**_default_inputs())
        assert isinstance(result, dict)

    def test_output_has_all_schema_fields(self):
        from libs.decision_models.schemas.bess_spread_call_strip import BESSSpreadCallOutput
        result = _run(**_default_inputs())
        schema_fields = {f.name for f in dataclasses.fields(BESSSpreadCallOutput)}
        assert schema_fields.issubset(set(result.keys()))

    def test_strip_value_positive_for_itm(self):
        # peak=350, offpeak=200, η=0.85 → F2_eff=235, net_spread=350-235=115 — deep ITM
        result = _run(**_default_inputs())
        assert result["strip_value_yuan"] > 0.0

    def test_intrinsic_plus_time_equals_strip_value(self):
        result = _run(**_default_inputs())
        assert abs(result["intrinsic_value_yuan"] + result["time_value_yuan"] - result["strip_value_yuan"]) < 1e-6

    def test_per_day_value_consistent(self):
        inputs = _default_inputs()
        result = _run(**inputs)
        expected = result["strip_value_yuan"] / inputs["n_days_remaining"]
        assert abs(result["per_day_value_yuan"] - expected) < 1e-6

    def test_q_max_computed_correctly(self):
        result = _run(**_default_inputs(roundtrip_eff=0.85, power_mw=100.0, duration_h=2.0))
        assert abs(result["q_max_mwh_per_day"] - 0.85 * 100.0 * 2.0) < 1e-10

    def test_net_spread_forward_correct(self):
        result = _run(**_default_inputs(
            peak_forward_yuan=350.0, offpeak_forward_yuan=200.0, roundtrip_eff=0.85
        ))
        expected = 350.0 - 200.0 / 0.85
        assert abs(result["net_spread_forward"] - expected) < 1e-10

    def test_spread_vol_used_correct(self):
        result = _run(**_default_inputs(peak_vol=0.30, offpeak_vol=0.25, peak_offpeak_corr=0.85))
        expected = _spread_vol(0.30, 0.25, 0.85)
        assert abs(result["spread_vol_used"] - expected) < 1e-10

    def test_effective_strike_equals_om_cost(self):
        result = _run(**_default_inputs(om_cost_yuan_per_mwh=50.0))
        assert result["effective_strike_yuan_per_mwh"] == 50.0

    def test_deep_otm_strip_value_near_zero(self):
        # peak=100, offpeak=400 — deeply OTM even with time value
        result = _run(**_default_inputs(
            peak_forward_yuan=100.0,
            offpeak_forward_yuan=400.0,
            n_days_remaining=30,
        ))
        # Strip value should be tiny relative to an ITM case
        assert result["strip_value_yuan"] < 1000.0  # ¥ — small for 100MW asset

    def test_higher_vol_increases_strip_value(self):
        base = _run(**_default_inputs(peak_vol=0.20))
        high = _run(**_default_inputs(peak_vol=0.50))
        assert high["strip_value_yuan"] > base["strip_value_yuan"]

    def test_more_days_increases_strip_value(self):
        short = _run(**_default_inputs(n_days_remaining=30))
        long_ = _run(**_default_inputs(n_days_remaining=252))
        assert long_["strip_value_yuan"] > short["strip_value_yuan"]

    def test_intrinsic_zero_for_otm(self):
        # When net_spread_fwd < K, intrinsic = 0
        result = _run(**_default_inputs(
            peak_forward_yuan=200.0,
            offpeak_forward_yuan=300.0,
            om_cost_yuan_per_mwh=0.0,
        ))
        assert result["intrinsic_value_yuan"] == 0.0

    def test_time_value_non_negative(self):
        result = _run(**_default_inputs())
        assert result["time_value_yuan"] >= -1e-6  # allow tiny float rounding

    def test_asset_code_and_date_passthrough(self):
        result = _run(**_default_inputs(asset_code="wulate", as_of_date="2026-01-01"))
        assert result["asset_code"] == "wulate"
        assert result["as_of_date"] == "2026-01-01"


# ---------------------------------------------------------------------------
# TestGreekSigns
# ---------------------------------------------------------------------------

class TestGreekSigns:
    """Greeks for a long spread call option: delta>0, vega>0, theta<=0."""

    def _result(self, **overrides):
        return _run(**_default_inputs(**overrides))

    def test_delta_positive(self):
        r = self._result()
        assert r["delta_yuan_per_yuan"] > 0.0

    def test_vega_positive(self):
        r = self._result()
        assert r["vega_yuan_per_vol_point"] > 0.0

    def test_theta_non_positive(self):
        r = self._result()
        assert r["theta_yuan_per_day"] <= 0.0

    def test_delta_otm_smaller_than_delta_itm(self):
        # Deep ITM has higher delta sensitivity than near-OTM
        itm = self._result(peak_forward_yuan=500.0)
        otm = self._result(peak_forward_yuan=150.0)
        assert itm["delta_yuan_per_yuan"] > otm["delta_yuan_per_yuan"]

    def test_vega_decreases_deep_itm(self):
        # Deep ITM → low time value → lower vega than near-the-money
        deep_itm = self._result(peak_forward_yuan=800.0, offpeak_forward_yuan=100.0)
        atm_ish = self._result(peak_forward_yuan=300.0, offpeak_forward_yuan=250.0)
        # ATM options have higher vega; compare absolute magnitudes
        assert atm_ish["vega_yuan_per_vol_point"] > deep_itm["vega_yuan_per_vol_point"]

    def test_theta_single_day_is_negative(self):
        r = self._result(n_days_remaining=1)
        assert r["theta_yuan_per_day"] <= 0.0


# ---------------------------------------------------------------------------
# TestMoneyness
# ---------------------------------------------------------------------------

class TestMoneyness:
    def test_positive_moneyness_itm(self):
        # net_spread_fwd > K
        r = _run(**_default_inputs(
            peak_forward_yuan=350.0, offpeak_forward_yuan=200.0,
            roundtrip_eff=0.85, om_cost_yuan_per_mwh=0.0,
        ))
        assert r["moneyness_pct"] > 0.0

    def test_negative_moneyness_otm(self):
        r = _run(**_default_inputs(
            peak_forward_yuan=200.0, offpeak_forward_yuan=500.0,
            roundtrip_eff=0.85, om_cost_yuan_per_mwh=0.0,
        ))
        assert r["moneyness_pct"] < 0.0

    def test_moneyness_denom_floor(self):
        # om_cost=0 → denom=max(0,1)=1; moneyness = net_spread_fwd * 100
        r = _run(**_default_inputs(
            peak_forward_yuan=350.0, offpeak_forward_yuan=200.0,
            roundtrip_eff=0.85, om_cost_yuan_per_mwh=0.0,
        ))
        net = 350.0 - 200.0 / 0.85
        expected = net * 100.0
        assert abs(r["moneyness_pct"] - expected) < 1e-8


# ---------------------------------------------------------------------------
# TestRegistry
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_model_registered(self):
        spec = registry.get("bess_spread_call_strip")
        assert spec is not None
        assert spec.name == "bess_spread_call_strip"

    def test_model_has_run_fn(self):
        spec = registry.get("bess_spread_call_strip")
        assert spec.run_fn is not None

    def test_model_has_input_output_schema(self):
        spec = registry.get("bess_spread_call_strip")
        assert spec.input_schema is not None
        assert spec.output_schema is not None

    def test_model_category_is_analytics(self):
        spec = registry.get("bess_spread_call_strip")
        assert spec.metadata["category"] == "analytics"

    def test_model_status_is_experimental(self):
        spec = registry.get("bess_spread_call_strip")
        assert spec.metadata["status"] == "experimental"

    def test_model_has_all_required_metadata_keys(self):
        from libs.decision_models.model_spec import REQUIRED_METADATA_KEYS
        spec = registry.get("bess_spread_call_strip")
        missing = REQUIRED_METADATA_KEYS - set(spec.metadata.keys())
        assert not missing, f"Missing keys: {sorted(missing)}"

    def test_model_version_semver(self):
        spec = registry.get("bess_spread_call_strip")
        parts = spec.version.split(".")
        assert len(parts) == 3
        assert all(p[0].isdigit() for p in parts)
