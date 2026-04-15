"""
libs/decision_models/tests/test_bess_dispatch_optimization.py

Tests for bess_dispatch_optimization (single-day) and
bess_dispatch_simulation_multiday models.

Requirements:
    - pulp and its CBC solver must be installed
    - bess-platform repo root on PYTHONPATH so services.bess_map resolves

Run:
    cd bess-platform
    pytest libs/decision_models/tests/test_bess_dispatch_optimization.py -v
"""
from __future__ import annotations

import math

import pytest

# Trigger registration before importing the runner
import libs.decision_models.bess_dispatch_optimization           # noqa: F401
import libs.decision_models.bess_dispatch_simulation_multiday    # noqa: F401

from libs.decision_models.bess_dispatch_optimization import MODEL_ASSUMPTIONS
from libs.decision_models.registry import registry
from libs.decision_models.runners.local import run


# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

_DEFAULT_PARAMS = {
    "power_mw": 10.0,
    "duration_h": 2.0,
    "roundtrip_eff": 0.85,
}

_CHEAP = 30.0
_PEAK = 120.0
# Two-cycle spread price curve: cheap night, morning peak, midday valley, evening peak, night
_SPREAD_PRICES = (
    [_CHEAP] * 8       # 00-07 cheap
    + [_PEAK] * 4      # 08-11 peak
    + [_CHEAP] * 4     # 12-15 cheap
    + [_PEAK] * 4      # 16-19 peak
    + [_CHEAP] * 4     # 20-23 cheap
)
assert len(_SPREAD_PRICES) == 24


def _run_single(prices_override=None, **extra):
    return run("bess_dispatch_optimization", {
        "prices_24": prices_override if prices_override is not None else _SPREAD_PRICES,
        **_DEFAULT_PARAMS,
        **extra,
    })


def _make_hourly_prices(prices_24: list, date_str: str) -> list:
    """Build a list of {datetime, price} dicts for one day."""
    return [
        {"datetime": f"{date_str}T{h:02d}:00:00", "price": p}
        for h, p in enumerate(prices_24)
    ]


# ---------------------------------------------------------------------------
# 1. Registration
# ---------------------------------------------------------------------------

class TestRegistration:
    def test_singleday_model_registered(self):
        spec = registry.get("bess_dispatch_optimization")
        assert spec.name == "bess_dispatch_optimization"
        assert spec.version == "1.0.0"

    def test_multiday_model_registered(self):
        spec = registry.get("bess_dispatch_simulation_multiday")
        assert spec.name == "bess_dispatch_simulation_multiday"
        assert spec.version == "1.0.0"

    def test_run_fn_callable(self):
        assert callable(registry.get("bess_dispatch_optimization").run_fn)
        assert callable(registry.get("bess_dispatch_simulation_multiday").run_fn)


# ---------------------------------------------------------------------------
# 2. Metadata matches contract
# ---------------------------------------------------------------------------

class TestMetadataContract:
    """
    These tests verify that the model's declared metadata is consistent with
    what the engine actually does. They guard against metadata drifting away
    from behaviour.
    """

    def test_scope_is_single_day(self):
        spec = registry.get("bess_dispatch_optimization")
        assert spec.metadata["scope"] == "single_day"

    def test_intervals_per_day_is_24(self):
        spec = registry.get("bess_dispatch_optimization")
        assert spec.metadata["intervals_per_day"] == 24
        assert spec.metadata["price_vector_length_required"] == 24

    def test_no_cross_day_carryover_declared(self):
        spec = registry.get("bess_dispatch_optimization")
        assert spec.metadata["cross_day_soc_carryover"] is False

    def test_initial_soc_declared_zero(self):
        spec = registry.get("bess_dispatch_optimization")
        assert spec.metadata["initial_soc"] == "zero"

    def test_terminal_soc_declared_unconstrained(self):
        spec = registry.get("bess_dispatch_optimization")
        assert spec.metadata["terminal_soc"] == "unconstrained"

    def test_solver_declared(self):
        spec = registry.get("bess_dispatch_optimization")
        assert spec.metadata["solver"] == "pulp_cbc"

    def test_source_module_points_to_engine(self):
        spec = registry.get("bess_dispatch_optimization")
        assert "optimisation_engine" in spec.metadata["source_module"]

    def test_multiday_cross_reference_present(self):
        spec = registry.get("bess_dispatch_optimization")
        assert spec.metadata.get("multiday_model") == "bess_dispatch_simulation_multiday"

    def test_model_assumptions_dict_imported(self):
        """MODEL_ASSUMPTIONS is importable and consistent with spec metadata."""
        assert MODEL_ASSUMPTIONS["intervals_per_day"] == 24
        assert MODEL_ASSUMPTIONS["cross_day_soc_carryover"] is False
        assert MODEL_ASSUMPTIONS["initial_soc"] == "zero"
        assert MODEL_ASSUMPTIONS["simultaneous_charge_discharge"] is False
        assert isinstance(MODEL_ASSUMPTIONS["limitations"], list)
        assert len(MODEL_ASSUMPTIONS["limitations"]) > 0

    def test_assumptions_no_cross_day_in_limitations(self):
        """The 'no cross-day SOC' limitation must be documented."""
        text = " ".join(MODEL_ASSUMPTIONS["limitations"]).lower()
        assert "cross-day" in text or "soc" in text

    def test_multiday_metadata_consistent(self):
        spec = registry.get("bess_dispatch_simulation_multiday")
        assert spec.metadata["cross_day_soc_carryover"] is False
        assert spec.metadata["initial_soc_per_day"] == "zero"
        assert spec.metadata["scope"] == "multi_day"

    def test_initial_soc_zero_is_enforced(self):
        """
        Verify metadata claim: SOC starts at 0.
        With cheap prices all day, optimal dispatch is do-nothing → SOC stays 0.
        """
        result = _run_single(prices_override=[50.0] * 24)
        # All prices equal → no arbitrage → no dispatch → SOC never leaves 0
        assert result["soc_mwh"][0] <= 1e-4, (
            "soc_mwh[0] should be ≈0 (initial_soc=zero) when no dispatch occurs"
        )


# ---------------------------------------------------------------------------
# 3. Single-day output contract
# ---------------------------------------------------------------------------

class TestSingleDayOutputContract:
    def test_all_output_keys_present(self):
        result = _run_single()
        expected = {
            "charge_mw", "discharge_mw", "dispatch_grid_mw",
            "soc_mwh", "profit", "solver_status", "energy_capacity_mwh",
        }
        assert expected.issubset(result.keys())

    def test_arrays_exactly_24(self):
        result = _run_single()
        assert len(result["charge_mw"]) == 24
        assert len(result["discharge_mw"]) == 24
        assert len(result["dispatch_grid_mw"]) == 24
        assert len(result["soc_mwh"]) == 24

    def test_dispatch_grid_derived_correctly(self):
        result = _run_single()
        for t in range(24):
            expected = result["discharge_mw"][t] - result["charge_mw"][t]
            assert abs(result["dispatch_grid_mw"][t] - expected) < 1e-6, \
                f"dispatch_grid_mw mismatch at hour {t}"

    def test_energy_capacity_equals_power_times_duration(self):
        result = _run_single()
        assert abs(result["energy_capacity_mwh"] - 10.0 * 2.0) < 1e-9

    def test_solver_status_optimal(self):
        result = _run_single()
        assert result["solver_status"] == "Optimal"

    def test_profit_finite(self):
        result = _run_single()
        assert math.isfinite(result["profit"])

    def test_all_values_are_floats_or_str(self):
        """Output arrays contain plain Python floats (JSON-serialisable)."""
        result = _run_single()
        for arr_key in ("charge_mw", "discharge_mw", "dispatch_grid_mw", "soc_mwh"):
            for v in result[arr_key]:
                assert isinstance(v, float), f"{arr_key} contains non-float: {type(v)}"


# ---------------------------------------------------------------------------
# 4. Single-day physics / solver smoke tests
# ---------------------------------------------------------------------------

class TestSingleDayPhysics:
    def test_positive_profit_on_spread_prices(self):
        result = _run_single()
        assert result["profit"] > 0

    def test_zero_profit_on_flat_prices(self):
        result = _run_single(prices_override=[50.0] * 24)
        assert abs(result["profit"]) < 1e-3

    def test_no_simultaneous_charge_and_discharge(self):
        result = _run_single()
        for t in range(24):
            ch, dis = result["charge_mw"][t], result["discharge_mw"][t]
            assert not (ch > 1e-4 and dis > 1e-4), \
                f"Simultaneous C/D at hour {t}: charge={ch:.3f} discharge={dis:.3f}"

    def test_soc_within_capacity_bounds(self):
        result = _run_single()
        e_cap = result["energy_capacity_mwh"]
        for t, soc in enumerate(result["soc_mwh"]):
            assert -1e-4 <= soc <= e_cap + 1e-4, \
                f"SOC {soc:.3f} out of [0, {e_cap}] at hour {t}"

    def test_charge_discharge_within_power_limit(self):
        result = _run_single()
        p = _DEFAULT_PARAMS["power_mw"]
        for t in range(24):
            assert result["charge_mw"][t] <= p + 1e-4
            assert result["discharge_mw"][t] <= p + 1e-4
            assert result["charge_mw"][t] >= -1e-4
            assert result["discharge_mw"][t] >= -1e-4

    def test_degradation_throughput_cap(self):
        cap_mwh = 5.0
        result = _run_single(max_throughput_mwh=cap_mwh)
        total_discharge = sum(result["discharge_mw"])
        assert total_discharge <= cap_mwh + 1e-4, \
            f"Discharge {total_discharge:.3f} MWh exceeds cap {cap_mwh} MWh"

    def test_degradation_cycle_cap(self):
        cap_cycles = 0.5
        e_cap = _DEFAULT_PARAMS["power_mw"] * _DEFAULT_PARAMS["duration_h"]
        result = _run_single(max_cycles_per_day=cap_cycles)
        total_discharge = sum(result["discharge_mw"])
        assert total_discharge <= cap_cycles * e_cap + 1e-4


# ---------------------------------------------------------------------------
# 5. Single-day input validation
# ---------------------------------------------------------------------------

class TestSingleDayInputValidation:
    def test_short_price_vector_raises(self):
        with pytest.raises(ValueError, match="24"):
            run("bess_dispatch_optimization", {
                "prices_24": [50.0] * 12,
                "power_mw": 10.0, "duration_h": 2.0, "roundtrip_eff": 0.85,
            })

    def test_long_price_vector_raises(self):
        with pytest.raises(ValueError, match="24"):
            run("bess_dispatch_optimization", {
                "prices_24": [50.0] * 96,   # 15-min, wrong for this model
                "power_mw": 10.0, "duration_h": 2.0, "roundtrip_eff": 0.85,
            })

    def test_zero_power_mw_raises(self):
        with pytest.raises(ValueError, match="power_mw"):
            run("bess_dispatch_optimization", {
                "prices_24": [50.0] * 24,
                "power_mw": 0.0, "duration_h": 2.0, "roundtrip_eff": 0.85,
            })

    def test_invalid_roundtrip_eff_raises(self):
        with pytest.raises(ValueError, match="roundtrip_eff"):
            run("bess_dispatch_optimization", {
                "prices_24": [50.0] * 24,
                "power_mw": 10.0, "duration_h": 2.0, "roundtrip_eff": 1.5,
            })

    def test_nan_price_raises(self):
        import math
        prices = [50.0] * 24
        prices[5] = math.nan
        with pytest.raises(ValueError):
            run("bess_dispatch_optimization", {
                "prices_24": prices,
                "power_mw": 10.0, "duration_h": 2.0, "roundtrip_eff": 0.85,
            })


# ---------------------------------------------------------------------------
# 6. Multi-day model: output shape
# ---------------------------------------------------------------------------

class TestMultiDayShape:
    def _run_multiday(self, days: list, **extra):
        all_prices = []
        for date_str, prices in days:
            all_prices.extend(_make_hourly_prices(prices, date_str))
        return run("bess_dispatch_simulation_multiday", {
            "hourly_prices": all_prices,
            **_DEFAULT_PARAMS,
            **extra,
        })

    def test_single_day_shape(self):
        result = self._run_multiday([("2026-01-01", _SPREAD_PRICES)])
        assert result["n_days_solved"] == 1
        assert result["n_days_skipped"] == 0
        assert len(result["dispatch_records"]) == 24
        assert len(result["daily_profit"]) == 1

    def test_two_day_shape(self):
        result = self._run_multiday([
            ("2026-01-01", _SPREAD_PRICES),
            ("2026-01-02", _SPREAD_PRICES),
        ])
        assert result["n_days_solved"] == 2
        assert len(result["dispatch_records"]) == 48
        assert len(result["daily_profit"]) == 2

    def test_dispatch_record_keys(self):
        result = self._run_multiday([("2026-01-01", _SPREAD_PRICES)])
        rec = result["dispatch_records"][0]
        for key in ("datetime", "charge_mw", "discharge_mw",
                    "dispatch_grid_mw", "soc_mwh", "solver_status"):
            assert key in rec, f"Missing key in dispatch_record: {key}"

    def test_daily_profit_keys(self):
        result = self._run_multiday([("2026-01-01", _SPREAD_PRICES)])
        day = result["daily_profit"][0]
        assert "date" in day
        assert "profit" in day

    def test_energy_capacity_correct(self):
        result = self._run_multiday([("2026-01-01", _SPREAD_PRICES)])
        assert abs(result["energy_capacity_mwh"] - 10.0 * 2.0) < 1e-9

    def test_positive_profit_on_spread(self):
        result = self._run_multiday([("2026-01-01", _SPREAD_PRICES)])
        assert result["daily_profit"][0]["profit"] > 0

    def test_each_day_solved_independently(self):
        """
        Verify no cross-day SOC carryover: day 2 SOC should start from 0
        just like day 1. Both days have the same prices so they should have
        identical profit.
        """
        result = self._run_multiday([
            ("2026-01-01", _SPREAD_PRICES),
            ("2026-01-02", _SPREAD_PRICES),
        ])
        p1 = result["daily_profit"][0]["profit"]
        p2 = result["daily_profit"][1]["profit"]
        assert abs(p1 - p2) < 1e-3, (
            f"Same price curve on consecutive days should yield same profit "
            f"(no SOC carryover). Got day1={p1:.2f}, day2={p2:.2f}"
        )

    def test_empty_input_raises(self):
        with pytest.raises((ValueError, Exception)):
            run("bess_dispatch_simulation_multiday", {
                "hourly_prices": [],
                "power_mw": 10.0, "duration_h": 2.0, "roundtrip_eff": 0.85,
            })

    def test_flat_prices_zero_profit_multiday(self):
        result = self._run_multiday([
            ("2026-01-01", [50.0] * 24),
            ("2026-01-02", [50.0] * 24),
        ])
        for day in result["daily_profit"]:
            assert abs(day["profit"]) < 1e-3, \
                f"Expected ~zero profit on flat prices, got {day['profit']}"
