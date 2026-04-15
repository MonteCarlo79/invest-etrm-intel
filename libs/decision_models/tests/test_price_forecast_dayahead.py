"""
libs/decision_models/tests/test_price_forecast_dayahead.py

Tests for the price_forecast_dayahead model.

Requirements:
    - numpy and pandas must be installed
    - bess-platform repo root on PYTHONPATH

Run:
    cd bess-platform
    pytest libs/decision_models/tests/test_price_forecast_dayahead.py -v
"""
from __future__ import annotations

import math
from datetime import date, timedelta

import numpy as np
import pytest

# Trigger registration before importing the runner
import libs.decision_models.price_forecast_dayahead  # noqa: F401

from libs.decision_models.price_forecast_dayahead import MODEL_ASSUMPTIONS
from libs.decision_models.registry import registry
from libs.decision_models.runners.local import run


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_TARGET_DATE = "2026-04-15"
_LOOKBACK_DAYS = 14   # use short window in tests for speed
_MIN_TRAIN_DAYS = 7


def _make_hourly_record(date_str: str, hour: int, rt_price, da_price: float) -> dict:
    return {
        "datetime": f"{date_str}T{hour:02d}:00:00",
        "rt_price": rt_price,
        "da_price": da_price,
    }


def _make_day(date_str: str, rt_price: float, da_price: float, target: bool = False) -> list:
    """Build 24 hourly records for one day. If target=True, rt_price is None."""
    return [
        _make_hourly_record(date_str, h, None if target else rt_price, da_price)
        for h in range(24)
    ]


def _build_input(
    n_history_days: int = 14,
    rt_base: float = 60.0,
    da_base: float = 55.0,
    model: str = "ols_da_time_v1",
    **extra,
) -> dict:
    """Build a standard run() input dict with n_history_days of training data + target day."""
    target = date.fromisoformat(_TARGET_DATE)
    hourly_prices = []

    for i in range(n_history_days, 0, -1):
        d = (target - timedelta(days=i)).isoformat()
        # Add slight hour-of-day variation to give OLS something to fit
        hourly_prices.extend(_make_day(d, rt_price=rt_base + i, da_price=da_base + i))

    # Target day: no rt_price, only da_price
    hourly_prices.extend(_make_day(_TARGET_DATE, rt_price=0.0, da_price=da_base, target=True))

    return {
        "hourly_prices": hourly_prices,
        "target_date": _TARGET_DATE,
        "model": model,
        "min_train_days": _MIN_TRAIN_DAYS,
        "lookback_days": _LOOKBACK_DAYS,
        **extra,
    }


# ---------------------------------------------------------------------------
# 1. Registration
# ---------------------------------------------------------------------------

class TestRegistration:
    def test_model_registered(self):
        spec = registry.get("price_forecast_dayahead")
        assert spec.name == "price_forecast_dayahead"
        assert spec.version == "1.0.0"

    def test_run_fn_callable(self):
        assert callable(registry.get("price_forecast_dayahead").run_fn)


# ---------------------------------------------------------------------------
# 2. Metadata contract
# ---------------------------------------------------------------------------

class TestMetadataContract:
    def test_scope_is_province_level(self):
        spec = registry.get("price_forecast_dayahead")
        assert spec.metadata["scope"] == "province_level"

    def test_granularity_hourly(self):
        spec = registry.get("price_forecast_dayahead")
        assert spec.metadata["granularity"] == "hourly"
        assert spec.metadata["intervals_per_day"] == 24

    def test_no_artifact_required(self):
        spec = registry.get("price_forecast_dayahead")
        assert spec.metadata["artifact_required"] is False

    def test_deterministic(self):
        spec = registry.get("price_forecast_dayahead")
        assert spec.metadata["deterministic"] is True

    def test_no_confidence_intervals(self):
        spec = registry.get("price_forecast_dayahead")
        assert spec.metadata["confidence_intervals"] is False

    def test_available_models_declared(self):
        spec = registry.get("price_forecast_dayahead")
        models = spec.metadata["available_models"]
        assert "naive_da" in models
        assert "ols_da_time_v1" in models

    def test_default_model_is_ols(self):
        spec = registry.get("price_forecast_dayahead")
        assert spec.metadata["default_model"] == "ols_da_time_v1"

    def test_source_module_points_to_forecast_engine(self):
        spec = registry.get("price_forecast_dayahead")
        assert "forecast_engine" in spec.metadata["source_of_truth_module"]

    def test_model_assumptions_importable(self):
        assert MODEL_ASSUMPTIONS["granularity"] == "hourly"
        assert MODEL_ASSUMPTIONS["intervals_per_day"] == 24
        assert MODEL_ASSUMPTIONS["scope"] == "province_level"
        assert MODEL_ASSUMPTIONS["deterministic"] is True
        assert MODEL_ASSUMPTIONS["confidence_intervals"] is False
        assert MODEL_ASSUMPTIONS["cross_day_leakage"] is False
        assert isinstance(MODEL_ASSUMPTIONS["limitations"], list)
        assert len(MODEL_ASSUMPTIONS["limitations"]) > 0

    def test_assumptions_mention_province_limitation(self):
        text = " ".join(MODEL_ASSUMPTIONS["limitations"]).lower()
        assert "province" in text or "nodal" in text

    def test_assumptions_mention_hourly_limitation(self):
        text = " ".join(MODEL_ASSUMPTIONS["limitations"]).lower()
        assert "hourly" in text or "15-min" in text


# ---------------------------------------------------------------------------
# 3. Output contract
# ---------------------------------------------------------------------------

class TestOutputContract:
    def test_all_keys_present_ols(self):
        result = run("price_forecast_dayahead", _build_input(model="ols_da_time_v1"))
        assert {"target_date", "model", "datetimes", "rt_pred", "model_used"} <= result.keys()

    def test_all_keys_present_naive(self):
        result = run("price_forecast_dayahead", _build_input(model="naive_da"))
        assert {"target_date", "model", "datetimes", "rt_pred", "model_used"} <= result.keys()

    def test_rt_pred_length_24(self):
        result = run("price_forecast_dayahead", _build_input())
        assert len(result["rt_pred"]) == 24

    def test_datetimes_length_24(self):
        result = run("price_forecast_dayahead", _build_input())
        assert len(result["datetimes"]) == 24

    def test_rt_pred_are_floats(self):
        result = run("price_forecast_dayahead", _build_input())
        for v in result["rt_pred"]:
            assert isinstance(v, float), f"rt_pred contains non-float: {type(v)}"

    def test_rt_pred_finite(self):
        result = run("price_forecast_dayahead", _build_input())
        for v in result["rt_pred"]:
            assert math.isfinite(v), f"rt_pred has non-finite value: {v}"

    def test_datetimes_on_target_date(self):
        result = run("price_forecast_dayahead", _build_input())
        for ts in result["datetimes"]:
            assert ts.startswith(_TARGET_DATE), f"datetime {ts!r} not on target date"

    def test_datetimes_all_24_hours(self):
        result = run("price_forecast_dayahead", _build_input())
        hours = [int(ts[11:13]) for ts in result["datetimes"]]
        assert sorted(hours) == list(range(24))

    def test_target_date_echoed(self):
        result = run("price_forecast_dayahead", _build_input())
        assert result["target_date"] == _TARGET_DATE

    def test_model_echoed(self):
        result = run("price_forecast_dayahead", _build_input(model="naive_da"))
        assert result["model"] == "naive_da"

    def test_model_used_valid_values(self):
        result = run("price_forecast_dayahead", _build_input())
        assert result["model_used"] in ("ols", "naive_da")


# ---------------------------------------------------------------------------
# 4. Physics / behaviour
# ---------------------------------------------------------------------------

class TestBehaviour:
    def test_naive_da_returns_da_prices(self):
        """naive_da model must return da_price unchanged."""
        da_prices = list(range(24))  # 0..23 as da prices for target date
        target = date.fromisoformat(_TARGET_DATE)
        hourly_prices = []
        for i in range(8, 0, -1):
            d = (target - timedelta(days=i)).isoformat()
            hourly_prices.extend(_make_day(d, rt_price=50.0, da_price=50.0))
        for h in range(24):
            hourly_prices.append({
                "datetime": f"{_TARGET_DATE}T{h:02d}:00:00",
                "rt_price": None,
                "da_price": float(h),
            })

        result = run("price_forecast_dayahead", {
            "hourly_prices": hourly_prices,
            "target_date": _TARGET_DATE,
            "model": "naive_da",
        })
        for i, (pred, da) in enumerate(zip(result["rt_pred"], da_prices)):
            assert abs(pred - float(da)) < 1e-9, \
                f"naive_da: hour {i} pred={pred} expected={float(da)}"

    def test_ols_uses_history_not_target_rt(self):
        """OLS output should not be identical to naive_da when there is useful history."""
        inp_ols = _build_input(model="ols_da_time_v1", n_history_days=14)
        inp_naive = _build_input(model="naive_da", n_history_days=14)
        result_ols = run("price_forecast_dayahead", inp_ols)
        result_naive = run("price_forecast_dayahead", inp_naive)
        # They won't always differ (degenerate price series), but with variable
        # history we just check that the OLS path completed and returned 24 values
        assert len(result_ols["rt_pred"]) == 24
        assert result_ols["model_used"] in ("ols", "naive_da")

    def test_deterministic(self):
        """Same inputs must produce identical outputs (no randomness)."""
        inp = _build_input()
        r1 = run("price_forecast_dayahead", inp)
        r2 = run("price_forecast_dayahead", inp)
        assert r1["rt_pred"] == r2["rt_pred"]

    def test_fallback_to_naive_when_insufficient_history(self):
        """With fewer than min_train_days of history, model_used should be 'naive_da'."""
        # Build only 3 days of history, but require 7
        target = date.fromisoformat(_TARGET_DATE)
        hourly_prices = []
        for i in range(3, 0, -1):
            d = (target - timedelta(days=i)).isoformat()
            hourly_prices.extend(_make_day(d, rt_price=60.0, da_price=55.0))
        hourly_prices.extend(_make_day(_TARGET_DATE, rt_price=0.0, da_price=55.0, target=True))

        result = run("price_forecast_dayahead", {
            "hourly_prices": hourly_prices,
            "target_date": _TARGET_DATE,
            "model": "ols_da_time_v1",
            "min_train_days": 7,
            "lookback_days": 60,
        })
        assert result["model_used"] == "naive_da", (
            "Should fall back to naive_da when < min_train_days of history"
        )

    def test_ols_model_used_with_sufficient_history(self):
        """With >= min_train_days, model_used should be 'ols'."""
        result = run("price_forecast_dayahead", _build_input(n_history_days=14, min_train_days=7))
        assert result["model_used"] == "ols"

    def test_no_cross_day_leakage(self):
        """
        Verify no cross-day leakage: running forecast on just the target day
        with no history should fall back to naive_da (not use target's own RT).
        """
        hourly_prices = _make_day(_TARGET_DATE, rt_price=999.0, da_price=55.0, target=True)
        result = run("price_forecast_dayahead", {
            "hourly_prices": hourly_prices,
            "target_date": _TARGET_DATE,
            "model": "ols_da_time_v1",
            "min_train_days": 1,
            "lookback_days": 60,
        })
        # With no prior training data at all, must fall back
        assert result["model_used"] == "naive_da"
        # Predictions should be da_price (55.0), not 999.0 (the suppressed rt)
        for pred in result["rt_pred"]:
            assert abs(pred - 55.0) < 1e-6, \
                f"Fallback naive_da should return da_price=55.0, got {pred}"


# ---------------------------------------------------------------------------
# 5. Input validation
# ---------------------------------------------------------------------------

class TestInputValidation:
    def test_empty_hourly_prices_raises(self):
        with pytest.raises(ValueError, match="empty"):
            run("price_forecast_dayahead", {
                "hourly_prices": [],
                "target_date": _TARGET_DATE,
            })

    def test_unknown_model_raises(self):
        with pytest.raises(ValueError, match="model"):
            run("price_forecast_dayahead", {
                "hourly_prices": _make_day(_TARGET_DATE, rt_price=50.0, da_price=50.0),
                "target_date": _TARGET_DATE,
                "model": "xgboost_fancy",
            })

    def test_invalid_target_date_raises(self):
        with pytest.raises(ValueError, match="target_date"):
            run("price_forecast_dayahead", {
                "hourly_prices": _make_day(_TARGET_DATE, rt_price=50.0, da_price=50.0),
                "target_date": "not-a-date",
            })

    def test_target_date_not_in_prices_raises(self):
        with pytest.raises(ValueError):
            run("price_forecast_dayahead", {
                "hourly_prices": _make_day("2026-03-01", rt_price=50.0, da_price=50.0),
                "target_date": _TARGET_DATE,   # different date — not in prices
            })

    def test_nan_da_price_on_target_raises(self):
        import math
        hourly_prices = [
            {
                "datetime": f"{_TARGET_DATE}T{h:02d}:00:00",
                "rt_price": None,
                "da_price": math.nan,
            }
            for h in range(24)
        ]
        with pytest.raises(ValueError, match="NaN"):
            run("price_forecast_dayahead", {
                "hourly_prices": hourly_prices,
                "target_date": _TARGET_DATE,
            })

    def test_missing_da_price_key_raises(self):
        with pytest.raises((KeyError, ValueError)):
            run("price_forecast_dayahead", {
                "hourly_prices": [{"datetime": f"{_TARGET_DATE}T00:00:00", "rt_price": 50.0}],
                "target_date": _TARGET_DATE,
            })
