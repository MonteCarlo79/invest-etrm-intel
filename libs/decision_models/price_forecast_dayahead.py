"""
libs/decision_models/price_forecast_dayahead.py

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SCOPE: PROVINCE-LEVEL HOURLY RT PRICE FORECAST (NOT NODAL / NOT 15-MIN)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Reusable model asset that wraps forecast_engine.py from
services/bess_map/forecast_engine.py.

Two available models:
    naive_da        — RT prediction = DA price. No training required.
    ols_da_time_v1  — Rolling OLS: [1, da_price, sin(2πh/24), cos(2πh/24)].
                      Default. Falls back to naive_da when training data is
                      insufficient.

Self-registers both model variants on import.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
USAGE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    import libs.decision_models.price_forecast_dayahead   # register
    from libs.decision_models.runners.local import run

    result = run("price_forecast_dayahead", {
        "hourly_prices": [
            {"datetime": "2026-04-14T00:00:00", "rt_price": 55.0,  "da_price": 52.0},
            {"datetime": "2026-04-14T01:00:00", "rt_price": 50.0,  "da_price": 48.0},
            # ... 24 hours of history per day for lookback window ...
            {"datetime": "2026-04-15T00:00:00", "rt_price": None,  "da_price": 60.0},
            # ... 24 hours of target date with only da_price ...
        ],
        "target_date": "2026-04-15",
        "model": "ols_da_time_v1",   # optional, default
        "min_train_days": 7,          # optional
        "lookback_days": 60,          # optional
    })
    # result["rt_pred"]      — list of 24 floats (hourly RT predictions)
    # result["datetimes"]    — list of 24 ISO timestamp strings
    # result["model_used"]   — "ols" or "naive_da" (actual model used)
    # result["target_date"]  — echoed back
    # result["model"]        — model name requested
"""
from __future__ import annotations

import dataclasses
import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from libs.decision_models.model_spec import ModelSpec
from libs.decision_models.registry import registry
from libs.decision_models.schemas.price_forecast_dayahead import (
    PriceForecastInput,
    PriceForecastOutput,
)

_MODEL_VERSION = "1.0.0"

MODEL_ASSUMPTIONS = {
    "scope": "province_level",
    "granularity": "hourly",
    "intervals_per_day": 24,
    "forecast_horizon": "day_ahead",
    "spatial_resolution": "province",  # NOT nodal / NOT asset-level
    "target": "rt_price",
    "features": {
        "naive_da": ["da_price"],
        "ols_da_time_v1": ["intercept", "da_price", "sin_hour", "cos_hour"],
    },
    "training": {
        "method": "rolling_ols",
        "artifact": None,           # no pretrained artifact — fit fresh each call
        "default_lookback_days": 60,
        "default_min_train_days": 7,
        "fallback": "naive_da",     # used when < min_train_days available
    },
    "deterministic": True,          # same inputs → same outputs, no randomness
    "confidence_intervals": False,
    "cross_day_leakage": False,     # only uses data strictly before target day
    "limitations": [
        "Province-level only — not nodal / not per-asset",
        "Hourly granularity only — not 15-min",
        "OLS features are DA price and hour-of-day only — no additional market signals",
        "Rolling OLS fitted fresh on each call — no persistent model artifact",
        "No confidence intervals in current implementation",
        "Falls back to naive_da when < min_train_days of training data available",
        "RT prices for target date must be absent (only DA prices used for prediction)",
    ],
}


def _run(
    hourly_prices: List[dict],
    target_date: str,
    model: str = "ols_rt_time_v1",
    min_train_days: int = 7,
    lookback_days: int = 60,
) -> Dict[str, Any]:
    """
    Forecast hourly RT prices for target_date using the selected model.

    RT-only models (naive_rt_lag1, naive_rt_lag7, ols_rt_time_v1) require only
    rt_price in hourly_prices — da_price is ignored.  DA-based models
    (naive_da, ols_da_time_v1) require da_price and are provided for
    markets that publish a day-ahead price (not Inner Mongolia Mengxi).

    Wraps services/bess_map/forecast_engine.build_forecast().
    """
    from services.bess_map.forecast_engine import RT_ONLY_MODELS, build_forecast

    # --- Input validation ---
    if not hourly_prices:
        raise ValueError("hourly_prices must not be empty")

    from services.bess_map.forecast_engine import SUPPORTED_MODELS
    model_key = model.lower().strip()
    if model_key not in SUPPORTED_MODELS:
        raise ValueError(
            f"model must be one of {SUPPORTED_MODELS}, got {model!r}"
        )

    try:
        target_day = datetime.date.fromisoformat(target_date)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"target_date must be an ISO date string (e.g. '2026-04-15'), got {target_date!r}"
        ) from exc

    if not (1 <= min_train_days):
        raise ValueError(f"min_train_days must be >= 1, got {min_train_days}")
    if not (1 <= lookback_days):
        raise ValueError(f"lookback_days must be >= 1, got {lookback_days}")

    # --- Build pd.DataFrame from JSON list ---
    # da_price is optional for RT-only models
    is_rt_only = model_key in RT_ONLY_MODELS
    try:
        rows = []
        for rec in hourly_prices:
            ts = pd.Timestamp(rec["datetime"])
            rt = rec.get("rt_price")
            da_raw = rec.get("da_price")
            da = float(da_raw) if da_raw is not None else float("nan")
            rows.append({
                "datetime": ts,
                "rt_price": float(rt) if rt is not None else float("nan"),
                "da_price": da,
            })
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(
            "hourly_prices must be a list of {datetime[, rt_price][, da_price]} dicts. "
            f"Parse error: {exc}"
        ) from exc

    df = (
        pd.DataFrame(rows)
        .set_index("datetime")
        .sort_index()
    )
    df.index = pd.DatetimeIndex(df.index)

    # --- Verify target_date rows exist ---
    target_mask = df.index.date == target_day
    target_df = df[target_mask]
    if target_df.empty:
        raise ValueError(
            f"target_date {target_date!r} has no rows in hourly_prices"
        )

    # DA-based models additionally require valid da_price on the target date
    if not is_rt_only and target_df["da_price"].isna().any():
        raise ValueError(
            f"target_date {target_date!r} has NaN da_price values — "
            f"model {model_key!r} requires valid da_price for all target hours"
        )

    # --- Run forecast ---
    rt_pred_series = build_forecast(
        df,
        model=model_key,
        min_train_days=min_train_days,
        lookback_days=lookback_days,
    )

    # --- Filter to target_date only ---
    target_pred = rt_pred_series[rt_pred_series.index.date == target_day].sort_index()

    if target_pred.empty:
        raise RuntimeError(
            f"Forecast produced no output for target_date {target_date!r}. "
            "Check that target_date rows are present in hourly_prices."
        )

    # Infer which model was actually used
    train_before_target = df.loc[df.index < pd.Timestamp(target_day)]
    if is_rt_only:
        train_days_available = (
            train_before_target.dropna(subset=["rt_price"]).index.normalize().nunique()
        )
        actual_model_used = (
            model_key if train_days_available >= min_train_days else "naive_rt_lag1"
        )
    else:
        train_days_available = (
            train_before_target.dropna(subset=["rt_price", "da_price"]).index.normalize().nunique()
        )
        actual_model_used = (
            "ols" if model_key == "ols_da_time_v1" and train_days_available >= min_train_days
            else "naive_da"
        )

    output = PriceForecastOutput(
        target_date=target_date,
        model=model_key,
        datetimes=[ts.isoformat() for ts in target_pred.index],
        rt_pred=[float(v) for v in target_pred.values],
        model_used=actual_model_used,
    )
    return dataclasses.asdict(output)


_SPEC = ModelSpec(
    name="price_forecast_dayahead",
    version=_MODEL_VERSION,
    description=(
        "Province-level day-ahead hourly RT price forecast. "
        "Wraps forecast_engine.build_forecast(). "
        "Models: naive_da (RT=DA) and ols_da_time_v1 (rolling OLS with DA price + hour features). "
        "Input: window of historical hourly RT+DA prices + target date. "
        "Output: 24 hourly RT predictions. "
        "Province-level only — not nodal/asset. Hourly only — not 15-min."
    ),
    input_schema=PriceForecastInput,
    output_schema=PriceForecastOutput,
    run_fn=_run,
    tags=["forecast", "price", "dayahead", "rt_price", "province", "hourly", "ols"],
    metadata={
        # Standard metadata contract keys
        "category": "forecast",
        "scope": "province_level",
        "market": None,
        "asset_type": "bess",
        "granularity": "hourly",
        "horizon": "day_ahead",
        "deterministic": True,
        "model_family": "ols",
        "source_of_truth_module": "services/bess_map/forecast_engine.py",
        "source_of_truth_functions": [
            "build_forecast",
            "forecast_ols_da_time_v1",
            "forecast_naive_da",
        ],
        "assumptions": MODEL_ASSUMPTIONS,
        "limitations": MODEL_ASSUMPTIONS["limitations"],
        "fallback_behavior": (
            "Falls back to naive_da (RT=DA) when fewer than min_train_days "
            "of complete training data is available before target_date"
        ),
        "status": "production",
        "owner": "bess-platform",

        # Domain-specific extras
        "production_pipeline": "services/bess_map/run_capture_pipeline.py",
        "spatial_resolution": "province",
        "intervals_per_day": 24,
        "forecast_horizon": "day_ahead",
        "target": "rt_price",
        "available_models": ["naive_da", "ols_da_time_v1"],
        "default_model": "ols_da_time_v1",
        "artifact_required": False,
        "confidence_intervals": False,
    },
)

registry.register(_SPEC)
