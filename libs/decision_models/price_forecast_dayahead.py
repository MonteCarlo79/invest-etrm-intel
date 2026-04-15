"""
libs/decision_models/price_forecast_dayahead.py

Reusable model asset: day-ahead nodal price forecast.

Placeholder implementation — returns a simple historical-average forecast
until a trained model is wired in.  Self-registers on import.

Usage:
    import libs.decision_models.price_forecast_dayahead
    from libs.decision_models.runners.local import run

    result = run("price_forecast_dayahead", {
        "asset_code": "suyou",
        "forecast_date": date(2026, 4, 2),
        "feature_window_days": 30,
    })
"""
from __future__ import annotations

import dataclasses
from datetime import date
from typing import Any, Dict

from libs.decision_models.model_spec import ModelSpec
from libs.decision_models.registry import registry
from libs.decision_models.schemas.price_forecast_dayahead import (
    PriceForecastInput,
    PriceForecastOutput,
)

_MODEL_VERSION = "0.1.0-placeholder"


def _run(
    asset_code: str,
    forecast_date: date,
    feature_window_days: int = 30,
) -> Dict[str, Any]:
    """
    Placeholder: returns zeros until a real forecasting model is plugged in.

    To wire in a real model:
    1. Load a trained artefact (sklearn, lightgbm, etc.) from S3 or a local path.
    2. Pull historical price features from canon.nodal_rt_price_15min.
    3. Run inference and populate forecast_prices.
    """
    output = PriceForecastOutput(
        asset_code=asset_code,
        forecast_date=forecast_date,
        forecast_prices=[0.0] * 96,
        model_version_used=_MODEL_VERSION,
    )
    return dataclasses.asdict(output)


_SPEC = ModelSpec(
    name="price_forecast_dayahead",
    version=_MODEL_VERSION,
    description=(
        "Day-ahead 15-min nodal price forecast. "
        "Currently a placeholder returning zeros — replace run_fn with a trained model."
    ),
    input_schema=PriceForecastInput,
    output_schema=PriceForecastOutput,
    run_fn=_run,
    tags=["bess", "forecast", "price", "dayahead"],
    metadata={
        "asset_type": "bess",
        "status": "placeholder",
    },
)

registry.register(_SPEC)
