# tests/services/price_lab/test_explorer_logic.py
import numpy as np
import pandas as pd
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "price_forecast_tab",
    Path(__file__).resolve().parents[3] / "apps" / "bess-map" / "price_forecast_tab.py")
pft = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pft)


def test_daily_stack_series_applies_markup():
    stack = [{"label": "c1", "vc_yuan_mwh": 250.0, "capacity_mw": 10000, "is_import": False}]
    demand = pd.Series([5000.0, 9500.0], index=pd.date_range("2026-09-01", periods=2, freq="h"))
    curve = [(0.0, 1.0), (1.0, 2.0)]
    out = pft.compute_daily_stack_series(stack, demand, curve, total_capacity_mw=10000)
    # DEVIATION from brief: expectations follow the COMMITTED apply_markup
    # (piecewise-linear between curve points → markup 1+t on this curve,
    # locked in by test_merit_order.py::test_apply_markup_interpolates).
    # The brief's 2t values (250.0 / 475.0) are unpassable against it.
    assert out.iloc[0] == 250.0 * 1.5              # tightness 0.5 → markup 1.5
    assert abs(out.iloc[1] - 250.0 * 1.95) < 1e-6  # tightness 0.95 → markup ≈1.95
