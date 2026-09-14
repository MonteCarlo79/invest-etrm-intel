import numpy as np
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "price_forecast_tab",
    Path(__file__).resolve().parents[3] / "apps" / "bess-map" / "price_forecast_tab.py")
pft = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pft)


def test_smape_definition():
    a = np.array([100.0, 200.0])
    p = np.array([110.0, 190.0])
    v = pft.smape(a, p)
    expect = np.mean([10/105, 10/195]) * 100
    assert abs(v - expect) < 1e-9


def test_smape_skips_zero_pairs():
    a = np.array([0.0, 200.0])
    p = np.array([0.0, 210.0])
    v = pft.smape(a, p)
    assert abs(v - (10/205*100)) < 1e-9
