# tests/services/price_lab/test_pca_section_logic.py
import pandas as pd
import numpy as np
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "price_forecast_tab",
    Path(__file__).resolve().parents[3] / "apps" / "bess-map" / "price_forecast_tab.py")
pft = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pft)

from services.bess_map.price_lab.pca_shapes import FEATURE_COLUMNS


def test_feature_frame_columns_and_calendar():
    idx = pd.date_range("2026-09-01", periods=48, freq="h")
    fund = pd.DataFrame({"load_d1_mw": 20000.0, "renewable_total_d1_mw": 5000.0,
                         "bidding_space_d1_mw": 15000.0, "wind_d1_mw": 3000.0,
                         "solar_d1_mw": 2000.0, "net_export_d1_mw": -1000.0}, index=idx)
    share = pft.compute_net_import_share(fund)
    ff = pft.build_feature_frame(fund, share, landing_price=300.0)
    assert list(ff.columns) == FEATURE_COLUMNS
    assert ff["dow"].iloc[0] == idx[0].dayofweek
    assert ff["month"].iloc[0] == 9
    assert (ff["landing_price"] == 300.0).all()


def test_feature_frame_none_landing_fills_zero():
    idx = pd.date_range("2026-09-01", periods=24, freq="h")
    fund = pd.DataFrame({"load_d1_mw": 20000.0, "renewable_total_d1_mw": 5000.0,
                         "bidding_space_d1_mw": 15000.0, "wind_d1_mw": 3000.0,
                         "solar_d1_mw": 2000.0, "net_export_d1_mw": -1000.0}, index=idx)
    ff = pft.build_feature_frame(fund, pft.compute_net_import_share(fund), None)
    assert (ff["landing_price"] == 0.0).all()
