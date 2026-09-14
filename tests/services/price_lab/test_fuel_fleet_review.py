# tests/services/price_lab/test_fuel_fleet_review.py
from unittest.mock import patch, MagicMock
import pandas as pd
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "price_forecast_tab",
    Path(__file__).resolve().parents[3] / "apps" / "bess-map" / "price_forecast_tab.py")
pft = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pft)


def test_confirm_sets_status():
    eng = MagicMock()
    pft.confirm_fuel_fleet(eng, 42)
    sql = str(eng.execute.call_args.args[0] if eng.execute.called else
              eng.begin.return_value.__enter__.return_value.execute.call_args.args[0])
    assert "confirmed" in sql and "province_fuel_fleet" in sql
