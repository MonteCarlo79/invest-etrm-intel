# tests/services/price_lab/test_tab_loaders.py
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "apps", "bess-map"))

from unittest.mock import patch
import pandas as pd
import price_forecast_tab as pft


def test_load_confirmed_fuel_fleet_returns_latest():
    df = pd.DataFrame({
        "coal_price_yuan_t": [850.0], "gas_price_yuan_m3": [3.1],
        "fleet_segments": [[{"fuel": "coal", "capacity_mw": 40000,
                             "heat_rate_kj_kwh": 8200, "vom_yuan_mwh": 12, "label": "c1"}]],
        "effective_date": [pd.Timestamp("2026-01-01").date()],
    })
    with patch("pandas.read_sql", return_value=df):
        out = pft.load_confirmed_fuel_fleet(None, "山东")
    assert out["coal_price_yuan_t"] == 850.0
    assert out["fleet_segments"][0]["label"] == "c1"


def test_load_confirmed_fuel_fleet_none_when_empty():
    with patch("pandas.read_sql", return_value=pd.DataFrame()):
        assert pft.load_confirmed_fuel_fleet(None, "山东") is None


def test_import_blocks_convert_price_and_capacity():
    trades = pd.DataFrame({"channel_1": ["锦苏直流"], "land_price": [0.30], "month_start": [pd.Timestamp("2026-08-01")]})
    channels = pd.DataFrame({"name": ["锦苏直流"], "gw": [7.2]})
    def fake_read_sql(sql, eng, params=None):
        return trades if "trades" in str(sql) else channels
    with patch("pandas.read_sql", side_effect=fake_read_sql):
        blocks = pft.load_import_blocks(None, "江苏")
    assert blocks[0]["price_yuan_mwh"] == 300.0
    assert blocks[0]["capacity_mw"] == 7200.0
    assert blocks[0]["label"] == "锦苏直流"


def test_net_import_share_sign():
    fund = pd.DataFrame({"net_export_d1_mw": [-2000.0, 1000.0], "load_d1_mw": [20000.0, 20000.0]})
    share = pft.compute_net_import_share(fund)
    assert share.iloc[0] == 0.1 and share.iloc[1] == -0.05
