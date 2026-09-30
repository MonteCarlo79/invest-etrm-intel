# tests/services/retail_risk/test_mtm.py
import datetime
import pandas as pd
import pytest
from services.retail_risk import mtm as mm


def _contracts():
    return pd.DataFrame([
        # fixed 380 vs fwd 400, 100 MWh remaining in Oct-Dec
        {"contract_type": "fixed", "price_cny_mwh": 380.0,
         "monthly_forecast": '{"10": 30.0, "11": 30.0, "12": 40.0}',
         "annual_forecast_mwh": 1200.0},
        # indexed 联动+上浮 6 元/MWh uplift, 100 MWh remaining
        {"contract_type": "indexed", "price_cny_mwh": 6.0,
         "monthly_forecast": '{"10": 30.0, "11": 30.0, "12": 40.0}',
         "annual_forecast_mwh": 1200.0},
    ])


def test_remaining_volume_from_monthly_json():
    vol = mm.remaining_volume(_contracts().iloc[0], as_of=datetime.date(2026, 9, 30))
    assert vol == pytest.approx(100.0)


def test_retail_mtm_fixed_vs_indexed():
    out = mm.retail_contract_mtm(_contracts(), forward_price=400.0,
                                 as_of=datetime.date(2026, 9, 30))
    fixed = out[out.contract_type == "fixed"].iloc[0]
    assert fixed["mtm_cny"] == pytest.approx((380.0 - 400.0) * 100.0)   # -2000
    indexed = out[out.contract_type == "indexed"].iloc[0]
    assert indexed["mtm_cny"] == pytest.approx(6.0 * 100.0)             # +600 service margin


def test_procurement_mtm_delegates():
    positions = [{"direction": "buy", "volume_mwh": 50.0, "price_cny_mwh": 350.0,
                  "province": "山东", "start_date": "2026-10-01", "end_date": "2026-10-31"}]
    res = mm.procurement_mtm(positions, {"山东": 400.0})
    assert res[0]["unrealized_pnl_cny"] == pytest.approx((400.0 - 350.0) * 50.0)
