"""Tests for libs/settlement/categorizer.py"""
import pytest
from libs.settlement.categorizer import categorize_items, mengxi_wind_settlement


def test_categorize_items_bess():
    """BESS items categorized by transaction type."""
    items = [
        {"category": "discharge_energy", "amount_cny": 5000, "volume_mwh": 10},
        {"category": "charge_energy", "amount_cny": -3000, "volume_mwh": 12},
    ]
    result = categorize_items(items, asset_type="bess", province="inner_mongolia_mengxi")
    assert result[0]["category"] == "discharge_energy"
    assert result[1]["category"] == "charge_energy"


def test_mengxi_wind_settlement_da_only():
    """When generation <= DA volume, all settled at DA price."""
    hourly = {
        "settled_mwh": 8.0,
        "da_volume_mwh": 10.0,
        "da_price_cny_mwh": 400.0,
        "rt_price_cny_mwh": 350.0,
        "annual_price_cny_mwh": 380.0,
    }
    result = mengxi_wind_settlement(hourly)
    assert result["da_settled_mwh"] == 8.0
    assert result["rt_settled_mwh"] == 0.0
    assert result["pnl_cny"] == pytest.approx(8.0 * 400.0)


def test_mengxi_wind_settlement_residual_at_rt():
    """When generation > DA volume, residual settled at RT node price."""
    hourly = {
        "settled_mwh": 12.0,
        "da_volume_mwh": 8.0,
        "da_price_cny_mwh": 400.0,
        "rt_price_cny_mwh": 350.0,
        "annual_price_cny_mwh": 380.0,
    }
    result = mengxi_wind_settlement(hourly)
    assert result["da_settled_mwh"] == 8.0
    assert result["rt_settled_mwh"] == 4.0
    expected_pnl = 8.0 * 400.0 + 4.0 * 350.0
    assert result["pnl_cny"] == pytest.approx(expected_pnl)


def test_mengxi_wind_settlement_bilateral_premium():
    """Bilateral contract premium applied on top."""
    hourly = {
        "settled_mwh": 10.0,
        "da_volume_mwh": 10.0,
        "da_price_cny_mwh": 400.0,
        "rt_price_cny_mwh": 350.0,
        "annual_price_cny_mwh": 420.0,
        "annual_volume_mwh": 5.0,
    }
    result = mengxi_wind_settlement(hourly)
    bilateral_premium = 5.0 * (420.0 - 400.0)
    assert result["bilateral_premium_cny"] == pytest.approx(bilateral_premium)
