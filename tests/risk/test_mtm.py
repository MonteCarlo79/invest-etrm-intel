"""Tests for libs/risk/mtm.py"""
import pytest
import pandas as pd
from libs.risk.mtm import compute_mtm, get_forward_price


def test_get_forward_price():
    """Forward price lookup returns latest curve price for province+date."""
    curves = pd.DataFrame({
        "province": ["inner_mongolia_mengxi", "inner_mongolia_mengxi"],
        "delivery_date": [pd.Timestamp("2026-08-01"), pd.Timestamp("2026-08-02")],
        "delivery_hour": [10, 10],
        "price_cny_kwh": [0.45, 0.46],
        "curve_date": [pd.Timestamp("2026-07-20"), pd.Timestamp("2026-07-20")],
    })
    price = get_forward_price(curves, "inner_mongolia_mengxi", pd.Timestamp("2026-08-01"), 10)
    assert price == pytest.approx(450.0)


def test_compute_mtm_buy_position():
    """MtM for a buy position: (forward - entry) * volume."""
    positions = [
        {
            "direction": "buy",
            "volume_mwh": 100.0,
            "price_cny_mwh": 400.0,
            "province": "inner_mongolia_mengxi",
            "start_date": pd.Timestamp("2026-08-01"),
            "end_date": pd.Timestamp("2026-08-31"),
        }
    ]
    forward_prices = {"inner_mongolia_mengxi": 450.0}
    result = compute_mtm(positions, forward_prices)
    assert result[0]["unrealized_pnl_cny"] == pytest.approx(5000.0)


def test_compute_mtm_sell_position():
    """MtM for a sell position: (entry - forward) * volume."""
    positions = [
        {
            "direction": "sell",
            "volume_mwh": 50.0,
            "price_cny_mwh": 420.0,
            "province": "inner_mongolia_mengxi",
            "start_date": pd.Timestamp("2026-08-01"),
            "end_date": pd.Timestamp("2026-08-31"),
        }
    ]
    forward_prices = {"inner_mongolia_mengxi": 450.0}
    result = compute_mtm(positions, forward_prices)
    assert result[0]["unrealized_pnl_cny"] == pytest.approx(-1500.0)
