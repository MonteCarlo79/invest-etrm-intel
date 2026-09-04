"""Tests for libs/risk/pnl.py"""
import pytest
import pandas as pd
from libs.risk.pnl import compute_pnl_waterfall


def test_bess_pnl_waterfall():
    """BESS P&L waterfall decomposes into discharge, charge, fees."""
    settlement_items = pd.DataFrame({
        "category": ["discharge_energy", "charge_energy", "capacity_compensation",
                     "transmission", "system_operation"],
        "amount_cny": [50000.0, -30000.0, 8000.0, -2000.0, -1500.0],
    })
    result = compute_pnl_waterfall(settlement_items, asset_type="bess")
    assert result["discharge_energy"] == pytest.approx(50000.0)
    assert result["charge_energy"] == pytest.approx(-30000.0)
    assert result["capacity_compensation"] == pytest.approx(8000.0)
    assert result["net_pnl"] == pytest.approx(24500.0)


def test_wind_pnl_waterfall():
    """Wind P&L includes generation revenue and curtailment."""
    settlement_items = pd.DataFrame({
        "category": ["generation_revenue", "curtailment", "transmission", "subsidy"],
        "amount_cny": [80000.0, -15000.0, -3000.0, 5000.0],
    })
    result = compute_pnl_waterfall(settlement_items, asset_type="wind")
    assert result["generation_revenue"] == pytest.approx(80000.0)
    assert result["curtailment"] == pytest.approx(-15000.0)
    assert result["net_pnl"] == pytest.approx(67000.0)
