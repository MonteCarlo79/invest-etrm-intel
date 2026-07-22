"""Tests for libs/risk/greeks.py"""
import pytest
from libs.risk.greeks import compute_book_greeks


def test_book_delta_long():
    """Book delta = sum of position deltas (buy = +, sell = -)."""
    positions = [
        {"direction": "buy", "volume_mwh": 100.0, "status": "open"},
        {"direction": "sell", "volume_mwh": 30.0, "status": "open"},
        {"direction": "buy", "volume_mwh": 50.0, "status": "closed"},
    ]
    result = compute_book_greeks(positions)
    assert result["delta_mwh"] == pytest.approx(70.0)
    assert result["gamma"] == 0.0
    assert result["vega"] == 0.0


def test_book_delta_net_short():
    """Net short book has negative delta."""
    positions = [
        {"direction": "sell", "volume_mwh": 200.0, "status": "open"},
        {"direction": "buy", "volume_mwh": 50.0, "status": "open"},
    ]
    result = compute_book_greeks(positions)
    assert result["delta_mwh"] == pytest.approx(-150.0)
