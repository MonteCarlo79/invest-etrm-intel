"""Tests for libs/settlement/parser.py"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from libs.settlement.parser import detect_format, parse_trade_capture


def test_detect_format_trade_capture():
    """Trade Capture.xlsx detected by 'Trades' sheet with expected columns."""
    mock_xl = MagicMock()
    mock_xl.sheet_names = ["Trades", "Summary"]
    mock_xl.parse.return_value = pd.DataFrame(columns=[
        "Date", "Market", "Station Name", "Volume (MWh)", "Price (¥/MWh)", "Total (¥)"
    ])
    result = detect_format(mock_xl)
    assert result == "trade_capture"


def test_detect_format_capacity_compensation():
    """容量补偿数据.xlsx detected by column pattern."""
    mock_xl = MagicMock()
    mock_xl.sheet_names = ["Sheet1"]
    mock_xl.parse.return_value = pd.DataFrame(columns=[
        "电站", "月份", "应收", "实际结算", "差异"
    ])
    result = detect_format(mock_xl)
    assert result == "capacity_compensation"


def test_detect_format_wind_farm_ops():
    """零碳46风电经营统计 detected by sheet name signature."""
    mock_xl = MagicMock()
    mock_xl.sheet_names = ["风场功率", "预测&实际电量", "结算明细", "市场价格", "经营统计", "Other1", "Other2"]
    result = detect_format(mock_xl)
    assert result == "wind_farm_ops"


def test_parse_trade_capture():
    """Trade Capture Trades sheet parsed to canonical settlement items."""
    df = pd.DataFrame({
        "Date": ["2026-01-15", "2026-01-15"],
        "Market": ["DA", "DA"],
        "Station Name": ["裕昭沙子坝", "裕昭沙子坝"],
        "Buy/Sell": ["Sell", "Buy"],
        "Transactions Type": ["discharge", "charge"],
        "Volume (MWh)": [10.5, 8.2],
        "Price (¥/MWh)": [450.0, 280.0],
        "Total (¥)": [4725.0, -2296.0],
    })
    items = parse_trade_capture(df)
    assert len(items) == 2
    assert items[0]["category"] == "discharge_energy"
    assert items[0]["volume_mwh"] == 10.5
    assert items[0]["amount_cny"] == 4725.0
    assert items[1]["category"] == "charge_energy"
