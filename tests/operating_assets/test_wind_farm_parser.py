"""Tests for wind farm parser (零碳46风电经营统计)."""
import pytest
import pandas as pd
from services.operating_assets.parsers.wind_farm import (
    aggregate_15min_to_hourly,
    parse_settlement_detail_row,
)


def test_aggregate_15min_to_hourly():
    """4 x 15-min intervals aggregated to 1 hourly row."""
    rows = [
        {"da_volume": 2.0, "da_price": 400.0, "rt_volume": 1.0, "rt_price": 350.0},
        {"da_volume": 2.5, "da_price": 410.0, "rt_volume": 0.5, "rt_price": 360.0},
        {"da_volume": 3.0, "da_price": 420.0, "rt_volume": 0.0, "rt_price": 0.0},
        {"da_volume": 2.5, "da_price": 390.0, "rt_volume": 1.5, "rt_price": 340.0},
    ]
    result = aggregate_15min_to_hourly(rows)
    assert result["da_volume_mwh"] == pytest.approx(10.0)
    assert result["rt_volume_mwh"] == pytest.approx(3.0)
    expected_da_price = (2*400 + 2.5*410 + 3*420 + 2.5*390) / 10.0
    assert result["da_price_cny_mwh"] == pytest.approx(expected_da_price)


def test_parse_settlement_detail_row():
    """Single 结算明细 row parsed to canonical dict."""
    row = pd.Series({
        "日期": "2025-04-01",
        "时间": "00:00",
        "省调电量": 8.5,
        "省级实时价格": 0.35,
        "省级实时节点价": 0.36,
        "省级日前价格": 0.40,
        "省级日前电量": 6.0,
        "省级月内撮合价格": 0.38,
        "省级月内撮合电量": 1.0,
        "市场合约价格": 0.42,
        "收益": 3200.0,
        "弃风量": -2.5,
    })
    result = parse_settlement_detail_row(row)
    assert result["settled_mwh"] == 8.5
    assert result["da_volume_mwh"] == 6.0
    assert result["da_price_cny_mwh"] == pytest.approx(400.0)
    assert result["deviation_grid_flow_mwh"] == -2.5
