# tests/services/retail_risk/test_trades_jinan.py
import datetime
from pathlib import Path
import pandas as pd
import pytest
from services.retail_risk.parsers.trades_jinan import parse_jinan_trades


def _make_result_xlsx(path: Path, rows):
    """Tiny 我的交易结果 sheet: [分时段类型, 交易单元, 买卖方向, 成交电量, 成交均价]."""
    df = pd.DataFrame(rows, columns=["分时段类型", "交易单元", "买卖方向", "成交电量", "成交均价"])
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="我的交易结果", index=False)


def test_monthly_auction_expands_per_day(tmp_path):
    d = tmp_path / "冀南" / "中长期交易结果" / "2026年3月"
    d.mkdir(parents=True)
    _make_result_xlsx(d / "2026年3月月度竞价.xlsx",
                      [["00:00-01:00", "景融绿色能源售电", "买入", 1.0, 450.0],
                       ["01:00-02:00", "景融绿色能源售电", "买入", 2.0, 419.9]])
    out = parse_jinan_trades(tmp_path)
    mar = out[out.channel == "monthly_auction"]
    assert set(mar.hour) == {0, 1}
    assert mar.delivery_date.nunique() == 31          # per-day across delivery month
    assert set(mar.direction) == {"buy"}
    day0 = mar[(mar.delivery_date == datetime.date(2026, 3, 1)) & (mar.hour == 0)]
    assert day0.volume_mwh.iloc[0] == 1.0 and day0.price_cny_mwh.iloc[0] == 450.0


def test_daily_rolling_uses_filename_date(tmp_path):
    d = tmp_path / "冀南" / "中长期交易结果" / "2026年3月"
    d.mkdir(parents=True)
    _make_result_xlsx(d / "2026年3月25日日滚动交易(2026-03-28).xlsx",
                      [["00:00-01:00", "景融绿色能源售电", "卖出", 5.0, 380.0]])
    out = parse_jinan_trades(tmp_path)
    row = out.iloc[0]
    assert row.channel == "intramonth_match" and row.direction == "sell"
    assert str(row.delivery_date) == "2026-03-28"
