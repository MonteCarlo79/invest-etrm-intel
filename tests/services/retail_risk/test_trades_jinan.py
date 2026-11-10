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


def test_daily_rolling_uses_filename_date(tmp_path):
    d = tmp_path / "冀南" / "中长期交易结果" / "2026年3月"
    d.mkdir(parents=True)
    _make_result_xlsx(d / "2026年3月25日日滚动交易(2026-03-28).xlsx",
                      [["00:00-01:00", "景融绿色能源售电", "卖出", 5.0, 380.0]])
    out = parse_jinan_trades(tmp_path)
    row = out.iloc[0]
    assert row.channel == "intramonth_match" and row.direction == "sell"
    assert str(row.delivery_date) == "2026-03-28"


def _make_counterparty_xlsx(path: Path, rows, buy_total=0):
    """Real 交易结果 layout: summary row, blank row, header on row 3."""
    header = ["购方名称", "售方名称", "时间段", "分时段编码", "成交电量", "成交电价",
              "购方市场成员名称", "售方市场成员名称", "备注"]
    data = [[f"购方电量合计:{buy_total}", None, f"售方电量合计:{buy_total}"] + [None] * 6,
            [None] * 9, header] + rows
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        pd.DataFrame(data).to_excel(w, sheet_name="交易结果", index=False, header=False)


def test_counterparty_format_annual_bilateral_spreads_period(tmp_path):
    """年度/年度双边/交易结果 (N).xlsx: channel from FOLDER, volume = period total
    per hour block, spread evenly over the days of 时间段; code 1 = hour 0."""
    d = tmp_path / "冀南" / "中长期交易结果" / "年度" / "年度双边"
    d.mkdir(parents=True)
    _make_counterparty_xlsx(d / "交易结果 (1).xlsx", [
        ["景融绿色能源售电", "茶山风电", "2026-02-01至2026-02-28", 1, 28.0, 348.3,
         "景融绿色能源科技有限公司", "蔚县新天风能有限公司", None]])
    out = parse_jinan_trades(tmp_path)
    assert set(out.channel) == {"annual"} and set(out.instrument_type) == {"bilateral"}
    assert out.delivery_date.nunique() == 28
    assert set(out.hour) == {0}
    assert out.volume_mwh.sum() == pytest.approx(28.0)          # conserved, not x28
    assert out.iloc[0].counterparty == "茶山风电"
    assert out.iloc[0].price_cny_mwh == 348.3


def test_counterparty_format_sell_side_and_xun_range(tmp_path):
    """景融 as 售方 => sell; 旬 auctions use the explicit 时间段 range."""
    d = tmp_path / "冀南" / "中长期交易结果" / "2026年6月"
    d.mkdir(parents=True)
    _make_counterparty_xlsx(d / "2026年6月上旬竞价.xlsx", [
        ["某用户售电", "景融绿色能源售电", "2026-06-01至2026-06-10", 19, 50.0, 300.0,
         "某售电公司", "景融绿色能源科技有限公司", None]])
    out = parse_jinan_trades(tmp_path)
    assert set(out.direction) == {"sell"}
    assert set(out.channel) == {"intramonth_match"}
    assert out.delivery_date.nunique() == 10
    assert set(out.hour) == {18}
    assert out.volume_mwh.sum() == pytest.approx(50.0)


def test_counterparty_format_monthly_auction(tmp_path):
    d = tmp_path / "冀南" / "中长期交易结果" / "2026年3月"
    d.mkdir(parents=True)
    _make_counterparty_xlsx(d / "2026年3月月度竞价2.xlsx", [
        ["景融绿色能源售电", "诚峰热电", "2026-03-01至2026-03-31", 21, 31.0, 494.46,
         "景融绿色能源科技有限公司", "石家庄诚峰", None]])
    out = parse_jinan_trades(tmp_path)
    assert set(out.channel) == {"monthly_auction"} and set(out.direction) == {"buy"}
    assert out.volume_mwh.sum() == pytest.approx(31.0)
