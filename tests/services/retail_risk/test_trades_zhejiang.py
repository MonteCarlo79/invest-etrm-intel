# tests/services/retail_risk/test_trades_zhejiang.py
from pathlib import Path
import pandas as pd
from services.retail_risk.parsers.trades_zhejiang import (
    parse_zhejiang_trades, parse_zhejiang_load,
)


def _make_csv(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([{
        "start_time": "2026/4/1 0:00", "end_time": "2026/4/1 1:00",
        "volume_pre": 42.2, "volume_rt": 38.7,
        "position_yr_sb_volume": 2.44, "position_yr_sb_price": 344.86,
        "position_yr_jj_volume": 8.41, "position_yr_jj_price": 344.842,
        "position_month_gp_volume": 1.5, "position_month_gp_price": 350.0,
        "position_green_total_volume": 0.5, "position_green_total_price": 360.0,
        "position_total_volume": 12.85, "position_total_price": 344.9,
    }])
    for c in ["position_yr_gp_volume","position_yr_gp_price","position_month_sb_volume",
              "position_month_sb_price","position_month_jj_volume","position_month_jj_price",
              "position_10days_jj_volume","position_10days_jj_price",
              "position_cm_volume","position_cm_price"]:
        df[c] = float("nan")
    df.to_csv(path, index=False)


def test_channels_melted(tmp_path):
    f = tmp_path / "浙江" / "交易记录" / "景融浙江持仓_test.csv"
    _make_csv(f)
    out = parse_zhejiang_trades(tmp_path)
    # annual rows: yr_sb (bilateral) and yr_jj (forward) are separate rows
    # (green_total also lands in annual per D9, marked by counterparty='绿电')
    annual = out[(out.channel == "annual") & (out.counterparty.isna())]
    assert set(annual.instrument_type) >= {"bilateral", "forward"}
    assert sorted(annual.volume_mwh) == [2.44, 8.41]
    listed = out[out.channel == "monthly_listed"].iloc[0]
    assert listed.volume_mwh == 1.5 and listed.price_cny_mwh == 350.0
    green = out[out.counterparty == "绿电"].iloc[0]
    assert green.volume_mwh == 0.5
    assert set(out.direction) == {"buy"}


def test_load_frame(tmp_path):
    f = tmp_path / "浙江" / "交易记录" / "景融浙江持仓_test.csv"
    _make_csv(f)
    load = parse_zhejiang_load(tmp_path)
    row = load.iloc[0]
    assert row.nominated_mwh == 42.2 and row.settled_mwh == 38.7 and row.hour == 0


# --- Jan–Mar: 浙江日滚动撮合汇总YYYYMM.xlsx (real layout: 分日持仓量 + 年度 grids) ---

def _make_summary_xlsx(path: Path, side, daily, grid_hour0=None):
    """side: [(label, vol, price)]; daily: [(date, cm_vol, cm_price)];
    grid_hour0: per-day hour-0 volume for the 2月年度双边 grid."""
    path.parent.mkdir(parents=True, exist_ok=True)
    hdr = [None] * 3 + ["日期", "预估比例", "预估用电", "中长期原持仓", "撮合成交量", "日持仓量",
                        "原持仓价格", "撮合均价", "中长期持仓比", "撮合后日均价", "日结算均价",
                        "距离90%差值", None, None, "量", "价"]
    rows = [hdr]
    n = max(len(side), len(daily)) + 1
    for i in range(n):
        r = [None] * 19
        if i < len(daily):
            d, v, p = daily[i]
            r[3], r[7], r[10] = pd.Timestamp(d), v, p
        if i < len(side):
            r[16], r[17], r[18] = side[i]
        rows.append(r)
    rows.append([None] * 6 + [999.0, 999.0] + [None] * 11)        # totals row (no date)
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        pd.DataFrame(rows).to_excel(w, sheet_name="202602分日持仓量", index=False, header=False)
        if grid_hour0 is not None:
            g = [["日期", "性质", "星期", "日系数", 28.0] + [f"{h}时" for h in range(1, 25)]]
            for day in pd.date_range("2026-02-01", "2026-02-28"):
                g.append([day, "工作日", 1, 1, grid_hour0] + [grid_hour0] + [0.0] * 23)
            g.append([None, None, None, 28, None] + [None] * 24)
            pd.DataFrame(g).to_excel(w, sheet_name="2月年度双边", index=False, header=False)


def _summary_path(root: Path, ym: str) -> Path:
    return root / "浙江" / "交易记录" / "滚动撮合" / f"浙江日滚动撮合汇总{ym}.xlsx"


def test_summary_side_table_channels(tmp_path):
    _make_summary_xlsx(
        _summary_path(tmp_path, "202602"),
        side=[("年度竞价", 0, 344.855), ("年度双边", 28.0, 344.86), ("中旬", 24.0, 350.0),
              ("2月月度黑龙江", 67.2, 322.6), ("滚撮", -1.0, 337.9), ("中旬总报量", 999, None)],
        daily=[], grid_hour0=1.0)
    out = parse_zhejiang_trades(tmp_path)
    bil = out[(out.channel == "annual") & (out.instrument_type == "bilateral")]
    assert abs(bil.volume_mwh.sum() - 28.0) < 1e-9 and set(bil.hour) == {0}   # grid shape
    assert set(bil.price_cny_mwh) == {344.86}
    mid = out[out.source_term == "中旬"]
    assert mid.channel.iloc[0] == "intramonth_match"
    assert {d.day for d in mid.delivery_date} == set(range(11, 21))      # 中旬 = days 11-20
    assert abs(mid.volume_mwh.sum() - 24.0) < 1e-9
    hlj = out[out.counterparty == "2月月度黑龙江"]
    assert hlj.channel.iloc[0] == "monthly_auction" and abs(hlj.volume_mwh.sum() - 67.2) < 1e-9
    assert hlj.delivery_date.nunique() == 28
    assert "中旬总报量" not in set(out.source_term) and (out.volume_mwh > 0).all()


def test_summary_daily_rolling_signed(tmp_path):
    _make_summary_xlsx(_summary_path(tmp_path, "202602"), side=[("滚撮", -24.0, 337.9)],
                       daily=[("2026-02-02", 24.0, 360.0), ("2026-02-27", -48.0, 366.0),
                              ("2026-02-03", 0, 0)])
    out = parse_zhejiang_trades(tmp_path)
    cm = out[out.source_term == "滚撮"]
    buy = cm[cm.direction == "buy"]
    sell = cm[cm.direction == "sell"]
    assert len(buy) == 24 and abs(buy.volume_mwh.sum() - 24.0) < 1e-9
    assert set(buy.price_cny_mwh) == {360.0}
    assert len(sell) == 24 and abs(sell.volume_mwh.sum() - 48.0) < 1e-9
    assert {str(d) for d in cm.delivery_date} == {"2026-02-02", "2026-02-27"}


def test_summary_skipped_for_months_covered_by_csv(tmp_path):
    _make_csv(tmp_path / "浙江" / "交易记录" / "景融浙江持仓_test.csv")       # April
    _make_summary_xlsx(_summary_path(tmp_path, "202604").with_name("浙江日滚动撮合汇总202604.xlsx"),
                       side=[("年度双边", 30.0, 344.86)], daily=[])
    out = parse_zhejiang_trades(tmp_path)
    assert out.source_file.str.endswith(".csv").all()
