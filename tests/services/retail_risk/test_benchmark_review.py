# tests/services/retail_risk/test_benchmark_review.py
"""Benchmarks from exchange disclosures and 复盘 decks — fixtures mirror real layouts."""
import datetime
from pathlib import Path

import pandas as pd
import pytest
from pptx import Presentation
from pptx.util import Inches

from services.retail_risk import schemas
from services.retail_risk.parsers import benchmark_review as br

SD_TABLE2 = """四、交易组织情况
表2：中长期交易情况
单位：亿千瓦时、元/兆瓦时
（一）中长期累计交易情况
交易品种 月度累计交易电量 加权平均电价
年度双边协商交易 - -
年度集中竞价交易 - -
月度集中竞价交易 26.13 302.96
月内集中竞价交易 9.49 327.28
月度双边协商交易 26.50 303.80
挂牌交易 106.74 298.07
连续撮合交易 1.56 299.40
（二）中长期交易历史净合约情况
日期 双边协商交易 集中竞价交易
03月01日 0.908 304.31 0.962 308.01
"""


def test_shandong_table2_rows():
    out = br.parse_shandong_table2(SD_TABLE2, datetime.date(2026, 3, 1), "f.pdf")
    assert list(out.columns) == schemas.BENCH_COLS
    got = dict(zip(out.channel, zip(out.avg_price_cny_mwh, out.volume_mwh)))
    assert set(got) == {"月度集中竞价交易", "月内集中竞价交易", "月度双边协商交易",
                        "挂牌交易", "滚动撮合交易"}                   # '-' rows skipped; 连续→滚动
    assert got["月度集中竞价交易"][0] == 302.96
    assert got["月度集中竞价交易"][1] == pytest.approx(26.13 * 100_000)   # 亿kWh -> MWh
    assert set(out.source) == {"disclosure"} and set(out.province) == {"山东"}
    assert set(out.month) == {"2026-03-01"}


def _deck(path: Path, slides):
    """slides: [(title, rows)] -> pptx with a text box + table per slide."""
    prs = Presentation()
    for title, rows in slides:
        s = prs.slides.add_slide(prs.slide_layouts[6])
        s.shapes.add_textbox(Inches(0.2), Inches(0.1), Inches(5), Inches(0.5)).text_frame.text = title
        t = s.shapes.add_table(len(rows), len(rows[0]), Inches(0.2), Inches(0.8),
                               Inches(9), Inches(5)).table
        for i, r in enumerate(rows):
            for j, v in enumerate(r):
                t.cell(i, j).text = str(v)
    path.parent.mkdir(parents=True, exist_ok=True)
    prs.save(path)


def test_zhejiang_review_monthly_table(tmp_path):
    rows = [["月份", "年度长协", "月度竞价", "月度双边", "上旬竞价", "中旬竞价", "下旬竞价", "日前均价", "实时均价"],
            ["1月（25）", "412.75", "412.39", "412.49", "-", "-", "-", "262.37", "286.72"],
            ["1月", "344.850", "344.850", "344.850", "344.850", "344.850", "351.100", "337.21", "342.99"],
            ["2月", "", "359.156", "365.080", "365.080", "332.240", "365.080", "253.06", "231.27"]]
    f = tmp_path / "浙江2026年3月交易复盘.pptx"
    _deck(f, [("价格", rows)])
    out = br.parse_zhejiang_review(f)
    jan = out[out.month == "2026-01-01"].set_index("channel").avg_price_cny_mwh
    assert jan["年度长协"] == 344.85 and jan["下旬竞价"] == 351.1
    assert "日前均价" not in set(out.channel)                    # spot refs not stored
    prior = out[out.month == "2025-01-01"].set_index("channel").avg_price_cny_mwh
    assert prior["年度长协"] == 412.75                            # （25） rows land in 2025
    feb = out[out.month == "2026-02-01"]
    assert "年度长协" not in set(feb.channel)                     # blank cell skipped
    assert set(out.source) == {"review"} and set(out.province) == {"浙江"}


def _jinan_rows(hdr, mkt_annual):
    rows = [hdr]
    for h in range(1, 25):
        rows.append([h, mkt_annual(h), 400, 410, 380, 390, h, 999, 999, 999, 380, 390])
    return rows


def test_jinan_review_hourly_month_from_slide_title(tmp_path):
    hdr_feb = ["1月全市场", "全网年度合同电价", "全网月度合同电价", "全网月内合同电价", "现货均价",
               "用户传导均价", "1月景融", "售电公司年度合同电价", "售电公司年度月度电价",
               "售电公司月内年度电价", "现货均价", "用户传导均价"]
    f = tmp_path / "2026年1-2月冀南区域经营复盘.pptx"
    _deck(f, [("2月用户结算联动电价", _jinan_rows(hdr_feb, lambda h: 300 + h))])
    out = br.parse_jinan_review_hourly(f)
    assert set(out.month) == {"2026-02-01"}                      # title wins over header '1月'
    ann = out[out.channel == "年度合同"].set_index("hour").price
    assert ann[0] == 301 and ann[23] == 324                      # market block, hour = col - 1
    assert set(out.channel) == {"年度合同", "月度合同", "月内合同"}


def test_benchmark_from_hourly_weights_by_our_buys():
    hourly = pd.DataFrame([
        {"province": "冀南", "channel": "年度合同", "month": "2026-03-01", "hour": 0, "price": 400.0,
         "source_file": "d.pptx"},
        {"province": "冀南", "channel": "年度合同", "month": "2026-03-01", "hour": 12, "price": 200.0,
         "source_file": "d.pptx"}])
    d = datetime.date(2026, 3, 1)
    trades = pd.DataFrame([
        [d, 0, "annual", "bilateral", "buy", 30.0, 410.0, None, "年度双边", "f"],
        [d, 12, "annual", "bilateral", "buy", 10.0, 190.0, None, "年度双边", "f"],
        [d, 12, "annual", "bilateral", "sell", 99.0, 190.0, None, "年度双边", "f"],   # sells ignored
    ], columns=schemas.TRADES_COLS)
    out = br.benchmark_from_hourly(hourly, trades)
    row = out.iloc[0]
    assert row.channel == "年度合同" and row.source == "review_hourly"
    assert row.avg_price_cny_mwh == pytest.approx((30 * 400 + 10 * 200) / 40)   # 350, not flat 300


def test_benchmark_from_hourly_flat_mean_without_trades():
    hourly = pd.DataFrame([{"province": "冀南", "channel": "月内合同", "month": "2026-03-01",
                            "hour": h, "price": p, "source_file": "d"} for h, p in [(0, 300.0), (1, 500.0)]])
    out = br.benchmark_from_hourly(hourly, pd.DataFrame(columns=schemas.TRADES_COLS))
    assert out.iloc[0].avg_price_cny_mwh == pytest.approx(400.0)
