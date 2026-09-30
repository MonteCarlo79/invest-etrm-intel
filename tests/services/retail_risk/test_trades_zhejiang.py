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
