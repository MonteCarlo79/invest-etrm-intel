# tests/services/retail_risk/test_trades_shandong.py
from pathlib import Path
import pandas as pd
import pytest
from services.retail_risk.parsers.trades_shandong import (
    month_shapes_from_curves, parse_shandong_load, parse_shandong_trades,
)


def _make_detail(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([
        ["2026-03-01", 7061, 7546.9, 138.7, 0, 0, 0, 0, 2000, 535.7, 98.4],
        ["2026-03-02", 7061, 7662.2, 138.7, 0, 0, 0, 0, 2000, 535.7, 98.4],
    ], columns=["日期", "预估用电量", "实际用电量", "绿电", "年度双边", "年度竞价",
                "年度挂牌", "月度双边", "月度竞价", "月度挂牌1", "月度挂牌2"])
    px = df.copy()
    for c in px.columns[3:]:
        px[c] = 350.0
    px["月度竞价"] = 400.0
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="月前中长期持仓", index=False)
        px.to_excel(w, sheet_name="月前中长期电价", index=False)


def _shapes() -> pd.DataFrame:
    """Day-level flat shapes for 2026-03-01/02."""
    rows = []
    for d in ["2026-03-01", "2026-03-02"]:
        for h in range(24):
            rows.append({"delivery_date": pd.to_datetime(d).date(), "hour": h,
                         "share": 1 / 24})
    return pd.DataFrame(rows)


def test_daily_spread_conserves_volume(tmp_path):
    _make_detail(tmp_path / "山东" / "202603" / "景融" / "2026年3月持仓明细.xlsx")
    out = parse_shandong_trades(tmp_path, _shapes())
    ma = out[out.channel == "monthly_auction"]
    day1 = ma[ma.delivery_date.astype(str) == "2026-03-01"]
    assert day1.volume_mwh.sum() == pytest.approx(2000.0, rel=1e-3)   # conservation
    assert set(day1.hour) == set(range(24))
    assert day1.price_cny_mwh.iloc[0] == 400.0
    assert "est" in day1.source_term.iloc[0]                          # estimated flag


def test_green_counterparty(tmp_path):
    _make_detail(tmp_path / "山东" / "202603" / "景融" / "2026年3月持仓明细.xlsx")
    out = parse_shandong_trades(tmp_path, _shapes())
    green = out[out.counterparty == "绿电"]
    assert not green.empty and green.volume_mwh.sum() == pytest.approx(138.7 * 2, rel=1e-3)


def _make_curve(path: Path, hourly_mwh: list[float]):
    """Tiny 日用电曲线 file: 2 customers x (电量 + 占比) rows, 24 hour cols."""
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = ["序号", "用户名称", "类型"] + [f"{h:02d}:00" for h in range(1, 25)]
    half = [v / 2 for v in hourly_mwh]
    rows = [
        [1, "客户A", "电量(兆瓦时)"] + half,
        [2, "客户A", "占比(%)"] + [4.0] * 24,
        [3, "客户B", "电量(兆瓦时)"] + half,
        [4, "客户B", "占比(%)"] + [4.0] * 24,
    ]
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        pd.DataFrame(rows, columns=cols).to_excel(w, sheet_name="sheet", index=False)


def test_month_shapes_from_curves(tmp_path):
    _make_curve(tmp_path / "山东" / "202603" / "景融" / "日用电曲线" / "20260301.xlsx",
                [240.0] * 12 + [120.0] * 12)     # day shape: heavy first 12h
    _make_curve(tmp_path / "山东" / "202603" / "景融" / "日用电曲线" / "20260302.xlsx",
                [240.0] * 12 + [120.0] * 12)
    shapes = month_shapes_from_curves(tmp_path)
    march = shapes[shapes.month == 3]
    assert march.ratio.sum() == pytest.approx(1.0)
    assert len(march) == 24
    assert march[march.hour == 0].ratio.iloc[0] == pytest.approx(240 / 4320)
    assert march[march.hour == 13].ratio.iloc[0] == pytest.approx(120 / 4320)


def test_load_hourly_from_curves(tmp_path):
    _make_detail(tmp_path / "山东" / "202603" / "景融" / "2026年3月持仓明细.xlsx")
    _make_curve(tmp_path / "山东" / "202603" / "景融" / "日用电曲线" / "20260301.xlsx",
                [240.0] * 12 + [120.0] * 12)
    load = parse_shandong_load(tmp_path)
    day1 = load[load.delivery_date.astype(str) == "2026-03-01"]
    assert len(day1) == 24
    # settled = sum of customers' hourly 电量 = 240 MWh per hour (first 12h)
    assert day1[day1.hour == 0].settled_mwh.iloc[0] == pytest.approx(240.0)
    # nominated = daily 预估 7061 spread by the day's shape
    assert day1.nominated_mwh.sum() == pytest.approx(7061.0, rel=1e-3)
