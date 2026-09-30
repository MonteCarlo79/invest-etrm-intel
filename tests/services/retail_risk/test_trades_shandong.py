# tests/services/retail_risk/test_trades_shandong.py
from pathlib import Path
import pandas as pd
import pytest
from services.retail_risk.parsers.trades_shandong import parse_shandong_trades


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


def _ratios() -> pd.DataFrame:
    return pd.DataFrame({"month": [3] * 24, "hour": list(range(24)),
                         "ratio": [1 / 24] * 24})


def test_daily_spread_conserves_volume(tmp_path):
    _make_detail(tmp_path / "山东" / "202603" / "景融" / "2026年3月持仓明细.xlsx")
    out = parse_shandong_trades(tmp_path, _ratios())
    ma = out[out.channel == "monthly_auction"]
    day1 = ma[ma.delivery_date.astype(str) == "2026-03-01"]
    assert day1.volume_mwh.sum() == pytest.approx(2000.0, rel=1e-3)   # conservation
    assert set(day1.hour) == set(range(24))
    assert day1.price_cny_mwh.iloc[0] == 400.0
    assert "est" in day1.source_term.iloc[0]                          # estimated flag


def test_green_counterparty(tmp_path):
    _make_detail(tmp_path / "山东" / "202603" / "景融" / "2026年3月持仓明细.xlsx")
    out = parse_shandong_trades(tmp_path, _ratios())
    green = out[out.counterparty == "绿电"]
    assert not green.empty and green.volume_mwh.sum() == pytest.approx(138.7 * 2, rel=1e-3)
