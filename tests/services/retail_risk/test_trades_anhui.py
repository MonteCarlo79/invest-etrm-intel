# tests/services/retail_risk/test_trades_anhui.py
from pathlib import Path
import pandas as pd
from services.retail_risk.parsers.trades_anhui import parse_anhui_trades


def _make_guncuo(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = ["申报日", "标的日", "交易类型", "平均"] + [f"{h}点" for h in range(1, 25)]
    vol = ["2026-02-27", "2026-03-01", "电量", None] + [100.0] * 24
    px = ["2026-02-27", "2026-03-01", "电价", 293.3] + [266.8] * 24
    mine_cols = ["交易日期", "标的日", "交易类型"] + [f"{h}点" for h in range(1, 25)]
    mine_vol = ["2026-02-27", "2026-03-01", "电量", 1.95, 18.0] + [None] * 22
    mine_px = ["2026-02-27", "2026-03-01", "电价", 246.15, 230.0] + [None] * 22
    empty = [None, None, "电量"] + [None] * 24                    # trailing blank pair
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        # MARKET-wide results (~2,000 MWh/h) — must NOT be read as our trades
        pd.DataFrame([vol, px], columns=cols).to_excel(w, sheet_name="滚搓市场成交结果", index=False)
        pd.DataFrame([mine_vol, mine_px, empty, [None, None, "电价"] + [None] * 24],
                     columns=mine_cols).to_excel(w, sheet_name="我的滚搓结果", index=False)


def _make_contracts(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [[1, None, "景融绿色能源科技有限公司", "2026-09-01-2026-09-01",
             "段9:08:00-09:00", 9.0, 175.0, "景融绿色能源科技有限公司"],
            [2, None, "景融绿色能源科技有限公司", "2026-09-02-2026-09-02",
             "段10:09:00-10:00", 9.5, 175.0, "景融绿色能源科技有限公司"]]
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        pd.DataFrame(rows).to_excel(w, sheet_name="9-12 批次1", index=False, header=False)


def test_guncuo_hourly_pairs(tmp_path):
    _make_guncuo(tmp_path / "安徽" / "交易记录" / "3月-中长期交易.xlsx")
    out = parse_anhui_trades(tmp_path)
    g = out[out.channel == "intramonth_match"]
    assert len(g) == 2                                    # our sheet only, not the market's 24
    row = g[g.hour == 0].iloc[0]
    assert row.volume_mwh == 1.95 and row.price_cny_mwh == 246.15
    assert str(row.delivery_date) == "2026-03-01" and row.direction == "buy"


def test_contract_batches(tmp_path):
    _make_contracts(tmp_path / "安徽" / "交易记录" / "安徽中长期合同2026-月度多月度.xlsx")
    out = parse_anhui_trades(tmp_path)
    b = out[out.source_term.str.contains("批次", na=False)]
    assert len(b) == 2
    assert set(b.channel) == {"monthly_auction"}
    assert b.iloc[0].volume_mwh == 9.0 and b.iloc[0].price_cny_mwh == 175.0
    assert b.iloc[0].hour == 8      # 段9 = 08:00-09:00 -> hour 8 (0-indexed)


def test_contract_batch_multi_day_range_expands(tmp_path):
    """I5: a range row 2026-09-01-2026-09-03 must expand to one row per day,
    not collapse four months/days of volume onto the first day."""
    rows = [[1, None, "景融绿色能源科技有限公司", "2026-09-01-2026-09-03",
             "段9:08:00-09:00", 9.0, 175.0, "景融绿色能源科技有限公司"]]
    f = tmp_path / "安徽" / "交易记录" / "安徽中长期合同2026-月度多月度.xlsx"
    f.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(f, engine="openpyxl") as w:
        pd.DataFrame(rows).to_excel(w, sheet_name="9-12 批次1", index=False, header=False)
    out = parse_anhui_trades(tmp_path)
    assert len(out) == 3
    assert sorted(str(d) for d in out.delivery_date) == \
        ["2026-09-01", "2026-09-02", "2026-09-03"]
