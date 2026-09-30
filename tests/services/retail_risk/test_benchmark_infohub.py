# tests/services/retail_risk/test_benchmark_infohub.py
from pathlib import Path
import pandas as pd
import pytest
from services.retail_risk.parsers.benchmark_infohub import parse_infohub_benchmarks


def _make_infohub(path: Path):
    # Right block at rows 2+, col D: 月份 | 双边协商交易(净合约量,加权均价) | 集中竞价交易(...)
    header1 = [None, None, None, "月份", "双边协商交易", None, "集中竞价交易", None]
    header2 = [None, None, None, None, "净合约量", "加权均价", "净合约量", "加权均价"]
    rows = [
        [None, None, None, "1 月", 72.1, 372.21, 18.6, 371.63],
        [None, None, None, "2 月", 76.1, 372.16, 17.3, 371.0],
        [None, None, None, "合计", 803.1, 372.11, 204.4, 373.08],
    ]
    pad = [[None] * 8, [None] * 8]
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        pd.DataFrame(pad + [header1, header2] + rows).to_excel(
            w, sheet_name="中长期价格", index=False, header=False)


def test_right_block_extracted(tmp_path):
    f = tmp_path / "山东电力市场信息汇总by20260131.xlsx"
    _make_infohub(f)
    out = parse_infohub_benchmarks(f)
    assert set(out.channel) == {"双边协商交易", "集中竞价交易"}
    assert len(out[out.channel == "双边协商交易"]) == 2          # 合计 row excluded
    row = out[(out.channel == "双边协商交易") & (out.month.astype(str) == "2026-01-01")]
    assert row.avg_price_cny_mwh.iloc[0] == pytest.approx(372.21)
    assert row.volume_mwh.iloc[0] == pytest.approx(72.1 * 10000)  # 亿度 -> MWh
    assert set(out.province) == {"山东"}


def test_newline_anchor_and_sparse_year(tmp_path):
    """Real file: cells hold '加权\\n均价' (embedded newline); year markers appear
    only on the first row of each year block and must forward-fill. An earlier
    block also contains 加权均价 (anchor trap), and a spot block follows the data
    (must not be parsed as benchmark rows)."""
    trap = [None, "挂牌交易", None, "加权均价", None, None]     # row 0: anchor trap
    header1 = [None, None, None, "月份", "双边协商交易", None]
    header2 = [None, None, None, None, "净合\n约量", "加权\n均价"]
    rows = [
        [2024, None, None, "1 月", 72.1, 372.21],
        [None, None, None, "2 月", 76.1, 372.16],     # still 2024 (sparse marker)
        [2025, None, None, "1 月", 80.0, 380.0],     # new year block
        [None, None, None, "2 月", 81.0, 381.0],     # still 2025
    ]
    spot_block = [[None, None, None, "月份", "日前出清均价", "实时出清均价"],
                  [None, None, None, "1 月", 400.0, 410.0]]
    f = tmp_path / "山东电力市场信息汇总by20260131.xlsx"
    with pd.ExcelWriter(f, engine="openpyxl") as w:
        pd.DataFrame([trap, header1, header2] + rows + spot_block).to_excel(
            w, sheet_name="中长期价格", index=False, header=False)
    out = parse_infohub_benchmarks(f)
    assert len(out) == 4                                   # spot block NOT parsed
    assert set(out.channel) == {"双边协商交易"}              # trap block NOT parsed
    years = sorted(out.month.astype(str).str[:4].unique())
    assert years == ["2024", "2025"]
    feb24 = out[out.month.astype(str) == "2024-02-01"]
    assert feb24.avg_price_cny_mwh.iloc[0] == pytest.approx(372.16)


def test_missing_price_sheet_returns_empty(tmp_path):
    """信息汇总 variants without a 中长期价格 sheet must not crash the backfill."""
    f = tmp_path / "蒙西电力市场信息汇总by20260110.xlsx"
    with pd.ExcelWriter(f, engine="openpyxl") as w:
        pd.DataFrame([["a", 1]]).to_excel(w, sheet_name="Sheet1", index=False)
    out = parse_infohub_benchmarks(f)
    assert out.empty
