"""Tests for per-year YTD subtotal rows in the settlement monthly breakdown.

The monthly table in apps/asset_risk/tab_settlement.py must show one subtotal
row per year present in the data ("2025 YTD", "2026 YTD", ...), positioned
after that year's last month, with per-unit metrics recalculated from that
year's own volumes (not mixed across years).
"""
import pandas as pd
import pytest

from apps.asset_risk.tab_settlement import _insert_year_subtotals


def _sample_pivot():
    """Two years of monthly rows, minimal columns used by the YTD logic."""
    idx = ["2025-11", "2025-12", "2026-01", "2026-02"]
    return pd.DataFrame({
        "净利润": [-100.0, -200.0, 500.0, 700.0],
        "放电收入": [0.0, 0.0, 800.0, 1000.0],
        "充电电费": [-300.0, -400.0, -300.0, -350.0],
        "容量补偿/非市场化": [0.0, 0.0, 80.0, 100.0],
        # 价差收入 = 放电收入 + 充电电费 (容量补偿不计入, 2026-08-11 起)
        "价差收入": [-300.0, -400.0, 500.0, 650.0],
        "放电量(MWh)": [0.0, 0.0, 10.0, 12.0],
        "充电量(MWh)": [30.0, 40.0, 11.0, 13.0],
        "度电总价差": [0.0, 0.0, 58.0, 62.5],
        "容量补偿价差": [0.0, 0.0, 8.0, 8.33],
        "套利价差": [0.0, 0.0, 50.0, 54.17],
        "日均充放次数": [0.33, 0.44, 0.37, 0.43],
        "转化率": [0.0, 0.0, 0.909, 0.923],
    }, index=idx)


def _sample_monthly():
    """Long-format monthly frame matching _sample_pivot (month, category_cn, amount, volume)."""
    rows = []
    data = {
        "2025-11": {"放电收入": (0.0, 0.0), "充电电费": (-300.0, 30.0)},
        "2025-12": {"放电收入": (0.0, 0.0), "充电电费": (-400.0, 40.0)},
        "2026-01": {"放电收入": (800.0, 10.0), "充电电费": (-300.0, 11.0)},
        "2026-02": {"放电收入": (1000.0, 12.0), "充电电费": (-350.0, 13.0)},
    }
    for month, cats in data.items():
        for cat, (amount, volume) in cats.items():
            rows.append({"month": month, "category_cn": cat, "amount": amount, "volume": volume})
    return pd.DataFrame(rows)


DAYS = [30, 31, 31, 28]  # days for 2025-11, 2025-12, 2026-01, 2026-02
ENERGY_PER_CYCLE = 100.0  # MWh (capacity x duration)


def test_one_subtotal_row_per_year():
    pivot = _insert_year_subtotals(_sample_pivot(), _sample_monthly(), DAYS,
                                   ENERGY_PER_CYCLE, "放电收入", "充电电费", "容量补偿/非市场化")
    assert "2025 YTD" in pivot.index
    assert "2026 YTD" in pivot.index
    # Subtotal row placed after its year's last month
    idx = list(pivot.index)
    assert idx.index("2025 YTD") == idx.index("2025-12") + 1
    assert idx[-1] == "2026 YTD"


def test_subtotal_sums_are_per_year():
    pivot = _insert_year_subtotals(_sample_pivot(), _sample_monthly(), DAYS,
                                   ENERGY_PER_CYCLE, "放电收入", "充电电费", "容量补偿/非市场化")
    assert pivot.loc["2025 YTD", "净利润"] == pytest.approx(-300.0)
    assert pivot.loc["2026 YTD", "净利润"] == pytest.approx(1200.0)
    assert pivot.loc["2025 YTD", "充电量(MWh)"] == pytest.approx(70.0)
    assert pivot.loc["2026 YTD", "充电量(MWh)"] == pytest.approx(24.0)


def test_subtotal_metrics_use_own_year_volumes():
    pivot = _insert_year_subtotals(_sample_pivot(), _sample_monthly(), DAYS,
                                   ENERGY_PER_CYCLE, "放电收入", "充电电费", "容量补偿/非市场化")
    # 2026: 度电总价差 = (价差收入 1150 + 容量补偿 180) / 放电量 22 = 60.45
    assert pivot.loc["2026 YTD", "度电总价差"] == pytest.approx(1330.0 / 22.0, rel=1e-3)
    # 2026 套利价差 = 价差收入 1150 / 22 (纯充放, 不含容量补偿)
    assert pivot.loc["2026 YTD", "套利价差"] == pytest.approx(1150.0 / 22.0, rel=1e-3)
    # 2026 转化率 = 22 / 24
    assert pivot.loc["2026 YTD", "转化率"] == pytest.approx(22.0 / 24.0, rel=1e-3)
    # 2025 has zero discharge volume -> per-MWh metrics stay 0, no NaN/inf
    assert pivot.loc["2025 YTD", "度电总价差"] == 0.0
    assert pivot.loc["2025 YTD", "转化率"] == 0.0
    # 2025 日均充放次数 = 70 MWh / 100 MWh-per-cycle / 61 days
    assert pivot.loc["2025 YTD", "日均充放次数"] == pytest.approx(round(70.0 / 100.0 / 61, 2))


def test_single_year_still_works():
    pivot1 = _sample_pivot().loc[["2026-01", "2026-02"]]
    monthly1 = _sample_monthly()
    monthly1 = monthly1[monthly1["month"].str.startswith("2026")]
    out = _insert_year_subtotals(pivot1, monthly1, [31, 28],
                                 ENERGY_PER_CYCLE, "放电收入", "充电电费", "容量补偿/非市场化")
    assert list(out.index) == ["2026-01", "2026-02", "2026 YTD"]
