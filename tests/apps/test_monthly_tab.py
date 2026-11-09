# tests/apps/test_monthly_tab.py
"""现货月报 tab: official-vs-LingFeng cross-check math and unit normalisation."""
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "apps" / "spot-market"))

import monthly_tab as mt  # noqa: E402

SEP = date(2026, 9, 1)


def _official(rows):
    return pd.DataFrame(rows, columns=["province_cn", "report_month", "rt_avg_price", "da_avg_price"])


def _hourly(rows):
    return pd.DataFrame(rows, columns=["province", "month", "rt_mean", "da_mean"])


def test_to_kwh_converts_mwh_scale():
    out = mt.to_kwh(pd.Series([393.0, 360.0, 410.0]))
    assert out.tolist() == [0.393, 0.36, 0.41]


def test_to_kwh_leaves_kwh_scale():
    s = pd.Series([0.393, 0.36])
    assert mt.to_kwh(s).tolist() == [0.393, 0.36]


def test_cross_check_within_threshold_not_flagged():
    off = _official([("山东", SEP, 0.393, 0.360)])
    hr = _hourly([("山东", SEP, 0.400, 0.350)])  # +1.8% / −2.8%
    cc = mt.cross_check(off, hr)
    assert not cc.loc[0, "flag"]
    assert round(cc.loc[0, "rt_dev_pct"], 1) == 1.8


def test_cross_check_flags_large_deviation():
    off = _official([("广东", SEP, 0.558, 0.492)])
    hr = _hourly([("广东", SEP, 0.700, 0.495)])  # RT +25%
    cc = mt.cross_check(off, hr)
    assert cc.loc[0, "flag"]


def test_cross_check_missing_hourly_marked_not_flagged():
    off = _official([("西藏", SEP, 0.30, 0.30)])
    cc = mt.cross_check(off, _hourly([]))
    assert cc.loc[0, "no_hourly"]
    assert not cc.loc[0, "flag"]


def test_cross_check_keeps_every_official_row():
    off = _official([("山东", SEP, 0.393, 0.36), ("广东", SEP, 0.558, 0.492)])
    hr = _hourly([("山东", SEP, 0.39, 0.36), ("蒙西", SEP, 0.25, 0.24)])
    cc = mt.cross_check(off, hr)
    assert sorted(cc["province_cn"]) == ["山东", "广东"]


def test_cross_check_low_coverage_not_flagged():
    # 浙江 2026-09: 2 valid days, 23 placeholder days — deviation is noise, not a finding
    off = _official([("浙江", SEP, 0.35, 0.356)])
    hr = pd.DataFrame([("浙江", SEP, 0.206, 0.271, 2, 23)],
                      columns=["province", "month", "rt_mean", "da_mean", "rt_days", "zero_days"])
    cc = mt.cross_check(off, hr)
    assert cc.loc[0, "low_cov"]
    assert not cc.loc[0, "flag"]


def test_cross_check_full_coverage_still_flags():
    off = _official([("安徽", SEP, 0.155, 0.163)])
    hr = pd.DataFrame([("安徽", SEP, 0.370, 0.356, 23, 7)],
                      columns=["province", "month", "rt_mean", "da_mean", "rt_days", "zero_days"])
    cc = mt.cross_check(off, hr)
    assert not cc.loc[0, "low_cov"]
    assert cc.loc[0, "flag"]


def test_decimals_to_float_keeps_date_and_text_columns():
    df = pd.DataFrame({
        "report_month": [date(2026, 9, 1), date(2026, 10, 1)],
        "source_file": ["a.pdf", "b.pdf"],
        "note": ["x", None],
        "rt_avg_price": [Decimal("0.393"), None],
    })
    mt._decimals_to_float(df)
    assert df["report_month"].tolist() == [date(2026, 9, 1), date(2026, 10, 1)]
    assert df["source_file"].tolist() == ["a.pdf", "b.pdf"]
    assert df["note"].iloc[0] == "x"
    assert df["rt_avg_price"].iloc[0] == 0.393 and df["rt_avg_price"].dtype == "float64"
