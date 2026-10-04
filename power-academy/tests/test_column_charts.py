import pandas as pd

from academy.column.charts import (da_rt_spread, png_size, price_duration_curve,
                                   province_compare, set_style)


def _csv(tmp_path, df):
    p = tmp_path / "in.csv"
    df.to_csv(p, index=False)
    return p


def test_duration_curve_900x500(tmp_path):
    set_style()
    csv = _csv(tmp_path, pd.DataFrame({"rt_price": [0.4, 0.1, 0.9, -0.05, 0.3]}))
    out = tmp_path / "c.png"
    price_duration_curve(csv, out, title="test 价格")
    assert png_size(out) == (900, 500) and out.stat().st_size > 5000


def test_da_rt_spread(tmp_path):
    csv = _csv(tmp_path, pd.DataFrame({
        "datetime": ["2026-10-01 10:00", "2026-10-01 11:00",
                     "2026-10-02 10:00", "2026-10-02 11:00"],
        "da_price": [0.40, 0.42, 0.38, 0.39],
        "rt_price": [0.45, 0.50, 0.30, 0.28]}))
    out = tmp_path / "s.png"
    da_rt_spread(csv, out)
    assert png_size(out) == (900, 500)


def test_province_compare_sorted(tmp_path):
    csv = _csv(tmp_path, pd.DataFrame({"province": ["山东", "山西", "广东"],
                                       "capture": [0.72, 0.85, 0.66]}))
    out = tmp_path / "p.png"
    province_compare(csv, out, value_col="capture")
    assert png_size(out) == (900, 500)
