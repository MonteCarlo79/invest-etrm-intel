from datetime import date

import pandas as pd
import pytest

from libs.curves.curve import (CURVE_COLUMNS, curve_lookup, reference_curve,
                               spot_seasonal, traded_points)


def _hourly():
    idx = pd.date_range("2024-01-01", "2025-12-31 23:00", freq="h")
    return pd.DataFrame({"datetime": idx, "price": idx.hour.astype(float)})


def test_spot_seasonal_blocks():
    h = _hourly()
    out = spot_seasonal(h, "蒙西", [date(2026, 1, 1)], lookback_years=2)
    assert list(out.columns) == CURVE_COLUMNS
    v = dict(zip(out.tou_block, out.price_cny_mwh))
    assert v["base"] == pytest.approx(11.5)
    assert v["peak"] == pytest.approx(19.0)
    assert v["offpeak"] == pytest.approx(12.5)
    assert set(out.source) == {"spot_seasonal"}
    assert set(out.province) == {"蒙西"}


def test_spot_seasonal_uncovered_month_no_row():
    h = _hourly()
    h = h[h.datetime.dt.month != 3]
    out = spot_seasonal(h, "蒙西", [date(2026, 3, 1)])
    assert out.empty and list(out.columns) == CURVE_COLUMNS


def test_spot_seasonal_lookback_excludes_target_and_future():
    h = _hourly()
    out = spot_seasonal(h, "蒙西", [date(2024, 1, 1)])
    assert out.empty


def test_traded_points_vwap():
    t = pd.DataFrame({
        "province": ["蒙西"] * 3, "delivery_month": [date(2026, 1, 1)] * 3,
        "tou_block": ["base"] * 3, "volume_mwh": [100, 300, 0],
        "price_cny_mwh": [300.0, 340.0, 999.0]})
    out = traded_points(t)
    assert out.price_cny_mwh.iloc[0] == pytest.approx(330.0)
    assert out.source.iloc[0] == "traded" and len(out) == 1


def _row(src, p, blk="base"):
    return pd.DataFrame([["蒙西", date(2026, 1, 1), blk, p, src]], columns=CURVE_COLUMNS)


def test_reference_curve_priority():
    c = reference_curve(_row("spot_seasonal", 1), _row("traded", 2), _row("manual", 3))
    assert len(c) == 1 and c.price_cny_mwh.iloc[0] == 3 and c.source.iloc[0] == "manual"
    c = reference_curve(_row("spot_seasonal", 1), _row("traded", 2))
    assert c.source.iloc[0] == "traded"


def test_curve_lookup_fallback():
    c = _row("manual", 300.0, "base")
    assert curve_lookup(c, "蒙西", date(2026, 1, 1), "peak") == 300.0
    assert curve_lookup(c, "蒙西", date(2026, 2, 1), "peak") is None
    c = pd.concat([c, _row("manual", 400.0, "peak")])
    assert curve_lookup(c, "蒙西", date(2026, 1, 1), "peak") == 400.0


def _tr(vols, prices=None, month=date(2026, 1, 1)):
    n = len(vols)
    return pd.DataFrame({"province": ["蒙西"] * n, "delivery_month": [month] * n,
                         "tou_block": ["base"] * n, "volume_mwh": vols,
                         "price_cny_mwh": prices or [300.0] * n})


def test_traded_points_netting_and_negative_ignored():
    assert traded_points(_tr([100, -100])).iloc[0:1].price_cny_mwh.tolist() == [300.0]
    assert len(traded_points(_tr([-50, -20]))) == 0


def test_traded_timestamp_month_lookup():
    out = traded_points(_tr([10], month=pd.Timestamp("2026-01-01")))
    assert curve_lookup(reference_curve(out), "蒙西", date(2026, 1, 1)) == 300.0
    assert curve_lookup(out, "蒙西", pd.Timestamp("2026-01-01")) == 300.0


def test_reference_curve_nan_manual_does_not_shadow():
    c = reference_curve(_row("manual", float("nan")), _row("traded", 2))
    assert len(c) == 1 and c.source.iloc[0] == "traded"


def test_spot_seasonal_ignores_constant_filler_and_thin_months():
    # 2026-11-16 audit: 蒙西 2025-11 was 0.2603 every hour; 2025-12 had only 48 real hours.
    import numpy as np
    nov = pd.DataFrame({"datetime": pd.date_range("2025-11-01", periods=720, freq="h"), "price": 0.2603})
    dec_real = pd.DataFrame({"datetime": pd.date_range("2025-12-01", periods=48, freq="h"),
                             "price": np.linspace(200, 700, 48)})
    jan = pd.DataFrame({"datetime": pd.date_range("2026-01-01", periods=744, freq="h"),
                        "price": np.linspace(100, 500, 744)})
    hourly = pd.concat([nov, dec_real, jan], ignore_index=True)
    out = spot_seasonal(hourly, "蒙西", [date(2026, 11, 1), date(2026, 12, 1), date(2027, 1, 1)])
    months = set(out["delivery_month"])
    assert date(2026, 11, 1) not in months      # filler month dropped entirely
    assert date(2026, 12, 1) not in months      # 48 h < 240 h minimum
    jan_base = out[(out.delivery_month == date(2027, 1, 1)) & (out.tou_block == "base")]
    assert abs(float(jan_base.price_cny_mwh.iloc[0]) - 300.0) < 1e-6
