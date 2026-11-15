import calendar
import datetime as dt
import math

import numpy as np
import pandas as pd

from libs.curves.curve import CURVE_COLUMNS
from libs.risk.bess_greeks import bess_monthly_greeks
from libs.risk.greeks import option_greeks
from libs.risk.var import monthly_price_scenarios, portfolio_var

AS_OF = dt.date(2026, 11, 15)
M = dt.date(2026, 12, 1)


def _dates(n):
    return pd.date_range("2026-01-01", periods=n, freq="D")


def _alt(n, sign=1.0):
    return pd.Series([sign * (1.0 if i % 2 == 0 else -1.0) for i in range(n)], index=_dates(n))


def _curve(rows):
    return pd.DataFrame([[p, m, b, v, "manual"] for p, m, b, v in rows], columns=CURVE_COLUMNS)


# --- VaR -------------------------------------------------------------------

def test_parametric_single_province():
    out = portfolio_var({"蒙西": 100.0}, {"蒙西": _alt(100)})
    assert out["n_scenarios"] == 100
    assert abs(out["parametric"] - 100 * 1.645) / (100 * 1.645) < 0.05
    assert isinstance(out["parametric"], float) and isinstance(out["historical"], float)


def test_anticorrelated_historical_zero():
    out = portfolio_var({"蒙西": 100.0, "山东": 100.0},
                        {"蒙西": _alt(100), "山东": _alt(100, -1.0)})
    assert abs(out["historical"]) < 1e-9
    assert abs(out["parametric"]) < 1e-6


def test_too_few_scenarios_none():
    out = portfolio_var({"蒙西": 100.0}, {"蒙西": _alt(10)})
    assert out["historical"] is None and out["parametric"] is None
    assert out["n_scenarios"] == 10


def test_alignment_inner_join_and_horizon():
    a = _alt(100)
    b = _alt(100).iloc[30:]
    out = portfolio_var({"蒙西": 50.0, "山东": 50.0}, {"蒙西": a, "山东": b}, horizon_days=4)
    assert out["n_scenarios"] == 70
    one = portfolio_var({"蒙西": 50.0, "山东": 50.0}, {"蒙西": a, "山东": b})
    assert abs(out["parametric"] - 2 * one["parametric"]) < 1e-9


def test_monthly_price_scenarios():
    dtidx = pd.date_range("2025-01-01", periods=400 * 24, freq="h")
    hourly = pd.DataFrame({"datetime": dtidx, "price": np.arange(len(dtidx)) // 24 * 1.0})
    s = monthly_price_scenarios(hourly, window_days=30, lookback_days=252)
    assert len(s) == 252
    # daily mean rises 1/day -> 30d rolling mean rises 1/day
    assert np.allclose(s.values, 1.0)


# --- option Greeks -----------------------------------------------------------

def test_option_greeks_empty_zero():
    out = option_greeks(pd.DataFrame(columns=["direction", "volume_mwh", "strike_cny_mwh",
                                              "option_type", "delivery_month", "province"]),
                        _curve([]), 0.3, AS_OF)
    assert all(v == 0.0 for v in out.values())
    assert {"delta_mwh", "gamma", "vega"} <= set(out)


def test_option_greeks_signed():
    curve = _curve([("蒙西", M, "base", 300.0)])
    pos = pd.DataFrame([dict(direction="buy", volume_mwh=100.0, strike_cny_mwh=300.0,
                             option_type="call", delivery_month=M, province="蒙西"),
                        dict(direction="sell", volume_mwh=100.0, strike_cny_mwh=300.0,
                             option_type="put", delivery_month=M, province="蒙西")])
    out = option_greeks(pos, curve, 0.3, AS_OF)
    # long call + short put at same strike = synthetic long forward: delta ≈ +100, gamma/vega ≈ 0
    assert abs(out["delta_mwh"] - 100.0) < 1e-6
    assert abs(out["gamma"]) < 1e-9 and abs(out["vega"]) < 1e-9


def test_option_greeks_expired_intrinsic():
    curve = _curve([("蒙西", dt.date(2026, 10, 1), "base", 300.0)])
    pos = pd.DataFrame([dict(direction="buy", volume_mwh=10.0, strike_cny_mwh=250.0,
                             option_type="c", delivery_month=dt.date(2026, 10, 1), province="蒙西")])
    out = option_greeks(pos, curve, 0.3, AS_OF)
    assert out["delta_mwh"] == 10.0 and out["gamma"] == 0.0 and out["vega"] == 0.0


# --- BESS strip Greeks -------------------------------------------------------

def test_bess_monthly_greeks_signs_and_bounds():
    m2 = dt.date(2027, 1, 1)
    curve = _curve([("蒙西", M, "peak", 450.0), ("蒙西", M, "offpeak", 200.0),
                    ("蒙西", m2, "peak", 420.0), ("蒙西", m2, "offpeak", 220.0)])
    df = bess_monthly_greeks(100.0, 2.0, 0.85, [M, m2], curve, "蒙西",
                             peak_vol=0.4, offpeak_vol=0.3, as_of=AS_OF)
    assert len(df) == 2
    for _, r in df.iterrows():
        days = calendar.monthrange(r.delivery_month.year, r.delivery_month.month)[1]
        bound = 100.0 * 2.0 * days
        assert 0 < r.delta_peak_mwh <= bound
        assert -bound <= r.delta_offpeak_mwh < 0
        assert r.vega_cny_per_volpt >= 0
        assert r.note == ""


def test_bess_missing_offpeak_nan_row():
    curve = _curve([("蒙西", M, "peak", 450.0), ("蒙西", M, "base", 300.0)])
    df = bess_monthly_greeks(100.0, 2.0, 0.85, [M], curve, "蒙西",
                             peak_vol=0.4, offpeak_vol=0.3, as_of=AS_OF)
    r = df.iloc[0]
    assert math.isnan(r.delta_peak_mwh) and math.isnan(r.delta_offpeak_mwh)
    assert math.isnan(r.vega_cny_per_volpt)
    assert r.note == "no peak/offpeak mark"
