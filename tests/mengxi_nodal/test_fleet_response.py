"""Fleet-response estimator tests (plan task 8).

_fit_bands is pure numpy — unit-tested here. estimate_stack is DB-bound
(same convention as book_forecast.run_day / the settlement bridge): only
its SQL frame invariants and window helpers are guarded.
"""
import numpy as np

from services.mengxi_nodal import fleet_response as fr
from services.mengxi_nodal.fleet_response import _fit_bands


def test_bands_monotone_response():
    rng = np.random.default_rng(1)
    prices = rng.uniform(0, 800, 500)
    response = 200 + 0.5 * prices + rng.normal(0, 30, 500)
    bands = _fit_bands(prices, response, n_bands=4)
    means = [b[2] for b in bands]
    assert all(m1 <= m2 for m1, m2 in zip(means, means[1:]))  # higher price ⇒ more response


def test_bands_cover_range():
    bands = _fit_bands(np.linspace(0, 800, 200), np.linspace(100, 500, 200), n_bands=4)
    assert bands[0][0] <= 0 and bands[-1][1] >= 800


def test_fit_bands_ignores_nan_pairs():
    prices = np.array([0.0, 100.0, np.nan, 800.0, 400.0])
    response = np.array([10.0, np.nan, 50.0, 90.0, 50.0])
    bands = _fit_bands(prices, response, n_bands=2)
    assert bands  # finite pairs (0,10), (800,90), (400,50) still fit
    assert all(np.isfinite(b[2]) for b in bands)


def test_fit_bands_empty_returns_empty():
    assert _fit_bands([], [], n_bands=4) == []
    assert _fit_bands([np.nan], [np.nan], n_bands=4) == []


def test_fit_bands_degenerate_prices_single_band():
    bands = _fit_bands(np.full(50, 300.0), np.linspace(100, 200, 50), n_bands=4)
    assert len(bands) == 1
    assert bands[0][0] == 300.0 and bands[0][1] == 300.0
    assert abs(bands[0][2] - 150.0) < 1e-9


def test_estimate_stack_sql_frames():
    """Filled table is CST-naive (v5): no '+08', no AT TIME ZONE in its SQL.
    spot_fundamentals_hourly.datetime is timestamptz: '+08' literals are the
    correct window there."""
    assert "+08" not in fr._PRICE_SQL
    assert "AT TIME ZONE" not in fr._PRICE_SQL.upper()
    assert "md_mengxi_nodal_price_96_filled" in fr._PRICE_SQL
    assert "spot_fundamentals_hourly" in fr._NETLOAD_SQL
    s, e = fr._naive_cst_window(60, end=__import__("datetime").date(2026, 10, 31))
    assert "+08" not in s and "+08" not in e
    s8, e8 = fr._cst_tz_window(60, end=__import__("datetime").date(2026, 10, 31))
    assert s8.endswith("+08") and e8.endswith("+08")
