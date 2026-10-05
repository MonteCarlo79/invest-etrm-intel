import numpy as np

from . import compute as c


def _run(month_vol=4.0):
    curves = c.curve_paths(n_reb=21, month_vol=month_vol)
    spot = c.realised_spot(curves)
    pnl = c.backtest(curves, spot, n_reb=21)
    ri = np.array([c.intrinsic_at(s) for s in spot])
    return pnl, ri, c.plant_pnl(spot)


def test_strategy_mean_matches_realised_intrinsic():
    pnl, ri, _ = _run()
    assert abs(pnl.mean() / ri.mean() - 1) < 0.01


def test_hedging_reduces_outcome_variance():
    pnl, _, unhedged = _run()
    assert pnl.std() < 0.75 * unhedged.std()


def test_tracking_error_scales_with_curve_vol():
    pnl4, ri4, _ = _run(4.0)
    pnl2, ri2, _ = _run(2.0)
    ratio = (pnl4 - ri4).std() / (pnl2 - ri2).std()
    assert 1.6 < ratio < 2.4


def test_low_vol_paths_converge_to_realised():
    pnl, ri, _ = _run(1.0)
    share = np.mean(np.abs(pnl - ri) / np.maximum(ri, 1) < 0.05)
    assert share > 0.40
