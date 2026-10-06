"""Worked example for rolling_intrinsic_strategy (anchor lab).

Rolling-intrinsic backtest on a synthetic year: 12 monthly products,
weekly rebalancing (4 weeks/month). Each week, hedge the plant's expected
production implied by the intrinsic schedule on the current forward curve.
Plant: 100 MW, K=52. Deterministic seed=19. Units: MW, EUR/MWh.
"""

import numpy as np

MEL, K = 100.0, 52.0
HOURS_PM = 730.0
F0 = np.array([45, 48, 52, 55, 60, 65, 70, 62, 55, 50, 47, 44], dtype=float)
MONTH_VOL = 4.0               # forward curve volatility per month, EUR/MWh
SPOT_IDIO = 3.0
SEED = 19


def curve_paths(n_paths=500, seed=SEED, n_reb=4, month_vol=MONTH_VOL):
    """(n_paths, 12*n_reb+1 steps, 12 months) forward levels; step 0 = initial.

    Independent random walk per month (12-factor); step vol scaled so the
    per-month curve volatility is MONTH_VOL regardless of n_reb.
    """
    rng = np.random.default_rng(seed)
    sd_step = month_vol / np.sqrt(n_reb)
    n_steps = 12 * n_reb
    walk = np.cumsum(rng.normal(0, sd_step, (n_paths, n_steps, 12)), axis=1)
    curves = F0[None, None, :] + np.concatenate(
        [np.zeros((n_paths, 1, 12)), walk], axis=1)
    return curves


def realised_spot(curves, seed=SEED + 1):
    rng = np.random.default_rng(seed)
    return curves[:, -1, :] + rng.normal(0, SPOT_IDIO, curves[:, -1, :].shape)


from scipy.stats import norm as _norm

DAILY_SD = 8.0    # daily price dispersion inside a month around its forward level


def run_mwh(f_level: np.ndarray) -> np.ndarray:
    """Intrinsic run volume for a month at forward level f (per path).

    Smoothed by intra-month daily dispersion: the plant runs the share of
    days whose price clears K, i.e. Phi((f-K)/DAILY_SD).
    """
    return MEL * HOURS_PM * _norm.cdf((f_level - K) / DAILY_SD)


def _bachelier(f):
    """E[max(P_day - K, 0)] for P_day ~ N(f, DAILY_SD^2), per MW."""
    z = (f - K) / DAILY_SD
    return DAILY_SD * _norm.pdf(z) + (f - K) * _norm.cdf(z)


def plant_pnl(spot):
    return (MEL * HOURS_PM * _bachelier(spot)).sum(axis=1)


def backtest(curves, spot, n_reb=4):
    """Rolling intrinsic: at each step, trade to intrinsic position at current curve."""
    n = curves.shape[0]
    position = np.zeros((n, 12))          # MWh sold forward per month
    entry_value = np.zeros((n, 12))       # EUR locked per month (sum v*price)
    for w in range(12 * n_reb):
        m_now = w // n_reb
        f_w = curves[:, w, :]
        target = np.zeros((n, 12))
        for m in range(m_now, 12):
            target[:, m] = run_mwh(f_w[:, m])
        trade = target - position
        entry_value += trade * f_w        # sell the adjustment at current curve
        position = target
    hedge_pnl = (entry_value - position * spot).sum(axis=1)
    return plant_pnl(spot) + hedge_pnl


def intrinsic_at(curve_row):
    return (MEL * HOURS_PM * _bachelier(curve_row)).sum()


if __name__ == "__main__":
    curves = curve_paths(n_reb=21)
    spot = realised_spot(curves)
    pnl = backtest(curves, spot, n_reb=21)
    unhedged = plant_pnl(spot)
    realised_intrinsic = np.array([intrinsic_at(s) for s in spot])
    within = np.mean(np.abs(pnl - realised_intrinsic) / np.maximum(realised_intrinsic, 1) < 0.05)
    print(f"initial intrinsic value (week 0): {intrinsic_at(F0):,.0f} EUR")
    print(f"rolling-intrinsic P&L (daily rebal): mean {pnl.mean():,.0f}  std {pnl.std():,.0f}")
    print(f"unhedged P&L:           mean {unhedged.mean():,.0f}  std {unhedged.std():,.0f}")
    print(f"realised intrinsic:     mean {realised_intrinsic.mean():,.0f}")
    print(f"paths within 5% of realised intrinsic: {within * 100:.0f}%")
    for mv in (4.0, 2.0, 1.0):
        c2 = curve_paths(n_reb=21, month_vol=mv)
        s2 = realised_spot(c2)
        p2 = backtest(c2, s2, n_reb=21)
        ri2 = np.array([intrinsic_at(r) for r in s2])
        w2 = np.mean(np.abs(p2 - ri2) / np.maximum(ri2, 1) < 0.05)
        print(f"  MONTH_VOL={mv}: tracking std {(p2 - ri2).std():,.0f}, within 5%: {w2 * 100:.0f}%")
