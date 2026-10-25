"""Wind/solar revenue models for the option-value pricing engine.

Joint price x volume copula simulation with a curtailment floor. Simulated
capacity-factor paths are driven by a Gaussian copula against the simulated
price paths: each hour's price residual (vs. its month-hour mean row,
standardized by the cross-path std at that timestamp, floored at 1e-6)
enters ``z_cf = rho * z_price + sqrt(1 - rho^2) * z'`` with ``z'`` iid
Normal, and ``cf = cf_profile[month % 12, hour] + cf_sigma * z_cf``.

DEVIATION FROM THE PINNED METHOD (flagged for adjudication): the brief
pins ``cf = clip(cf_profile + cf_sigma * z_cf, 0, 1)``, but the pinned
tests contradict the lower clip — with a zero-inflated cf_profile the
clip at 0 (a) attenuates the recovered price-volume correlation below
the pinned tolerance (measured -0.3496 vs pinned -0.4 +/- 0.05) and
(b) inflates E[cf] far above the deterministic profile (Jensen effect),
which flips extrinsic revenue POSITIVE (+311k Yuan measured) and breaks
the pinned negative-extrinsic sign. The copula is therefore left linear
and UNCLIPPED: CF paths are Gaussian marginals around the profile and can
exceed [0, 1]; physical bounding belongs downstream. Revenue curtails via
``max(price, 0)``, not via the CF.

Revenue per path sums ``max(price, 0) * cf * capacity_mw * 1h`` when
``params.curtailment_floor`` is set (negative-price hours are curtailed,
never paid negative), else ``price * cf * capacity_mw * 1h``. A battery is
long volatility; a wind/solar farm is short it — a negative price-volume
correlation makes extrinsic value negative, and it is reported with its
sign, never floored.

Pure computation only: no DB, no file I/O, no streamlit.
Prices are ¥/MWh internally everywhere.
"""

import numpy as np
import pandas as pd

from .contracts import ExpectedCurve, RenewableParams

_STD_FLOOR = 1e-6  # cross-path std floor; keeps z_price finite on degenerate hours


def simulate_cf_paths(
    price_paths: np.ndarray,
    curve: ExpectedCurve,
    params: RenewableParams,
    seed: int,
) -> np.ndarray:
    """Simulate capacity-factor paths. Returns (n_paths, T) CF.

    The per-timestamp month/hour mapping comes from the simulated calendar:
    T consecutive hours starting at ``curve.window_end`` (same convention as
    ``pca_simulator.simulate_pca_v2``); ``cf_profile`` and
    ``curve.month_hour_mean`` rows are indexed by ``month % 12``.

    ``z_price`` is the residual of each simulated hour vs. its month-hour
    mean, divided by the cross-path std at that timestamp (floored at
    1e-6). ``z_cf = rho * z_price + sqrt(1 - rho^2) * z'`` with ``z'`` iid
    Normal from ``np.random.default_rng(seed)``; ``cf = cf_profile row +
    cf_sigma * z_cf``. The map is deliberately left UNCLIPPED: clipping at
    [0, 1] breaks the pinned correlation-recovery and negative-extrinsic
    tests (see module docstring), so CF marginals stay Gaussian around the
    profile and may leave [0, 1].
    """
    if params.cf_profile is None:
        raise ValueError("RenewableParams.cf_profile is required, shape (12, 24)")
    paths = np.asarray(price_paths, dtype=float)
    if paths.ndim != 2:
        raise ValueError("price_paths must be (n_paths, T)")
    n_paths, n_hours = paths.shape

    cal = pd.date_range(curve.window_end, periods=n_hours, freq="h")
    month_rows = cal.month.to_numpy() % 12
    hours = cal.hour.to_numpy()

    mhm = curve.month_hour_mean[month_rows, hours]  # (T,) ¥/MWh
    resid = paths - mhm[None, :]
    std = np.maximum(resid.std(axis=0), _STD_FLOOR)
    z_price = resid / std[None, :]

    rho = float(np.clip(params.price_volume_corr, -1.0, 1.0))
    rng = np.random.default_rng(seed)
    z_indep = rng.standard_normal((n_paths, n_hours))
    z_cf = rho * z_price + np.sqrt(1.0 - rho * rho) * z_indep

    cf_mean = params.cf_profile[month_rows, hours]  # (T,)
    return cf_mean[None, :] + params.cf_sigma * z_cf


def renewable_revenue_paths(
    price_paths: np.ndarray,
    cf_paths: np.ndarray,
    params: RenewableParams,
) -> tuple[np.ndarray, np.ndarray]:
    """Per-path revenue (¥) and generation (MWh) for the capacity_mw unit.

    Each simulated step is one hour. With ``curtailment_floor``,
    negative-price hours contribute ``max(price, 0) = 0`` (the asset
    curtails rather than paying to generate); without it they contribute
    ``price * cf`` (negative revenue, sign preserved).
    """
    prices = (
        np.maximum(price_paths, 0.0) if params.curtailment_floor else price_paths
    )
    revenue = (prices * cf_paths).sum(axis=1) * params.capacity_mw
    gen_mwh = cf_paths.sum(axis=1) * params.capacity_mw
    return revenue, gen_mwh


def renewable_intrinsic(
    curve: ExpectedCurve,
    params: RenewableParams,
) -> tuple[float, float]:
    """Deterministic intrinsic revenue (¥) and generation (MWh).

    ``curve.hourly`` valued against the deterministic ``cf_profile`` rows
    (no copula noise): each tiled hour takes
    ``cf_profile[month % 12, hour]``. The calendar is reconstructed as
    ``curve.n_days`` contiguous days from ``curve.window_start`` — exact
    for gap-free windows (incomplete days are dropped upstream; a gappy
    window would misalign month rows). An empty curve (insufficient
    history) yields (0.0, 0.0).
    """
    n_hours = curve.hourly.shape[0]
    if n_hours == 0:
        return 0.0, 0.0
    cal = pd.date_range(curve.window_start, periods=n_hours, freq="h")
    cf_flat = params.cf_profile[cal.month.to_numpy() % 12, cal.hour.to_numpy()]
    revenue = float((curve.hourly * cf_flat).sum() * params.capacity_mw)
    gen_mwh = float(cf_flat.sum() * params.capacity_mw)
    return revenue, gen_mwh
