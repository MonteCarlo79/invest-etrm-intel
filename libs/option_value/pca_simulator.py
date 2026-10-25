"""PCA Monte Carlo price simulator v2 for the option-value pricing engine.

Seasonal-PCA model: daily complete-day residual profiles (actual minus the
curve's month-hour mean row) are decomposed via an eigendecomposition of the
(24, 24) hour covariance matrix. Daily PC scores follow independent AR(1)
processes with Student-t innovations (Normal fallback for degenerate or
platykurtic score series). Simulated paths reconstruct the seasonal row plus
the per-PC score*loading contributions, clipped to the observed [floor, cap].

Pure computation only: no DB, no file I/O, no streamlit.
Prices are ¥/MWh internally everywhere.
"""

from datetime import date

import numpy as np
import pandas as pd

from .contracts import ExpectedCurve, PCASimConfig, PCAv2Params

_MIN_INNO_STD = 1e-9  # below this the PC carries no signal → Normal fallback
_DF_MIN, _DF_MAX = 3.0, 30.0
_PHI_CLIP = 0.999  # keeps the stationary s_0 variance e.var()/(1-phi²) defined


def _complete_day_profiles(
    prices: pd.Series, window_start: date
) -> tuple[np.ndarray, np.ndarray]:
    """Complete-day (24 non-NaN hours) profiles at/after window_start.

    Returns (values, row_months): values is (n_days, 24) actual ¥/MWh in
    chronological order; row_months is the calendar month of each row.
    Mirrors curves.build_expected_curve's lower-bound and complete-day rules.
    """
    s = prices[prices.index.date >= window_start]
    hours_per_day = s.groupby(s.index.date).count()  # count() ignores NaN
    complete_days = set(hours_per_day[hours_per_day == 24].index)

    frame = pd.DataFrame(
        {
            "price": s.to_numpy(dtype=float),
            "day": s.index.date,
            "month": s.index.month.to_numpy(),
            "hour": s.index.hour.to_numpy(),
        }
    )
    frame = frame[frame["day"].isin(complete_days)]

    mat = frame.pivot_table(
        index="day", columns="hour", values="price", aggfunc="mean"
    ).reindex(columns=range(24))
    mat = mat.sort_index()
    values = mat.to_numpy(dtype=float)
    row_months = np.array([d.month for d in mat.index])
    return values, row_months


def fit_pca_v2(
    prices: pd.Series, curve: ExpectedCurve, cfg: PCASimConfig
) -> PCAv2Params:
    """Fit seasonal-PCA + AR(1)/Student-t score dynamics.

    Residuals are daily complete-day profiles minus their month-hour mean
    row (row index = month % 12). The (24, 24) residual covariance is
    decomposed with np.linalg.eigh, eigenpairs ordered by descending
    variance, keeping cfg.n_components loadings. Per-PC daily scores get an
    AR(1) coefficient phi = Σ(s_t·s_{t-1}) / Σ(s_{t-1}²) and innovations
    e = s_t − phi·s_{t-1}. Student-t df comes from the moment estimator
    df = 6/kurt_excess + 4 (clipped to [3, 30]) when kurt_excess > 0;
    otherwise df = NaN → Normal innovations. Degenerate PCs
    (e.std() < 1e-9) also fall back to Normal. The t-innovation scale is
    e.std() / sqrt(df/(df−2)) so simulated innovations have variance
    e.std()² (raw standard_t has variance df/(df−2)).
    """
    values, row_months = _complete_day_profiles(prices, curve.window_start)
    n_days = values.shape[0]
    k = cfg.n_components

    seasonal = curve.month_hour_mean[row_months % 12]  # (n_days, 24)
    resid = values - seasonal

    # Eigendecomposition of the (24×24) covariance — avoids a tall-matrix
    # factorization that triggers the Fargate OpenBLAS segfault.
    cov = resid.T @ resid  # (24, 24)
    _, eigenvectors = np.linalg.eigh(cov)  # ascending; cols = eigenvectors
    loadings = eigenvectors[:, ::-1].T[:k]  # (k, 24), descending variance
    scores = resid @ loadings.T  # (n_days, k) — small (days×24)@(24×k)

    phi = np.zeros(k)
    inno_scale = np.zeros(k)
    inno_df = np.full(k, np.nan)

    for p in range(k):
        s_prev, s_cur = scores[:-1, p], scores[1:, p]
        denom = float(s_prev @ s_prev)
        phi_p = float(s_cur @ s_prev) / denom if denom > 0 else 0.0
        phi[p] = np.clip(phi_p, -_PHI_CLIP, _PHI_CLIP)

        # Innovations from the CLIPPED phi: when the ±0.999 clip binds, the
        # residuals must be consistent with the phi the simulator actually
        # recurses on.
        e = s_cur - phi[p] * s_prev
        e_std = float(e.std())
        if e_std < _MIN_INNO_STD:
            inno_scale[p] = e_std  # ≈ 0: PC contributes nothing
            continue

        kurt_excess = float(np.mean(e**4) / e_std**4 - 3.0)
        if kurt_excess > 0:
            df = min(max(6.0 / kurt_excess + 4.0, _DF_MIN), _DF_MAX)
            inno_df[p] = df
            inno_scale[p] = e_std / np.sqrt(df / (df - 2.0))
        else:
            inno_scale[p] = e_std  # Normal fallback

    return PCAv2Params(
        loadings=loadings, phi=phi, inno_scale=inno_scale, inno_df=inno_df
    )


def simulate_pca_v2(
    params: PCAv2Params, curve: ExpectedCurve, n_days: int, cfg: PCASimConfig
) -> np.ndarray:
    """Simulate price paths. Returns (cfg.n_paths, n_days*24) ¥/MWh.

    The simulated calendar is n_days consecutive days starting at
    curve.window_end, rolling forward; each day's seasonal row is
    curve.month_hour_mean[month % 12]. Per PC, AR(1) scores are recursed
    over days (vectorized across paths) from a stationary start
    s_0 ~ N(0, e.std()²/(1−phi²)); innovations are Normal (df NaN) or
    scaled standard_t. Profiles are rebuilt by per-PC np.outer
    accumulation (no large matmul) and clipped to [curve.floor, curve.cap].
    """
    rng = np.random.default_rng(cfg.seed)
    n_paths = cfg.n_paths
    k = params.loadings.shape[0]

    cal = pd.date_range(curve.window_end, periods=n_days, freq="D")
    month_rows = (cal.month.to_numpy()) % 12  # (n_days,)

    # Path-major seasonal base: path i occupies rows [i*n_days, (i+1)*n_days).
    daily = np.tile(curve.month_hour_mean[month_rows], (n_paths, 1))

    for p in range(k):
        phi = params.phi[p]
        df = params.inno_df[p]
        if np.isnan(df):
            e_std = params.inno_scale[p]
        else:
            e_std = params.inno_scale[p] * np.sqrt(df / (df - 2.0))
        s0_std = e_std / np.sqrt(1.0 - phi * phi)

        s = np.empty((n_paths, n_days))
        s[:, 0] = rng.normal(0.0, s0_std, size=n_paths)
        for d in range(1, n_days):
            if np.isnan(df):
                eps = rng.normal(0.0, params.inno_scale[p], size=n_paths)
            else:
                eps = rng.standard_t(df, size=n_paths) * params.inno_scale[p]
            s[:, d] = phi * s[:, d - 1] + eps

        daily += np.outer(s.reshape(-1), params.loadings[p])

    np.clip(daily, curve.floor, curve.cap, out=daily)
    return daily.reshape(n_paths, n_days * 24)
