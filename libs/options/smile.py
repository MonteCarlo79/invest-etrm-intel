"""
libs/options/smile.py

SVI (Stochastic Volatility Inspired) volatility smile calibration.

Model
-----
  w(k) = a + b * (rho * (k - m) + sqrt((k - m)^2 + sigma^2))

where:
  k  = log(K/F)          log-moneyness
  w  = sigma_impl^2 * T  total implied variance
  a  = vertical shift
  b  >= 0 : slope of the wings
  rho in (-1, 1) : skew rotation
  m  = horizontal shift (ATM offset in log-moneyness)
  sigma > 0 : ATM curvature

Fitted via scipy.optimize.minimize (Nelder-Mead) minimising weighted RMSE
of total variance.

Functions
---------
svi_vol(k, params, T) -> float
fit_svi(log_strikes, market_vols, T, weights) -> SVIParams
calibrate_from_quotes(quotes_df, F, T) -> (SVIParams, augmented_df)
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
from scipy.optimize import minimize

from libs.options.black_scholes import b76_price


# ---------------------------------------------------------------------------
# SVI model
# ---------------------------------------------------------------------------

@dataclass
class SVIParams:
    a: float
    b: float
    rho: float
    m: float
    sigma: float

    def __str__(self) -> str:
        return (
            f"SVI(a={self.a:.4f}, b={self.b:.4f}, "
            f"rho={self.rho:.4f}, m={self.m:.4f}, sigma={self.sigma:.4f})"
        )


def svi_vol(k: float, params: SVIParams, T: float) -> float:
    """
    Annualised implied volatility from SVI total-variance model.

    Parameters
    ----------
    k : log(K/F) — log-moneyness
    params : fitted SVIParams
    T : time to expiry in years

    Returns
    -------
    float : annualised implied vol (e.g. 0.25 = 25%)
    """
    if T <= 0:
        return 0.0
    diff = k - params.m
    w = params.a + params.b * (
        params.rho * diff + math.sqrt(diff ** 2 + params.sigma ** 2)
    )
    w = max(w, 1e-12)  # total variance must be positive
    return math.sqrt(w / T)


# ---------------------------------------------------------------------------
# Calibration
# ---------------------------------------------------------------------------

def fit_svi(
    log_strikes: np.ndarray,
    market_vols: np.ndarray,
    T: float,
    weights: Optional[np.ndarray] = None,
) -> SVIParams:
    """
    Fit SVI to market implied vols via Nelder-Mead optimisation.

    Parameters
    ----------
    log_strikes : array of k = log(K/F)
    market_vols : array of annualised implied vols (same length as log_strikes)
    T : time to expiry in years
    weights : optional weight array (e.g. 1/bid-ask spread); default = uniform

    Returns
    -------
    SVIParams : fitted parameters
    """
    if weights is None:
        weights = np.ones(len(market_vols))

    # Convert market vols to total variance
    market_w = market_vols ** 2 * T

    def objective(x: np.ndarray) -> float:
        a, b, rho, m, sigma = x
        # Evaluate SVI total variance for all strikes
        diff = log_strikes - m
        w_model = a + b * (rho * diff + np.sqrt(diff ** 2 + sigma ** 2))
        w_model = np.maximum(w_model, 1e-12)
        residuals = (w_model - market_w) * weights
        return float(np.sum(residuals ** 2))

    # Initial guess: flat vol = mean(market_vols)
    mean_w = float(np.mean(market_w))
    x0 = [mean_w * 0.5, 0.1, -0.3, 0.0, 0.1]

    # Bounds: b > 0, rho in (-1, 1), sigma > 0; a unconstrained
    bounds = [
        (None, None),   # a
        (1e-6, None),   # b >= 0
        (-0.999, 0.999),  # rho
        (None, None),   # m
        (1e-6, None),   # sigma > 0
    ]

    result = minimize(
        objective, x0, method="Nelder-Mead",
        options={"maxiter": 10000, "xatol": 1e-8, "fatol": 1e-10},
    )

    # Nelder-Mead ignores bounds — project solution back
    a, b, rho, m, sigma = result.x
    b = max(b, 1e-6)
    rho = max(-0.999, min(0.999, rho))
    sigma = max(sigma, 1e-6)

    return SVIParams(a=float(a), b=float(b), rho=float(rho), m=float(m), sigma=float(sigma))


def calibrate_from_quotes(
    quotes_df: pd.DataFrame,
    F: float,
    T: float,
) -> tuple[SVIParams, pd.DataFrame]:
    """
    Calibrate SVI from market bid/ask quotes and compute mispricing.

    Parameters
    ----------
    quotes_df : DataFrame with columns:
        - strike   : option strike
        - bid_vol  : bid implied vol (annualised, e.g. 0.20 = 20%)
        - ask_vol  : ask implied vol (annualised)
        - mid_vol  : (optional) mid vol; computed as (bid+ask)/2 if absent
    F : forward / spot reference used for log-moneyness
    T : time to expiry in years

    Returns
    -------
    (SVIParams, augmented_df) where augmented_df adds:
      log_k           : log(strike / F)
      svi_vol         : SVI model vol at each strike
      model_price_call : Black-76 call price at SVI vol
      bid_price_call  : market bid call price (at bid_vol)
      ask_price_call  : market ask call price (at ask_vol)
      mid_price_call  : market mid call price
      mispricing      : (model_price - mid_price) / half_spread
                        > +1: model expensive vs market (market is cheap)
                        < -1: model cheap vs market (market is expensive)
    """
    df = quotes_df.copy()

    # Ensure mid_vol
    if "mid_vol" not in df.columns:
        df["mid_vol"] = (df["bid_vol"] + df["ask_vol"]) / 2.0

    df["log_k"] = np.log(df["strike"].values / F)

    # Fit SVI on mid vols
    log_strikes = df["log_k"].values
    mid_vols = df["mid_vol"].values
    half_spread = ((df["ask_vol"] - df["bid_vol"]) / 2.0).values
    weights = np.where(half_spread > 1e-4, 1.0 / half_spread, 1.0)

    params = fit_svi(log_strikes, mid_vols, T, weights)

    # Compute SVI vol at each strike
    df["svi_vol"] = [svi_vol(k, params, T) for k in log_strikes]

    # Compute Black-76 prices (r=0 for simplicity; caller can override if needed)
    r = 0.0
    def _call_price(strike: float, vol: float) -> float:
        if vol <= 0 or T <= 0:
            return max(F - strike, 0.0)
        return b76_price(F, strike, T, r, vol, flag="c")

    df["model_price_call"] = df.apply(lambda row: _call_price(row["strike"], row["svi_vol"]), axis=1)
    df["bid_price_call"]   = df.apply(lambda row: _call_price(row["strike"], row["bid_vol"]),  axis=1)
    df["ask_price_call"]   = df.apply(lambda row: _call_price(row["strike"], row["ask_vol"]),  axis=1)
    df["mid_price_call"]   = (df["bid_price_call"] + df["ask_price_call"]) / 2.0

    # Mispricing in units of half-spread
    half_price_spread = (df["ask_price_call"] - df["bid_price_call"]) / 2.0
    # Avoid division by zero for very narrow spreads
    denom = half_price_spread.clip(lower=1e-8)
    df["mispricing"] = (df["model_price_call"] - df["mid_price_call"]) / denom

    return params, df
