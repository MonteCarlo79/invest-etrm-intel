"""
libs/options/black_scholes.py

Pure-Python Black-Scholes and Black-76 option pricer with analytical Greeks
and an implied-vol solver (Newton-Raphson + bisection fallback).

No external dependencies — CDF via math.erfc (same approach as bess_spread_call_strip.py).

Functions
---------
bs_price(S, K, T, r, sigma, q, flag)  → float
b76_price(F, K, T, r, sigma, flag)     → float
bs_greeks(S, K, T, r, sigma, q, flag)  → dict
b76_greeks(F, K, T, r, sigma, flag)    → dict
implied_vol(market_price, S_or_F, K, T, r, flag, mode, q, tol, max_iter) → float
"""
from __future__ import annotations

import math

# ---------------------------------------------------------------------------
# CDF / PDF
# ---------------------------------------------------------------------------

def _norm_cdf(x: float) -> float:
    """Standard normal CDF via math.erfc — no scipy dependency."""
    return math.erfc(-x / math.sqrt(2)) / 2.0


def _norm_pdf(x: float) -> float:
    """Standard normal PDF."""
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


# ---------------------------------------------------------------------------
# d1 / d2 helpers
# ---------------------------------------------------------------------------

def _d1d2(F: float, K: float, T: float, sigma: float) -> tuple[float, float]:
    """
    d1 and d2 for forward F (pre-adjusted for dividends/discount by caller).
    Handles T=0 or sigma=0 by returning +inf / -inf (intrinsic payoff).
    """
    if T <= 0.0 or sigma <= 0.0:
        # Signals caller to fall back to intrinsic
        return (float("inf"), float("inf")) if F > K else (float("-inf"), float("-inf"))
    sq_T = math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma ** 2 * T) / (sigma * sq_T)
    d2 = d1 - sigma * sq_T
    return d1, d2


# ---------------------------------------------------------------------------
# Pricers
# ---------------------------------------------------------------------------

def bs_price(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    q: float = 0.0,
    flag: str = "c",
) -> float:
    """
    Black-Scholes option price for equity with continuous dividend yield q.

    Parameters
    ----------
    S : spot price
    K : strike
    T : time to expiry in years
    r : risk-free rate (annualised, e.g. 0.05 = 5%)
    sigma : implied volatility (annualised, e.g. 0.20 = 20%)
    q : continuous dividend yield (default 0)
    flag : "c" = call, "p" = put
    """
    if T <= 0.0:
        intrinsic = max(S - K, 0.0) if flag == "c" else max(K - S, 0.0)
        return intrinsic
    F = S * math.exp((r - q) * T)  # cost-of-carry adjusted forward
    d1, d2 = _d1d2(F, K, T, sigma)
    disc = math.exp(-r * T)
    if flag == "c":
        return disc * (F * _norm_cdf(d1) - K * _norm_cdf(d2))
    else:
        return disc * (K * _norm_cdf(-d2) - F * _norm_cdf(-d1))


def b76_price(
    F: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    flag: str = "c",
) -> float:
    """
    Black-76 option price for futures / forwards.
    Equivalent to bs_price(S=F, K=K, T=T, r=r, sigma=sigma, q=r).
    """
    return bs_price(F, K, T, r, sigma, q=r, flag=flag)


# ---------------------------------------------------------------------------
# Greeks
# ---------------------------------------------------------------------------

def bs_greeks(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    q: float = 0.0,
    flag: str = "c",
) -> dict:
    """
    Analytical Black-Scholes Greeks.

    Returns
    -------
    dict with keys:
      delta   — dV/dS
      gamma   — d²V/dS²
      vega    — dV/d(sigma) per 1% move in vol (i.e. divided by 100)
      theta   — dV/dt per calendar day (negative for long options)
      rho     — dV/dr per 1% move in r (i.e. divided by 100)
    """
    if T <= 0.0 or sigma <= 0.0:
        return {"delta": 0.0, "gamma": 0.0, "vega": 0.0, "theta": 0.0, "rho": 0.0}

    F = S * math.exp((r - q) * T)
    d1, d2 = _d1d2(F, K, T, sigma)
    disc = math.exp(-r * T)
    disc_q = math.exp(-q * T)
    pdf_d1 = _norm_pdf(d1)
    sq_T = math.sqrt(T)

    if flag == "c":
        delta = disc_q * _norm_cdf(d1)
        rho_raw = K * T * disc * _norm_cdf(d2)
    else:
        delta = -disc_q * _norm_cdf(-d1)
        rho_raw = -K * T * disc * _norm_cdf(-d2)

    gamma = disc_q * pdf_d1 / (S * sigma * sq_T)

    # Vega: dV / d(sigma) → per 1% move = divide by 100
    vega_raw = S * disc_q * pdf_d1 * sq_T
    vega = vega_raw / 100.0

    # Theta: dV/dt (annualised) → per calendar day = divide by 365
    if flag == "c":
        theta_ann = (
            -S * disc_q * pdf_d1 * sigma / (2.0 * sq_T)
            - r * K * disc * _norm_cdf(d2)
            + q * S * disc_q * _norm_cdf(d1)
        )
    else:
        theta_ann = (
            -S * disc_q * pdf_d1 * sigma / (2.0 * sq_T)
            + r * K * disc * _norm_cdf(-d2)
            - q * S * disc_q * _norm_cdf(-d1)
        )
    theta = theta_ann / 365.0

    return {
        "delta": delta,
        "gamma": gamma,
        "vega": vega,
        "theta": theta,
        "rho": rho_raw / 100.0,
    }


def b76_greeks(
    F: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    flag: str = "c",
) -> dict:
    """Black-76 analytical Greeks (futures/forwards)."""
    return bs_greeks(F, K, T, r, sigma, q=r, flag=flag)


# ---------------------------------------------------------------------------
# Implied vol solver
# ---------------------------------------------------------------------------

def implied_vol(
    market_price: float,
    S_or_F: float,
    K: float,
    T: float,
    r: float,
    flag: str = "c",
    mode: str = "bs",
    q: float = 0.0,
    tol: float = 1e-7,
    max_iter: int = 100,
) -> float:
    """
    Implied volatility via Newton-Raphson with bisection fallback.

    Parameters
    ----------
    market_price : observed option market price
    S_or_F : spot (mode="bs") or forward (mode="b76")
    K, T, r, flag : standard option parameters
    mode : "bs" = Black-Scholes, "b76" = Black-76
    q : dividend yield (only used when mode="bs")
    tol : convergence tolerance on price error
    max_iter : maximum iterations

    Returns
    -------
    float : implied vol (annualised). Returns float("nan") if not solvable
            (below intrinsic, near-zero vega, or no convergence).
    """
    pricer = b76_price if mode == "b76" else bs_price
    q_eff = r if mode == "b76" else q

    # Intrinsic bound check
    disc = math.exp(-r * T)
    if flag == "c":
        intrinsic = max(S_or_F * math.exp(-q_eff * T) - K * disc, 0.0)
    else:
        intrinsic = max(K * disc - S_or_F * math.exp(-q_eff * T), 0.0)

    if market_price < intrinsic - tol:
        return float("nan")
    if market_price < tol:
        return float("nan")

    def _price(sig: float) -> float:
        if mode == "b76":
            return b76_price(S_or_F, K, T, r, sig, flag)
        return bs_price(S_or_F, K, T, r, sig, q, flag)

    def _vega(sig: float) -> float:
        if mode == "b76":
            return b76_greeks(S_or_F, K, T, r, sig, flag)["vega"] * 100.0
        return bs_greeks(S_or_F, K, T, r, sig, q, flag)["vega"] * 100.0

    # Newton-Raphson starting from a reasonable initial guess
    sigma = 0.20
    for _ in range(max_iter):
        price = _price(sigma)
        err = price - market_price
        if abs(err) < tol:
            return sigma
        vega = _vega(sigma)
        if abs(vega) < 1e-12:
            break
        sigma -= err / vega
        if sigma <= 0.0:
            sigma = 0.001
        if sigma > 20.0:
            sigma = 20.0

    # Bisection fallback
    lo, hi = 1e-4, 10.0
    p_lo = _price(lo) - market_price
    p_hi = _price(hi) - market_price
    if p_lo * p_hi > 0:
        return float("nan")

    for _ in range(200):
        mid = (lo + hi) / 2.0
        p_mid = _price(mid) - market_price
        if abs(p_mid) < tol or (hi - lo) / 2.0 < tol:
            return mid
        if p_lo * p_mid < 0:
            hi, p_hi = mid, p_mid
        else:
            lo, p_lo = mid, p_mid

    return (lo + hi) / 2.0
