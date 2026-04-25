"""
libs/options/structures.py

Multi-leg option structure builder.

Supported structures
--------------------
  vanilla     — single call or put
  straddle    — long call + long put at same strike
  strangle    — long OTM call + long OTM put at different strikes
  bull_spread — long call at K_low + short call at K_high (call debit spread)
  bear_spread — long put at K_high + short put at K_low (put debit spread)
  butterfly   — long K_low + short 2x K_atm + long K_high (call butterfly)
  condor      — long K1 + short K2 + short K3 + long K4 (K1 < K2 < K3 < K4, call condor)

All legs priced with Black-Scholes (mode="bs") or Black-76 (mode="b76").
Greeks are the net sum across legs.
Payoff profile is sampled at expiry over a range centred on the strikes.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import List

from libs.options.black_scholes import bs_price, b76_price, bs_greeks, b76_greeks


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Leg:
    flag: str       # "c" or "p"
    strike: float
    quantity: float # +1 = long, -1 = short
    price: float    # option premium (per unit notional)


@dataclass
class StructureResult:
    name: str
    legs: List[Leg]
    net_premium: float      # total cost (positive = debit, negative = credit)
    delta: float
    gamma: float
    vega: float
    theta: float
    breakeven_lower: float | None
    breakeven_upper: float | None
    payoff_at_expiry: List[tuple]  # [(spot, payoff), ...]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _price_one(S_or_F: float, K: float, T: float, r: float, sigma: float,
               q: float, flag: str, mode: str) -> float:
    if mode == "b76":
        return b76_price(S_or_F, K, T, r, sigma, flag)
    return bs_price(S_or_F, K, T, r, sigma, q, flag)


def _greeks_one(S_or_F: float, K: float, T: float, r: float, sigma: float,
                q: float, flag: str, mode: str) -> dict:
    if mode == "b76":
        return b76_greeks(S_or_F, K, T, r, sigma, flag)
    return bs_greeks(S_or_F, K, T, r, sigma, q, flag)


def _payoff_call(S: float, K: float, qty: float, premium: float) -> float:
    """Expiry P&L for a single call leg (premium already paid/received)."""
    return qty * (max(S - K, 0.0) - premium)


def _payoff_put(S: float, K: float, qty: float, premium: float) -> float:
    """Expiry P&L for a single put leg."""
    return qty * (max(K - S, 0.0) - premium)


def _profile(legs: List[Leg], spot_range: list) -> List[tuple]:
    """Compute expiry payoff profile across spot_range."""
    result = []
    for S in spot_range:
        pnl = 0.0
        for leg in legs:
            if leg.flag == "c":
                pnl += _payoff_call(S, leg.strike, leg.quantity, leg.price)
            else:
                pnl += _payoff_put(S, leg.strike, leg.quantity, leg.price)
        result.append((S, pnl))
    return result


def _spot_range(all_strikes: list, n: int = 200) -> list:
    lo = min(all_strikes) * 0.5
    hi = max(all_strikes) * 1.5
    step = (hi - lo) / (n - 1)
    return [lo + i * step for i in range(n)]


def _net_greeks(legs_greeks: list, quantities: list) -> dict:
    keys = ("delta", "gamma", "vega", "theta", "rho")
    net = {k: 0.0 for k in keys}
    for g, q in zip(legs_greeks, quantities):
        for k in keys:
            net[k] += q * g.get(k, 0.0)
    return net


def _find_breakevens(profile: List[tuple]) -> tuple:
    """Find lower and upper breakeven spots from the payoff profile."""
    lower = None
    upper = None
    for i in range(len(profile) - 1):
        s0, p0 = profile[i]
        s1, p1 = profile[i + 1]
        if p0 * p1 < 0:
            # Linear interpolation
            be = s0 + (s1 - s0) * (-p0) / (p1 - p0)
            if lower is None:
                lower = be
            else:
                upper = be
    return lower, upper


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_structure(
    name: str,
    S_or_F: float,
    T: float,
    r: float,
    sigma: float,
    q: float = 0.0,
    mode: str = "bs",
    # per-structure params
    strike: float = None,
    flag: str = "c",
    strike_atm: float = None,
    strike_low: float = None,
    strike_high: float = None,
    strike_lo2: float = None,
    strike_hi2: float = None,
) -> StructureResult:
    """
    Build a named multi-leg structure and return pricing + payoff data.

    Parameters
    ----------
    name : one of vanilla | straddle | strangle | bull_spread | bear_spread |
                  butterfly | condor
    S_or_F : spot (mode="bs") or futures price (mode="b76")
    T : time to expiry in years
    r : risk-free rate
    sigma : flat implied vol used for all legs (smile not modelled here)
    q : dividend yield (ignored for b76)
    mode : "bs" or "b76"
    strike : used by vanilla
    flag : "c" or "p", used by vanilla and spreads
    strike_atm : ATM strike (straddle, butterfly)
    strike_low : lower wing (strangle, spreads, butterfly, condor)
    strike_high : upper wing (strangle, spreads, butterfly, condor)
    strike_lo2 : inner lower wing (condor)
    strike_hi2 : inner upper wing (condor)
    """
    name_lower = name.lower()

    if name_lower == "vanilla":
        K = strike if strike is not None else S_or_F
        p = _price_one(S_or_F, K, T, r, sigma, q, flag, mode)
        g = _greeks_one(S_or_F, K, T, r, sigma, q, flag, mode)
        legs = [Leg(flag=flag, strike=K, quantity=1.0, price=p)]
        profile = _profile(legs, _spot_range([K]))
        be_lo, be_hi = _find_breakevens(profile)
        return StructureResult(
            name="Vanilla " + ("Call" if flag == "c" else "Put"),
            legs=legs, net_premium=p,
            delta=g["delta"], gamma=g["gamma"], vega=g["vega"], theta=g["theta"],
            breakeven_lower=be_lo, breakeven_upper=be_hi,
            payoff_at_expiry=profile,
        )

    if name_lower == "straddle":
        K = strike_atm if strike_atm is not None else S_or_F
        pc = _price_one(S_or_F, K, T, r, sigma, q, "c", mode)
        pp = _price_one(S_or_F, K, T, r, sigma, q, "p", mode)
        gc = _greeks_one(S_or_F, K, T, r, sigma, q, "c", mode)
        gp = _greeks_one(S_or_F, K, T, r, sigma, q, "p", mode)
        legs = [
            Leg(flag="c", strike=K, quantity=1.0, price=pc),
            Leg(flag="p", strike=K, quantity=1.0, price=pp),
        ]
        net = pc + pp
        ng = _net_greeks([gc, gp], [1.0, 1.0])
        profile = _profile(legs, _spot_range([K]))
        be_lo, be_hi = _find_breakevens(profile)
        return StructureResult(
            name="Straddle", legs=legs, net_premium=net,
            delta=ng["delta"], gamma=ng["gamma"], vega=ng["vega"], theta=ng["theta"],
            breakeven_lower=be_lo, breakeven_upper=be_hi,
            payoff_at_expiry=profile,
        )

    if name_lower == "strangle":
        Kl = strike_low if strike_low is not None else S_or_F * 0.95
        Kh = strike_high if strike_high is not None else S_or_F * 1.05
        pp = _price_one(S_or_F, Kl, T, r, sigma, q, "p", mode)
        pc = _price_one(S_or_F, Kh, T, r, sigma, q, "c", mode)
        gp = _greeks_one(S_or_F, Kl, T, r, sigma, q, "p", mode)
        gc = _greeks_one(S_or_F, Kh, T, r, sigma, q, "c", mode)
        legs = [
            Leg(flag="p", strike=Kl, quantity=1.0, price=pp),
            Leg(flag="c", strike=Kh, quantity=1.0, price=pc),
        ]
        net = pp + pc
        ng = _net_greeks([gp, gc], [1.0, 1.0])
        profile = _profile(legs, _spot_range([Kl, Kh]))
        be_lo, be_hi = _find_breakevens(profile)
        return StructureResult(
            name="Strangle", legs=legs, net_premium=net,
            delta=ng["delta"], gamma=ng["gamma"], vega=ng["vega"], theta=ng["theta"],
            breakeven_lower=be_lo, breakeven_upper=be_hi,
            payoff_at_expiry=profile,
        )

    if name_lower == "bull_spread":
        Kl = strike_low if strike_low is not None else S_or_F * 0.95
        Kh = strike_high if strike_high is not None else S_or_F * 1.05
        # Long call at K_low, short call at K_high
        pl = _price_one(S_or_F, Kl, T, r, sigma, q, "c", mode)
        ph = _price_one(S_or_F, Kh, T, r, sigma, q, "c", mode)
        gl = _greeks_one(S_or_F, Kl, T, r, sigma, q, "c", mode)
        gh = _greeks_one(S_or_F, Kh, T, r, sigma, q, "c", mode)
        legs = [
            Leg(flag="c", strike=Kl, quantity=1.0, price=pl),
            Leg(flag="c", strike=Kh, quantity=-1.0, price=ph),
        ]
        net = pl - ph
        ng = _net_greeks([gl, gh], [1.0, -1.0])
        profile = _profile(legs, _spot_range([Kl, Kh]))
        be_lo, be_hi = _find_breakevens(profile)
        return StructureResult(
            name="Bull Spread (Call Debit)", legs=legs, net_premium=net,
            delta=ng["delta"], gamma=ng["gamma"], vega=ng["vega"], theta=ng["theta"],
            breakeven_lower=be_lo, breakeven_upper=be_hi,
            payoff_at_expiry=profile,
        )

    if name_lower == "bear_spread":
        Kl = strike_low if strike_low is not None else S_or_F * 0.95
        Kh = strike_high if strike_high is not None else S_or_F * 1.05
        # Long put at K_high, short put at K_low
        ph = _price_one(S_or_F, Kh, T, r, sigma, q, "p", mode)
        pl = _price_one(S_or_F, Kl, T, r, sigma, q, "p", mode)
        gh = _greeks_one(S_or_F, Kh, T, r, sigma, q, "p", mode)
        gl = _greeks_one(S_or_F, Kl, T, r, sigma, q, "p", mode)
        legs = [
            Leg(flag="p", strike=Kh, quantity=1.0, price=ph),
            Leg(flag="p", strike=Kl, quantity=-1.0, price=pl),
        ]
        net = ph - pl
        ng = _net_greeks([gh, gl], [1.0, -1.0])
        profile = _profile(legs, _spot_range([Kl, Kh]))
        be_lo, be_hi = _find_breakevens(profile)
        return StructureResult(
            name="Bear Spread (Put Debit)", legs=legs, net_premium=net,
            delta=ng["delta"], gamma=ng["gamma"], vega=ng["vega"], theta=ng["theta"],
            breakeven_lower=be_lo, breakeven_upper=be_hi,
            payoff_at_expiry=profile,
        )

    if name_lower == "butterfly":
        Kl = strike_low if strike_low is not None else S_or_F * 0.95
        Km = strike_atm if strike_atm is not None else S_or_F
        Kh = strike_high if strike_high is not None else S_or_F * 1.05
        pl = _price_one(S_or_F, Kl, T, r, sigma, q, "c", mode)
        pm = _price_one(S_or_F, Km, T, r, sigma, q, "c", mode)
        ph = _price_one(S_or_F, Kh, T, r, sigma, q, "c", mode)
        gl = _greeks_one(S_or_F, Kl, T, r, sigma, q, "c", mode)
        gm = _greeks_one(S_or_F, Km, T, r, sigma, q, "c", mode)
        gh = _greeks_one(S_or_F, Kh, T, r, sigma, q, "c", mode)
        legs = [
            Leg(flag="c", strike=Kl, quantity=1.0, price=pl),
            Leg(flag="c", strike=Km, quantity=-2.0, price=pm),
            Leg(flag="c", strike=Kh, quantity=1.0, price=ph),
        ]
        net = pl - 2.0 * pm + ph
        ng = _net_greeks([gl, gm, gh], [1.0, -2.0, 1.0])
        profile = _profile(legs, _spot_range([Kl, Kh]))
        be_lo, be_hi = _find_breakevens(profile)
        return StructureResult(
            name="Butterfly (Call)", legs=legs, net_premium=net,
            delta=ng["delta"], gamma=ng["gamma"], vega=ng["vega"], theta=ng["theta"],
            breakeven_lower=be_lo, breakeven_upper=be_hi,
            payoff_at_expiry=profile,
        )

    if name_lower == "condor":
        # Long K1, short K2, short K3, long K4 (K1 < K2 < K3 < K4)
        K1 = strike_low if strike_low is not None else S_or_F * 0.90
        K2 = strike_lo2 if strike_lo2 is not None else S_or_F * 0.95
        K3 = strike_hi2 if strike_hi2 is not None else S_or_F * 1.05
        K4 = strike_high if strike_high is not None else S_or_F * 1.10
        p1 = _price_one(S_or_F, K1, T, r, sigma, q, "c", mode)
        p2 = _price_one(S_or_F, K2, T, r, sigma, q, "c", mode)
        p3 = _price_one(S_or_F, K3, T, r, sigma, q, "c", mode)
        p4 = _price_one(S_or_F, K4, T, r, sigma, q, "c", mode)
        g1 = _greeks_one(S_or_F, K1, T, r, sigma, q, "c", mode)
        g2 = _greeks_one(S_or_F, K2, T, r, sigma, q, "c", mode)
        g3 = _greeks_one(S_or_F, K3, T, r, sigma, q, "c", mode)
        g4 = _greeks_one(S_or_F, K4, T, r, sigma, q, "c", mode)
        legs = [
            Leg(flag="c", strike=K1, quantity=1.0, price=p1),
            Leg(flag="c", strike=K2, quantity=-1.0, price=p2),
            Leg(flag="c", strike=K3, quantity=-1.0, price=p3),
            Leg(flag="c", strike=K4, quantity=1.0, price=p4),
        ]
        net = p1 - p2 - p3 + p4
        ng = _net_greeks([g1, g2, g3, g4], [1.0, -1.0, -1.0, 1.0])
        profile = _profile(legs, _spot_range([K1, K4]))
        be_lo, be_hi = _find_breakevens(profile)
        return StructureResult(
            name="Condor (Call)", legs=legs, net_premium=net,
            delta=ng["delta"], gamma=ng["gamma"], vega=ng["vega"], theta=ng["theta"],
            breakeven_lower=be_lo, breakeven_upper=be_hi,
            payoff_at_expiry=profile,
        )

    raise ValueError(f"Unknown structure name: {name!r}. "
                     f"Expected one of: vanilla, straddle, strangle, bull_spread, "
                     f"bear_spread, butterfly, condor.")
