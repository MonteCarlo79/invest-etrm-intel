"""Book-level Greeks aggregation.

Delta: net MWh exposure from open linear positions.
Gamma/Vega: aggregated from libs/options/ when option positions exist;
            otherwise 0.
"""
from __future__ import annotations

from typing import Any


def compute_book_greeks(positions: list[dict[str, Any]]) -> dict[str, float]:
    """Compute aggregated Greeks for a trading book.

    Args:
        positions: list of position dicts with keys:
            direction ('buy'/'sell'), volume_mwh, status ('open'/'closed'/'expired')

    Returns:
        dict with keys: delta_mwh, gamma, vega
    """
    delta = 0.0
    gamma = 0.0
    vega = 0.0

    for pos in positions:
        if pos.get("status") != "open":
            continue

        volume = float(pos.get("volume_mwh", 0) or 0)
        direction = pos.get("direction", "buy")

        if direction == "buy":
            delta += volume
        else:
            delta -= volume

        gamma += float(pos.get("gamma", 0) or 0)
        vega += float(pos.get("vega", 0) or 0)

    return {
        "delta_mwh": delta,
        "gamma": gamma,
        "vega": vega,
    }


def option_greeks(option_positions, curve, vol: float, as_of) -> dict[str, float]:
    """Book Black-76 Greeks for option positions, scaled by signed volume.

    Forward = curve mark for (province, delivery_month, tou_block or 'base').
    T = years from `as_of` to delivery-month start; T <= 0 → intrinsic-only
    (delta ±1 if in the money else 0; gamma/vega/theta 0). Rows without a
    forward mark are skipped and counted in `n_unpriced` (int).
    Units: delta in MWh (CNY per CNY/MWh), gamma in MWh per CNY/MWh, vega in
    CNY per 0.01 vol, theta in CNY per calendar day; T uses calendar days / 365.
    """
    import pandas as pd

    from libs.curves.curve import curve_lookup
    from libs.options.black_scholes import b76_greeks
    from libs.positions.conventions import signed_mwh

    out = {"delta_mwh": 0.0, "gamma": 0.0, "vega": 0.0, "theta": 0.0}
    n_unpriced = 0
    if option_positions is None or option_positions.empty:
        return {**out, "n_unpriced": 0}
    as_of_ts = pd.Timestamp(as_of)
    for r in option_positions.to_dict("records"):
        block = r.get("tou_block") or "base"
        F = curve_lookup(curve, r["province"], r["delivery_month"], block)
        if F is None:
            n_unpriced += 1
            continue
        K = float(r["strike_cny_mwh"])
        flag = "c" if str(r["option_type"]).lower().startswith("c") else "p"
        q = signed_mwh(r["direction"], float(r["volume_mwh"]))
        ts = pd.Timestamp(r["delivery_month"])
        T = max((pd.Timestamp(ts.year, ts.month, 1) - as_of_ts).days / 365.0, 0.0)
        if T <= 0.0 or vol <= 0.0:
            if flag == "c":
                g = {"delta": 1.0 if F > K else 0.0}
            else:
                g = {"delta": -1.0 if F < K else 0.0}
            g.update(gamma=0.0, vega=0.0, theta=0.0)
        else:
            g = b76_greeks(F, K, T, 0.0, vol, flag=flag)
        out["delta_mwh"] += q * g["delta"]
        out["gamma"] += q * g["gamma"]
        out["vega"] += q * g["vega"]
        out["theta"] += q * g["theta"]
    return {**{k: float(v) for k, v in out.items()}, "n_unpriced": n_unpriced}
