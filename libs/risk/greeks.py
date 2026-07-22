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
