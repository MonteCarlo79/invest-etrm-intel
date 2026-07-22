"""Mark-to-Market valuation for open positions.

Uses forward curves to value remaining open position volume
against entry/contract prices.
"""
from __future__ import annotations

import pandas as pd
from typing import Any


def get_forward_price(
    curves: pd.DataFrame,
    province: str,
    delivery_date: pd.Timestamp,
    delivery_hour: int | None = None,
) -> float | None:
    """Look up forward price from curves DataFrame.

    Args:
        curves: DataFrame with columns: province, delivery_date, delivery_hour,
                price_cny_kwh, curve_date
        province: Province to filter on
        delivery_date: Target delivery date
        delivery_hour: Target hour (None for daily average)

    Returns:
        Price in CNY/MWh (converted from CNY/kWh), or None if not found.
    """
    mask = (curves["province"] == province) & (curves["delivery_date"] == delivery_date)
    if delivery_hour is not None:
        mask = mask & (curves["delivery_hour"] == delivery_hour)

    subset = curves[mask]
    if subset.empty:
        return None

    latest = subset.sort_values("curve_date", ascending=False).iloc[0]
    return float(latest["price_cny_kwh"]) * 1000.0


def compute_mtm(
    positions: list[dict[str, Any]],
    forward_prices: dict[str, float],
) -> list[dict[str, Any]]:
    """Compute unrealised MtM P&L for open positions.

    Args:
        positions: list of position dicts with keys:
            direction, volume_mwh, price_cny_mwh, province, start_date, end_date
        forward_prices: dict mapping province → current forward price (CNY/MWh)

    Returns:
        List of position dicts enriched with 'unrealized_pnl_cny' and 'forward_price_cny_mwh'.
    """
    results = []
    for pos in positions:
        province = pos["province"]
        entry_price = float(pos.get("price_cny_mwh", 0) or 0)
        volume = float(pos.get("volume_mwh", 0) or 0)
        direction = pos.get("direction", "buy")
        fwd_price = forward_prices.get(province, entry_price)

        if direction == "buy":
            unrealized = (fwd_price - entry_price) * volume
        else:
            unrealized = (entry_price - fwd_price) * volume

        result = dict(pos)
        result["forward_price_cny_mwh"] = fwd_price
        result["unrealized_pnl_cny"] = unrealized
        results.append(result)

    return results
