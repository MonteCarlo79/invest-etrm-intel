"""Rule-based settlement item categorization.

Includes province-specific settlement rules (e.g., Mengxi wind).
"""
from __future__ import annotations

from typing import Any


def categorize_items(
    items: list[dict[str, Any]],
    asset_type: str,
    province: str,
) -> list[dict[str, Any]]:
    """Apply province+asset_type specific categorization rules to settlement items.

    For most cases, items arrive pre-categorized from the parser.
    This function validates and may reclassify based on rules.
    """
    result = []
    for item in items:
        categorized = dict(item)
        # Wind generation: reclassify discharge_energy → generation_revenue
        if asset_type in ("wind", "solar") and item["category"] == "discharge_energy":
            categorized["category"] = "generation_revenue"
        result.append(categorized)
    return result


def mengxi_wind_settlement(hourly: dict[str, Any]) -> dict[str, Any]:
    """Apply Inner Mongolia (Mengxi) wind settlement rule.

    Rule:
    - If generation <= DA volume: all settled at DA price
    - Residual above DA: settled at RT node price
    - Bilateral (annual) contract premium/discount applied on top

    Args:
        hourly: dict with keys: settled_mwh, da_volume_mwh, da_price_cny_mwh,
                rt_price_cny_mwh, annual_price_cny_mwh, annual_volume_mwh (optional)

    Returns:
        dict with: da_settled_mwh, rt_settled_mwh, pnl_cny, bilateral_premium_cny
    """
    settled = float(hourly.get("settled_mwh", 0) or 0)
    da_vol = float(hourly.get("da_volume_mwh", 0) or 0)
    da_price = float(hourly.get("da_price_cny_mwh", 0) or 0)
    rt_price = float(hourly.get("rt_price_cny_mwh", 0) or 0)
    annual_price = float(hourly.get("annual_price_cny_mwh", 0) or 0)
    annual_vol = float(hourly.get("annual_volume_mwh", 0) or 0)

    # Step 1: DA allocation (min of settled vs DA volume)
    da_settled = min(settled, da_vol)
    # Step 2: Residual at RT
    rt_settled = max(0.0, settled - da_vol)

    # Base P&L
    pnl = da_settled * da_price + rt_settled * rt_price

    # Step 3: Bilateral premium (annual contract volume at premium over DA)
    bilateral_premium = 0.0
    if annual_vol > 0 and annual_price > 0:
        # Premium is on the lesser of annual_vol and da_settled (bilateral replaces DA)
        bilateral_mwh = min(annual_vol, da_settled)
        bilateral_premium = bilateral_mwh * (annual_price - da_price)

    return {
        "da_settled_mwh": da_settled,
        "rt_settled_mwh": rt_settled,
        "pnl_cny": pnl,
        "bilateral_premium_cny": bilateral_premium,
    }
