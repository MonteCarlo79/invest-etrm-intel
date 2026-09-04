"""P&L waterfall decomposition by settlement category.

Aggregates settlement items into a structured waterfall suitable
for Plotly waterfall chart rendering.
"""
from __future__ import annotations

import pandas as pd
from typing import Any

REVENUE_CATEGORIES = {
    "discharge_energy", "generation_revenue", "capacity_compensation",
    "bilateral_energy", "subsidy", "rebate",
}

COST_CATEGORIES = {
    "charge_energy", "transmission", "govt_surcharges", "system_operation",
    "coal_capacity_charge", "basic_fee", "penalty", "curtailment",
    "flex_fees", "imbalance", "market_redistribution", "rule_charges", "frequency",
}


def compute_pnl_waterfall(
    settlement_items: pd.DataFrame,
    asset_type: str,
) -> dict[str, float]:
    """Compute P&L waterfall from settlement items.

    Args:
        settlement_items: DataFrame with columns: category, amount_cny
        asset_type: 'bess', 'wind', 'solar', 'thermal'

    Returns:
        Dict mapping category → total amount, plus 'net_pnl' key.
    """
    result: dict[str, float] = {}
    net = 0.0

    for category, group in settlement_items.groupby("category"):
        total = float(group["amount_cny"].sum())
        result[str(category)] = total
        net += total

    result["net_pnl"] = net
    return result
