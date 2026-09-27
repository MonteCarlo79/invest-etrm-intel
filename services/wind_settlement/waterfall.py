# -*- coding: utf-8 -*-
"""收益瀑布 helpers: assemble the settlement cascade from bill items + replicated inputs.

The bill's own 现货 row nets spot value and contract CfD together, so the
waterfall replaces it with the two replicated components; every other bill
line is a fee that flows through as printed.
"""
from __future__ import annotations

import pandas as pd


def waterfall_components(
    bill_items: pd.DataFrame,
    spot_value: float,
    cfd: float,
    green: float,
) -> list[tuple[str, float]]:
    """Ordered (label, value) cascade: spot + CfD + green − fees.

    bill_items: rm_settlement_items rows for the month (category/volume_mwh/
    amount_cny/notes). The 现货 energy rows and 绿电/填电 rows are excluded
    from the fee bucket — they are represented by the replicated inputs.
    """
    amt = bill_items["amount_cny"].fillna(0.0)
    notes = bill_items["notes"].fillna("")
    cat = bill_items["category"].fillna("")

    is_spot_row = (cat == "discharge_energy") & notes.str.contains("现货")
    is_green_row = notes.str.contains("绿电|填电")
    fees = bill_items[~(is_spot_row | is_green_row)]
    fee_amt = fees["amount_cny"].fillna(0.0)
    fee_cat = fees["category"].fillna("")

    storage = float(fee_amt[fee_cat == "capacity_compensation"].sum())
    freq = float(fee_amt[fee_cat == "frequency"].sum())
    other = float(fee_amt[~fee_cat.isin(["capacity_compensation", "frequency"])].sum())

    return [
        ("现货电能价值", float(spot_value)),
        ("合约差价", float(cfd)),
        ("绿电溢价", float(green)),
        ("储能分摊", storage),
        ("调频", freq),
        ("其他费用", other),
    ]


def settle_price(bill_spot_rows: pd.DataFrame) -> float | None:
    """All-in settlement price (元/MWh) from the bill's 现货 rows."""
    vol = bill_spot_rows["volume_mwh"].dropna().sum()
    if vol == 0:
        return None
    return float(bill_spot_rows["amount_cny"].dropna().sum() / vol)
