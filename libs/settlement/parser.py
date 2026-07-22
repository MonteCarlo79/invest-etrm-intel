"""Multi-format settlement file parser.

Detects file format from sheet names and column signatures,
then parses to a canonical list of settlement item dicts.
"""
from __future__ import annotations

import pandas as pd
from typing import Any


def detect_format(xl: pd.ExcelFile) -> str:
    """Detect settlement file format from sheet names and column headers.

    Returns one of: 'trade_capture', 'capacity_compensation', 'subsidy',
                    'wind_farm_ops', 'unknown'
    """
    sheets = xl.sheet_names

    # Wind farm ops: has characteristic sheet names
    wind_sheets = {"风场功率", "结算明细", "市场价格", "经营统计"}
    if wind_sheets.issubset(set(sheets)):
        return "wind_farm_ops"

    # Trade Capture: has "Trades" sheet with expected columns
    if "Trades" in sheets:
        df = xl.parse("Trades", nrows=0)
        if "Volume (MWh)" in df.columns and "Price (¥/MWh)" in df.columns:
            return "trade_capture"

    # Capacity compensation or subsidy: wide-format with 应收/实际结算/差异
    first_sheet = sheets[0]
    df = xl.parse(first_sheet, nrows=5)
    cols = set(df.columns)
    if {"应收", "实际结算", "差异"}.issubset(cols):
        # Three-column pattern (receivable / settled / difference) is capacity compensation
        return "capacity_compensation"
    if {"应收金额", "实际结算金额"}.issubset(cols):
        return "subsidy"

    return "unknown"


def parse_trade_capture(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Parse Trade Capture 'Trades' sheet to canonical settlement items.

    Each row becomes one settlement item dict with keys:
      category, volume_mwh, price_cny_kwh, amount_cny, delivery_date,
      counterparty, peak_period, notes
    """
    items = []
    for _, row in df.iterrows():
        tx_type = str(row.get("Transactions Type", "")).lower().strip()
        if "discharge" in tx_type or "sell" in str(row.get("Buy/Sell", "")).lower():
            category = "discharge_energy"
        elif "charge" in tx_type or "buy" in str(row.get("Buy/Sell", "")).lower():
            category = "charge_energy"
        else:
            category = "other"

        items.append({
            "category": category,
            "delivery_date": pd.to_datetime(row.get("Date")).date() if pd.notna(row.get("Date")) else None,
            "volume_mwh": float(row.get("Volume (MWh)", 0)),
            "price_cny_kwh": float(row.get("Price (¥/MWh)", 0)) / 1000.0,
            "amount_cny": float(row.get("Total (¥)", 0)),
            "counterparty": row.get("Station Name"),
            "peak_period": None,
            "notes": row.get("Market"),
        })
    return items


def parse_capacity_compensation(xl: pd.ExcelFile) -> list[dict[str, Any]]:
    """Parse 容量补偿数据.xlsx — wide-format multi-station × multi-month.

    Melts to long format: one item per station per month.
    """
    df = xl.parse(xl.sheet_names[0])
    items = []
    for _, row in df.iterrows():
        items.append({
            "category": "capacity_compensation",
            "delivery_date": None,
            "volume_mwh": None,
            "price_cny_kwh": None,
            "amount_cny": float(row.get("实际结算", 0) or row.get("实际结算金额", 0)),
            "amount_receivable_cny": float(row.get("应收", 0) or row.get("应收金额", 0)),
            "amount_settled_cny": float(row.get("实际结算", 0) or row.get("实际结算金额", 0)),
            "amount_diff_cny": float(row.get("差异", 0) or 0),
            "counterparty": row.get("电站"),
            "peak_period": None,
            "notes": str(row.get("月份", "")),
        })
    return items
