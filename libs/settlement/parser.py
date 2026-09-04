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


def parse_pdf_settlement(file_path_or_bytes) -> list[dict[str, Any]]:
    """Parse PDF settlement statement (上网电费结算单) using pdfplumber.

    Extracts tables from the PDF and maps rows to settlement items.
    Handles both file paths and BytesIO objects.

    Args:
        file_path_or_bytes: Path string or file-like object (BytesIO)

    Returns:
        List of settlement item dicts with keys:
          category, volume_mwh, price_cny_kwh, amount_cny, peak_period, notes
    """
    import pdfplumber

    if isinstance(file_path_or_bytes, str):
        pdf = pdfplumber.open(file_path_or_bytes)
    else:
        pdf = pdfplumber.open(file_path_or_bytes)

    items = []
    for page in pdf.pages:
        tables = page.extract_tables()
        for table in tables:
            if not table or len(table) < 2:
                continue
            # Use first row as headers
            headers = [str(h).strip() if h else "" for h in table[0]]
            for row in table[1:]:
                if not row or all(c is None or str(c).strip() == "" for c in row):
                    continue
                row_dict = dict(zip(headers, row))

                # Detect category from row content
                category = _classify_pdf_row(row_dict)

                # Extract numeric fields
                volume = _safe_float(row_dict.get("电量", row_dict.get("结算电量", row_dict.get("MWh", ""))))
                price = _safe_float(row_dict.get("电价", row_dict.get("单价", row_dict.get("元/kWh", ""))))
                amount = _safe_float(row_dict.get("金额", row_dict.get("电费", row_dict.get("合计", ""))))

                # Detect TOU period
                peak_period = None
                for key in row_dict:
                    if "峰" in str(key) or "peak" in str(key).lower():
                        peak_period = "peak"
                    elif "谷" in str(key) or "valley" in str(key).lower():
                        peak_period = "valley"
                    elif "平" in str(key) or "flat" in str(key).lower():
                        peak_period = "flat"

                if amount is not None and amount != 0:
                    items.append({
                        "category": category,
                        "volume_mwh": volume,
                        "price_cny_kwh": price / 1000.0 if price and price > 1 else price,
                        "amount_cny": amount,
                        "peak_period": peak_period,
                        "notes": " | ".join(str(v) for v in row if v),
                    })

    pdf.close()
    return items


def _classify_pdf_row(row_dict: dict) -> str:
    """Classify a PDF table row into a settlement category."""
    text = " ".join(str(v) for v in row_dict.values() if v).lower()
    if "放电" in text or "discharge" in text:
        return "discharge_energy"
    elif "充电" in text or "charge" in text:
        return "charge_energy"
    elif "容量补偿" in text or "capacity" in text:
        return "capacity_compensation"
    elif "输配" in text or "输电" in text or "transmission" in text:
        return "transmission"
    elif "政府性基金" in text or "surcharge" in text:
        return "govt_surcharges"
    elif "系统运行" in text or "system" in text:
        return "system_operation"
    elif "煤电容量" in text or "coal" in text:
        return "coal_capacity_charge"
    elif "基本电费" in text or "basic" in text:
        return "basic_fee"
    elif "补贴" in text or "subsidy" in text:
        return "subsidy"
    elif "罚" in text or "penalty" in text:
        return "penalty"
    elif "发电" in text or "generation" in text:
        return "generation_revenue"
    return "other"


def _safe_float(value) -> float | None:
    """Safely convert a value to float, returning None on failure."""
    if value is None:
        return None
    try:
        # Remove commas, spaces, currency symbols
        cleaned = str(value).replace(",", "").replace("¥", "").replace(" ", "").strip()
        if not cleaned or cleaned == "-":
            return None
        return float(cleaned)
    except (ValueError, TypeError):
        return None
