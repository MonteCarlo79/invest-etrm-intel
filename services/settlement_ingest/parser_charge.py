"""Parser for BESS charging cost PDFs (电费清单 / 下网电费结算单).

These PDFs have extractable text. Key line items are extracted by regex
from the text content of pages 2-3.

Structure (Mengxi BESS):
1. 市场化购电费 = 电能电费 + 市场运行调整费用 + 合同偏差费用
2. 输配电费
3. 目录电费
4. 容(需)量电费
5. 上网环节线损费用
6. 绿电费用
7. 系统运行费（燃气容量电费）
8. 功率因数调整电费
9. 政府基金及附加
10. 退补电费
11. 其他电费
= 总电费
"""
from __future__ import annotations

import re
from typing import Any

import pdfplumber


def parse_charging_cost_pdf(file_path: str) -> list[dict[str, Any]]:
    """Parse a BESS charging cost PDF and return settlement items.

    Args:
        file_path: Path to the PDF file

    Returns:
        List of settlement item dicts with: category, volume_mwh, price_cny_kwh, amount_cny, notes
    """
    pdf = pdfplumber.open(file_path)

    # Extract all text from all pages
    full_text = ""
    for page in pdf.pages:
        text = page.extract_text()
        if text:
            full_text += text + "\n"
    pdf.close()

    if not full_text.strip():
        return []

    items = []

    # Extract total volume (计费电量) from first section
    total_volume_kwh = _extract_first_number(full_text, r'计费电量kWh\s+电价标准.*?元\s+([\d,.]+)')
    if not total_volume_kwh:
        # Try alternative: first large number after 计费电量
        m = re.search(r'([\d,]+\.\d)\s+[\d.]+\s+[\d,.]+\.\d{2}', full_text)
        if m:
            total_volume_kwh = _parse_number(m.group(1))

    volume_mwh = total_volume_kwh / 1000.0 if total_volume_kwh else None

    # Extract each category amount
    # 1. 电能电费
    amt = _extract_amount(full_text, r'电能电费元\s+([\d,.]+|-[\d,.]+)')
    if amt is not None:
        items.append({"category": "charge_energy", "volume_mwh": volume_mwh, "amount_cny": amt, "notes": "电能电费(市场化购电)"})

    # Market adjustment fee
    amt = _extract_amount(full_text, r'市场运行调整费用元\s+([\d,.]+|-[\d,.]+)')
    if amt and amt != 0:
        items.append({"category": "charge_energy", "amount_cny": amt, "notes": "市场运行调整费用"})

    # Contract deviation
    amt = _extract_amount(full_text, r'合同偏差电费元\s+([\d,.]+|-[\d,.]+)')
    if amt and amt != 0:
        items.append({"category": "penalty", "amount_cny": amt, "notes": "合同偏差费用"})

    # 2. 输配电费
    amt = _extract_amount(full_text, r'输配电费元\s+([\d,.]+|-[\d,.]+)')
    if amt and amt != 0:
        items.append({"category": "transmission", "amount_cny": amt, "notes": "输配电费"})

    # 3. 目录电费
    amt = _extract_amount(full_text, r'目录电费元\s+([\d,.]+|-[\d,.]+)')
    if amt and amt != 0:
        items.append({"category": "other", "amount_cny": amt, "notes": "目录电费"})

    # 4. 容(需)量电费
    amt = _extract_amount(full_text, r'容\(需\)量电费元\s+([\d,.]+|-[\d,.]+)')
    if amt and amt != 0:
        items.append({"category": "basic_fee", "amount_cny": amt, "notes": "容(需)量电费"})

    # 5. 上网环节线损费用
    amt = _extract_amount(full_text, r'上网环节线损费用元\s+([\d,.]+|-[\d,.]+)')
    if amt is not None and amt != 0:
        items.append({"category": "system_operation", "amount_cny": amt, "notes": "上网环节线损费用"})

    # 6. 绿电费用
    amt = _extract_amount(full_text, r'绿电结算费用元.*?([\d,.]+|-[\d,.]+)')
    if amt and amt != 0:
        items.append({"category": "other", "amount_cny": amt, "notes": "绿电费用"})

    # 7. 系统运行费
    amt = _extract_amount(full_text, r'系统运行费.*?([\d,.]+)\s*$', re.MULTILINE)
    if not amt:
        # Try: 系统运行费（燃气容量电费） followed by numbers
        m = re.search(r'系统运行费.*?(\d[\d,.]*\.\d{2})', full_text)
        if m:
            amt = _parse_number(m.group(1))
    if amt and amt != 0:
        items.append({"category": "coal_capacity_charge", "amount_cny": amt, "notes": "系统运行费(燃气容量电费)"})

    # 8. 功率因数调整电费
    amt = _extract_amount(full_text, r'功率因数调整电费元\s+([\d,.]+|-[\d,.]+)')
    if amt is not None and amt != 0:
        items.append({"category": "basic_fee", "amount_cny": amt, "notes": "功率因数调整电费"})

    # 9. 政府基金及附加
    amt = _extract_amount(full_text, r'政府基金及附加元\s+([\d,.]+|-[\d,.]+)')
    if amt is not None:
        items.append({"category": "govt_surcharges", "amount_cny": amt, "notes": "政府基金及附加"})

    # Total for validation
    total_amt = _extract_amount(full_text, r'总电费\(元\)\s*([\d,.]+|-[\d,.]+)')
    if total_amt:
        # Validate: sum of items should be close to total
        item_sum = sum(i["amount_cny"] for i in items)
        if abs(item_sum - total_amt) > 1.0:
            # Add residual as "other"
            residual = total_amt - item_sum
            if abs(residual) > 0.01:
                items.append({"category": "other", "amount_cny": residual, "notes": f"Residual (total={total_amt}, sum={item_sum})"})

    # Negate all amounts (charging = cost = negative for P&L)
    for item in items:
        if item["amount_cny"] > 0:
            item["amount_cny"] = -item["amount_cny"]

    return items


def _extract_amount(text: str, pattern: str, flags: int = 0) -> float | None:
    """Extract a numeric amount using regex pattern."""
    m = re.search(pattern, text, flags)
    if m:
        return _parse_number(m.group(1))
    return None


def _extract_first_number(text: str, pattern: str) -> float | None:
    """Extract the first number matching a pattern."""
    m = re.search(pattern, text)
    if m:
        return _parse_number(m.group(1))
    return None


def _parse_number(s: str) -> float | None:
    """Parse a number string (handles commas, negative signs)."""
    if not s:
        return None
    try:
        cleaned = s.replace(",", "").replace(" ", "").strip()
        if not cleaned or cleaned == "-":
            return None
        return float(cleaned)
    except (ValueError, TypeError):
        return None
