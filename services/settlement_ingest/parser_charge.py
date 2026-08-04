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

import os
import re
from typing import Any

import pdfplumber


def extract_billing_period(file_path_or_text: str) -> str | None:
    """Extract the billing period (YYYY-MM-01) from a charging cost PDF.

    Looks for date patterns like '2026-01-01' to '2026-01-31' on page 1,
    or '结算周期: 2026-01' in the text.

    Args:
        file_path_or_text: Either a file path or already-extracted text

    Returns:
        Settlement month as 'YYYY-MM-01' string, or None if not found.
    """
    if os.path.exists(file_path_or_text):
        pdf = pdfplumber.open(file_path_or_text)
        text = ""
        for page in pdf.pages[:2]:
            t = page.extract_text()
            if t:
                text += t + "\n"
        pdf.close()
    else:
        text = file_path_or_text

    # Pattern: 结算周期: YYYY-MM or 结算周期：YYYY-MM
    m = re.search(r'结算周期[：:]\s*(\d{4})-(\d{1,2})', text)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-01"

    # Pattern: YYYY-MM-01 (billing start date on page 1)
    m = re.search(r'(\d{4})-(\d{2})-01', text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"

    # Pattern: YYYY年MM月 in text
    m = re.search(r'(\d{4})年(\d{1,2})月', text)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-01"

    return None


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

    # Extract total charging volume (计费电量 kWh)
    # Strategy: find lines with pattern "VOLUME PRICE AMOUNT" where volume > 10000
    # (typical BESS monthly charge volume is 500,000 - 30,000,000 kWh)
    total_volume_kwh = None

    # Pattern 1: data rows "NUMBER.N NUMBER.NNNNNN NUMBER.NN" (volume, price, amount)
    # These appear after "工商业输配" or similar category labels
    data_rows = re.findall(r'([\d,]+\.?\d*)\s+([\d.]+)\s+([\d,.]+\.\d{2})', full_text)
    if data_rows:
        # Filter for plausible volumes (> 10000 kWh = 10 MWh minimum)
        volumes = []
        for vol_str, price_str, amt_str in data_rows:
            vol = _parse_number(vol_str)
            if vol and vol > 10000:  # > 10 MWh in kWh
                volumes.append(vol)
        if volumes:
            total_volume_kwh = max(volumes)

    # Pattern 2: page 1 header "= 计费电量" newline NUMBER
    if not total_volume_kwh:
        m = re.search(r'=\s*计费电量\s*\n\s*([\d,]+)', full_text)
        if m:
            v = _parse_number(m.group(1))
            if v and v > 1000:
                total_volume_kwh = v

    # Pattern 3: after "计费电量" in table header, the number on the next few lines
    if not total_volume_kwh:
        m = re.search(r'计费电量\s*\n.*?\n.*?([\d,]+)\s', full_text, re.DOTALL)
        if m:
            v = _parse_number(m.group(1))
            if v and v > 1000:
                total_volume_kwh = v

    volume_mwh = total_volume_kwh / 1000.0 if total_volume_kwh else None

    # Extract each category amount
    # 1. 电能电费
    amt = _extract_amount(full_text, r'电能电费元\s+([\d,.]+|-[\d,.]+)')
    if amt and amt != 0:
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
    if amt and amt != 0:
        items.append({"category": "govt_surcharges", "amount_cny": amt, "notes": "政府基金及附加"})

    # 10. 退补电费 (refund/supplement — sometimes the only non-zero line)
    # Pattern: "退补电费" section with a number, or the row "目录/输配电费 容量电费 ... <amount>"
    amt = _extract_amount(full_text, r'退补电费\s*\n.*?([\d,]+\.\d{2})')
    if not amt:
        # Alternative: look for the summary row after "退补电费" heading
        m = re.search(r'目录/输配电费\s+容量电费.*?([\d,]+\.\d{2})\s*$', full_text, re.MULTILINE)
        if m:
            amt = _parse_number(m.group(1))
    if amt and amt != 0:
        items.append({"category": "charge_energy", "amount_cny": amt, "notes": "退补电费"})

    # Total for validation — try multiple patterns
    total_amt = _extract_amount(full_text, r'总电费\s*[（(]元[)）]\s*\n?\s*([\d,.]+|-[\d,.]+)')
    if not total_amt:
        total_amt = _extract_amount(full_text, r'=总电费\(元\)\s*([\d,.]+|-[\d,.]+)')
    if not total_amt:
        # "电费构成 AMOUNT" on page 1
        total_amt = _extract_amount(full_text, r'电费构成\s+([\d,.]+|-[\d,.]+)')

    if total_amt:
        item_sum = sum(i["amount_cny"] for i in items)
        if abs(item_sum - total_amt) > 1.0:
            residual = total_amt - item_sum
            if abs(residual) > 0.01:
                items.append({"category": "charge_energy", "amount_cny": residual, "notes": f"总电费差额 (total={total_amt:,.0f})"})

    # If still no items found, use the total directly
    if not items and total_amt and total_amt != 0:
        items.append({"category": "charge_energy", "amount_cny": total_amt, "volume_mwh": volume_mwh, "notes": "总电费(全额)"})

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
