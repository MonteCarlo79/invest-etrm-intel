"""Parser for 国网甘肃 discharge settlement bills (上网电费结算单, text-layer PDF).

Gansu grid bills share a rigid layout (observed: 民勤储能 2026-01..06):

    上网电量 1190371 [千瓦时]  上网均价 0.408930 元/千瓦时  结算金额 486778.88元
    类别 电量 电价 电费
    一、购电费
    （一）电能量电费 1190371 448515.11
    （二）辅助服务交易 0 38263.77
    （三）系统运行费用 338314.98
    ...

These bills have a text layer — deterministic regex beats vision here.
The vision discharge parser expects the Mengxi 成分明细 layout and returns
nothing usable for this family (observed 2026-08-24: 5 of 6 months failed).

Units: 电量=千瓦时(kWh), 电价=元/千瓦时, 电费=元.
"""
from __future__ import annotations

import re
from typing import Any

import pdfplumber

GANSU_BILL_SIGNATURE = "国网甘肃省电力公司"

# 类别 rows we turn into settlement items: (row label, category, sign)
# Sign convention: take amounts AS PRINTED — the Gansu discharge bill signs its
# deductions negatively in the document (e.g. 两个细则费用 -88,646.4 in 2026-06)
# and computes 结算金额 as the sum of rows as printed:
#   430,687.06 + 424,087.34 − 88,646.40 + 36,505.54 = 802,633.54 (2026-06) ✓
_FEE_ROWS = [
    ("电能量电费", "discharge_energy", +1),
    # 系统运行费用 on the discharge bill is 辅助服务交易 / 省内调频服务 (frequency
    # revenue) per the bill's own detail pages — NOT a system-operation fee
    # (observed 民勤 2026-05 detail: 系统运行费用 → 辅助服务交易 → 省内调频服务).
    ("系统运行费用", "frequency", +1),
    ("辅助服务交易", "frequency", +1),
    # 两个细则费用 = ancillary-services assessment (as printed, may be negative)
    ("两个细则费用", "frequency", +1),
    ("市场运营费用", "system_operation", +1),
    ("双轨制资金分摊", "system_operation", +1),
    ("偏差费用", "penalty", +1),
    ("容量费用", "capacity_compensation", +1),
    ("清算", "other", +1),
]


def _num(s: str) -> float:
    return float(s.replace(",", "").strip())


def is_gansu_bill(text: str) -> bool:
    """True if the text carries the 国网甘肃 bill signature."""
    return GANSU_BILL_SIGNATURE in text and "上网电量" in text


def _row_amount(text: str, label: str) -> float | None:
    """Amount (last decimal number) of a 类别 fee row, or None.

    Rows look like "（一）电能量电费 1190371 448515.11" or "（二）系统运行费用 338314.98"
    — the volume column is an integer (kWh, no decimals) and may be absent.
    The amount always carries decimals. The optional volume group must end in
    whitespace, otherwise backtracking splits "338314.98" into "33831" + "4.98"
    (observed 2026-05).
    """
    m = re.search(re.escape(label) + r"\s+(?:-\s+|/\s*|\d[\d,]*\s+)?(-?[\d,]+\.\d{1,2})(?=\s|$)", text)
    return _num(m.group(1)) if m else None


def _row_volume_kwh(text: str, label: str) -> float | None:
    """Integer volume (kWh) preceding the amount on a fee row, or None."""
    m = re.search(re.escape(label) + r"\s+(\d[\d,]*)\s+[\d,]+\.\d{2}", text)
    return _num(m.group(1)) if m else None


def parse_gansu_discharge_text(text: str) -> list[dict[str, Any]]:
    """Parse a Gansu discharge bill's text into settlement items.

    Returns items with: category, volume_mwh, price_cny_kwh, amount_cny, notes.
    Empty list when the key fields are missing.
    """
    # The summary 类别 table (本期电费明细) ends at the 二、机制电费 marker.
    # Pages 2+ carry detail sections where the same labels recur with amounts —
    # parsing past the marker double-counts them (observed: 辅助服务交易
    # matching the 系统运行费用 detail total, 2026-05).
    text = text.split("二、机制电费")[0]

    # Total settlement amount (label and value are adjacent across months)
    m_total = re.search(r"结算金额\s*([\d,]+\.\d{2})\s*元", text)
    # Volume: prefer the 电能量电费 row's integer volume (kWh); fall back to
    # an inline 上网电量 label or a detached "NNN千瓦" figure — pdfplumber's
    # reading order detaches values from labels in some months (observed 2026-05).
    vol_kwh = _row_volume_kwh(text, "电能量电费")
    if vol_kwh is None:
        m_vol = re.search(r"上网电量\s+([\d,]+)", text)
        vol_kwh = _num(m_vol.group(1)) if m_vol else None
    if vol_kwh is None:
        m_vol = re.search(r"([\d,]+)\s*千瓦", text)
        vol_kwh = _num(m_vol.group(1)) if m_vol else None

    if not (m_total and vol_kwh):
        return []

    vol_mwh = vol_kwh / 1000.0
    total_cny = _num(m_total.group(1))

    items: list[dict[str, Any]] = []
    fee_sum = 0.0
    for label, category, sign in _FEE_ROWS:
        amt = _row_amount(text, label)
        if amt is None or amt == 0:
            continue
        signed = sign * amt
        fee_sum += signed
        items.append({
            "category": category,
            "volume_mwh": vol_mwh if label == "电能量电费" else None,
            "price_cny_kwh": None,
            "amount_cny": signed,
            "notes": f"甘肃上网结算单: {label}",
        })

    if not items:
        return []

    # Validation: extracted fees must sum to 结算金额 within a rounding yuan
    if abs(fee_sum - total_cny) > 1.0:
        return []

    return items


def parse_gansu_discharge_pdf(file_path: str) -> list[dict[str, Any]]:
    """File-level wrapper: extract text from all pages, then parse."""
    pdf = pdfplumber.open(file_path)
    text = ""
    for page in pdf.pages:
        t = page.extract_text()
        if t:
            text += t + "\n"
    pdf.close()
    return parse_gansu_discharge_text(text)


GANSU_CHARGE_SIGNATURE_MARKERS = ("市场化购电电费", "输配电量电费")

# Gansu 下网 charge bill 费用组成 rows: (label, category)
# Amounts are taken AS PRINTED, then NEGATED (charge-side P&L convention:
# costs are negative; a 退补 refund bill prints negative amounts, which then
# become positive P&L — observed 民勤 2026-04（退补）: -610,866.63 → +610,866.63).
# Validation: sum of rows as printed == 合计 (the bill's own arithmetic check).
_CHARGE_FEE_ROWS = [
    ("市场化购电电费", "charge_energy"),
    ("输配电量电费", "transmission"),
    ("输配容（需）量电费", "basic_fee"),
    ("上网环节线损费用", "system_operation"),
    # Gansu charge-side 系统运行费 is the system-operation charge (Mengxi's
    # 燃气容量电费 family) → 系统运行费 column, NOT 上网线损费.
    ("系统运行费", "coal_capacity_charge"),
    ("功率因数调整电费", "basic_fee"),
    ("政府性基金及附加", "govt_surcharges"),
    ("代理服务费", "other"),
    ("非市场化电费", "other"),
    ("自备电厂系统备用费", "other"),
    ("市场运营费用及不平衡资金", "system_operation"),
    ("退补电费", "charge_energy"),
    ("分次结算清算费用", "other"),
]


def is_gansu_charge_bill(text: str) -> bool:
    """True for the Gansu 下网 charge bill layout (费用组成 with ①市场化购电电费)."""
    return all(m in text for m in GANSU_CHARGE_SIGNATURE_MARKERS)


def parse_gansu_charge_text(text: str) -> list[dict[str, Any]]:
    """Parse a Gansu 下网 charge bill's text into settlement items.

    Layout (observed 民勤储能下网电费结算单 2026-01 + 04（退补）):
      本期电量 1551995千瓦时 本期电费 179574.29元
      费用组成 计收数量 电费
      ①市场化购电电费 1551995 179574.29
      ②输配电量电费 1551995 0.00
      ...
      合计 ¥179574.29
    """
    # Truncate at 备注 — the 用能分析 panel after it holds 峰平谷 volume tables
    # whose numbers previously leaked into 系统运行费 (the -1,142,286 junk).
    text = text.split("备注")[0]

    # Volume (kWh) and total (元) from the header line
    m_vol = re.search(r"本期电量\s*([\d,]+)\s*千瓦时", text)
    m_total = re.search(r"本期电费\s*(-?[\d,]+\.\d{1,2})\s*元", text)
    if not (m_vol and m_total):
        return []
    vol_mwh = _num(m_vol.group(1)) / 1000.0
    total_cny = _num(m_total.group(1))

    items: list[dict[str, Any]] = []
    printed_sum = 0.0
    for label, category in _CHARGE_FEE_ROWS:
        # Row: ①市场化购电电费 1551995 179574.29  — volume may be "/" (none)
        amt = _row_amount(text, label)
        if amt is None or amt == 0:
            continue
        printed_sum += amt
        items.append({
            "category": category,
            "volume_mwh": vol_mwh if category == "charge_energy" and vol_mwh > 0 else None,
            "price_cny_kwh": None,
            "amount_cny": -amt,  # negate: printed cost → negative P&L
            "notes": f"甘肃下网结算单: {label}",
        })

    if not items:
        return []

    # Validation: rows as printed must sum to 合计/本期电费
    if abs(printed_sum - total_cny) > 1.0:
        return []

    return items


def parse_gansu_charge_pdf(file_path: str) -> list[dict[str, Any]]:
    """File-level wrapper for the Gansu charge parser."""
    pdf = pdfplumber.open(file_path)
    text = ""
    for page in pdf.pages:
        t = page.extract_text()
        if t:
            text += t + "\n"
    pdf.close()
    return parse_gansu_charge_text(text)
