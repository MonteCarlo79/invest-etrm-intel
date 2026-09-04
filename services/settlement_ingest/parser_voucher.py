"""Parser for 发电侧结算凭证 (trading-center generation-side settlement voucher).

Text-layer PDF issued by the exchange. Used as the discharge (上网) data source
for months that have no grid 上网结算单 (observed: 乌海 2026-01/02 — vouchers only).

Key fields (兆瓦时/元 declared on the 单位 header line):
  月度上网电量 12559.32   → volume (MWh)
  电能电费 4213848.61     → amount (CNY)
  现货市场月度加权均价 335.581 → price (CNY/MWh)
"""
from __future__ import annotations

import re
from typing import Any

import pdfplumber


def _num(s: str) -> float:
    return float(s.replace(",", "").strip())


def parse_generation_voucher(file_path: str) -> list[dict[str, Any]]:
    """Parse a 发电侧结算凭证 into settlement items.

    Returns one discharge_energy item, or [] if the key fields are missing.
    """
    pdf = pdfplumber.open(file_path)
    text = ""
    for page in pdf.pages:
        t = page.extract_text()
        if t:
            text += t + "\n"
    pdf.close()

    return parse_generation_voucher_text(text)


def parse_generation_voucher_text(text: str) -> list[dict[str, Any]]:
    """Text-level parse (separated for unit testing)."""
    m_vol = re.search(r"月度上网电量\s+([\d,.]+)", text)
    m_amt = re.search(r"电能电费\s+([\d,.]+)", text)
    if not (m_vol and m_amt):
        return []

    vol = _num(m_vol.group(1))
    amt = _num(m_amt.group(1))

    # Unit guard: the voucher declares 单位:兆瓦时、元/兆瓦时、元 (gen side) —
    # but if a voucher ever declares 千瓦时, convert volume kWh → MWh.
    m_unit = re.search(r"单位[：:]\s*([^\n]+)", text)
    unit_line = m_unit.group(1) if m_unit else ""
    vol_mwh = vol if "兆瓦时" in unit_line else (vol / 1000.0 if "千瓦时" in unit_line else vol)

    price_kwh = None
    m_price = re.search(r"现货市场月度加权均价\s+([\d.]+)", text)
    if m_price:
        price = _num(m_price.group(1))
        # price unit mirrors the volume unit (元/兆瓦时 vs 元/千瓦时)
        price_kwh = price / 1000.0 if "兆瓦时" in unit_line else (price if "千瓦时" in unit_line else price / 1000.0)

    return [{
        "category": "discharge_energy",
        "volume_mwh": vol_mwh,
        "price_cny_kwh": price_kwh,
        "amount_cny": amt,
        "notes": "发电侧结算凭证: 电能电费",
    }]


def parse_capcomp_table_text(text: str, station_name: str) -> float | None:
    """Extract one station's capacity compensation from a provincial 统计表.

    The 储能容量补偿费用统计表 is a province-wide document listing every storage
    station's monthly compensation. Two row layouts exist:
      2025-04..11: "6 <公司> <电站> 7,125,694.16 7,125,694.16"
      2025-12:     "6 <公司> <电站> - 6,421,466.08 6,421,466.08"  (dash = no prior clearing)
    Returns the 补偿费用 (first numeric after the station name), or None.
    """
    m = re.search(re.escape(station_name) + r"\s+(?:-\s+)?([\d,]+\.\d{2})", text)
    return _num(m.group(1)) if m else None


def parse_capcomp_table(file_path: str, station_name: str) -> float | None:
    """File-level wrapper for parse_capcomp_table_text."""
    import pdfplumber
    pdf = pdfplumber.open(file_path)
    text = ""
    for page in pdf.pages:
        t = page.extract_text()
        if t:
            text += t + "\n"
    pdf.close()
    return parse_capcomp_table_text(text, station_name)
