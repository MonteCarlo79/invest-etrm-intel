"""Parser for BESS discharge settlement PDFs (电费结算单 / 上网电费结算单).

These are SCANNED IMAGE PDFs. Uses Claude Vision (via Bedrock) to OCR and extract
the simple table structure:

| 成分明细 | 电量 | 电价 | 电费 | 电费年月 |
|---------|------|------|------|---------|
| 现货     | vol  | price| amt  | YYYY-MM |
| 非市场化  | vol  | price| amt  | YYYY-MM |
| 调频     | vol  | price| amt  | YYYY-MM |
| 机组合计  | vol  | price| amt  |         |

Units: usually 电量=千千瓦时(MWh), 电价=元/千千瓦时(CNY/MWh), 电费=元(CNY).
WARNING: some invoices (e.g. small commercial stations like 乌兰察布) print
电量 in 千瓦时(kWh) and 电价 in 元/千瓦时 — the prompt reports the printed
units and _normalize_units() converts to MWh / CNY/kWh, with a volume×price
vs amount cross-check as safety net.
"""
from __future__ import annotations

import base64
import json
import os
from typing import Any

from shared.anthropic_client import make_client as _make_anthropic_client


def _volume_to_mwh(value: float, unit: str) -> float:
    """Convert printed volume to MWh using the printed unit."""
    u = (unit or "").replace(" ", "")
    if "千千瓦" in u or "兆瓦" in u or "MWh" in u:
        return float(value)
    if "千瓦" in u or "kWh" in u:
        return float(value) / 1000.0
    # Unit unreadable: default to the common 千千瓦时 (MWh) convention
    return float(value)


def _price_to_cny_kwh(value: float, unit: str) -> float:
    """Convert printed price to CNY/kWh using the printed unit."""
    u = (unit or "").replace(" ", "")
    if "千千瓦" in u or "兆瓦" in u or "MWh" in u:
        return float(value) / 1000.0
    if "千瓦" in u or "kWh" in u:
        return float(value)
    return float(value) / 1000.0


def _normalize_units(volume: float | None, volume_unit: str,
                     price: float | None, price_unit: str,
                     amount_cny: float | None) -> tuple[float | None, float | None, bool]:
    """Normalize volume→MWh and price→CNY/kWh.

    Returns (volume_mwh, price_cny_kwh, corrected). `corrected` is True when
    the volume×price≈amount cross-check disagreed with the stated units by
    ~1000x and the units were flipped (misread or misprinted unit).
    """
    if volume is None:
        return None, None, False
    vol_mwh = _volume_to_mwh(volume, volume_unit)
    price_kwh = _price_to_cny_kwh(price, price_unit) if price is not None else None

    # Cross-check: amount ≈ volume_mwh × price_cny_kwh × 1000
    # ~1000x high → volume was kWh misread as MWh (fix volume only).
    # ~1000x low  → price was CNY/kWh misread as CNY/MWh (fix price only).
    # (A double misread cancels out in this check — the printed-unit prompt
    # is the defense there.)
    if amount_cny and price_kwh and vol_mwh:
        implied = vol_mwh * price_kwh * 1000.0
        ratio = implied / float(amount_cny) if float(amount_cny) != 0 else 1.0
        if 500.0 <= abs(ratio) <= 2000.0:
            vol_mwh = vol_mwh / 1000.0
            return vol_mwh, price_kwh, True
        if 1 / 2000.0 <= abs(ratio) <= 1 / 500.0:
            price_kwh = price_kwh * 1000.0
            return vol_mwh, price_kwh, True
    return vol_mwh, price_kwh, False


def parse_discharge_settlement_pdf(file_path: str) -> list[dict[str, Any]]:
    """Parse a discharge settlement PDF (or image) using Claude Vision.

    Delegates to the format-agnostic parser in parser_vision — works with any
    provincial grid company layout (蒙西, 甘肃, etc.), not just the Mengxi table.

    Args:
        file_path: Path to the PDF or image file

    Returns:
        List of settlement item dicts with: category, volume_mwh, price_cny_kwh, amount_cny, notes
    """
    from services.settlement_ingest.parser_vision import parse_settlement_bill_vision
    return parse_settlement_bill_vision(file_path, side="discharge")



def _map_category(category_cn: str) -> str:
    """Map Chinese category name to rm_settlement_items category enum."""
    if "现货" in category_cn:
        return "discharge_energy"
    elif "非市场" in category_cn or "容量" in category_cn:
        return "capacity_compensation"
    elif "调频" in category_cn or "辅助" in category_cn:
        return "frequency"
    elif "补贴" in category_cn:
        return "subsidy"
    return "other"


def _pdf_page_to_image(file_path: str, page_num: int = 0) -> bytes | None:
    """Convert a PDF page to PNG image bytes.

    Uses pdfplumber to extract the embedded image directly (faster than rendering).
    Falls back to pdf2image if no embedded image found.
    """
    import pdfplumber

    pdf = pdfplumber.open(file_path)
    if page_num >= len(pdf.pages):
        pdf.close()
        return None

    page = pdf.pages[page_num]

    # If page has embedded images, extract the largest one
    if page.images:
        # Get the image stream directly
        largest = max(page.images, key=lambda img: img.get("width", 0) * img.get("height", 0))
        stream = largest.get("stream")
        if stream:
            raw_data = stream.get_data()
            # DCTDecode = JPEG
            filters = stream.get("/Filter")
            if filters and "DCTDecode" in str(filters):
                pdf.close()
                return raw_data

    # Fallback: render page to image using page.to_image()
    try:
        im = page.to_image(resolution=200)
        import io
        buf = io.BytesIO()
        im.original.save(buf, format="PNG")
        pdf.close()
        return buf.getvalue()
    except Exception:
        pass

    pdf.close()
    return None
