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
    """Parse a discharge settlement PDF (scanned image) using Claude Vision.

    Args:
        file_path: Path to the scanned PDF file

    Returns:
        List of settlement item dicts with: category, volume_mwh, price_cny_kwh, amount_cny, notes
    """
    # Convert PDF page to image bytes
    image_bytes = _pdf_page_to_image(file_path, page_num=0)
    if not image_bytes:
        return []

    # Use Claude Vision to extract table
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    client = _make_anthropic_client(api_key)

    prompt = """Extract the settlement table from this image. The table has columns:
成分明细 (category), 电量 (volume), 电价 (price), 电费 (amount in CNY), 电费年月 (month).

IMPORTANT — report units exactly as printed on the invoice. Volume unit is usually
千千瓦时 (thousand kWh = MWh), but some invoices print 千瓦时 (kWh). Price unit is
usually 元/千千瓦时, but some print 元/千瓦时. Read the column headers / unit labels
carefully; do not assume.

Return ONLY a JSON array of objects with these exact keys:
- "category_cn": the Chinese category name (现货, 非市场化, 调频, etc.)
- "volume": number (电量, raw value as printed)
- "volume_unit": string ("千千瓦时", "千瓦时", or "兆瓦时" as printed)
- "price": number (电价, raw value as printed)
- "price_unit": string ("元/千千瓦时" or "元/千瓦时" as printed)
- "amount_cny": number (电费 in 元)
- "month": string (电费年月, format YYYY-MM)

Do NOT include the 机组合计 (total) row. Return raw JSON only, no markdown."""

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": base64.b64encode(image_bytes).decode("utf-8"),
                    },
                },
                {"type": "text", "text": prompt},
            ],
        }],
    )

    # Parse response
    text = response.content[0].text.strip()
    # Remove markdown code fences if present
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
        if text.endswith("```"):
            text = text[:-3]

    try:
        rows = json.loads(text)
    except json.JSONDecodeError:
        return []

    # Map to settlement items
    items = []
    for row in rows:
        category_cn = row.get("category_cn", "")
        category = _map_category(category_cn)

        # Backward-compatible field handling: new prompt returns raw value+unit,
        # older cached responses may return volume_mwh / price_cny_mwh directly
        if "volume" in row or "volume_unit" in row:
            vol_mwh, price_kwh, corrected = _normalize_units(
                row.get("volume"), row.get("volume_unit", ""),
                row.get("price"), row.get("price_unit", ""),
                row.get("amount_cny"),
            )
        else:
            vol_mwh, price_kwh, corrected = (
                row.get("volume_mwh"),
                (row.get("price_cny_mwh") or 0) / 1000.0 if row.get("price_cny_mwh") is not None else None,
                False,
            )

        notes = f"放电结算: {category_cn}"
        if corrected:
            notes += " [units auto-corrected: kWh invoice]"

        items.append({
            "category": category,
            "volume_mwh": vol_mwh,
            "price_cny_kwh": price_kwh,
            "amount_cny": row.get("amount_cny", 0),
            "notes": notes,
        })

    return items


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
