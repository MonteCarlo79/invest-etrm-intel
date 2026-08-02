"""Parser for BESS discharge settlement PDFs (电费结算单 / 上网电费结算单).

These are SCANNED IMAGE PDFs. Uses Claude Vision (via Bedrock) to OCR and extract
the simple table structure:

| 成分明细 | 电量 | 电价 | 电费 | 电费年月 |
|---------|------|------|------|---------|
| 现货     | vol  | price| amt  | YYYY-MM |
| 非市场化  | vol  | price| amt  | YYYY-MM |
| 调频     | vol  | price| amt  | YYYY-MM |
| 机组合计  | vol  | price| amt  |         |

Units: 电量=千千瓦时(MWh), 电价=元/千千瓦时(CNY/MWh), 电费=元(CNY)
"""
from __future__ import annotations

import base64
import json
import os
from typing import Any

from shared.anthropic_client import make_client as _make_anthropic_client


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
成分明细 (category), 电量 (volume in MWh), 电价 (price in CNY/MWh), 电费 (amount in CNY), 电费年月 (month).

Return ONLY a JSON array of objects with these exact keys:
- "category_cn": the Chinese category name (现货, 非市场化, 调频, etc.)
- "volume_mwh": number (电量, already in MWh since unit is 千千瓦时=MWh)
- "price_cny_mwh": number (电价 in 元/千千瓦时 = CNY/MWh)
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

        items.append({
            "category": category,
            "volume_mwh": row.get("volume_mwh"),
            "price_cny_kwh": (row.get("price_cny_mwh") or 0) / 1000.0,  # CNY/MWh → CNY/kWh
            "amount_cny": row.get("amount_cny", 0),
            "notes": f"放电结算: {category_cn}",
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
