"""Generic vision parser for provincial settlement bills (any grid company).

Layout-agnostic: instead of assuming the 蒙西 table structure, the prompt asks
Claude Vision to read whatever table the bill contains and report line items
with their printed units. Unit normalization + category mapping happen in code.

Used for:
- discharge (上网/发电侧) bills from any province
- charge (下网/购电) bills as a fallback when the regex parser (parser_charge)
  finds nothing (e.g. 甘肃 民勤 format — observed 2026-08-23)
"""
from __future__ import annotations

import base64
import json
import os
from typing import Any

from shared.anthropic_client import make_client as _make_anthropic_client
from services.settlement_ingest.parser_discharge import (
    _pdf_page_to_image, _normalize_units,
)


def map_settlement_category(label: str, side: str = "discharge") -> str:
    """Map an arbitrary Chinese settlement line-item label to the canonical category.

    side: "discharge" (energy lines → discharge_energy) or
          "charge" (energy lines → charge_energy).
    Ordering matters: specific fee labels before generic energy keywords.
    """
    s = label or ""
    energy_cat = "discharge_energy" if side == "discharge" else "charge_energy"
    if "燃煤" in s or "燃气" in s or "容量电费" in s:
        return "coal_capacity_charge"
    if "容量补偿" in s or "容量电价" in s or "非市场" in s:
        return "capacity_compensation"
    if "功率因数" in s or "力调" in s or "基本电费" in s or "容(需)量" in s or "容（需）量" in s:
        return "basic_fee"
    if "系统运行" in s or "线损" in s:
        return "system_operation"
    if "政府" in s or "基金" in s or "附加" in s:
        return "govt_surcharges"
    if "输配" in s or "输电" in s:
        return "transmission"
    if "调频" in s or "辅助" in s or "两个细则" in s:
        return "frequency"
    if "偏差" in s or "考核" in s:
        return "penalty"
    if "补贴" in s:
        return "subsidy"
    if "退补" in s or "返还" in s:
        return "rebate" if side == "discharge" else energy_cat
    if "现货" in s or "上网" in s or "电能电费" in s or "电能量" in s or "购电" in s or "下网" in s:
        return energy_cat
    return "other"


_PROMPT_TEMPLATE = """This image is a monthly electricity settlement bill issued by a Chinese provincial grid company or power exchange, for an energy storage station's {side_cn} side ({side_en}).

Layouts vary by province and issuer — do NOT assume any specific table structure. Read whatever is printed.

Extract every fee/revenue line item. Return ONLY a JSON array of objects with these keys:
- "item_cn": the line-item name exactly as printed (e.g. 上网电费, 现货电费, 电能电费, 容量补偿费用, 输配电费, 系统运行费, 政府性基金及附加, 偏差考核费用)
- "volume": number, raw value as printed (null if the line has no volume)
- "volume_unit": "千瓦时" / "千千瓦时" / "兆瓦时" exactly as printed (null if none)
- "price": number, raw value as printed (null if none)
- "price_unit": e.g. "元/千瓦时" or "元/千千瓦时" as printed (null if none)
- "amount_cny": number, the line's amount in 元 (negative if it is a charge/deduction)
- "month": settlement month as printed (YYYY-MM), or null

Skip subtotal/total rows (合计/总计/机组合计) and rows where everything is zero.
Return raw JSON only, no markdown."""


def parse_settlement_bill_vision(file_path: str, side: str = "discharge") -> list[dict[str, Any]]:
    """Parse any provincial settlement bill (PDF or image) via Claude Vision.

    Args:
        file_path: PDF or PNG/JPG/WEBP image
        side: "discharge" (上网/发电侧) or "charge" (下网/购电)

    Returns:
        List of settlement item dicts: category, volume_mwh, price_cny_kwh,
        amount_cny, month, notes
    """
    # Convert input to image bytes (first two pages max — bills may span 2 pages)
    ext = file_path.lower().rsplit(".", 1)[-1]
    image_media_types = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg", "webp": "image/webp"}
    pages = []
    if ext in image_media_types:
        with open(file_path, "rb") as f:
            pages.append((f.read(), image_media_types[ext]))
    else:
        for page_num in (0, 1):
            img = _pdf_page_to_image(file_path, page_num=page_num)
            if img:
                pages.append((img, "image/png"))
    if not pages:
        return []

    side_cn = "放电（上网/发电侧）" if side == "discharge" else "充电（下网/购电侧）"
    prompt = _PROMPT_TEMPLATE.format(side_cn=side_cn, side_en=side)

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    client = _make_anthropic_client(api_key)

    rows = []
    for image_bytes, media_type in pages:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image",
                     "source": {"type": "base64", "media_type": media_type,
                                "data": base64.b64encode(image_bytes).decode("utf-8")}},
                    {"type": "text", "text": prompt},
                ],
            }],
        )
        text = response.content[0].text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1]
            if text.endswith("```"):
                text = text[:-3]
        try:
            page_rows = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(page_rows, list):
            rows.extend(page_rows)

    items = []
    for row in rows:
        label = row.get("item_cn", "")
        category = map_settlement_category(label, side)

        vol_mwh, price_kwh, corrected = _normalize_units(
            row.get("volume"), row.get("volume_unit", ""),
            row.get("price"), row.get("price_unit", ""),
            row.get("amount_cny"),
        )

        amount = row.get("amount_cny", 0)
        # Charge side convention: charging = cost = negative (matches parser_charge)
        if side == "charge" and amount and amount > 0:
            amount = -amount

        notes = f"{'放电' if side == 'discharge' else '充电'}结算: {label}"
        if corrected:
            notes += " [units auto-corrected]"

        items.append({
            "category": category,
            "volume_mwh": vol_mwh,
            "price_cny_kwh": price_kwh,
            "amount_cny": amount,
            "month": row.get("month"),
            "notes": notes,
        })

    return items


def parse_charge_bill_vision(file_path: str) -> list[dict[str, Any]]:
    """Charge-side wrapper (下网/购电 bills in non-Mengxi layouts)."""
    return parse_settlement_bill_vision(file_path, side="charge")
