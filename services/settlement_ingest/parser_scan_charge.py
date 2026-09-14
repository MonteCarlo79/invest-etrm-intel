"""Vision+verify parser for scanned 电费计算明细 (下网 charge sheets).

The sheet family (电网省公司 电费计算明细, e.g. 定远 2026) has a hard
self-check printed on every page:

    市场化交易电费 + 输配电费 + 尖峰加收 + 上网环节线损费用
    + 系统运行费用合计 + 代收政府性基金及附加费合计 + 力调电费 == 电费合计

Vision extracts only the ~9 totals per page (small surface, high accuracy);
a page's parse is ACCEPTED only when the identity reconciles within
max(¥1, 0.01% of 电费合计). Pages that fail verification, parse, or format
detection are refused to manual processing — the model cannot silently
corrupt amounts (the lesson of the vision-era settlement corruption).

分次结算 handling: sheets are 第1期 (advance, half-month) + 终期 (complement)
or 分次结算合并 (STANDALONE full-month). A PDF mixing an advance 第1期 sheet
and a 合并 sheet must ingest ONLY the 合并 sheet (April 定远 pattern).
"""
from __future__ import annotations

import base64
import json
import os
import re
from typing import Any

from shared.anthropic_client import make_client as _make_anthropic_client
from services.settlement_ingest.parser_discharge import _pdf_page_to_image

_PROMPT = """This is a scanned 电费计算明细 (electricity fee detail sheet) from a Chinese grid company.
Extract ONLY these fields as JSON (numbers exactly as printed — negative if printed negative):
{
  "format": "电费计算明细" or null,
  "billing_month": "YYYYMM",
  "settlement_kind": "第1期" | "终期" | "合并" (from the 分次结算第1期/终期/合并 label),
  "total_cny": number (电费合计, as printed),
  "active_kwh": number (有功电量, integer kWh),
  "market_energy_cny": number (市场化交易电费, as printed),
  "transmission_cny": number (输配电费),
  "peak_surcharge_cny": number (尖峰加收; 0 if the row is absent),
  "line_loss_cny": number (上网环节线损费用),
  "system_operation_cny": number (系统运行费用 — the 合计 row of that block ONLY, never sub-items),
  "govt_fund_cny": number (代收政府性基金及附加费 — the 合计 row of that block ONLY),
  "power_factor_adj_cny": number (力调电费, as printed; 0 if no such row)
}
Return raw JSON only, no markdown. If the image is NOT a 电费计算明细 sheet, return {"format": null}."""

# rows summed by the printed identity, in order
_IDENTITY_ROWS = ("market_energy_cny", "transmission_cny", "peak_surcharge_cny",
                  "line_loss_cny", "system_operation_cny", "govt_fund_cny",
                  "power_factor_adj_cny")

_ITEM_MAP = [
    ("market_energy_cny", "charge_energy", "市场化交易电费"),
    ("transmission_cny", "transmission", "输配电费"),
    ("peak_surcharge_cny", "other", "尖峰加收"),
    ("line_loss_cny", "system_operation", "上网环节线损费用"),
    ("system_operation_cny", "coal_capacity_charge", "系统运行费用"),
    ("govt_fund_cny", "govt_surcharges", "政府性基金及附加"),
    ("power_factor_adj_cny", "basic_fee", "力调电费"),
]


def _reconcile_ok(rec: dict) -> bool:
    """The printed identity must hold within max(¥1, 0.01% of 电费合计)."""
    total = float(rec.get("total_cny") or 0)
    rows = sum(float(rec.get(k) or 0) for k in _IDENTITY_ROWS)
    return abs(rows - total) <= max(1.0, 0.0001 * abs(total))


def build_items_from_page_records(records: list[dict]) -> list[dict[str, Any]] | None:
    """Apply 分次结算 rules and build charge items from extracted page records.

    Returns None (refuse) when: no records, a record fails format detection or
    reconciliation, or months are mixed. 合并 sheets supersede 第1期 advance
    sheets within the same document. stored = −printed (charge = cost).
    """
    if not records:
        return None
    for rec in records:
        if rec.get("format") != "电费计算明细":
            return None
        if not rec.get("billing_month"):
            return None
        if not _reconcile_ok(rec):
            return None
    months = {str(r["billing_month"]) for r in records}
    if len(months) != 1:
        return None

    merged = [r for r in records if r.get("settlement_kind") == "合并"]
    use = merged if merged else records

    items: list[dict[str, Any]] = []
    for rec in use:
        kind = rec.get("settlement_kind") or "?"
        vol = round(float(rec.get("active_kwh") or 0) / 1000.0, 3)
        for key, category, label in _ITEM_MAP:
            amt = float(rec.get(key) or 0)
            if amt == 0:
                continue
            items.append({
                "category": category,
                "volume_mwh": vol if key == "market_energy_cny" else None,
                "price_cny_kwh": None,
                "amount_cny": -amt,
                "notes": f"充电结算: {label} [{kind}]",
            })
    return items or None


def _extract_page_record(client, image_bytes: bytes) -> dict | None:
    """One vision call for one page → the extracted record dict, or None."""
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": [
                {"type": "image",
                 "source": {"type": "base64", "media_type": "image/png",
                            "data": base64.b64encode(image_bytes).decode("utf-8")}},
                {"type": "text", "text": _PROMPT},
            ],
        }],
    )
    text = response.content[0].text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
        if text.endswith("```"):
            text = text[:-3]
    try:
        rec = json.loads(text)
    except json.JSONDecodeError:
        return None
    return rec if isinstance(rec, dict) else None


def parse_scanned_charge_bill_vision(file_path: str) -> list[dict[str, Any]] | None:
    """Parse a scanned 电费计算明细 PDF via vision, verified per page.

    Returns charge items, or None when any page fails extraction or the
    printed identity does not reconcile (caller should refuse the upload).
    """
    import pdfplumber

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    client = _make_anthropic_client(api_key)

    records: list[dict] = []
    with pdfplumber.open(file_path) as pdf:
        n_pages = len(pdf.pages)
    for page_num in range(n_pages):
        img = _pdf_page_to_image(file_path, page_num=page_num)
        if not img:
            return None
        rec = _extract_page_record(client, img)
        if rec is None:
            return None
        records.append(rec)
    return build_items_from_page_records(records)
