"""国网安徽 下网 电费明细账 / 电费计算明细 — deterministic parse of OCR text.

The scanned/photographed charge sheets (定远 2024 PNG/JPG, 2025-09 分次结算 scans)
print a self-check: 市场化交易电费 + 输配电费 + 尖峰 + 线损 + 系统运行费用合计
+ 代收政府性基金合计 + 力调/调整电费 [+ 其他费用及运营费用合计] == (结算)电费合计.

Each 电费年月 block becomes one record in parser_scan_charge's schema, so the same
reconcile / 分次结算 / spilled-sheet merge rules (build_items_from_page_records)
apply as on the vision path — an OCR misread fails the identity and returns [],
letting the router fall through to vision.
"""
from __future__ import annotations

import re
from typing import Any

from services.settlement_ingest.parser_scan_charge import build_items_from_page_records

_NUM = r"-?\d[\d,]*(?:\.\d+)?"
_SKIP = r"[ \t|：:]*"

# 合计 rows are attributed to the block whose keyword sits nearest before them
_SUM_BLOCKS = {
    "system_operation_cny": ("系统运行",),
    "govt_fund_cny": ("代收政府性", "可再生能源", "重大水利"),
    "other_fees_cny": ("市场平衡", "启动成本", "其他费用", "必开机组"),
}


def is_ah_grid_charge(text: str) -> bool:
    return ("电费年月" in text and "市场化交易电费" in text
            and ("电费明细账" in text or "电费计算明细" in text))


def _f(tok: str) -> float:
    return float(tok.replace(",", ""))


def _after(text: str, label: str) -> float | None:
    """Last number in the run right after ``label`` on the same line (OCR prefixes
    stray digits: "市场化交易电费：2 2822166.83", "有功电量：6 6765704")."""
    m = re.search(label + _SKIP + r"((?:" + _NUM + r"[ \t|]*)+)", text)
    return _f(re.findall(_NUM, m.group(1))[-1]) if m else None


def _record(block: str) -> dict[str, Any]:
    rec: dict[str, Any] = {"format": "电费计算明细"}
    m = re.search(r"电费年月" + _SKIP + r"(\d{6})", block)
    rec["billing_month"] = m.group(1) if m else None
    m = re.search(r"分次结算\s*(第1期|终期|合并)", block)
    rec["settlement_kind"] = m.group(1) if m else None
    rec["total_cny"] = _after(block, "结算电费合计")
    if rec["total_cny"] is None:
        rec["total_cny"] = _after(block, r"(?<!结算)电费合计")
    rec["active_kwh"] = _after(block, "有功电量")
    rec["market_energy_cny"] = _after(block, "市场化交易电费")
    rec["transmission_cny"] = _after(block, "输配电费")
    rec["line_loss_cny"] = _after(block, "线损费用")
    rec["power_factor_adj_cny"] = _after(block, r"(?:调整电费|(?<!参与)力调电费(?!损益))")
    m = re.search(r"(?:季节性尖峰电费" + _SKIP + r"[^=\n]*=|尖峰加收)" + _SKIP + "(" + _NUM + ")", block)
    rec["peak_surcharge_cny"] = _f(m.group(1)) if m else None
    rec["govt_fund_cny"] = _after(block, "代征合计")

    prev = 0
    for m in re.finditer(r"(?<![费征])合计" + _SKIP + "(" + _NUM + ")", block):
        window = block[prev:m.start()]
        prev = m.end()
        best = max(((max(window.rfind(k) for k in kws), key) for key, kws in _SUM_BLOCKS.items()),
                   default=(-1, None))
        if best[0] < 0:
            continue
        if rec.get(best[1]) is not None:     # two 合计 rows claim one block → refuse
            rec["total_cny"] = None
            rec["billing_month"] = None
            break
        rec[best[1]] = _f(m.group(1))
    return rec


def parse_ah_grid_charge_text(text: str) -> list[dict[str, Any]]:
    text = re.sub(r"(\d)\.\s+(\d)", r"\1.\2", text)          # OCR "16. 17" → "16.17"
    blocks = re.split(r"(?=电费年月)", text)[1:]
    if not blocks:
        return []
    recs = [_record(b) for b in blocks]
    if not any(r["settlement_kind"] for r in recs):
        for r in recs:
            r["settlement_kind"] = "全月"
    items = build_items_from_page_records(recs) or []
    # OCR text is exact: demand a cent-level tie to the printed totals (the vision
    # tolerance of 0.01% would accept a misread digit on a ¥2.8M sheet)
    used = [r for r in recs if r["total_cny"] is not None]
    if any(r["settlement_kind"] == "合并" for r in used):
        used = [r for r in used if r["settlement_kind"] == "合并"]
    if items and abs(sum(i["amount_cny"] for i in items) + sum(r["total_cny"] for r in used)) > 0.05:
        return []
    return items
