"""Deterministic parser for 网上国网 (State Grid) 电费账单 documents — 山东 format.

Two document families, both digital text-layer PDFs:

1. **发电侧上网电费账单** (discharge): page 1 账单概况 lists 上网电费 (the
   settlement TOTAL — equals header 结算金额 and the 合计 row) followed by its
   decomposing components (实时偏差电费, 两个细则费用, 调频服务补偿费用,
   容量补偿费, 优发优购曲线匹配偏差费, 跨月偏差退补费, 运行成本补偿费用).
   Page 2 repeats the identical table (本期电费明细) — vision read both pages
   and double-counted every row (observed 滨州 2026: all records 2-4x).
   Rule: parse page 1 ONLY, drop 上网电费 and 合计, keep components, verify
   their sum equals the dropped total. 实时偏差电费 is the RT energy
   settlement → discharge_energy (carries volume+price).

2. **用电侧下网电费账单** (charge): page 1 账单概况 is a 3-level hierarchy:
   工商业电费 (grand total) → numbered groups ①..⑤ → leaf fees. Groups whose
   children are printed on page 1 (①市场化购电电费, ③输配电费, ④系统运行费)
   are dropped; the leaf rows kept. ②上网环节线损费用 and ⑤政府性基金及附加
   have their children only on page 2 — they are themselves the leaf rows.
   Sum must equal 合计. Sign convention: stored = −printed (charge = cost).

Labels may wrap mid-word with the amount landing on the next line, and the
right-hand 用能分析 column interleaves text into some lines — both handled.
"""
from __future__ import annotations

import re
from typing import Any

_NUM = r"-?\d+(?:\.\d+)?"
_TOTAL_LINE = re.compile(r"^(合计|工商业电费)")

# discharge component → category (实时偏差电费 mapped explicitly to discharge_energy)
_DISCHARGE_CAT = {
    "实时偏差电费": "discharge_energy",
    "两个细则费用": "frequency",
    "调频服务补偿费用": "frequency",
    "调频辅助服务补偿": "frequency",
    "容量补偿费": "capacity_compensation",
    "容量补偿费用": "capacity_compensation",
    "优发优购曲线匹配偏差费": "penalty",
    "跨月偏差退补费": "rebate",
    "运行成本补偿费用": "other",
}

# charge leaf fee → category
_CHARGE_CAT = {
    "直接交易电费": "charge_energy",
    "容量补偿电费": "capacity_compensation",
    "市场运行费用": "other",
    "上网环节线损费用": "system_operation",
    "电量电费": "transmission",
    "抽水蓄能容量电费": "coal_capacity_charge",
    "煤电容量电费": "coal_capacity_charge",
    "上网环节线损代理采购损益": "system_operation",
    "电价交叉补贴新增损益": "subsidy",
    "系统电价交叉补贴新增损益": "subsidy",
    "容量补偿电价峰谷损益分摊费用": "capacity_compensation",
    "容量补偿电价峰谷损益": "capacity_compensation",
    "新能源可持续发展价格结算机制差价结算费用": "other",
    "可再生能源附加": "govt_surcharges",
    "国家重大水利工程建设基金": "govt_surcharges",
    "大中型水库移民后期扶持资金（中央）": "govt_surcharges",
    "大中型水库移民后期扶持资金": "govt_surcharges",
    "功率因数调整电费": "basic_fee",
    "政府性基金及附加": "govt_surcharges",
}

# numbered group rows that are SUBTOTALS with page-1 children — dropped.
# (②上网环节线损费用 / ⑤政府性基金及附加 are NOT here: their children live on
# page 2, so on page 1 they are the leaf rows themselves.)
_GROUP_ROW = re.compile(
    rf"^[①②③④⑤其中:：\s]*\(?\d+\)?\s*(市场化购电电费|输配电费|系统运行费)\s+({_NUM})\s*$")


def is_stategrid_discharge(text: str) -> bool:
    return "账单概况" in text and "上网电费" in text and "发电户名称" in text and (
        "实时偏差电费" in text or "调频服务补偿" in text or "两个细则" in text
    )


def is_stategrid_charge(text: str) -> bool:
    return "账单概况" in text and "工商业电费" in text and "市场化购电电费" in text


def _label_amount(line: str) -> tuple[str, float | None]:
    """Split a bill row into (label, amount) — the FIRST number after the
    label is the row's amount; trailing right-column text is ignored."""
    m = re.match(rf"^(\D.*?)\s+({_NUM})(?:\s|$)", line.strip())
    if m:
        return m.group(1).strip(), float(m.group(2))
    return line.strip(), None


def parse_stategrid_discharge(text: str) -> list[dict[str, Any]]:
    """Parse page-1 账单概况 of a 发电侧上网电费账单 into discharge items.

    Drops the 上网电费/合计 total rows; keeps the decomposing components.
    Verify with parse_stategrid_discharge_total: sum(items) must equal it.
    """
    items: list[dict[str, Any]] = []
    lines = [l.strip() for l in text.split("\n")]
    in_table = False
    for line in lines:
        if "账单概况" in line:
            in_table = True
            continue
        if not in_table:
            continue
        if "合计" in line or line.startswith("备注"):
            break
        if not line or line.startswith(("单位", "类别", "电量分析")):
            continue
        if line.startswith("上网电费"):
            continue  # the settlement TOTAL — components below decompose it
        # match against known labels (longest first: 容量补偿费用 before 容量补偿费)
        for lbl in sorted(_DISCHARGE_CAT, key=len, reverse=True):
            if not line.startswith(lbl):
                continue
            nums = re.findall(_NUM, line[len(lbl):])
            if not nums:
                break
            # Rows in this table are [电量, 电价, 电费]: components carry
            # "0 0 amount", the energy rows real "vol price amount". Trailing
            # right-column numbers must not be mistaken for these columns.
            if (len(nums) >= 3 and re.fullmatch(r"\d+", nums[0])
                    and abs(float(nums[1])) < 100):
                amt = float(nums[2])
                has_vol = float(nums[0]) > 0
                vol = float(nums[0]) / 1000.0 if has_vol else None
                price = float(nums[1]) if has_vol else None
            else:
                vol, price, amt = None, None, float(nums[0])
            items.append({
                "category": _DISCHARGE_CAT[lbl],
                "volume_mwh": vol,
                "price_cny_kwh": price,
                "amount_cny": amt,
                "notes": f"放电结算: {lbl}",
            })
            break
    return items


def parse_stategrid_discharge_total(text: str) -> float | None:
    """The bill's printed settlement total (上网电费 row / 合计 / 结算金额)."""
    for pat in (rf"^上网电费\s+\d+\s+{_NUM}\s+({_NUM})\s*$",
                rf"^合计\s*￥?\s*({_NUM})\s*$",
                rf"结算金额\s*({_NUM})元"):
        m = re.search(pat, text, re.MULTILINE)
        if m:
            return float(m.group(1))
    return None


def parse_stategrid_charge_volume(text: str) -> float | None:
    """Total monthly charging volume in MWh from the page-1 header
    (本期电量 X千瓦时)."""
    m = re.search(r"本期电量\s*([\d,]+)\s*千瓦时", text)
    if m:
        return float(m.group(1).replace(",", "")) / 1000.0
    return None


def parse_stategrid_charge(text: str) -> list[dict[str, Any]]:
    """Parse page-1 账单概况 of a 用电侧下网电费账单 into charge items.

    Keeps leaf fees, drops grand total + numbered subtotal groups + 合计.
    stored = −printed. Handles wrapped labels (amount on next line) and
    right-column 用能分析 interleave. The month's total volume (本期电量)
    is attached to the energy row (直接交易电费 → charge_energy).
    """
    total_vol_mwh = parse_stategrid_charge_volume(text)
    items: list[dict[str, Any]] = []
    lines = [l.strip() for l in text.split("\n")]
    i = 0
    in_table = False
    while i < len(lines):
        line = lines[i]
        if "账单概况" in line:
            in_table = True
            i += 1
            continue
        if not in_table:
            i += 1
            continue
        if line.startswith("备注"):
            break
        if not line or line.startswith(("单位", "费用组成")):
            i += 1
            continue
        if _TOTAL_LINE.match(line) or _GROUP_ROW.match(line):
            i += 1
            continue
        label, amt = _label_amount(line)
        if amt is None:
            # wrapped label: amount on next line, label tail on the line after
            if i + 1 < len(lines) and re.fullmatch(_NUM, lines[i + 1].strip()):
                amt = float(lines[i + 1].strip())
                if i + 2 < len(lines):
                    tail = re.match(r"^([一-鿿]+)", lines[i + 2].strip())
                    if tail and not tail.group(1).startswith(("本期", "第", "上期", "合计")):
                        label += tail.group(1)
                i += 1  # consume the amount line
            else:
                i += 1
                continue
        label = re.sub(r"^[①②③④⑤其中:：\s]+", "", label)
        if label in _CHARGE_CAT and amt != 0:
            cat = _CHARGE_CAT[label]
            items.append({
                "category": cat,
                "volume_mwh": total_vol_mwh if cat == "charge_energy" else None,
                "price_cny_kwh": None,
                "amount_cny": -amt,
                "notes": f"充电结算: {label}",
            })
        i += 1
    return items


def parse_stategrid_charge_total(text: str) -> float | None:
    """The bill's printed grand total (合计 ￥X, else 本期电费 header)."""
    m = re.search(rf"^合计\s*￥\s*({_NUM})\s*$", text, re.MULTILINE)
    if m:
        return float(m.group(1))
    m = re.search(rf"本期电费\s*({_NUM})元", text)
    if m:
        return float(m.group(1))
    return None
