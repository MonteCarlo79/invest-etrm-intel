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

# discharge component → category (日前/实时偏差电费 mapped explicitly to discharge_energy)
_DISCHARGE_CAT = {
    "日前偏差电费": "discharge_energy",
    "实时偏差电费": "discharge_energy",
    "两个细则费用": "frequency",
    "调频服务补偿费用": "frequency",
    "调频服务分摊费用": "frequency",
    "调频辅助服务补偿": "frequency",
    "爬坡辅助服务补偿及分摊": "frequency",
    "容量补偿费": "capacity_compensation",
    "容量补偿费用": "capacity_compensation",
    "独立储能容量电价补偿": "capacity_compensation",
    "优发优购曲线匹配偏差费": "penalty",
    "居民农业新增损益-预测偏差-考核费用": "penalty",
    "新能源偏差收益回收及返还费": "penalty",
    "预测偏差费用": "penalty",
    "用户侧中长期偏差收益回收及返还费用": "penalty",
    "发电侧中长期偏差收益回收及返还费用": "penalty",
    "跨月偏差退补费": "rebate",
    "退补费用": "rebate",
    "其他退补费用": "rebate",
    "阻塞返还电费": "rebate",
    "阻塞费用": "other",
    "中长期合约电费": "other",
    "启动分摊费用": "other",
    "特殊机组分摊费用": "other",
    "优先电费": "other",
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
    "优发优购曲线匹配偏差、发电侧返还费用、阻塞费用": "penalty",
    "优发优购曲线匹配偏差、发电侧返还费用": "penalty",
    "优发优购曲线匹配偏差、发电侧返还": "penalty",
}

# numbered group rows that are SUBTOTALS with page-1 children — dropped.
# (②上网环节线损费用 / ⑤政府性基金及附加 are NOT here: their children live on
# page 2, so on page 1 they are the leaf rows themselves.)
_GROUP_ROW = re.compile(
    rf"^[①②③④⑤其中:：\s]*\(?\d+\)?\s*(市场化购电电费|输配电费|系统运行费)\s+({_NUM})\s*$")


def is_stategrid_discharge(text: str) -> bool:
    if "上网电费" not in text or "发电户名称" not in text:
        return False
    if not any(k in text for k in ("实时偏差电费", "日前偏差电费", "调频服务补偿", "两个细则")):
        return False
    # 账单概况 layout (2026) or the 电量分析/本期电费明细 variant (2025-03)
    return any(k in text for k in ("账单概况", "本期电费明细", "电量分析"))


def is_stategrid_charge(text: str) -> bool:
    return "账单概况" in text and "工商业电费" in text and "市场化购电电费" in text


def is_local_discharge(text: str) -> bool:
    """山东省地方电厂市场化结算单 (2025-01/02 layout)."""
    return "山东省地方电厂市场化结算单" in text and "总费用合计" in text


def _label_amount(line: str) -> tuple[str, float | None]:
    """Split a bill row into (label, amount) — the FIRST number after the
    label is the row's amount; trailing right-column text is ignored."""
    m = re.match(rf"^(\D.*?)\s+({_NUM})(?:\s|$)", line.strip())
    if m:
        return m.group(1).strip(), float(m.group(2))
    return line.strip(), None


def _row_amounts(nums: list[str]) -> tuple[float | None, float | None, float]:
    """Interpret numbers from a component row of the 电费明细 table.

    Row shapes seen across bills:
    - "vol price amount" energy rows (vol may be NEGATIVE: "-312128 0.3329 -103910.89")
    - "0 0 amount" components with zero vol+price columns (2026-01, format A)
    - "0 amount" components with zero vol, no price column (2025-05/03)
    - "amount" flat components (2026-04), possibly followed by right-column noise
    Returns (vol_mwh, price_cny_kwh, amount).
    """
    if (len(nums) >= 3 and re.fullmatch(r"-?\d+", nums[0]) and float(nums[0]) != 0
            and abs(float(nums[1])) < 100):
        # energy row: vol price amount
        return float(nums[0]) / 1000.0, float(nums[1]), float(nums[2])
    if re.fullmatch(r"0+", nums[0]):
        # zero-volume component: amount = first non-zero number after the vol
        amt = next((float(x) for x in nums[1:] if float(x) != 0), 0.0)
        return None, None, amt
    return None, None, float(nums[0])


def parse_stategrid_discharge(text: str) -> list[dict[str, Any]]:
    """Parse the 电费明细 component table of a 发电侧上网电费账单.

    Drops the 上网电费/合计 total rows; keeps the decomposing components.
    Handles wrapped labels (label prefix on one line, "0 amount" on the next,
    label tail on the third) and right-column 电量分析 interleave.
    Verify with parse_stategrid_discharge_total: sum(items) must equal it.
    """
    items: list[dict[str, Any]] = []
    lines = [l.strip() for l in text.split("\n")]
    # locate the table start: the 上网电费 total row (always first row)
    start = next((i for i, l in enumerate(lines) if l.startswith("上网电费")), len(lines))
    labels_by_len = sorted(_DISCHARGE_CAT, key=len, reverse=True)
    i = start + 1
    while i < len(lines):
        line = lines[i]
        if "合计" in line or line.startswith("备注"):
            break
        if not line or line.startswith(("单位", "类别")):
            i += 1
            continue
        matched = False
        for lbl in labels_by_len:
            if not line.startswith(lbl):
                continue
            nums = re.findall(_NUM, line[len(lbl):])
            if nums:
                vol, price, amt = _row_amounts(nums)
                items.append({"category": _DISCHARGE_CAT[lbl], "volume_mwh": vol,
                              "price_cny_kwh": price, "amount_cny": amt,
                              "notes": f"放电结算: {lbl}"})
                matched = True
            break
        if not matched:
            # wrapped label: this line is a prefix of a known label, numbers on
            # the next line, label tail on the line after
            if not re.search(r"\d", line) and i + 1 < len(lines):
                prefix_hit = next((lbl for lbl in labels_by_len if lbl.startswith(line)), None)
                if prefix_hit:
                    nums = re.findall(_NUM, lines[i + 1])
                    if nums:
                        vol, price, amt = _row_amounts(nums)
                        items.append({"category": _DISCHARGE_CAT[prefix_hit],
                                      "volume_mwh": vol, "price_cny_kwh": price,
                                      "amount_cny": amt, "notes": f"放电结算: {prefix_hit}"})
                        i += 2  # skip the numbers line (and the tail line is skipped next)
                        continue
        i += 1
    return items


def parse_stategrid_discharge_total(text: str) -> float | None:
    """The bill's printed settlement total (上网电费 row / 合计 / 结算金额)."""
    for pat in (rf"^上网电费\s+\d+\s+{_NUM}\s+({_NUM})\s*$",
                rf"^上网电费\s+\d+\s+千瓦时\s+上网均价\s+{_NUM}\s+元/千瓦时\s+结算金额\s+({_NUM})\s*元?$",
                rf"^合计\s*￥?\s*({_NUM})\s*$",
                rf"结算金额\s*({_NUM})\s*元"):
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
                    nxt = lines[i + 2].strip()
                    # tail line: entirely CJK+punct, short (e.g. "费用、阻塞费用")
                    tail = None
                    if re.fullmatch(r"[一-鿿、，（）()]+", nxt) and len(nxt) <= 20:
                        tail = nxt
                    else:
                        t = re.match(r"^([一-鿿]+)", nxt)
                        if t:
                            tail = t.group(1)
                    if tail and not tail.startswith(("本期", "第", "上期", "合计")):
                        label += tail
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


# ─────────────────────────────────────────────────────────────────────────────
# Format A: 山东省地方电厂市场化结算单 (2025-01/02)
# Energy block (日前/实时偏差电费, 退补费用 with vol+price) + 小计/合计, then
# flat fee rows, ending with 总费用合计 (the document total).
# ─────────────────────────────────────────────────────────────────────────────

def parse_local_discharge(text: str) -> list[dict[str, Any]]:
    """Parse 山东省地方电厂市场化结算单 into discharge items.
    Skips 小计/合计 (energy-block subtotals); total = 总费用合计."""
    items: list[dict[str, Any]] = []
    labels_by_len = sorted(_DISCHARGE_CAT, key=len, reverse=True)
    for raw in text.split("\n"):
        line = raw.strip()
        if line.startswith("总费用合计"):
            break
        if not line or line.startswith(("单位", "户号", "计量点名称", "小计", "合计", "山东省")):
            continue
        for lbl in labels_by_len:
            if not line.startswith(lbl):
                continue
            nums = re.findall(_NUM, line[len(lbl):])
            if not nums:
                break
            vol, price, amt = _row_amounts(nums)
            if amt != 0:
                items.append({"category": _DISCHARGE_CAT[lbl], "volume_mwh": vol,
                              "price_cny_kwh": price, "amount_cny": amt,
                              "notes": f"放电结算: {lbl}"})
            break
    return items


def parse_local_discharge_total(text: str) -> float | None:
    m = re.search(rf"总费用合计\s*({_NUM})\s*$", text, re.MULTILINE)
    return float(m.group(1)) if m else None


# ─────────────────────────────────────────────────────────────────────────────
# Format C: 山东电力交易中心交易结算单 (watermark-garbled multi-page doc).
# Page 1 lists 购电侧/售电侧 volumes; detail pages end with per-side 合计 rows
# (合计 电量 均价 结算电费). 大写金额 = 售电侧电费 + |购电侧电费| (gross).
# The 购电侧合计电费 is printed negative already (cost) — stored as printed.
# ─────────────────────────────────────────────────────────────────────────────

def is_sdtc_settlement(text: str) -> bool:
    return "山东电力交易中心" in text and "交易结算单" in text and "购电侧" in text and "售电侧" in text


def parse_sdtc_settlement(text: str) -> dict[str, dict[str, Any]]:
    """Parse 山东电力交易中心交易结算单 → {"售电侧": item, "购电侧": item}.

    Each side's 合计 row in the detail pages is matched to the side's page-1
    volume. 售电侧 → discharge_energy (printed positive); 购电侧 →
    charge_energy (printed negative, stored as printed).
    """
    out: dict[str, dict[str, Any]] = {}
    vol_p1: dict[str, float] = {}
    for side in ("购电侧", "售电侧"):
        m = re.search(rf"{side}\s+([\d,]+\.\d+)", text)
        if m:
            vol_p1[side] = float(m.group(1).replace(",", ""))
    for m in re.finditer(rf"^合计\s+([\d,]+\.\d+)\s+(-?[\d,]+\.\d+|-)\s+(-?[\d,]+\.\d+)\s*$",
                         text, re.MULTILINE):
        vol = float(m.group(1).replace(",", ""))
        avg = None if m.group(2) == "-" else float(m.group(2))
        amt = float(m.group(3).replace(",", ""))
        side = next((s for s, v in vol_p1.items() if abs(v - vol) < 0.01), None)
        if side is None:
            continue
        if side == "售电侧":
            out[side] = {"category": "discharge_energy", "volume_mwh": vol,
                         "price_cny_kwh": round(avg / 1000.0, 6) if avg is not None else None,
                         "amount_cny": amt, "notes": "放电结算: 交易中心售电侧合计"}
        else:
            out[side] = {"category": "charge_energy", "volume_mwh": vol,
                         "price_cny_kwh": round(avg / 1000.0, 6) if avg is not None else None,
                         "amount_cny": amt, "notes": "充电结算: 交易中心购电侧合计"}
    return out
