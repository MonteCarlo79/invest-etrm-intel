"""Deterministic parser for W-B-2 (悦盛昌渠) settlement documents — 零碳46.

Two document families (both text-layer PDFs):

1. **下网电费结算单 / 核查票** (charge): fee rows in 电费信息/交易费用/
   系统运行费/绿电费用/代征费用/基本电费 sections + 需量电费 + 力率电费,
   with a printed 电费合计 ground truth. Rows are [label, 时段, 电量, 电价,
   电费]; interleaved 两部制/220千伏及以上 column text wraps around them.
   stored = −printed (charge = cost).

2. **上网电费结算单** (discharge, 成分明细): rows "label vol price amount"
   (单位: 千千瓦时/元/千千瓦时 = MWh / CNY/MWh) with fee rows
   "label 0.00 amount", closing with 当月机组小计/本月应开发票金额
   (totals — dropped, used for verification). Vision previously divided
   these already-MWh volumes by 1000 (observed 零碳46 2025-03).

The **YT0477…交易结算凭证** is detected and skipped: its 电能电费 equals the
grid 上网结算单's energy row (and every fee row reconciles with it), so
ingesting both double-counts (observed 零碳46 2025-04/2025-07).
"""
from __future__ import annotations

import re
from typing import Any

_NUM = r"-?\d+(?:\.\d+)?"


def is_wb2_voucher(text: str) -> bool:
    """交易结算凭证 (trading-center voucher) — duplicates the grid 上网结算单."""
    return "交易结算凭证" in text and "电能电费" in text and "发行费用合计" in text


def is_wb2_charge_bill(text: str) -> bool:
    return "核查票" in text and "电费合计" in text and "代购购电电费" in text


def is_wb2_discharge_bill(text: str) -> bool:
    return "成分明细" in text and ("当月机组小计" in text or "本月应开发票金额" in text)


# ───────────────────────────── 核查票 (charge) ─────────────────────────────

def _last_nums(line: str, n: int = 3) -> list[float] | None:
    nums = re.findall(_NUM, line)
    if len(nums) < n:
        return None
    return [float(x) for x in nums[-n:]]


def _fee_row(lines: list[str], keyword: str, n: int = 3) -> tuple[float, float, float] | None:
    """Find the row containing keyword; return (vol, price, amount) from its
    last n numbers (vol/price/amount). Amount sign as printed."""
    for i, line in enumerate(lines):
        if keyword in line:
            # value may be on the keyword line or wrapped onto the next
            for j in (i, i + 1):
                if j < len(lines):
                    nums = re.findall(_NUM, lines[j])
                    if len(nums) >= n:
                        vals = [float(x) for x in nums[-n:]]
                        return vals[0], vals[1], vals[2]
    return None


def parse_wb2_charge_total(text: str) -> float | None:
    m = re.search(rf"电费合计\s*({_NUM})", text)
    if m:
        return float(m.group(1))
    m = re.search(rf"总电费[：:]\s*({_NUM})", text)
    return float(m.group(1)) if m else None


def parse_wb2_charge(text: str) -> list[dict[str, Any]]:
    """Parse a 核查票/下网电费结算单 into charge items.

    One row per fee group (购电电费/输配电费/上网环节线损/系统运行费/绿电费用/
    代征费用/需量电费/力率电费). stored = −printed. Verify:
    sum(items as printed) == parse_wb2_charge_total.
    """
    lines = [l.strip() for l in text.split("\n")]
    items: list[dict[str, Any]] = []

    # 购电电费 per 时段 → one charge_energy row
    energy_amt = 0.0
    energy_vol_kwh = 0.0
    for m in re.finditer(rf"代购购电电费\s*(尖峰|深谷|峰|平|谷)\s+({_NUM})\s+({_NUM})\s+({_NUM})", text):
        vol, price, amt = float(m.group(2)), float(m.group(3)), float(m.group(4))
        energy_amt += amt
        energy_vol_kwh += vol
    m = re.search(rf"电量合计\s*({_NUM})", text)
    total_vol_kwh = float(m.group(1)) if m else energy_vol_kwh
    if energy_amt != 0:
        items.append({"category": "charge_energy", "volume_mwh": round(total_vol_kwh / 1000.0, 3),
                      "price_cny_kwh": None, "amount_cny": -energy_amt,
                      "notes": "充电结算: 电网代购购电电费"})

    row_specs = [
        ("电网代购输配电费", "transmission", "输配电费"),
        ("上网环节线损费用", "system_operation", "上网环节线损费用"),
    ]
    for kw, cat, label in row_specs:
        got = _fee_row(lines, kw)
        if got and got[2] != 0:
            items.append({"category": cat, "volume_mwh": None, "price_cny_kwh": None,
                          "amount_cny": -got[2], "notes": f"充电结算: {label}"})

    # 系统运行费（…） sub-rows → one grouped row (labels may wrap mid-word;
    # numbers can sit on the label line or up to 2 lines below)
    sysop = 0.0
    for i, line in enumerate(lines):
        if "系统运行费（" in line:
            for j in range(i, min(i + 3, len(lines))):
                nums = re.findall(_NUM, lines[j])
                if len(nums) >= 3:
                    sysop += float(nums[-1])
                    break
    if sysop != 0:
        items.append({"category": "coal_capacity_charge", "volume_mwh": None,
                      "price_cny_kwh": None, "amount_cny": -sysop,
                      "notes": "充电结算: 系统运行费用"})

    # 绿电 rows (交易电费 + 偏差补偿(费) + 差值; labels may wrap mid-word) → one grouped row
    green = 0.0
    for i, line in enumerate(lines):
        if "绿电" not in line or "绿电比例" in line:
            continue
        nums = re.findall(_NUM, line)
        if len(nums) >= 2:
            green += float(nums[-1])
            continue
        # wrapped label (or interleaved "220千" fragment): first value line within 2 lines
        for j in range(i + 1, min(i + 3, len(lines))):
            if re.match(r"^(平|峰|谷|尖峰|深谷)?\s*" + _NUM, lines[j]):
                v = re.findall(_NUM, lines[j])
                if v:
                    green += float(v[-1])
                break
    if green != 0:
        items.append({"category": "other", "volume_mwh": None, "price_cny_kwh": None,
                      "amount_cny": -green, "notes": "充电结算: 绿电费用"})

    # 代征费用 rows → one grouped govt_surcharges row
    fund = 0.0
    fund_section = re.search(r"代征费用(.+?)(?:基本电费|系统备用费|合计信息|力率信息)", text, re.DOTALL)
    if fund_section:
        for m in re.finditer(rf"(库区移民|可再生能源|重大水利基金)\s+({_NUM})\s+({_NUM})\s+({_NUM})",
                             fund_section.group(1)):
            fund += float(m.group(4))
    if fund != 0:
        items.append({"category": "govt_surcharges", "volume_mwh": None, "price_cny_kwh": None,
                      "amount_cny": -fund, "notes": "充电结算: 政府性基金及附加"})

    # 需量电费 (基本电费)
    got = _fee_row(lines, "需量电费")
    if got and got[2] != 0:
        items.append({"category": "basic_fee", "volume_mwh": None, "price_cny_kwh": None,
                      "amount_cny": -got[2], "notes": "充电结算: 需量电费"})

    # 力率电费 (力率信息 row: 6th number = 力率电费)
    lf_sec = re.search(r"力率信息\s*\n?[^\n]*\n?((?:\s*" + _NUM + r"){6,})", text)
    if lf_sec:
        nums = [float(x) for x in re.findall(_NUM, lf_sec.group(1))]
        if len(nums) >= 6 and nums[5] != 0:
            items.append({"category": "basic_fee", "volume_mwh": None, "price_cny_kwh": None,
                          "amount_cny": -nums[5], "notes": "充电结算: 力率电费"})

    return items


# ───────────────────────────── 成分明细 (discharge) ─────────────────────────

_WB2_ROW_CAT = [
    ("现货交易", "discharge_energy"),
    ("保量保价", "discharge_energy"),
    ("调频电费", "frequency"),
    ("调频", "frequency"),
    ("储能容量补偿费用", "capacity_compensation"),
    ("市场平衡类费用", "other"),
    ("市场调节类费用", "other"),
    ("不平衡资金", "other"),
    ("绿电", "other"),
]


def parse_wb2_discharge(text: str) -> list[dict[str, Any]]:
    """Parse an 上网电费结算单 (成分明细 table) into discharge items.

    Rows: "label vol price amount" (vol in 千千瓦时 = MWh, price in
    元/千千瓦时 = CNY/MWh → CNY/kWh = price/1000) or fee rows
    "label 0.00 amount". Label date prefixes ("2024年12月保量保价") and
    备注 date suffixes ("2025年03月电费") are stripped BEFORE number
    extraction — otherwise the year/month leak into vol/amount slots
    (observed 零碳46 2025-03). 当月机组小计/以前月度发票差异调整/
    本月应开发票金额 are dropped. Revenue positive, fees negative.
    """
    items: list[dict[str, Any]] = []
    for raw in text.split("\n"):
        line = raw.strip()
        if not line or any(k in line for k in ("小计", "差异调整", "应开发票金额", "成分明细", "结算周期", "机组")):
            continue
        # strip leading YYYY年M月 date prefix and trailing YYYY年M月电费 备注
        core = re.sub(r"^\d{4}年\d{1,2}月", "", line)
        core = re.sub(r"\d{4}年\d{1,2}月电费\s*$", "", core).strip()
        for label, cat in _WB2_ROW_CAT:
            if not core.startswith(label) and label not in core:
                continue
            nums = [x.replace(",", "") for x in re.findall(r"-?[\d,]+(?:\.\d+)?", core)]
            if len(nums) >= 3:
                vol, price, amt = float(nums[0]), float(nums[1]), float(nums[2])
            elif len(nums) == 2:
                vol, price, amt = 0.0, None, float(nums[1])
            else:
                continue
            if amt == 0:
                continue
            items.append({"category": cat,
                          "volume_mwh": vol if vol else None,
                          "price_cny_kwh": round(price / 1000.0, 6) if price is not None else None,
                          "amount_cny": amt,
                          "notes": f"放电结算: {label}"})
            break
    return items


def parse_wb2_discharge_total(text: str) -> float | None:
    for pat in (rf"本月应开发票金额\s+[\d,.]+\s+[\d,.]+\s+(-?[\d,]+\.\d+)",
                rf"当月机组小计\s+[\d,.]+\s+[\d,.]+\s+(-?[\d,]+\.\d+)"):
        m = re.search(pat, text)
        if m:
            return float(m.group(1).replace(",", ""))
    return None
