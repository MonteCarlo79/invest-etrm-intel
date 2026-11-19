"""国网江苏省电力有限公司 电费核查联 — 射阳储能 (B-4-下) grid charging bill.

Deterministic, no vision. The bill lists one page per metering point (a small
station-service meter and the main 1,760,000-multiplier storage meter); every page
repeats the bill-level ``金额合计（元）``. Charging cost = −金额合计 (stored sign:
costs negative), volume = Σ 有功总 kWh over the meter pages / 1000.

In 江苏 peak months (Dec/Jan/Jul/Aug) the main meter's energy is billed at 0 and
金额合计 is only the station-service amount (~¥400) — that is the real bill, not a
parse failure. Requires the grey "syyj_" watermark to be filtered first
(libs.asset_risk.settlement.router.read_bill).
"""
from __future__ import annotations

import re

_TOTAL = re.compile(r"金额合计（元）\s*([\d,]+\.\d{2})")
_ACTIVE = re.compile(r"有功总\s*(\d+)")


def is_jiangsu_grid_check(text: str) -> bool:
    flat = re.sub(r"\s+", "", text)
    return "国网江苏省电力有限公司" in flat and "电费核查联" in flat and bool(_TOTAL.search(text))


def parse_jiangsu_grid_check(text: str) -> list[dict]:
    m = _TOTAL.search(text)
    if not m:
        return []
    total = float(m.group(1).replace(",", ""))
    pages = [p for p in re.split(r"\n(?=国\s*网\s*江\s*苏)", text) if "有功总" in p]
    kwh = sum(int(_ACTIVE.search(p).group(1)) for p in pages if _ACTIVE.search(p))
    return [{
        "category": "charge_energy",
        "volume_mwh": kwh / 1000.0 if kwh else None,
        "price_cny_kwh": (total / kwh) if kwh else None,
        "amount_cny": -total,
        "notes": "充电结算: 国网江苏电费核查联 金额合计",
    }]
