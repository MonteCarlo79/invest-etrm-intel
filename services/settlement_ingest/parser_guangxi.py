"""Deterministic parser for 广西 (Guangxi) settlement documents — text-layer PDFs.

Two document families:

1. **电费通知单** (charge side, 下网, 中国南方电网/广西电网公司): one page per
   计量点 (metering point), rows (1)–(16). Rows (1)–(10) are energy rows
   "label volume_kWh price amount" (label may wrap mid-word onto the next line);
   (11) 基本电费 and (12) 功率因数调整 are block rows (amount = last number of the
   values line); (13)–(16) share a 4-column header with values on the next line.
   Sign convention: printed positive = cost → stored negative; printed negative
   (退补/功率因数奖励) → stored positive.

2. **广西电力交易月度结算依据** (discharge side, 上网, 交易中心): top-level rows
   1/2/3 (电量 MWh, 单价 元/MWh, 电费 元; rows 2/3 may carry amount only).
   Dotted sub-rows (1.1, 2.x) decompose their parent and are skipped — vision
   ingested parent AND children, double-counting (observed 灵山 2026-02).

Why deterministic: vision parsing of these bills produced 1000x unit errors
(基本电费 100000 kVA × 21.4 read as 1000 × 21.4), small digit misreads, and
hallucinated total rows (灵山 cleanup 2026-09-09). The bills have a clean text
layer — regex extraction is exact and self-checkable against 计量点小计/合计.
"""
from __future__ import annotations

import re
from typing import Any

from services.settlement_ingest.parser_vision import map_settlement_category

_NUM = r"-?\d+(?:\.\d+)?"
_ROW_MARK = re.compile(r"\((\d{1,2})\)")
_NUMBERS_LINE = re.compile(rf"^({_NUM})\s+({_NUM})\s+({_NUM})$")
_SUBTOTAL = re.compile(r"计量点电费小计（小写）：(" + _NUM + r")元")
_MP_ID = re.compile(r"计量点编号：(\d+)")
_TOP_ROW = re.compile(r"^(\d)\s+")
_SUB_ROW = re.compile(r"^\d+\.\d+")
_PAGE_END = re.compile(r"^(用电状况栏|备注)")


def is_guangxi_charge_bill(text: str) -> bool:
    return "广西电网公司" in text and "电费通知单" in text


def is_guangxi_discharge_bill(text: str) -> bool:
    return "广西电力交易月度结算依据" in text


def is_guangxi_grid_duplicate(text: str) -> bool:
    """电网版上网结算单 (电网名称：广西电网…) — overlaps the 交易中心结算依据;
    ingest would double-count the market portion. Skip, do not vision-parse."""
    return "电网名称：广西电网" in text and "结算依据" not in text


def _float(token: str) -> float:
    return float(token)


def _last_number(line: str) -> float | None:
    nums = re.findall(_NUM, line)
    return float(nums[-1]) if nums else None


def _charge_item(label: str, vol_kwh: float | None, price: float | None,
                 printed_amount: float, mp_id: str | None) -> dict[str, Any] | None:
    """Build one charge-side item (cost = negative). Returns None for all-zero rows."""
    if printed_amount == 0 and not vol_kwh:
        return None
    vol_mwh = round(vol_kwh / 1000.0, 4) if vol_kwh else None
    label = label.strip()
    mp_tag = f" [计量点{mp_id[-4:]}]" if mp_id else ""
    return {
        "category": map_settlement_category(label, side="charge"),
        "volume_mwh": vol_mwh,
        "price_cny_kwh": price,
        "amount_cny": -printed_amount,
        "notes": f"充电结算: {label}{mp_tag}",
    }


def parse_guangxi_charge_text(text: str) -> list[dict[str, Any]]:
    """Parse 电费通知单 text (all pages concatenated) into charge-side items.

    Rows are matched by STRUCTURE, not row number — the numbering shifts
    between bill revisions (2026: energy rows (1)-(10), blocks (11)(12)/(13)-16;
    2025: energy rows to (12), blocks (13)(14)/(15)-18):
    - energy row: (N) + label + 3 numbers on the same line, or on the next
      line when the label wraps mid-word (the (N) may also be preceded by
      interleaved 类型-column text, e.g. "220千伏以下 (10)交易电量分摊居民农业价")
    - 基本电费 block: values line carries "--"; amount = its last number
      (volume is kVA — deliberately not stored as volume_mwh)
    - 功率因数调整 block: amount = last number of the values line
    - shared 4-column block (N)(N)(N)(N)（元）: values on the next line
    Footnote sections (备注…) also contain "(N)" references — pages are
    truncated at 用电状况栏/备注 so they can't produce phantom rows.
    """
    items: list[dict[str, Any]] = []
    # One 计量点 per page; each page repeats the 中国南方电网公司 header
    chunks = re.split(r"中国南方电网公司", text)

    for chunk in chunks:
        if "计量点电费信息" not in chunk:
            continue
        m = _MP_ID.search(chunk)
        mp_id = m.group(1) if m else None
        lines = chunk.split("\n")
        # Drop the footnote section — its 备注 formulas reference (N) too
        for k, l in enumerate(lines):
            if _PAGE_END.match(l.strip()):
                lines = lines[:k]
                break
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            mark = _ROW_MARK.search(line)
            if not mark:
                i += 1
                continue

            tail = line[mark.end():]
            tokens = tail.split()
            nums = [t for t in tokens if re.fullmatch(_NUM, t)]
            label_parts = []
            for t in tokens:
                if re.fullmatch(_NUM, t):
                    break
                label_parts.append(t)
            label = "".join(label_parts)

            # Shared multi-column block: ≥2 (N) markers on one line, values next
            # (4-column (13)-(16) on 2025+ bills; 2-column (14)(15) on 2024 bills —
            # a 2-col block with 退补 went unseen and dropped a ¥2.27M credit, 2024-06.
            # The table region is already truncated at 用电状况栏/备注, so ≥2 markers
            # here can only be a shared-column header.)
            if len(_ROW_MARK.findall(line)) >= 2:
                cols = re.findall(r"\((\d{1,2})\)([^\s(]+)", line)
                if i + 1 < len(lines):
                    vals = re.findall(_NUM, lines[i + 1])
                    for (n, lbl), v in zip(cols, vals):
                        it = _charge_item(f"({n}){lbl}", None, None, _float(v), mp_id)
                        if it:
                            items.append(it)
                    i += 2
                    continue
                i += 1
                continue

            # Same-line energy row: ≥3 numbers after the label
            if len(nums) >= 3:
                it = _charge_item(label, _float(nums[0]), _float(nums[1]), _float(nums[2]), mp_id)
                if it:
                    items.append(it)
                i += 1
                continue

            # 基本电费 block: 计费容量(kVA) 电价 折扣 -- 金额
            if "基本电费" in label:
                for j in range(i + 1, min(i + 4, len(lines))):
                    if "--" in lines[j]:
                        amt = _last_number(lines[j])
                        if amt is not None:
                            it = _charge_item("基本电费", None, None, amt, mp_id)
                            if it:
                                items.append(it)
                            i = j + 1
                            break
                else:
                    i += 1
                continue

            # 功率因数调整 block: amount = last number of the values line
            if "功率因数" in label:
                for j in range(i + 1, min(i + 4, len(lines))):
                    amt = _last_number(lines[j])
                    if amt is not None:
                        it = _charge_item("功率因数调整电费", None, None, amt, mp_id)
                        if it:
                            items.append(it)
                        i = j + 1
                        break
                else:
                    i += 1
                continue

            # Wrapped energy row: numbers on the next line
            if i + 1 < len(lines):
                nm = _NUMBERS_LINE.match(lines[i + 1].strip())
                if nm:
                    it = _charge_item(label, _float(nm.group(1)), _float(nm.group(2)), _float(nm.group(3)), mp_id)
                    if it:
                        items.append(it)
                    i += 2
                    continue
            i += 1
    return items


def charge_page_subtotals(text: str) -> list[float]:
    """Printed 计量点电费小计 per page — ground truth for cross-checking parses."""
    return [float(m.group(1)) for m in _SUBTOTAL.finditer(text)]


def parse_guangxi_discharge_text(text: str) -> list[dict[str, Any]]:
    """Parse 广西电力交易月度结算依据 text into discharge-side items.

    Top-level numbered rows only; dotted sub-rows (parent decomposition) and
    the 合计 total row are skipped. Units: 电量 MWh, 单价 元/MWh → CNY/kWh.
    """
    items: list[dict[str, Any]] = []
    for raw in text.split("\n"):
        line = raw.strip()
        if not line or _SUB_ROW.match(line) or line.startswith("合计"):
            continue
        if not _TOP_ROW.match(line):
            continue
        tokens = line.split()
        numbers: list[float] = []
        label_parts: list[str] = []
        for tok in tokens[1:]:
            if re.fullmatch(_NUM, tok):
                numbers.append(float(tok))
            elif not numbers:
                label_parts.append(tok)
            # a CJK token after numbers would be a wrapped label tail — not seen; ignore
        label = " ".join(label_parts)
        if not label or not numbers:
            continue
        if len(numbers) == 3:
            vol, price, amount = numbers
        elif len(numbers) == 1:
            vol, price, amount = None, None, numbers[0]
        else:
            continue
        if amount == 0 and not vol:
            continue
        items.append({
            "category": map_settlement_category(label, side="discharge"),
            "volume_mwh": vol,
            "price_cny_kwh": round(price / 1000.0, 6) if price is not None else None,
            "amount_cny": amount,
            "notes": f"放电结算: {label}",
        })
    return items
