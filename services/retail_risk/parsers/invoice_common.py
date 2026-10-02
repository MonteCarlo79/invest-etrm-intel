# services/retail_risk/parsers/invoice_common.py
"""Shared invoice parsing: 科目编码 subject lines -> InvoiceItem list.

Line shape: <code> <name> <plan_vol> <settle_vol> <avg_price> <amount> [备注]
Numerics may be '-' (missing). Category via longest-prefix rules (schemas).
"""
from __future__ import annotations

import re
from pathlib import Path

import pdfplumber

from services.retail_risk import schemas

_LINE_RE = re.compile(
    r"^(?P<code>\d{2,10})\s+(?:(?P<name>[^\s\d].*?)\s+)?"
    r"(?P<nums>[\-\d.,]+(?:\s+[\-\d.,]+){0,3})(?:\s+(?P<note>[^\d].*))?$"
)


def _num(s: str) -> float | None:
    s = s.replace(",", "").strip()
    if s in ("-", "—", ""):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def extract_pdf_text(path: str | Path, drop_fonts: frozenset[str] = frozenset()) -> str:
    """All-page text. drop_fonts: fontnames to exclude at char level (watermark layers,
    e.g. 安徽统推 stamps render in STSong-Light which carries no real content)."""
    with pdfplumber.open(str(path)) as pdf:
        texts = []
        for page in pdf.pages:
            if drop_fonts:
                page = page.filter(
                    lambda o: o.get("fontname") not in drop_fonts
                    if o.get("object_type") == "char" else True)
            texts.append(page.extract_text() or "")
        return "\n".join(texts)


def parse_subject_lines(text: str, rules: list[tuple[str, str]]) -> list[schemas.InvoiceItem]:
    """Parse 科目编码 lines, token-based. Layouts:
    - standard 4-numeric: plan, settle_volume, price, amount
    - 浙江 7-column (appended 年累计 year-cumulative columns): the FIRST four numerics
      hold the month values — amount is numerics[3], never the last
    - 1-3 numerics: amount = last numeric; volume/price left None
    - name missing (code + numerics only): label_cn = ''
    - page-boundary repeats: dedup on (code, amount)
    - 购电侧/售电侧 marker lines flip `side`; sell-side 01-subtree -> 'retail_revenue'.
    Fee aggregation downstream must use code length >= 4 lines; the 2-digit top
    lines ('01 电量清分') exist for settled-volume, not for category sums.
    """
    items = []
    seen: set[tuple[str, float]] = set()
    side = "buy"
    seen_buy_01 = False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped == "购电侧":
            side = "buy"
            continue
        if stripped == "售电侧":
            side = "sell"
            continue
        toks = stripped.split()
        if not toks or not re.fullmatch(r"\d{2,10}", toks[0]):
            continue
        code = toks[0]
        # 山东 retail block starts at the SECOND '01 电量清分' with no marker
        if code == "01":
            if side == "buy" and seen_buy_01:
                side = "sell"
            elif side == "buy":
                seen_buy_01 = True
        name_parts: list[str] = []
        nums: list[float | None] = []
        for t in toks[1:]:
            if not nums and not re.fullmatch(r"[\-\d.,]+", t):
                name_parts.append(t)            # name tokens precede numerics
            elif re.fullmatch(r"[\-\d.,]+", t):
                nums.append(_num(t))
            else:
                break                            # trailing 备注 text ends the row
        if not nums:
            continue                             # name-only rows: skip
        amount = nums[3] if len(nums) >= 4 and nums[3] is not None else \
            next((v for v in reversed(nums) if v is not None), None)
        if amount is None:
            continue
        if (code, amount) in seen:
            continue                             # page-repeat duplicate
        seen.add((code, amount))
        if side == "sell" and code.startswith("01"):
            category = "retail_revenue"
        else:
            category = schemas.category_for_code(code, rules)
        full = len(nums) >= 4
        three = len(nums) == 3
        items.append(schemas.InvoiceItem(
            category=category,
            label_cn=" ".join(name_parts).strip(),
            volume_mwh=nums[1] if full else (nums[0] if three else None),
            price_cny_mwh=nums[2] if full else (nums[1] if three else None),
            amount_cny=amount,
            delivery_date=None,
            notes=code,
            side=side,
        ))
    return items


_TOTAL_LINE_RE = re.compile(r"^本月\s+(.*)$")


def parse_total_from_summary(text: str) -> float | None:
    """Money total from the 本月 summary line = the LAST numeric on that line
    (冀南: 本月 175251.68; 浙江: 本月 <用电量> <结算电量> <合同电量> - <结算电费>)."""
    for line in text.splitlines():
        m = _TOTAL_LINE_RE.match(line.strip())
        if not m:
            continue
        nums = [_num(x) for x in m.group(1).split()]
        nums = [v for v in nums if v is not None]
        if nums:
            return nums[-1]
    return None


def parse_summary_volume(text: str) -> float | None:
    """Total settled volume (MWh) from the invoice summary header.

    The subject lines don't always carry it （安徽's '01 电量清分' is amount-less),
    so the bridge needs the summary row:
    - '购电侧 <结算电量> <合同电量> [偏差电量]' (安徽/河北) -> first numeric
    - '本月 <实际用电量> <结算电量> ... <结算电费>' (浙江, >=3 numerics) -> first numeric
    A single-number 本月 line （冀南 margin) is NOT a volume.
    """
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("购电侧 "):
            nums = [_num(t) for t in s.split()[1:]]
            nums = [v for v in nums if v is not None]
            if nums:
                return nums[0]
        m = _TOTAL_LINE_RE.match(s)
        if m:
            nums = [_num(t) for t in m.group(1).split()]
            nums = [v for v in nums if v is not None]
            if len(nums) >= 3:
                return nums[0]
    return None
