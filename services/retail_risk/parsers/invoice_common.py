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
_TOTAL_RE = re.compile(r"本月\s+([\d,]+\.\d{2})")


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
    """Parse 科目编码 lines. Tolerates degraded shapes:
    - 1-3 numerics: amount = last numeric; volume/price left None
    - 4 numerics: plan, settle_volume, price, amount
    - name missing (code + numerics only): label_cn = ''
    - page-boundary repeats: dedup on (code, amount)
    Fee aggregation downstream must use code length >= 4 lines; the 2-digit top
    lines ('01 电量清分') exist for settled-volume, not for category sums.
    """
    items = []
    seen: set[tuple[str, float]] = set()
    for line in text.splitlines():
        m = _LINE_RE.match(line.strip())
        if not m:
            continue
        code = m.group("code")
        nums = [_num(x) for x in m.group("nums").split()]
        amount = next((v for v in reversed(nums) if v is not None), None)
        if amount is None:
            continue                     # name-only rows / headers: skip
        if (code, amount) in seen:
            continue                     # page-repeat duplicate
        seen.add((code, amount))
        full = len(nums) == 4
        items.append(schemas.InvoiceItem(
            category=schemas.category_for_code(code, rules),
            label_cn=(m.group("name") or "").strip(),
            volume_mwh=nums[1] if full else None,
            price_cny_mwh=nums[2] if full else None,
            amount_cny=amount,
            delivery_date=None,
            notes=code,
        ))
    return items


def parse_total_from_summary(text: str) -> float | None:
    m = _TOTAL_RE.search(text)
    return float(m.group(1).replace(",", "")) if m else None
