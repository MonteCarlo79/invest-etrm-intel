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
    r"^(?P<code>\d{4,10})\s+(?P<name>.+?)\s+"
    r"(?P<v1>[\-\d.,]+)\s+(?P<v2>[\-\d.,]+)\s+(?P<v3>[\-\d.,]+)\s+(?P<v4>[\-\d.,]+)"
    r"(?:\s+(?P<note>.*))?$"
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


def extract_pdf_text(path: str | Path) -> str:
    with pdfplumber.open(str(path)) as pdf:
        return "\n".join((p.extract_text() or "") for p in pdf.pages)


def parse_subject_lines(text: str, rules: list[tuple[str, str]]) -> list[schemas.InvoiceItem]:
    items = []
    for line in text.splitlines():
        m = _LINE_RE.match(line.strip())
        if not m:
            continue
        code = m.group("code")
        amount = _num(m.group("v4"))
        if amount is None:
            continue                     # header rows without amounts: skip
        items.append(schemas.InvoiceItem(
            category=schemas.category_for_code(code, rules),
            label_cn=m.group("name").strip(),
            volume_mwh=_num(m.group("v2")),
            price_cny_mwh=_num(m.group("v3")),
            amount_cny=amount,
            delivery_date=None,
            notes=code,
        ))
    return items


def parse_total_from_summary(text: str) -> float | None:
    m = _TOTAL_RE.search(text)
    return float(m.group(1).replace(",", "")) if m else None
