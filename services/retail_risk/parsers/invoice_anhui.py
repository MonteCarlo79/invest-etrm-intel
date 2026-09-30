# services/retail_risk/parsers/invoice_anhui.py
"""安徽统推 invoice PDF -> InvoiceDoc, with diagonal-stamp watermark cleaning."""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from services.retail_risk import schemas
from services.retail_risk.parsers.invoice_common import (
    extract_pdf_text, parse_subject_lines, parse_total_from_summary,
)

_MONTH_RE = re.compile(r"(\d{4})年(\d{1,2})月")
_WATERMARK_RE = re.compile(
    r"^(20\d{2}|年\d+|月\d+|\d{2}:\d{2}:\d{2}|[景融绿色能源科技有限司公日\s]+)$"
)


def clean_anhui_watermark(text: str) -> str:
    """Drop stamp-watermark fragments. Real subject lines start with >=4-digit codes
    and are never short; short CJK fragments and date/time stamp pieces are noise."""
    keep = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        if len(s) <= 3 and not re.match(r"^\d{4}", s):
            continue
        if _WATERMARK_RE.match(s) and not re.match(r"^\d{4}\s", s):
            continue
        keep.append(line)
    return "\n".join(keep)


def parse_anhui_invoice(path: str | Path) -> schemas.InvoiceDoc:
    path = Path(path)
    ym = _MONTH_RE.search(path.name)
    # 统推 watermark stamps render in STSong-Light (no real content in that font);
    # drop at char level, then apply the text cleaner as a backstop.
    text = clean_anhui_watermark(extract_pdf_text(path, drop_fonts={"STSong-Light"}))
    return schemas.InvoiceDoc(
        settlement_month=pd.to_datetime(f"{ym.group(1)}-{int(ym.group(2)):02d}-01").date(),
        items=parse_subject_lines(text, schemas.ANHUI_CATEGORY_RULES),
        total_amount_cny=parse_total_from_summary(text),
    )
