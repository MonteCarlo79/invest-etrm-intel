# services/retail_risk/parsers/invoice_zhejiang.py
"""浙江 monthly invoice PDF -> InvoiceDoc."""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from services.retail_risk import schemas
from services.retail_risk.parsers.invoice_common import (
    extract_pdf_text, parse_subject_lines, parse_summary_volume, parse_total_from_summary,
)

_MONTH_RE = re.compile(r"(\d{4})年(\d{1,2})月")


def parse_zhejiang_invoice(path: str | Path) -> schemas.InvoiceDoc:
    path = Path(path)
    ym = _MONTH_RE.search(path.name)
    text = extract_pdf_text(path)
    return schemas.InvoiceDoc(
        settlement_month=pd.to_datetime(f"{ym.group(1)}-{int(ym.group(2)):02d}-01").date(),
        items=parse_subject_lines(text, schemas.ZHEJIANG_CATEGORY_RULES),
        total_amount_cny=parse_total_from_summary(text),
        settled_volume_mwh=parse_summary_volume(text),
    )
