# services/retail_risk/parsers/invoice_shandong_pdf.py
"""山东 monthly settlement PDF (SDPX) -> InvoiceDoc.

The full two-sided bill: 购电侧 (midlong as CfD adjustment, can be negative-price;
RT full-volume spot; 辅助服务; 市场运营费用) and 售电侧 (零售电能量收入, 售电服务费).
Watermark stamps render in STSong-Light — dropped at char level.
Supersedes the 7021 spot-side Excel for monthly totals (7021 ties to the
实时分时 line inside this PDF to the cent).
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from services.retail_risk import schemas
from services.retail_risk.parsers.invoice_common import (
    extract_pdf_text, parse_subject_lines, parse_summary_volume, parse_total_from_summary,
)

_MONTH_RE = re.compile(r"(\d{4})年(\d{1,2})月")


def parse_shandong_pdf_invoice(path: str | Path) -> schemas.InvoiceDoc:
    path = Path(path)
    ym = _MONTH_RE.search(path.name)
    text = extract_pdf_text(path, drop_fonts={"STSong-Light"})
    return schemas.InvoiceDoc(
        settlement_month=pd.to_datetime(f"{ym.group(1)}-{int(ym.group(2)):02d}-01").date(),
        items=parse_subject_lines(text, schemas.SHANDONG_CATEGORY_RULES),
        total_amount_cny=parse_total_from_summary(text),
        settled_volume_mwh=parse_summary_volume(text),
    )
