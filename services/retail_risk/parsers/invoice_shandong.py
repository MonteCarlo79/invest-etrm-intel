# services/retail_risk/parsers/invoice_shandong.py
"""山东 wholesale invoice: 7021-*结算单.xlsx 结算依据 sheet (daily RT settlement)."""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from services.retail_risk import schemas

_MONTH_RE = re.compile(r"7021-(\d{4})-(\d{2})")


def parse_shandong_invoice(path: str | Path) -> schemas.InvoiceDoc:
    path = Path(path)
    ym = _MONTH_RE.search(path.name)
    month = f"{ym.group(1)}-{ym.group(2)}-01" if ym else None
    df = pd.read_excel(path, sheet_name="结算依据", header=None)
    # data starts after the 2-row header (row containing 实时用电量), ends before 合计
    start = next(i for i in range(len(df)) if "实时用电量" in str(df.iloc[i, 2])) + 1
    items: list[schemas.InvoiceItem] = []
    total = None
    for i in range(start, len(df)):
        label = str(df.iloc[i, 0])
        if "合计" in label:
            total = float(df.iloc[i, 4]) if pd.notna(df.iloc[i, 4]) else None
            continue
        date_v = df.iloc[i, 1]
        if pd.isna(date_v):
            continue
        delivery = pd.to_datetime(date_v).date()
        rt_vol, rt_px, rt_amt = df.iloc[i, 2], df.iloc[i, 3], df.iloc[i, 4]
        if pd.notna(rt_amt) and float(rt_amt) != 0:
            items.append(schemas.InvoiceItem(
                category="spot_energy", label_cn="实时电能量电费",
                volume_mwh=float(rt_vol) if pd.notna(rt_vol) else None,
                price_cny_mwh=float(rt_px) if pd.notna(rt_px) else None,
                amount_cny=float(rt_amt), delivery_date=delivery, notes="RT"))
        da_vol, da_px, da_amt = df.iloc[i, 5], df.iloc[i, 6], df.iloc[i, 7]
        if pd.notna(da_amt) and float(da_amt) != 0:
            items.append(schemas.InvoiceItem(
                category="spot_energy", label_cn="日前电能量电费",
                volume_mwh=float(da_vol) if pd.notna(da_vol) else None,
                price_cny_mwh=float(da_px) if pd.notna(da_px) else None,
                amount_cny=float(da_amt), delivery_date=delivery, notes="DA"))
    return schemas.InvoiceDoc(settlement_month=pd.to_datetime(month).date(),
                              items=items, total_amount_cny=total)
