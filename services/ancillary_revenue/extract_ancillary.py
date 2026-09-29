# services/ancillary_revenue/extract_ancillary.py
"""Extract BESS ancillary-service (调频) revenue from exchange reports.

Confirmed pattern — 新疆 省内辅助服务市场运行情况 (monthly PDF):
    "独立储能补偿费用 534.99 万元" (inside 调频补偿费用 breakdown)

Rows land as status='draft' for human review (same pattern as province_fuel_fleet)
— PDF extraction misfires must not flow straight into the chart.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

_DDL = """
CREATE TABLE IF NOT EXISTS marketdata.province_ancillary_revenue (
    id           SERIAL PRIMARY KEY,
    province     TEXT NOT NULL,
    month        DATE NOT NULL,          -- first of month
    metric       TEXT NOT NULL,          -- e.g. '调频补偿费用_独立储能'
    amount_yuan  NUMERIC NOT NULL,
    source_file  TEXT,
    status       TEXT NOT NULL DEFAULT 'draft',   -- draft/confirmed
    notes        TEXT,
    extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_par_prov_month_metric
    ON marketdata.province_ancillary_revenue (province, month, metric, COALESCE(source_file,''));
"""

_UPSERT = """
INSERT INTO marketdata.province_ancillary_revenue
    (province, month, metric, amount_yuan, source_file, status)
VALUES (%s, %s, %s, %s, %s, 'draft')
ON CONFLICT (province, month, metric, COALESCE(source_file,'')) DO UPDATE SET
    amount_yuan = EXCLUDED.amount_yuan,
    extracted_at = NOW()
"""

# 独立储能补偿费用 534.99 万元 / 独立储能补偿费用534.99万元
_RE_STORAGE_COMP = re.compile(r"独立储能补偿费用\s*([0-9]+(?:\.[0-9]+)?)\s*万元")
# filename month: 2026年6月 or 2026-06
_RE_MONTH_CN = re.compile(r"(20\d{2})年(\d{1,2})月")
_RE_MONTH_ISO = re.compile(r"(20\d{2})-(\d{2})")


@dataclass
class AncillaryRow:
    province: str
    month: str          # YYYY-MM-01
    metric: str
    amount_yuan: float
    source_file: str


def _month_from_name(name: str) -> str | None:
    m = _RE_MONTH_CN.search(name) or _RE_MONTH_ISO.search(name)
    if not m:
        return None
    return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-01"


def extract_xinjiang_monthly(pdf_path: str | Path) -> AncillaryRow | None:
    """Parse one 新疆 monthly ancillary-service PDF. None if no BESS figure."""
    import pdfplumber
    pdf_path = Path(pdf_path)
    month = _month_from_name(pdf_path.name)
    if not month:
        return None
    with pdfplumber.open(str(pdf_path)) as pdf:
        text = "\n".join((p.extract_text() or "") for p in pdf.pages)
    m = _RE_STORAGE_COMP.search(text)
    if not m:
        return None
    return AncillaryRow(
        province="新疆", month=month,
        metric="调频补偿费用_独立储能",
        amount_yuan=float(m.group(1)) * 1e4,   # 万元 → 元
        source_file=pdf_path.name,
    )


# province → extractor for its monthly-report PDFs
EXTRACTORS = {
    "新疆": extract_xinjiang_monthly,
}


def ensure_table(cur) -> None:
    cur.execute(_DDL)


def upsert_rows(rows: list[AncillaryRow], pg_url: str) -> int:
    import psycopg2
    conn = psycopg2.connect(pg_url)
    n = 0
    try:
        with conn.cursor() as cur:
            ensure_table(cur)
            for r in rows:
                cur.execute(_UPSERT, (r.province, r.month, r.metric,
                                      r.amount_yuan, r.source_file))
                n += 1
        conn.commit()
    finally:
        conn.close()
    return n
