"""Recognise and ingest 电力现货市场价格与运行月报 (national spot monthly report) PDFs.

Separate from the daily pipeline (spot_ingest_bridge / is_spot_pdf), which only
handles 日报. Monthly files previously fell through to is_exchange_report and were
misrouted into staging.exchange_monthly_reports — this module takes precedence.
"""
from __future__ import annotations

import datetime as dt
import logging
import re
from typing import Optional

from services.spot_ingest.provinces import PROVINCES_MAP

logger = logging.getLogger(__name__)

# Filename patterns that identify the national spot monthly report (case-insensitive)
SPOT_MONTHLY_PATTERNS = ["电力现货市场价格与运行月报"]


def is_spot_monthly_pdf(filename: str) -> bool:
    name_lower = filename.lower()
    return name_lower.endswith(".pdf") and any(
        p.lower() in name_lower for p in SPOT_MONTHLY_PATTERNS
    )


def infer_report_month(filename: str) -> Optional[dt.date]:
    """Infer report month (first of month) from filename, e.g. （2026年6月）.

    Returns None if no explicit year+month — never stamps the current year
    (same rule as settlement ingest, commit 1064925).
    """
    m = re.search(r"(\d{4})年(\d{1,2})月", filename)
    if not m:
        return None
    try:
        return dt.date(int(m.group(1)), int(m.group(2)), 1)
    except ValueError:
        return None


def extract_pages_text(pdf_path, max_pages: int = 10) -> str:
    """Extract text from the first max_pages pages. layout=True preserves column
    positions, which the wrapped multi-line headers of 表2 require."""
    import pdfplumber  # lazy — keeps recognizer tests free of the dependency

    parts = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for i, page in enumerate(pdf.pages[:max_pages]):
            text = page.extract_text(layout=True) or ""
            parts.append(f"=== page {i + 1} ===\n{text}")
    return "\n\n".join(parts)


_EXTRACT_SYSTEM = """You are a data extraction assistant for China electricity market reports.
The user provides text from 《电力现货市场价格与运行月报》 (national spot market monthly report), first pages only.

Extract TWO things:
1. 总体情况 (section 一, national summary): RT/DA total cleared volume & avg price, 中长期合约覆盖电量/占比/成交均价.
2. 表2 连续运行地区运行情况一览表: one row per province/region.

Return ONLY valid JSON, no markdown:
{
  "national": {
    "rt_total_volume_yi_kwh": number|null, "rt_avg_price": number|null,
    "da_total_volume_yi_kwh": number|null, "da_avg_price": number|null,
    "mlt_coverage_volume_yi_kwh": number|null, "mlt_coverage_pct": number|null,
    "mlt_avg_price": number|null
  },
  "provinces": [
    {
      "province_cn": "地区名(按原文)", "run_status": "正式运行/试运行|null",
      "mlt_volume_yi_kwh": number|null, "mlt_avg_price": number|null,
      "mlt_coverage_pct": number|null,
      "rt_volume_yi_kwh": number|null, "rt_avg_price": number|null, "rt_mom_pct": number|null,
      "da_volume_yi_kwh": number|null, "da_avg_price": number|null, "da_mom_pct": number|null
    }
  ]
}

Rules:
- Volumes in 亿千瓦时, prices in 元/千瓦时, percentages as numbers (4.82 for 4.82%). Negative MoM keeps its sign.
- mlt_volume_yi_kwh = 中长期市场成交电量 (NOT 合约覆盖电量); mlt_coverage_pct = 中长期合约覆盖电量占比.
- "/" or unreadable/missing → null, never 0.
- SKIP 表1 省间现货市场 (四川主网/灵绍配套电源/天中配套电源 etc. belong to 表1 — exclude them).
- SKIP chart captions (图 N …) and narrative paragraphs.
- provinces come ONLY from 表2 (连续运行地区)."""


# Input cap for the Claude call. The 2026-06 report's 表2 ends ~36k chars in
# (layout=True is whitespace-heavy); 45k covers denser future reports with margin.
_EXTRACT_MAX_CHARS = 45_000


def extract_monthly_json(text: str, report_month: dt.date, api_key: str) -> dict:
    """Structure extracted page text via Claude. Raises ValueError on bad output."""
    import json

    from shared.anthropic_client import make_client  # lazy, house style

    client = make_client(api_key)
    resp = client.messages.create(
        model="claude-sonnet-4-6",  # haiku-4-5 requires use-case form on this Bedrock account
        max_tokens=4000,
        system=_EXTRACT_SYSTEM,
        messages=[{
            "role": "user",
            "content": (
                f"Report month: {report_month.strftime('%Y年%m月')} "
                f"(all data is for {report_month.year}-{report_month.month:02d}).\n\n"
                f"Content:\n{text[:_EXTRACT_MAX_CHARS]}"
            ),
        }],
    )
    raw = resp.content[0].text.strip()
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if not m:
        raise ValueError(f"Claude returned no JSON: {raw[:200]}")
    try:
        data = json.loads(m.group(0))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Claude returned invalid JSON: {exc}: {raw[:200]}") from exc
    if "provinces" not in data or "national" not in data:
        raise ValueError(f"Claude JSON missing required keys: {list(data.keys())}")
    return data


_PRICE_FIELDS = ("rt_avg_price", "da_avg_price", "mlt_avg_price")
_VOLUME_FIELDS = (
    "rt_volume_yi_kwh", "da_volume_yi_kwh", "mlt_volume_yi_kwh",
    "rt_total_volume_yi_kwh", "da_total_volume_yi_kwh", "mlt_coverage_volume_yi_kwh",
)
_PCT_FIELDS = ("rt_mom_pct", "da_mom_pct")


def _clean_number(record: dict, field: str, lo: float, hi: float, label: str, warnings: list[str]) -> None:
    val = record.get(field)
    if val is None:
        return
    try:
        val = float(val)
    except (TypeError, ValueError):
        record[field] = None
        warnings.append(f"{label}: {field} 非数值已置空")
        return
    if not (lo <= val <= hi):
        record[field] = None
        warnings.append(f"{label}: {field}={val} 超出范围[{lo},{hi}]已置空")
    else:
        record[field] = val


def validate_monthly_data(data: dict) -> list[str]:
    """Validate in place. Returns warnings. Raises ValueError on hard failure."""
    warnings: list[str] = []
    provinces = data.get("provinces") or []
    if not provinces:
        raise ValueError("未提取到任何省份数据")
    if len(provinces) < 20:
        warnings.append(f"省份数量仅 {len(provinces)}（预期 ≥20）")

    national = data.get("national") or {}
    for f in _PRICE_FIELDS:
        _clean_number(national, f, 0.0, 2.0, "全国", warnings)
    for f in _VOLUME_FIELDS:
        _clean_number(national, f, 0.0, 20000.0, "全国", warnings)
    _clean_number(national, "mlt_coverage_pct", 0.0, 400.0, "全国", warnings)

    kept = []
    for row in provinces:
        cn = (row.get("province_cn") or "").strip()
        if cn not in PROVINCES_MAP:
            warnings.append(f"无法识别地区「{cn}」，该行已丢弃")
            continue
        for f in _PRICE_FIELDS:
            _clean_number(row, f, 0.0, 2.0, cn, warnings)
        for f in _VOLUME_FIELDS:
            if f in row:
                _clean_number(row, f, 0.0, 20000.0, cn, warnings)
        for f in _PCT_FIELDS:
            _clean_number(row, f, -1000.0, 1000.0, cn, warnings)
        _clean_number(row, "mlt_coverage_pct", 0.0, 400.0, cn, warnings)
        row["province_cn"] = cn  # write back stripped name — upsert keys on raw value
        kept.append(row)
    data["provinces"] = kept
    if not kept:
        raise ValueError("所有省份行均无法识别")
    return warnings


_UPSERT_NATIONAL_SQL = """
INSERT INTO spot_monthly_national (
    report_month, rt_total_volume_yi_kwh, rt_avg_price,
    da_total_volume_yi_kwh, da_avg_price,
    mlt_coverage_volume_yi_kwh, mlt_coverage_pct, mlt_avg_price, source_file
) VALUES (
    %(report_month)s, %(rt_total_volume_yi_kwh)s, %(rt_avg_price)s,
    %(da_total_volume_yi_kwh)s, %(da_avg_price)s,
    %(mlt_coverage_volume_yi_kwh)s, %(mlt_coverage_pct)s, %(mlt_avg_price)s, %(source_file)s
)
ON CONFLICT (report_month) DO UPDATE SET
    rt_total_volume_yi_kwh     = COALESCE(EXCLUDED.rt_total_volume_yi_kwh, spot_monthly_national.rt_total_volume_yi_kwh),
    rt_avg_price               = COALESCE(EXCLUDED.rt_avg_price, spot_monthly_national.rt_avg_price),
    da_total_volume_yi_kwh     = COALESCE(EXCLUDED.da_total_volume_yi_kwh, spot_monthly_national.da_total_volume_yi_kwh),
    da_avg_price               = COALESCE(EXCLUDED.da_avg_price, spot_monthly_national.da_avg_price),
    mlt_coverage_volume_yi_kwh = COALESCE(EXCLUDED.mlt_coverage_volume_yi_kwh, spot_monthly_national.mlt_coverage_volume_yi_kwh),
    mlt_coverage_pct           = COALESCE(EXCLUDED.mlt_coverage_pct, spot_monthly_national.mlt_coverage_pct),
    mlt_avg_price              = COALESCE(EXCLUDED.mlt_avg_price, spot_monthly_national.mlt_avg_price),
    source_file                = EXCLUDED.source_file,
    ingested_at                = now();
"""

_UPSERT_PROVINCE_SQL = """
INSERT INTO spot_monthly_province (
    report_month, province_en, province_cn, run_status,
    mlt_volume_yi_kwh, mlt_avg_price, mlt_coverage_pct,
    rt_volume_yi_kwh, rt_avg_price, rt_mom_pct,
    da_volume_yi_kwh, da_avg_price, da_mom_pct, source_file
) VALUES (
    %(report_month)s, %(province_en)s, %(province_cn)s, %(run_status)s,
    %(mlt_volume_yi_kwh)s, %(mlt_avg_price)s, %(mlt_coverage_pct)s,
    %(rt_volume_yi_kwh)s, %(rt_avg_price)s, %(rt_mom_pct)s,
    %(da_volume_yi_kwh)s, %(da_avg_price)s, %(da_mom_pct)s, %(source_file)s
)
ON CONFLICT (report_month, province_en) DO UPDATE SET
    province_cn       = EXCLUDED.province_cn,
    run_status        = COALESCE(EXCLUDED.run_status, spot_monthly_province.run_status),
    mlt_volume_yi_kwh = COALESCE(EXCLUDED.mlt_volume_yi_kwh, spot_monthly_province.mlt_volume_yi_kwh),
    mlt_avg_price     = COALESCE(EXCLUDED.mlt_avg_price, spot_monthly_province.mlt_avg_price),
    mlt_coverage_pct  = COALESCE(EXCLUDED.mlt_coverage_pct, spot_monthly_province.mlt_coverage_pct),
    rt_volume_yi_kwh  = COALESCE(EXCLUDED.rt_volume_yi_kwh, spot_monthly_province.rt_volume_yi_kwh),
    rt_avg_price      = COALESCE(EXCLUDED.rt_avg_price, spot_monthly_province.rt_avg_price),
    rt_mom_pct        = COALESCE(EXCLUDED.rt_mom_pct, spot_monthly_province.rt_mom_pct),
    da_volume_yi_kwh  = COALESCE(EXCLUDED.da_volume_yi_kwh, spot_monthly_province.da_volume_yi_kwh),
    da_avg_price      = COALESCE(EXCLUDED.da_avg_price, spot_monthly_province.da_avg_price),
    da_mom_pct        = COALESCE(EXCLUDED.da_mom_pct, spot_monthly_province.da_mom_pct),
    source_file       = EXCLUDED.source_file,
    ingested_at       = now();
"""


def upsert_monthly_rows(national: dict, provinces: list[dict], report_month: dt.date, source_file: str) -> dict:
    """Upsert national + province monthly rows in a single transaction."""
    from services.knowledge_pool.db import get_conn  # lazy, house style

    national_params = {
        "report_month": report_month,
        "rt_total_volume_yi_kwh": national.get("rt_total_volume_yi_kwh"),
        "rt_avg_price": national.get("rt_avg_price"),
        "da_total_volume_yi_kwh": national.get("da_total_volume_yi_kwh"),
        "da_avg_price": national.get("da_avg_price"),
        "mlt_coverage_volume_yi_kwh": national.get("mlt_coverage_volume_yi_kwh"),
        "mlt_coverage_pct": national.get("mlt_coverage_pct"),
        "mlt_avg_price": national.get("mlt_avg_price"),
        "source_file": source_file,
    }
    province_params = []
    for row in provinces:
        params = {k: row.get(k) for k in (
            "run_status", "mlt_volume_yi_kwh", "mlt_avg_price", "mlt_coverage_pct",
            "rt_volume_yi_kwh", "rt_avg_price", "rt_mom_pct",
            "da_volume_yi_kwh", "da_avg_price", "da_mom_pct",
        )}
        params["report_month"] = report_month
        params["province_cn"] = row["province_cn"]
        params["province_en"] = PROVINCES_MAP[row["province_cn"]]
        params["source_file"] = source_file
        province_params.append(params)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(_UPSERT_NATIONAL_SQL, national_params)
            for params in province_params:
                cur.execute(_UPSERT_PROVINCE_SQL, params)
        conn.commit()
    return {"national_written": True, "provinces_upserted": len(province_params)}


def ingest_monthly_report(filename: str, pdf_bytes: bytes, api_key: str) -> dict:
    """Parse a spot monthly report PDF and upsert into DB. Single entry point
    used by the Hermes handlers and the backfill CLI.

    Raises ValueError if the filename has no explicit year+month.
    """
    import tempfile
    from pathlib import Path

    report_month = infer_report_month(filename)
    if report_month is None:
        raise ValueError(
            f"无法从文件名推断报告月份：{filename}。"
            "请重命名为包含年份和月份的形式（如「（2026年6月）」）后重发。"
        )

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(pdf_bytes)
        tmp_path = Path(tmp.name)
    try:
        text = extract_pages_text(tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)

    data = extract_monthly_json(text, report_month, api_key)
    warnings = validate_monthly_data(data)
    result = upsert_monthly_rows(data["national"], data["provinces"], report_month, filename)
    logger.info(
        "spot monthly: %s → %s, %d provinces, %d warnings",
        filename, report_month, result["provinces_upserted"], len(warnings),
    )
    return {
        "month": report_month.strftime("%Y-%m"),
        "n_provinces": result["provinces_upserted"],
        "national_rt_avg": data["national"].get("rt_avg_price"),
        "provinces_upserted": result["provinces_upserted"],
        "warnings": warnings,
    }
