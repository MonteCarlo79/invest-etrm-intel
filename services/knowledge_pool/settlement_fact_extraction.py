"""
services/knowledge_pool/settlement_fact_extraction.py

Fact extraction from settlement invoice page text.
Handles: grid_injection, grid_withdrawal, rural_grid, capacity_compensation.

Component taxonomy derived from actual B-6 下网 invoice text (Inner Mongolia State Grid bill
format). The bill uses "component_name元 amount" table-header lines, NOT numbered summary rows.

Key invoice components observed in actual PDFs:
  市场化购电费     → 市场上网电费   (energy)
  上网环节线损费用 → 上网环节线损费  (ancillary / line-loss tax)
  系统运行费       → 系统运行费     (system)
  功率因数调整电费 → 力率调整费     (power_quality)
  政府基金及附加   → 政策性损益     (policy)
  退补电费         → 退补电费       (adjustment)
  输配电费         → 输配电费       (system)
  目录电费         → 目录电费       (energy)
  容（需）量电费   → 容量电费       (capacity)
  绿电费用         → 绿电费         (policy)
"""
from __future__ import annotations

import re
from typing import Optional

from .db import get_conn

# ── Reconciliation thresholds ────────────────────────────────────────────────
RECON_FLAG_PCT_THRESHOLD = 1.0    # percent
RECON_FLAG_ABS_THRESHOLD = 500.0  # yuan

# ── Component normalization taxonomy ─────────────────────────────────────────
_COMPONENT_ALIASES: dict[str, str] = {
    # Energy — market purchase
    "市场化购电费": "市场上网电费",
    "市场上网电费（含规市场中市场交易电量+合同偏差电量）": "市场上网电费",
    "市场上网电费": "市场上网电费",
    "直接交易电费": "市场上网电费",  # 核查票 format alias
    "电能电费": "电能电费",           # sub-item of 市场化购电费
    "合同偏差电费": "合同偏差电费",
    # Energy — other
    "上网电量": "上网电量",
    "下网电量": "下网电量",
    "发电量": "上网电量",
    "市场上网电量": "上网电量",
    "目录电费": "目录电费",
    "峰谷电费差": "峰谷电费差",
    # Ancillary / line loss
    "上网环节线损费用": "上网环节线损费",
    "低碳辅助服务费": "低碳辅助服务费",
    "调频辅助服务结算": "调频辅助服务费",
    "调频辅助服务费": "调频辅助服务费",
    "备用辅助服务费": "备用辅助服务费",
    "辅助服务费": "辅助服务费",
    "市场运行调整费用": "市场运行调整费",
    # System
    "输配电费": "输配电费",
    "系统运行费": "系统运行费",
    "燃煤发电费": "燃煤发电费",
    "水库维护费": "水库维护费",
    "电力市场摊销费": "电力市场摊销费",
    # Power quality
    "功率因数调整电费": "力率调整费",
    "力率调整费": "力率调整费",
    "功率因数调整费": "力率调整费",
    "无功电量费": "无功电量费",
    # Capacity
    "容（需）量电费": "容量电费",
    "容(需)量电费": "容量电费",
    "容量电费": "容量电费",
    "基本电费": "基本电费",
    # Compensation
    "储能容量补偿费用": "储能容量补偿费",
    "容量补偿费": "储能容量补偿费",
    "储能容量补偿": "储能容量补偿费",
    "容量补偿": "储能容量补偿费",
    # Policy / subsidy
    "政府基金及附加": "政策性损益",
    "政策性损益及附加": "政策性损益",
    "政策性损益": "政策性损益",
    "绿电费用": "绿电费",
    "绿电费": "绿电费",
    "贴（补）贴": "补贴电费",
    "贴(补)贴": "补贴电费",
    "补贴电费": "补贴电费",
    # Adjustment
    "退补电费": "退补电费",
    "退补费": "退补费",
    "其他电费": "其他电费",
    # Total
    "总电费": "总电费",
    "合计": "总电费",
    "应付金额": "应付金额",
    "应收金额": "应收金额",
    "结算总金额": "总电费",
}

_COMPONENT_GROUPS: dict[str, str] = {
    "市场上网电费": "energy",
    "电能电费": "energy",
    "合同偏差电费": "energy",
    "上网电量": "energy",
    "下网电量": "energy",
    "目录电费": "energy",
    "峰谷电费差": "energy",
    "上网环节线损费": "ancillary",
    "低碳辅助服务费": "ancillary",
    "调频辅助服务费": "ancillary",
    "备用辅助服务费": "ancillary",
    "辅助服务费": "ancillary",
    "市场运行调整费": "ancillary",
    "输配电费": "system",
    "系统运行费": "system",
    "燃煤发电费": "system",
    "水库维护费": "system",
    "电力市场摊销费": "system",
    "力率调整费": "power_quality",
    "无功电量费": "power_quality",
    "容量电费": "capacity",
    "基本电费": "capacity",
    "储能容量补偿费": "compensation",
    "政策性损益": "policy",
    "绿电费": "policy",
    "补贴电费": "subsidy",
    "退补电费": "adjustment",
    "退补费": "adjustment",
    "其他电费": "adjustment",
    "总电费": "total",
    "应付金额": "total",
    "应收金额": "total",
}


def normalize_component(raw: str) -> str:
    """Return canonical component name; prefix unknown with 'unknown:'."""
    raw = raw.strip()
    if raw in _COMPONENT_ALIASES:
        return _COMPONENT_ALIASES[raw]
    # Partial match for long decorated names
    for alias, canonical in _COMPONENT_ALIASES.items():
        if len(alias) >= 4 and (alias in raw or raw in alias):
            return canonical
    return f"unknown:{raw[:50]}"


def _parse_amount(s: str) -> Optional[float]:
    """Parse '6,576,452.43' or '-53,709.58' to float."""
    try:
        cleaned = s.replace(",", "").strip()
        cleaned = cleaned.replace("−", "-").replace("–", "-")
        return float(cleaned)
    except (ValueError, AttributeError):
        return None


# ── Main extraction patterns ─────────────────────────────────────────────────

# Format A (电费账单 bill from Inner Mongolia State Grid):
# Table-header amount lines: "市场化购电费元 6576452.43" or "功率因数调整电费元 -53709.58"
_RE_TABLE_AMOUNT = re.compile(
    r"([\u4e00-\u9fff（）()/\.%\-]+(?:费用?|费|电量|附加|损益|调整))"  # component name
    r"元\s+"                                                              # 元 separator
    r"(-?[\d,]+\.?\d*)"                                                   # amount
)

# Format A total from page 3: "=总电费(元)\n7107568.13"
_RE_TOTAL_P3 = re.compile(
    r"=\s*总电费\s*[（(][元Ԫ][）)]\s*\n\s*(-?[\d,]+\.?\d+)"
)

# Format A total from page 1 bill header: "总电费 （元）\n电费构成 7107568.13"
_RE_TOTAL_P1 = re.compile(
    r"总电费\s*[（(][元Ԫ][）)]\s*\n.*?电费构成\s+(-?[\d,]+\.?\d+)",
    re.DOTALL,
)

# Format B (核查票 verification bill — different tabular structure):
# Rows: "{component} 平 {qty} {price} {amount}"  e.g. "上网环节线损费用 平 267700 0.0086 2322.83"
_RE_TABULAR_AMOUNT = re.compile(
    r"([\u4e00-\u9fff（）()/\-]+(?:费用?|费|电费|损益))\s+"  # component name
    r"[平峰谷尖总]\s+"                                          # time period flag
    r"[-\d,]+\s+"                                               # quantity
    r"[-\d.]+\s+"                                               # unit price
    r"(-?[\d,]+\.?\d+)"                                         # amount (last column)
)

# Format B total: "总电费： 76469.9" or "售电量： 267700 总电费： 76469.9"
_RE_TOTAL_COLON = re.compile(r"总电费[：:]\s*(-?[\d,]+\.?\d+)")

# Format B energy sold: "售电量： 267700"
_RE_ENERGY_SOLD = re.compile(r"售电量[：:]\s*([\d,]+)")

# Format C (上网电量结算单 from trading center): simple 3-column table
# "合计应收发票金额 5258.040 844.53 4,440,587.54"
# Take the last 2-decimal-place amount on a line containing 合计/应收/应付
_RE_TOTAL_YINGSHOU = re.compile(
    r"(?:合计|应[收付])[^\n]*([\d,]+\.\d{2})\s*(?:\n|$)"
)

# Format C energy (MWh): e.g. "现货市场 5258.040 MWh ..." or just the qty column
# When kWh absent, try MWh — take first number > 0.1 before price column
_RE_ENERGY_MWH = re.compile(r"([\d,]+\.?\d*)\s*[Mm][Ww][Hh]", re.IGNORECASE)

# Energy quantity (Format A): "16,961,290.0 kWh" — take first large value only
_RE_ENERGY_KWH = re.compile(r"([\d,]+\.?\d*)\s*kWh", re.IGNORECASE)


def _extract_grid_invoice_facts(text: str, page_no: int) -> list[dict]:
    """
    Extract facts from grid injection / withdrawal / rural_grid invoice text.

    Handles three invoice formats:
    Format A (电费账单 bill): "{component}元 {amount}" table-header lines
    Format B (核查票 verification): "{component} 平 {qty} {price} {amount}" rows
                                    and "总电费： {amount}" total line
    Format C (上网电量结算单 trading center): 3-column table, total on "合计/应收" line
    """
    results = []
    seen_components: set[str] = set()

    def _add(raw_name: str, raw_amount: str, fact_text: str, source: str):
        canonical = normalize_component(raw_name)
        amount = _parse_amount(raw_amount)
        if amount is None:
            return
        if canonical in seen_components or canonical.startswith("unknown:"):
            return
        if len(raw_name) < 3:
            return
        seen_components.add(canonical)
        fact_type = "total_amount" if canonical in ("总电费", "应付金额", "应收金额") else "charge_component"
        results.append({
            "fact_type": fact_type,
            "component_name": canonical,
            "component_group": _COMPONENT_GROUPS.get(canonical, "other"),
            "metric_value": amount,
            "metric_unit": "yuan",
            "fact_text": fact_text[:300],
            "page_no": page_no,
            "confidence": "medium",
            "source_method": source,
        })

    # ── Format A: table-header amount lines ──────────────────────────────────
    for m in _RE_TABLE_AMOUNT.finditer(text):
        _add(m.group(1), m.group(2), m.group(0), "pdf_regex")

    # ── Format B: tabular rows "{component} 平 qty price amount" ─────────────
    for m in _RE_TABULAR_AMOUNT.finditer(text):
        _add(m.group(1), m.group(2), m.group(0), "pdf_regex")

    # ── Total extraction (all formats) ───────────────────────────────────────
    if "总电费" not in seen_components:
        for pattern in (_RE_TOTAL_P3, _RE_TOTAL_P1, _RE_TOTAL_COLON):
            m = pattern.search(text)
            if m:
                total = _parse_amount(m.group(1))
                if total and total > 0:
                    seen_components.add("总电费")
                    results.append({
                        "fact_type": "total_amount",
                        "component_name": "总电费",
                        "component_group": "total",
                        "metric_value": total,
                        "metric_unit": "yuan",
                        "fact_text": m.group(0)[:200],
                        "page_no": page_no,
                        "confidence": "medium",
                        "source_method": "pdf_regex",
                    })
                    break

    # ── Format C total: "合计/应收/应付 qty price {total}" ────────────────────
    # Greedy [^\n]* in the pattern eats leading digits; use re.findall on the
    # full matched line and take the LAST *.## value (the rightmost/total column).
    if "总电费" not in seen_components:
        m = _RE_TOTAL_YINGSHOU.search(text)
        if m:
            amounts = re.findall(r"[\d,]+\.\d{2}", m.group(0))
            if amounts:
                total = _parse_amount(amounts[-1])
                if total and total > 100:
                    seen_components.add("总电费")
                    results.append({
                        "fact_type": "total_amount",
                        "component_name": "总电费",
                        "component_group": "total",
                        "metric_value": total,
                        "metric_unit": "yuan",
                        "fact_text": m.group(0).strip()[:200],
                        "page_no": page_no,
                        "confidence": "medium",
                        "source_method": "pdf_regex",
                    })

    # ── Energy quantity ───────────────────────────────────────────────────────
    # Format A: "16,961,290 kWh"
    for m in _RE_ENERGY_KWH.finditer(text):
        kwh = _parse_amount(m.group(1))
        if kwh and kwh > 1000:
            results.append({
                "fact_type": "energy_kwh",
                "component_name": "上网电量",
                "component_group": "energy",
                "metric_value": kwh,
                "metric_unit": "kWh",
                "fact_text": text[max(0, m.start() - 30):m.end() + 10][:200],
                "page_no": page_no,
                "confidence": "medium",
                "source_method": "pdf_regex",
            })
            break

    # Format B: "售电量： 267700"
    if "上网电量" not in seen_components:
        m = _RE_ENERGY_SOLD.search(text)
        if m:
            kwh = _parse_amount(m.group(1))
            if kwh and kwh > 100:
                results.append({
                    "fact_type": "energy_kwh",
                    "component_name": "上网电量",
                    "component_group": "energy",
                    "metric_value": kwh,
                    "metric_unit": "kWh",
                    "fact_text": m.group(0)[:100],
                    "page_no": page_no,
                    "confidence": "medium",
                    "source_method": "pdf_regex",
                })

    # Format C: "{qty} MWh" explicitly tagged — only when kWh not already found
    energy_found = any(
        f.get("fact_type") in ("energy_kwh", "energy_mwh") for f in results
    )
    if not energy_found:
        m = _RE_ENERGY_MWH.search(text)
        if m:
            mwh = _parse_amount(m.group(1))
            if mwh and mwh > 0.1:
                results.append({
                    "fact_type": "energy_mwh",
                    "component_name": "上网电量",
                    "component_group": "energy",
                    "metric_value": mwh,
                    "metric_unit": "MWh",
                    "fact_text": text[max(0, m.start() - 20):m.end() + 20][:100],
                    "page_no": page_no,
                    "confidence": "medium",
                    "source_method": "pdf_regex",
                })

    return results


def _extract_compensation_facts(text: str, page_no: int, asset_map: dict) -> list[dict]:
    """
    Extract per-asset compensation facts from capacity_compensation table PDF.
    asset_map: {dispatch_unit_name_cn: asset_slug}
    """
    results = []
    for line in text.split("\n"):
        line = line.strip()
        for dispatch_name, slug in asset_map.items():
            if dispatch_name in line:
                # Extract numeric values — take last positive value (合计 column)
                nums = re.findall(r"(-?[\d,]+\.?\d+)", line)
                for n in reversed(nums):
                    val = _parse_amount(n)
                    if val is not None and val > 0:
                        results.append({
                            "asset_slug_override": slug,
                            "fact_type": "capacity_compensation",
                            "component_name": "储能容量补偿费",
                            "component_group": "compensation",
                            "metric_value": val,
                            "metric_unit": "yuan",
                            "fact_text": line[:300],
                            "page_no": page_no,
                            "confidence": "high",
                            "source_method": "table_extraction",
                        })
                        break
    return results


def extract_facts_for_settlement_document(
    doc_id: int,
    invoice_type: str,
    asset_slug: str,
    settlement_year: int,
    settlement_month: int,
    period_half: str = "full",
    asset_map: Optional[dict] = None,
) -> int:
    """
    Read pages for doc_id, extract facts, store in staging.settlement_report_facts.
    Returns count of fact rows written.
    asset_map required for capacity_compensation: {dispatch_unit_name_cn: asset_slug}
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT page_no, extracted_text FROM staging.settlement_report_pages "
                "WHERE document_id = %s ORDER BY page_no",
                (doc_id,),
            )
            pages = cur.fetchall()

    all_facts: list[dict] = []

    for page_no, text in pages:
        if not text or not text.strip():
            continue

        if invoice_type == "capacity_compensation":
            if asset_map is None:
                continue
            facts = _extract_compensation_facts(text, page_no, asset_map)
        else:
            facts = _extract_grid_invoice_facts(text, page_no)

        all_facts.extend(facts)

    if not all_facts:
        return 0

    # Deduplicate by (asset_slug, fact_type, component_name, period_half) — keep highest confidence
    seen: dict[tuple, dict] = {}
    CONF_ORDER = {"high": 2, "medium": 1, "low": 0}
    for f in all_facts:
        slug = f.pop("asset_slug_override", asset_slug)
        key = (slug, f["fact_type"], f.get("component_name", ""), period_half)
        existing = seen.get(key)
        if existing is None or CONF_ORDER.get(f["confidence"], 0) > CONF_ORDER.get(existing.get("confidence", "low"), 0):
            seen[key] = {**f, "_asset_slug": slug}

    inserts = []
    for (slug, ft, cn, ph), f in seen.items():
        inserts.append((
            doc_id, slug, settlement_year, settlement_month, ph,
            invoice_type, ft, cn, f.get("component_group"),
            f.get("metric_value"), f.get("metric_unit"),
            f["fact_text"], f["page_no"],
            f["confidence"], f["source_method"],
        ))

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO staging.settlement_report_facts
                    (document_id, asset_slug, settlement_year, settlement_month, period_half,
                     invoice_type, fact_type, component_name, component_group,
                     metric_value, metric_unit, fact_text, page_no, confidence, source_method)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (document_id, asset_slug, fact_type, component_name, period_half)
                DO UPDATE SET
                    metric_value   = EXCLUDED.metric_value,
                    fact_text      = EXCLUDED.fact_text,
                    source_method  = EXCLUDED.source_method
                """,
                inserts,
            )
        conn.commit()

    return len(inserts)


def run_reconciliation_check(
    doc_id: int,
    asset_slug: str,
    settlement_year: int,
    settlement_month: int,
    invoice_type: str,
    period_half: str,
) -> int:
    """
    Check for prior parsed docs with same asset/period/type/period_half.
    Compare facts and write reconciliation rows for any matches.
    Returns number of reconciliation rows written.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM staging.settlement_report_documents
                WHERE asset_slug = %s
                  AND settlement_year = %s AND settlement_month = %s
                  AND invoice_type = %s AND period_half = %s
                  AND id != %s AND ingest_status = 'parsed'
                """,
                (asset_slug, settlement_year, settlement_month, invoice_type, period_half, doc_id),
            )
            prior_ids = [r[0] for r in cur.fetchall()]

    if not prior_ids:
        return 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT fact_type, component_name, metric_value FROM staging.settlement_report_facts "
                "WHERE document_id = %s",
                (doc_id,),
            )
            new_facts = {(r[0], r[1]): r[2] for r in cur.fetchall()}

    recon_rows = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            for prior_id in prior_ids:
                cur.execute(
                    "SELECT fact_type, component_name, metric_value FROM staging.settlement_report_facts "
                    "WHERE document_id = %s",
                    (prior_id,),
                )
                prior_facts = {(r[0], r[1]): r[2] for r in cur.fetchall()}

                for (ft, cn), val_b in new_facts.items():
                    val_a = prior_facts.get((ft, cn))
                    if val_a is None or val_b is None:
                        continue
                    delta = float(val_b) - float(val_a)
                    flagged = False
                    flag_parts = []
                    if float(val_a) != 0:
                        pct = abs(delta) / abs(float(val_a)) * 100
                        if pct >= RECON_FLAG_PCT_THRESHOLD:
                            flagged = True
                            flag_parts.append(f"{pct:.2f}% exceeds {RECON_FLAG_PCT_THRESHOLD}% threshold")
                    if abs(delta) >= RECON_FLAG_ABS_THRESHOLD:
                        flagged = True
                        flag_parts.append(f"delta {abs(delta):.2f} yuan exceeds {RECON_FLAG_ABS_THRESHOLD} yuan threshold")

                    recon_rows.append((
                        asset_slug, settlement_year, settlement_month, invoice_type,
                        ft, cn, prior_id, doc_id, val_a, val_b,
                        flagged, "; ".join(flag_parts) if flag_parts else None,
                        RECON_FLAG_PCT_THRESHOLD, RECON_FLAG_ABS_THRESHOLD,
                    ))

    if not recon_rows:
        return 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO staging.settlement_reconciliation
                    (asset_slug, settlement_year, settlement_month, invoice_type,
                     fact_type, component_name, version_a_doc_id, version_b_doc_id,
                     value_a, value_b, flagged, flag_reason,
                     flag_threshold_pct, flag_threshold_abs)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                recon_rows,
            )
        conn.commit()

    return len(recon_rows)
