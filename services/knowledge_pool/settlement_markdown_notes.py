"""
services/knowledge_pool/settlement_markdown_notes.py

Obsidian-compatible markdown note generation for settlement invoices.

Four note types:
  monthly_asset   — one note per asset × year-month (charge breakdown + total)
  asset_summary   — rolling index per asset (monthly totals table)
  charge_component— cross-asset component note (one per canonical component name)
  reconciliation  — diff note when ≥2 invoice versions exist for same period

Notes are deterministic across re-runs (no random IDs, no timestamps in body).
Notes are written to knowledge/settlement/ and registered in
staging.settlement_report_notes.
"""
from __future__ import annotations

import datetime as dt
import re
from pathlib import Path
from typing import Optional

from .db import get_conn

_REPO_ROOT = Path(__file__).resolve().parents[2]
_VAULT_ROOT = _REPO_ROOT / "knowledge" / "settlement"

_MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December",
}

_INVOICE_TYPE_LABELS = {
    "grid_injection": "上网 (Grid Injection)",
    "grid_withdrawal": "下网 (Grid Withdrawal)",
    "rural_grid": "农网 (Rural Grid)",
    "capacity_compensation": "容量补偿 (Capacity Compensation)",
}

_ASSET_DISPLAY: dict[str, str] = {
    "suyou":      "苏右 (SuYou)",
    "wulate":     "乌拉特 (WuLaTe)",
    "wuhai":      "乌海 (WuHai)",
    "wulanchabu": "乌兰察布 (WuLanChaBu)",
    "hetao":      "河套 (HeTao)",
    "hangjinqi":  "杭锦旗 (HangJinQi)",
    "siziwangqi": "四子王旗 (SiZiWangQi)",
    "gushanliang":"谷山梁 (GuShanLiang)",
}

_GROUP_ORDER = [
    "energy", "ancillary", "system", "capacity",
    "power_quality", "policy", "subsidy", "adjustment",
    "compensation", "total", "other",
]


# ── Common helpers ──────────────────────────────────────────────────────────────

def _frontmatter(**kwargs) -> str:
    lines = ["---"]
    for k, v in kwargs.items():
        if v is None:
            continue
        if isinstance(v, list):
            lines.append(f"{k}:")
            for item in v:
                lines.append(f"  - {item}")
        else:
            lines.append(f"{k}: {v}")
    lines.append("---")
    return "\n".join(lines)


def _fmt_yuan(v) -> str:
    if v is None:
        return "—"
    return f"{float(v):,.2f}"


def _register_note(
    note_type: str,
    note_key: str,
    note_path: Path,
    note_title: str,
    document_id: Optional[int],
    settlement_year: Optional[int],
    settlement_month: Optional[int],
    asset_slug: Optional[str],
):
    rel_path = str(note_path.relative_to(_REPO_ROOT)).replace("\\", "/")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO staging.settlement_report_notes
                    (document_id, note_type, note_key, note_path, note_title,
                     settlement_year, settlement_month, asset_slug)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (note_type, note_key) DO UPDATE SET
                    note_path        = EXCLUDED.note_path,
                    note_title       = EXCLUDED.note_title,
                    settlement_year  = EXCLUDED.settlement_year,
                    settlement_month = EXCLUDED.settlement_month,
                    asset_slug       = EXCLUDED.asset_slug,
                    updated_at       = now()
                """,
                (document_id, note_type, note_key, rel_path, note_title,
                 settlement_year, settlement_month, asset_slug),
            )
        conn.commit()


# ── Monthly asset note ───────────────────────────────────────────────────────────

def generate_monthly_asset_note(
    asset_slug: str,
    settlement_year: int,
    settlement_month: int,
) -> Path:
    """
    Generate knowledge/settlement/{asset_slug}/YYYY-MM.md

    Covers all invoice_types for this asset × period. Shows charge breakdown
    tables and reconciliation flags.
    """
    ym = f"{settlement_year}-{settlement_month:02d}"
    note_key = f"{asset_slug}_{ym}"
    display = _ASSET_DISPLAY.get(asset_slug, asset_slug)
    note_path = _VAULT_ROOT / asset_slug / f"{ym}.md"

    with get_conn() as conn:
        with conn.cursor() as cur:
            # All facts for this asset + period, grouped by invoice_type
            cur.execute(
                """
                SELECT f.invoice_type, f.fact_type, f.component_name,
                       f.component_group, f.metric_value, f.metric_unit,
                       f.period_half, f.page_no, f.confidence,
                       d.file_name, d.id AS doc_id
                FROM staging.settlement_report_facts f
                JOIN staging.settlement_report_documents d ON d.id = f.document_id
                WHERE f.asset_slug = %s
                  AND f.settlement_year = %s AND f.settlement_month = %s
                ORDER BY f.invoice_type, f.component_group, f.component_name
                """,
                (asset_slug, settlement_year, settlement_month),
            )
            facts = cur.fetchall()

            # Source documents
            cur.execute(
                """
                SELECT DISTINCT d.id, d.file_name, d.invoice_type, d.period_half,
                       d.ingest_status, d.page_count
                FROM staging.settlement_report_documents d
                WHERE d.asset_slug = %s
                  AND d.settlement_year = %s AND d.settlement_month = %s
                ORDER BY d.invoice_type, d.period_half
                """,
                (asset_slug, settlement_year, settlement_month),
            )
            docs = cur.fetchall()

            # Reconciliation rows
            cur.execute(
                """
                SELECT r.invoice_type, r.fact_type, r.component_name,
                       r.value_a, r.value_b, r.delta, r.delta_pct,
                       r.flagged, r.flag_reason,
                       r.version_a_doc_id, r.version_b_doc_id
                FROM staging.settlement_reconciliation r
                WHERE r.asset_slug = %s
                  AND r.settlement_year = %s AND r.settlement_month = %s
                ORDER BY r.flagged DESC, ABS(r.delta) DESC NULLS LAST
                """,
                (asset_slug, settlement_year, settlement_month),
            )
            recon_rows = cur.fetchall()

    # Organise facts by invoice_type
    by_type: dict[str, list] = {}
    for row in facts:
        inv_type = row[0]
        by_type.setdefault(inv_type, []).append(row)

    primary_doc_id = docs[0][0] if docs else None
    month_name = _MONTH_NAMES.get(settlement_month, str(settlement_month))

    fm = _frontmatter(
        title=f"{display} — Settlement {ym}",
        asset_slug=asset_slug,
        settlement_year=settlement_year,
        settlement_month=settlement_month,
        period=ym,
        note_type="monthly_asset",
        invoice_types=sorted(by_type.keys()),
        tags=["settlement", asset_slug, ym, str(settlement_year)],
    )

    sections = [
        fm,
        "",
        f"# {display} — Settlement {month_name} {settlement_year}",
        "",
        "## Source Documents",
        "",
    ]
    if docs:
        sections.append("| Doc ID | File | Type | Half | Status | Pages |")
        sections.append("|--------|------|------|------|--------|-------|")
        for did, fname, itype, half, status, pages in docs:
            label = _INVOICE_TYPE_LABELS.get(itype, itype)
            sections.append(
                f"| {did} | `{fname}` | {label} | {half} | {status} | {pages or '—'} |"
            )
    else:
        sections.append("_No source documents found for this period._")

    # Per invoice_type charge breakdowns
    for inv_type, type_facts in sorted(by_type.items()):
        label = _INVOICE_TYPE_LABELS.get(inv_type, inv_type)
        sections += [
            "",
            f"## {label}",
            "",
        ]

        # Split by period_half if more than one value present
        halves = sorted(set(f[6] for f in type_facts))

        for half in halves:
            half_facts = [f for f in type_facts if f[6] == half]
            if len(halves) > 1:
                sections.append(f"### Period: {half}")
                sections.append("")

            # Sort by group order then component name
            def _sort_key(r):
                grp = r[3] or "other"
                gi = _GROUP_ORDER.index(grp) if grp in _GROUP_ORDER else 99
                return (gi, r[2] or "")

            half_facts.sort(key=_sort_key)

            # Table: component | group | amount | unit | confidence
            charge_rows = [f for f in half_facts if f[1] in ("charge_component", "total_amount")]
            energy_rows = [f for f in half_facts if f[1] in ("energy_kwh", "energy_mwh")]
            comp_rows   = [f for f in half_facts if f[1] == "capacity_compensation"]

            if charge_rows:
                sections.append("| Component | Group | Amount | Unit | Confidence |")
                sections.append("|-----------|-------|--------|------|------------|")
                for _, ft, cn, grp, val, unit, ph, pg, conf, _, _ in charge_rows:
                    marker = " **[TOTAL]**" if ft == "total_amount" else ""
                    sections.append(
                        f"| {cn or '—'}{marker} | {grp or '—'} | "
                        f"{_fmt_yuan(val)} | {unit or 'yuan'} | {conf} |"
                    )

            if energy_rows:
                sections += ["", "_Energy quantities:_", ""]
                for _, ft, cn, grp, val, unit, ph, pg, conf, _, _ in energy_rows:
                    sections.append(f"- **{cn}**: {_fmt_yuan(val)} {unit or ''}")

            if comp_rows:
                sections += ["", "| Component | Amount (yuan) | Confidence |",
                             "|-----------|---------------|------------|"]
                for _, ft, cn, grp, val, unit, ph, pg, conf, _, _ in comp_rows:
                    sections.append(f"| {cn or '—'} | {_fmt_yuan(val)} | {conf} |")

    # Reconciliation section
    if recon_rows:
        flagged = [r for r in recon_rows if r[7]]
        sections += [
            "",
            "## Reconciliation",
            "",
            f"Comparing {len(set((r[9], r[10]) for r in recon_rows))} document version pairs.",
            f"**Flagged differences: {len(flagged)}**",
            "",
            "| Invoice Type | Component | Value A | Value B | Delta | Δ% | Flagged |",
            "|-------------|-----------|---------|---------|-------|-----|---------|",
        ]
        for inv_type, ft, cn, va, vb, delta, dpct, flagged_b, flag_reason, da, db in recon_rows[:30]:
            flag_marker = "⚠️" if flagged_b else ""
            dpct_str = f"{float(dpct):.2f}%" if dpct is not None else "—"
            sections.append(
                f"| {inv_type} | {cn or ft} | {_fmt_yuan(va)} | {_fmt_yuan(vb)} | "
                f"{_fmt_yuan(delta)} | {dpct_str} | {flag_marker} |"
            )
        if any(r[7] for r in recon_rows):
            sections += ["", "### Flag Details", ""]
            for inv_type, ft, cn, va, vb, delta, dpct, flagged_b, flag_reason, da, db in recon_rows:
                if flagged_b and flag_reason:
                    sections.append(f"- **{cn or ft}** ({inv_type}): {flag_reason}")
    else:
        sections += [
            "",
            "## Reconciliation",
            "",
            "_No prior invoice versions found for this period — reconciliation not applicable._",
        ]

    sections += [
        "",
        "## Analyst Notes",
        "",
        "_Add observations here._",
    ]

    note_path.parent.mkdir(parents=True, exist_ok=True)
    note_path.write_text("\n".join(sections), encoding="utf-8")

    _register_note(
        "monthly_asset", note_key, note_path,
        f"{display} {ym}", primary_doc_id,
        settlement_year, settlement_month, asset_slug,
    )
    return note_path


# ── Asset summary (rolling index) note ─────────────────────────────────────────

def generate_asset_summary_note(asset_slug: str) -> Path:
    """
    Generate/update knowledge/settlement/{asset_slug}/index.md

    Rolling monthly totals table across all available periods.
    """
    display = _ASSET_DISPLAY.get(asset_slug, asset_slug)
    note_path = _VAULT_ROOT / asset_slug / "index.md"

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Monthly total by invoice_type
            cur.execute(
                """
                SELECT f.settlement_year, f.settlement_month, f.invoice_type,
                       f.metric_value
                FROM staging.settlement_report_facts f
                WHERE f.asset_slug = %s AND f.fact_type = 'total_amount'
                  AND f.component_name = '总电费'
                ORDER BY f.settlement_year, f.settlement_month, f.invoice_type
                """,
                (asset_slug,),
            )
            totals = cur.fetchall()

            # Count docs per period
            cur.execute(
                """
                SELECT settlement_year, settlement_month, invoice_type, COUNT(*) AS cnt
                FROM staging.settlement_report_documents
                WHERE asset_slug = %s AND ingest_status = 'parsed'
                GROUP BY settlement_year, settlement_month, invoice_type
                ORDER BY settlement_year, settlement_month
                """,
                (asset_slug,),
            )
            doc_counts = cur.fetchall()

            # Flagged reconciliation summary
            cur.execute(
                """
                SELECT settlement_year, settlement_month, invoice_type,
                       COUNT(*) AS flagged_count
                FROM staging.settlement_reconciliation
                WHERE asset_slug = %s AND flagged = TRUE
                GROUP BY settlement_year, settlement_month, invoice_type
                ORDER BY settlement_year, settlement_month
                """,
                (asset_slug,),
            )
            flags = cur.fetchall()

    flag_lookup: dict[tuple, int] = {(r[0], r[1], r[2]): r[3] for r in flags}

    # Build monthly totals by period × invoice_type
    invoice_types_seen: list[str] = sorted(set(r[2] for r in totals))
    periods: list[tuple[int, int]] = sorted(set((r[0], r[1]) for r in totals))

    totals_by_period: dict[tuple, dict[str, float]] = {}
    for yr, mo, itype, val in totals:
        totals_by_period.setdefault((yr, mo), {})[itype] = float(val) if val else 0.0

    # Monthly notes links
    period_links = [
        f"- [[{yr}-{mo:02d}]] — "
        + " | ".join(
            f"{_INVOICE_TYPE_LABELS.get(it, it)}: ¥{totals_by_period.get((yr, mo), {}).get(it, 0):,.0f}"
            for it in invoice_types_seen
            if totals_by_period.get((yr, mo), {}).get(it) is not None
        )
        for yr, mo in sorted(periods, reverse=True)
    ]

    fm = _frontmatter(
        title=f"{display} — Settlement Summary",
        asset_slug=asset_slug,
        note_type="asset_summary",
        periods_covered=len(periods),
        invoice_types=invoice_types_seen,
        tags=["settlement", asset_slug, "summary"],
    )

    sections = [
        fm,
        "",
        f"# {display} — Settlement Summary",
        "",
        "## Monthly Totals",
        "",
        "_Source: `staging.settlement_report_facts` where `fact_type = 'total_amount'` "
        "and `component_name = '总电费'`._",
        "",
    ]

    # Build table header
    header_types = [_INVOICE_TYPE_LABELS.get(it, it) for it in invoice_types_seen]
    if invoice_types_seen:
        sections.append("| Period | " + " | ".join(header_types) + " | Flags |")
        sections.append("|--------|" + "--------|" * len(invoice_types_seen) + "-------|")
        for yr, mo in sorted(periods, reverse=True):
            period_totals = totals_by_period.get((yr, mo), {})
            cells = [
                _fmt_yuan(period_totals.get(it)) for it in invoice_types_seen
            ]
            total_flags = sum(flag_lookup.get((yr, mo, it), 0) for it in invoice_types_seen)
            flag_str = f"⚠️ {total_flags}" if total_flags else "—"
            sections.append(
                f"| {yr}-{mo:02d} | " + " | ".join(cells) + f" | {flag_str} |"
            )
    else:
        sections.append("_No total_amount facts found for this asset yet._")

    sections += [
        "",
        "## Monthly Note Links",
        "",
    ]
    sections += period_links if period_links else ["_No monthly notes generated yet._"]

    sections += [
        "",
        "## Recurring Observations",
        "",
        "_Accumulate asset-level observations here over time._",
    ]

    note_path.parent.mkdir(parents=True, exist_ok=True)
    note_path.write_text("\n".join(sections), encoding="utf-8")

    _register_note(
        "asset_summary", asset_slug, note_path,
        f"{display} Summary", None, None, None, asset_slug,
    )
    return note_path


# ── Charge component cross-asset note ──────────────────────────────────────────

def generate_charge_component_note(component_name: str) -> Path:
    """
    Generate knowledge/settlement/components/{component_name}.md

    Cross-asset component note: shows all assets × periods where this component
    appears, with values and trends.
    """
    safe_name = re.sub(r'[^\w\u4e00-\u9fff\-]', '_', component_name)
    note_path = _VAULT_ROOT / "components" / f"{safe_name}.md"
    note_key = f"component_{component_name}"

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT f.asset_slug, f.settlement_year, f.settlement_month,
                       f.invoice_type, f.metric_value, f.metric_unit,
                       f.component_group, f.confidence, f.period_half
                FROM staging.settlement_report_facts f
                WHERE f.component_name = %s
                ORDER BY f.asset_slug, f.settlement_year, f.settlement_month,
                         f.invoice_type
                """,
                (component_name,),
            )
            rows = cur.fetchall()

    assets_seen = sorted(set(r[0] for r in rows))
    periods_seen = sorted(set((r[1], r[2]) for r in rows))
    component_group = rows[0][6] if rows else "other"

    # Cross-asset table: rows = periods, cols = assets
    data: dict[tuple, dict[str, float]] = {}
    for slug, yr, mo, itype, val, unit, grp, conf, half in rows:
        data.setdefault((yr, mo), {})[slug] = float(val) if val else 0.0

    fm = _frontmatter(
        title=f"Charge Component: {component_name}",
        component_name=component_name,
        component_group=component_group,
        note_type="charge_component",
        assets=assets_seen,
        periods_covered=len(periods_seen),
        tags=["settlement", "component", component_name, component_group],
    )

    sections = [
        fm,
        "",
        f"# Charge Component: {component_name}",
        "",
        f"**Group**: {component_group}",
        f"**Assets with this component**: {len(assets_seen)}",
        f"**Periods covered**: {len(periods_seen)}",
        "",
        "## Cross-Asset Values  _(yuan)_",
        "",
    ]

    if assets_seen:
        asset_headers = [_ASSET_DISPLAY.get(s, s) for s in assets_seen]
        sections.append("| Period | " + " | ".join(asset_headers) + " |")
        sections.append("|--------|" + "--------|" * len(assets_seen))
        for yr, mo in sorted(periods_seen, reverse=True):
            period_data = data.get((yr, mo), {})
            cells = [_fmt_yuan(period_data.get(s)) for s in assets_seen]
            sections.append(f"| {yr}-{mo:02d} | " + " | ".join(cells) + " |")
    else:
        sections.append("_No data found for this component._")

    # Asset links
    sections += [
        "",
        "## Asset Notes",
        "",
    ]
    for slug in assets_seen:
        display = _ASSET_DISPLAY.get(slug, slug)
        sections.append(f"- [[{slug}/index]] — {display}")

    sections += [
        "",
        "## Pattern Analysis",
        "",
        "_Accumulate cross-asset observations here._",
    ]

    note_path.parent.mkdir(parents=True, exist_ok=True)
    note_path.write_text("\n".join(sections), encoding="utf-8")

    _register_note(
        "charge_component", note_key, note_path,
        f"Component: {component_name}", None, None, None, None,
    )
    return note_path


# ── Reconciliation note ─────────────────────────────────────────────────────────

def generate_reconciliation_note(
    asset_slug: str,
    settlement_year: int,
    settlement_month: int,
    invoice_type: str,
) -> Optional[Path]:
    """
    Generate knowledge/settlement/reconciliation/{key}.md

    Only generated if ≥1 reconciliation row exists for this combination.
    Returns None if no reconciliation data.
    """
    ym = f"{settlement_year}-{settlement_month:02d}"
    note_key = f"recon_{asset_slug}_{ym}_{invoice_type}"
    display = _ASSET_DISPLAY.get(asset_slug, asset_slug)
    inv_label = _INVOICE_TYPE_LABELS.get(invoice_type, invoice_type)
    note_path = _VAULT_ROOT / "reconciliation" / f"{note_key}.md"

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.fact_type, r.component_name,
                       r.value_a, r.value_b, r.delta, r.delta_pct,
                       r.flagged, r.flag_reason,
                       r.flag_threshold_pct, r.flag_threshold_abs,
                       da.file_name AS file_a, db2.file_name AS file_b,
                       r.version_a_doc_id, r.version_b_doc_id
                FROM staging.settlement_reconciliation r
                JOIN staging.settlement_report_documents da ON da.id = r.version_a_doc_id
                JOIN staging.settlement_report_documents db2 ON db2.id = r.version_b_doc_id
                WHERE r.asset_slug = %s
                  AND r.settlement_year = %s AND r.settlement_month = %s
                  AND r.invoice_type = %s
                ORDER BY r.flagged DESC, ABS(r.delta) DESC NULLS LAST
                """,
                (asset_slug, settlement_year, settlement_month, invoice_type),
            )
            rows = cur.fetchall()

    if not rows:
        return None

    flagged_rows = [r for r in rows if r[6]]
    doc_pairs = set((r[12], r[13]) for r in rows)

    fm = _frontmatter(
        title=f"Reconciliation: {display} {ym} {invoice_type}",
        asset_slug=asset_slug,
        settlement_year=settlement_year,
        settlement_month=settlement_month,
        invoice_type=invoice_type,
        note_type="reconciliation",
        flagged_count=len(flagged_rows),
        tags=["settlement", "reconciliation", asset_slug, ym],
    )

    sections = [
        fm,
        "",
        f"# Reconciliation: {display} — {inv_label} — {ym}",
        "",
        f"**Document version pairs compared**: {len(doc_pairs)}",
        f"**Total fact rows**: {len(rows)}",
        f"**Flagged differences**: {len(flagged_rows)}",
        "",
        "## Document Versions",
        "",
    ]
    for doc_a_id, doc_b_id in sorted(doc_pairs):
        file_a = next((r[10] for r in rows if r[12] == doc_a_id), str(doc_a_id))
        file_b = next((r[11] for r in rows if r[13] == doc_b_id), str(doc_b_id))
        sections.append(f"- **v_a** (doc {doc_a_id}): `{file_a}`")
        sections.append(f"- **v_b** (doc {doc_b_id}): `{file_b}`")

    sections += [
        "",
        "## Comparison Table",
        "",
        "| Component | Value A | Value B | Delta | Δ% | Flag |",
        "|-----------|---------|---------|-------|-----|------|",
    ]
    thr_pct = rows[0][8] if rows else 1.0
    thr_abs = rows[0][9] if rows else 500.0
    for ft, cn, va, vb, delta, dpct, flagged, flag_reason, _, _, fa, fb, da_id, db_id in rows:
        flag_marker = "⚠️" if flagged else ""
        dpct_str = f"{float(dpct):.2f}%" if dpct is not None else "—"
        sections.append(
            f"| {cn or ft} | {_fmt_yuan(va)} | {_fmt_yuan(vb)} | "
            f"{_fmt_yuan(delta)} | {dpct_str} | {flag_marker} |"
        )

    sections += [
        "",
        f"_Thresholds: Δ% ≥ {thr_pct}% or |Δ| ≥ ¥{thr_abs:,.0f} triggers flag._",
    ]

    if flagged_rows:
        sections += [
            "",
            "## Flagged Differences",
            "",
        ]
        for ft, cn, va, vb, delta, dpct, flagged, flag_reason, _, _, fa, fb, da_id, db_id in flagged_rows:
            dpct_str = f"{float(dpct):.2f}%" if dpct is not None else "n/a"
            sections.append(
                f"- **{cn or ft}**: v_a={_fmt_yuan(va)}, v_b={_fmt_yuan(vb)}, "
                f"Δ={_fmt_yuan(delta)} ({dpct_str})"
            )
            if flag_reason:
                sections.append(f"  - _{flag_reason}_")

    sections += [
        "",
        "## Resolution Notes",
        "",
        "_Add investigation notes and resolution here._",
    ]

    note_path.parent.mkdir(parents=True, exist_ok=True)
    note_path.write_text("\n".join(sections), encoding="utf-8")

    _register_note(
        "reconciliation", note_key, note_path,
        f"Recon: {display} {ym} {invoice_type}", None,
        settlement_year, settlement_month, asset_slug,
    )
    return note_path


# ── Settlement index note ────────────────────────────────────────────────────────

def generate_settlement_index_note() -> Path:
    """Generate knowledge/settlement/index.md — master index."""
    note_path = _VAULT_ROOT / "index.md"

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*), MIN(settlement_year * 100 + settlement_month),
                       MAX(settlement_year * 100 + settlement_month)
                FROM staging.settlement_report_documents
                WHERE ingest_status = 'parsed'
                """
            )
            doc_count, period_min_raw, period_max_raw = cur.fetchone()

            cur.execute(
                "SELECT COUNT(*), COUNT(DISTINCT asset_slug), COUNT(DISTINCT invoice_type) "
                "FROM staging.settlement_report_facts"
            )
            fact_row = cur.fetchone()
            fact_count = fact_row[0] or 0
            asset_count = fact_row[1] or 0

            cur.execute(
                """
                SELECT asset_slug, COUNT(DISTINCT settlement_year * 100 + settlement_month)
                FROM staging.settlement_report_documents
                WHERE ingest_status = 'parsed' AND asset_slug IS NOT NULL
                GROUP BY asset_slug ORDER BY asset_slug
                """
            )
            asset_period_counts = cur.fetchall()

            cur.execute(
                """
                SELECT invoice_type, COUNT(*)
                FROM staging.settlement_report_documents
                WHERE ingest_status = 'parsed'
                GROUP BY invoice_type ORDER BY invoice_type
                """
            )
            type_counts = cur.fetchall()

            cur.execute(
                "SELECT COUNT(*) FROM staging.settlement_reconciliation WHERE flagged = TRUE"
            )
            flagged_count = (cur.fetchone() or [0])[0]

    def _period_display(raw) -> str:
        if raw is None:
            return "—"
        yr, mo = divmod(raw, 100)
        return f"{yr}-{mo:02d}"

    sections = [
        "---",
        "title: Settlement Knowledge Pool — Index",
        "note_type: settlement_index",
        f"generated_at: {dt.date.today()}",
        "tags:",
        "  - settlement",
        "  - index",
        "---",
        "",
        "# Settlement Knowledge Pool",
        "",
        "## Statistics",
        "",
        f"- **PDFs parsed**: {doc_count or 0}",
        f"- **Assets covered**: {asset_count}",
        f"- **Facts extracted**: {fact_count:,}",
        f"- **Period range**: {_period_display(period_min_raw)} → {_period_display(period_max_raw)}",
        f"- **Flagged reconciliation differences**: {flagged_count}",
        "",
        "## Asset Notes",
        "",
    ]
    for slug, period_cnt in asset_period_counts:
        display = _ASSET_DISPLAY.get(slug, slug)
        sections.append(f"- [[{slug}/index]] — {display} ({period_cnt} months)")

    sections += [
        "",
        "## Invoice Type Coverage",
        "",
        "| Type | Documents |",
        "|------|-----------|",
    ]
    for itype, cnt in type_counts:
        label = _INVOICE_TYPE_LABELS.get(itype, itype)
        sections.append(f"| {label} | {cnt} |")

    sections += [
        "",
        "## Sub-Vaults",
        "",
        "- [[components/]] — Per-component cross-asset analysis",
        "- [[reconciliation/]] — Invoice version diff notes",
    ]

    note_path.parent.mkdir(parents=True, exist_ok=True)
    note_path.write_text("\n".join(sections), encoding="utf-8")
    return note_path
