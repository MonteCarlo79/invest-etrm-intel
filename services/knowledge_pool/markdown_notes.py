"""
Obsidian-compatible markdown note generation.

Three note types:
  daily_report  — one note per report date
  province      — one rolling note per province (accumulates cross-report)
  concept       — one note per recurring driver theme

Notes are written to knowledge/spot_market/{01_daily_reports|02_provinces|03_concepts}/
All notes have YAML frontmatter and explicitly separate source vs interpretation sections.
Notes are deterministic across re-runs (no random IDs, no generated timestamps in body).
"""
from __future__ import annotations

import datetime as dt
import re
from pathlib import Path
from typing import List, Optional

from .db import get_conn

# Repo root is 3 levels up from this file
_REPO_ROOT = Path(__file__).resolve().parents[2]
_VAULT_ROOT = _REPO_ROOT / "knowledge" / "spot_market"


# ── Common helpers ─────────────────────────────────────────────────────────────

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


def _register_note(
    note_type: str,
    note_key: str,
    note_path: Path,
    note_title: str,
    document_id: Optional[int],
    date_min: Optional[dt.date],
    date_max: Optional[dt.date],
):
    rel_path = str(note_path.relative_to(_REPO_ROOT)).replace("\\", "/")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO staging.spot_report_notes
                    (document_id, note_type, note_key, note_path, note_title,
                     report_date_min, report_date_max)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (note_type, note_key) DO UPDATE SET
                    note_path       = EXCLUDED.note_path,
                    note_title      = EXCLUDED.note_title,
                    report_date_min = LEAST(staging.spot_report_notes.report_date_min, EXCLUDED.report_date_min),
                    report_date_max = GREATEST(staging.spot_report_notes.report_date_max, EXCLUDED.report_date_max),
                    updated_at      = now()
                """,
                (document_id, note_type, note_key, rel_path, note_title,
                 date_min, date_max),
            )
        conn.commit()


# ── Daily report note ─────────────────────────────────────────────────────────

def generate_daily_report_note(
    report_date: dt.date,
    document_id: int,
    source_path: str,
) -> Path:
    """
    Generate knowledge/spot_market/01_daily_reports/YYYY-MM-DD.md
    """
    note_path = _VAULT_ROOT / "01_daily_reports" / f"{report_date}.md"

    # Fetch price facts from DB (spot_daily_bridge = structured, high-confidence)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT province_cn, province_en, fact_type, metric_name, metric_value
                FROM staging.spot_report_facts
                WHERE document_id = %s AND report_date = %s
                  AND fact_type IN ('price_da', 'price_rt')
                  AND source_method = 'spot_daily_bridge'
                ORDER BY province_cn, fact_type, metric_name
                """,
                (document_id, report_date),
            )
            price_rows = cur.fetchall()

            cur.execute(
                """
                SELECT province_cn, fact_text, page_no
                FROM staging.spot_report_facts
                WHERE document_id = %s AND report_date = %s
                  AND fact_type = 'driver'
                ORDER BY province_cn, page_no
                """,
                (document_id, report_date),
            )
            driver_rows = cur.fetchall()

            # Interprovincial facts
            cur.execute(
                """
                SELECT fact_text, page_no
                FROM staging.spot_report_facts
                WHERE document_id = %s AND report_date = %s
                  AND fact_type = 'interprovincial'
                ORDER BY page_no
                LIMIT 5
                """,
                (document_id, report_date),
            )
            interprov_rows = cur.fetchall()

            # Page provenance: which pages covered this date
            cur.execute(
                """
                SELECT page_no, char_count
                FROM staging.spot_report_pages
                WHERE document_id = %s AND page_date = %s
                ORDER BY page_no
                """,
                (document_id, report_date),
            )
            page_prov = cur.fetchall()

    # Source file name (shorter display)
    source_name = Path(source_path).name if source_path else "unknown"

    # Build price table
    prices: dict[str, dict] = {}
    for pcn, pen, ftype, metric, val in price_rows:
        key = pcn or pen or "Unknown"
        prices.setdefault(key, {})[f"{ftype}_{metric}"] = val

    price_lines = []
    if prices:
        price_lines.append("| Province | DA Avg | DA Max | DA Min | RT Avg | RT Max | RT Min |")
        price_lines.append("|----------|--------|--------|--------|--------|--------|--------|")
        for prov, vals in sorted(prices.items()):
            def _v(k):
                v = vals.get(k)
                return f"{v:.1f}" if v is not None else "—"
            price_lines.append(
                f"| {prov} | {_v('price_da_da_avg')} | {_v('price_da_da_max')} | "
                f"{_v('price_da_da_min')} | {_v('price_rt_rt_avg')} | "
                f"{_v('price_rt_rt_max')} | {_v('price_rt_rt_min')} |"
            )

    # Build drivers section (deduplicated by province)
    driver_lines = []
    seen_provs: set[str] = set()
    for pcn, fact_text, page_no in driver_rows:
        if pcn and pcn not in seen_provs:
            driver_lines.append(f"- **{pcn}** (p.{page_no}): {fact_text}")
            seen_provs.add(pcn)

    # Interprovincial
    interprov_lines = [f"- (p.{pg}): {ft[:200]}" for ft, pg in interprov_rows]

    # Province links
    province_links = sorted(set(p for p, _, _, _, _ in price_rows if p))
    prov_link_lines = [f"- [[{p}]]" for p in province_links]

    # Concept links: scan driver text for known concept keywords
    CONCEPT_KEYS = list(CONCEPT_PATTERNS.keys())
    concept_hits: list[str] = []
    all_driver_text = " ".join(ft for _, ft, _ in driver_rows)
    for ck in CONCEPT_KEYS:
        if ck in all_driver_text:
            concept_hits.append(f"- [[{ck}]]")

    # Provenance: pages → date
    prov_lines = []
    for pg, chars in page_prov:
        prov_lines.append(f"- Page {pg}: {chars} chars")

    # Assemble note
    fm = _frontmatter(
        title=f"Spot Market Daily Report {report_date}",
        date=str(report_date),
        source_file=source_name,
        source_path=source_path,
        document_id=document_id,
        note_type="daily_report",
        provinces=province_links,
        tags=["spot-market", "daily-report", str(report_date.year)],
    )

    sections = [
        fm,
        "",
        f"# Spot Market Daily Report — {report_date}",
        "",
        "## Provenance",
        "",
        f"- **Source file**: `{source_name}`",
        f"- **Document ID**: {document_id}",
        f"- **Pages covering this date**: {len(page_prov)}",
    ]
    sections += prov_lines if prov_lines else ["- _No pages with this date found_"]

    sections += [
        "",
        "## Source-backed Summary",
        "",
        "### DA / RT Prices  _(source: public.spot_daily bridge)_",
        "",
    ]
    if price_lines:
        sections += price_lines
    else:
        sections.append("_Price data not available in public.spot_daily for this date._")

    sections += [
        "",
        "### Market Drivers  _(source: PDF regex)_",
        "",
    ]
    if driver_lines:
        sections += driver_lines
    else:
        sections.append("_No driver sentences extracted._")

    if interprov_lines:
        sections += [
            "",
            "### Interprovincial Transactions  _(source: PDF regex)_",
            "",
        ]
        sections += interprov_lines

    sections += [
        "",
        "## Province Notes",
        "",
    ]
    sections += prov_link_lines if prov_link_lines else ["_No province links._"]

    sections += [
        "",
        "## Concept Links",
        "",
    ]
    sections += concept_hits if concept_hits else ["_No known concept keywords detected in drivers._"]

    sections += [
        "",
        "## Analyst / LLM Interpretation",
        "",
        "_Not yet generated._",
        "",
        "## Open Questions / Parser Caveats",
        "",
    ]

    # Add parser caveats if applicable
    if not price_rows:
        sections.append("- Price data absent from public.spot_daily for this date — spot_daily_bridge yielded no rows.")
    if not driver_rows:
        sections.append("- No driver sentences matched by regex — PDF text may be image-based or layout differs.")
    if not prov_lines:
        sections.append("- No pages matched this date via page-date inference — date may only appear in filename.")
    if not any([not price_rows, not driver_rows, not prov_lines]):
        sections.append("_None recorded._")

    note_path.parent.mkdir(parents=True, exist_ok=True)
    note_path.write_text("\n".join(sections), encoding="utf-8")

    _register_note(
        "daily_report", str(report_date), note_path,
        f"Spot Market {report_date}", document_id,
        report_date, report_date,
    )

    return note_path


# ── Province rolling note ─────────────────────────────────────────────────────

def generate_province_note(province_cn: str, province_en: str) -> Path:
    """
    Generate/update knowledge/spot_market/02_provinces/{province_cn}.md
    Accumulates all report dates where this province appears.
    """
    note_path = _VAULT_ROOT / "02_provinces" / f"{province_cn}.md"

    with get_conn() as conn:
        with conn.cursor() as cur:
            # All dates with price data
            cur.execute(
                """
                SELECT DISTINCT report_date
                FROM staging.spot_report_facts
                WHERE province_cn = %s AND fact_type IN ('price_da','price_rt')
                ORDER BY report_date DESC
                LIMIT 60
                """,
                (province_cn,),
            )
            dates = [r[0] for r in cur.fetchall()]

            # Recent DA/RT avg by date (bridge data only — structured)
            cur.execute(
                """
                SELECT report_date,
                       MAX(CASE WHEN metric_name='da_avg' THEN metric_value END) AS da_avg,
                       MAX(CASE WHEN metric_name='rt_avg' THEN metric_value END) AS rt_avg,
                       MAX(CASE WHEN source_method = 'spot_daily_bridge' THEN 'bridge'
                                ELSE 'pdf' END) AS data_source
                FROM staging.spot_report_facts
                WHERE province_cn = %s AND fact_type IN ('price_da','price_rt')
                GROUP BY report_date
                ORDER BY report_date DESC
                LIMIT 30
                """,
                (province_cn,),
            )
            price_series = cur.fetchall()

            # Recent drivers with page provenance
            cur.execute(
                """
                SELECT DISTINCT ON (report_date) report_date, fact_text, page_no, document_id
                FROM staging.spot_report_facts
                WHERE province_cn = %s AND fact_type = 'driver'
                ORDER BY report_date DESC, id
                LIMIT 20
                """,
                (province_cn,),
            )
            drivers = cur.fetchall()

            # Source documents that mention this province
            cur.execute(
                """
                SELECT DISTINCT d.file_name, d.id, d.report_date_min, d.report_date_max
                FROM staging.spot_report_facts f
                JOIN staging.spot_report_documents d ON d.id = f.document_id
                WHERE f.province_cn = %s
                ORDER BY d.report_date_min DESC
                LIMIT 20
                """,
                (province_cn,),
            )
            source_docs = cur.fetchall()

    date_min = min(dates) if dates else None
    date_max = max(dates) if dates else None

    # Build price series table
    ps_lines = []
    if price_series:
        ps_lines.append("| Date | DA Avg | RT Avg | Source |")
        ps_lines.append("|------|--------|--------|--------|")
        for rd, da, rt, src in price_series:
            ps_lines.append(
                f"| {rd} | "
                f"{'%.1f' % da if da else '—'} | "
                f"{'%.1f' % rt if rt else '—'} | "
                f"{'bridge' if src == 'bridge' else 'pdf'} |"
            )

    driver_lines = [
        f"- **{rd}** (doc={did} p.{pg}): {ft}"
        for rd, ft, pg, did in drivers
    ]

    # Report date links
    report_links = [f"- [[{d}]]" for d in sorted(dates, reverse=True)[:20]]

    # Source document list
    doc_lines = [
        f"- `{fn}` (doc={did}, {dmin}→{dmax})"
        for fn, did, dmin, dmax in source_docs
    ]

    # Concept links: scan driver text for known concept keywords
    all_driver_text = " ".join(ft for _, ft, _, _ in drivers)
    concept_hits = [
        f"- [[{ck}]]"
        for ck in CONCEPT_PATTERNS
        if ck in all_driver_text
    ]

    fm = _frontmatter(
        title=f"Province: {province_cn} ({province_en})",
        province_cn=province_cn,
        province_en=province_en,
        note_type="province",
        date_range=f"{date_min} → {date_max}" if date_min else "unknown",
        report_count=len(dates),
        tags=["spot-market", "province", province_cn, province_en],
    )

    sections = [
        fm,
        "",
        f"# {province_cn} ({province_en}) — Province Knowledge Note",
        "",
        "## Scope",
        f"Covers all spot market daily reports mentioning **{province_cn}**.",
        f"Period: {date_min} → {date_max}. Dates with price data: {len(dates)}.",
        "",
        "## Source Documents",
        "",
    ]
    sections += doc_lines if doc_lines else ["_No source documents linked._"]

    sections += [
        "",
        "## Recent DA/RT Average Prices",
        "",
        "_Source: spot_daily_bridge (structured) where available; pdf inline where not._",
        "",
    ]
    sections += ps_lines if ps_lines else ["_No price data yet._"]

    sections += [
        "",
        "## Recent Market Drivers  _(source: PDF regex)_",
        "",
    ]
    sections += driver_lines if driver_lines else ["_No driver sentences extracted._"]

    sections += [
        "",
        "## Linked Daily Reports",
        "",
    ]
    sections += report_links if report_links else ["_No reports linked._"]

    sections += [
        "",
        "## Related Concepts",
        "",
    ]
    sections += concept_hits if concept_hits else ["_No concept keywords detected in recent drivers._"]

    sections += [
        "",
        "## Recurring Patterns",
        "",
        "_Accumulate analyst observations here over time._",
        "",
        "## Parser Caveats",
        "",
    ]

    # Explicit parser caveats
    if not price_series:
        sections.append("- No price data found — spot_daily may not cover this province.")
    if not drivers:
        sections.append("- No driver sentences extracted — PDF text may not include '原因为' pattern for this province.")
    if not any([not price_series, not drivers]):
        sections.append("_None recorded._")

    note_path.parent.mkdir(parents=True, exist_ok=True)
    note_path.write_text("\n".join(sections), encoding="utf-8")

    _register_note(
        "province", province_cn, note_path,
        f"Province: {province_cn}", None, date_min, date_max,
    )

    return note_path


# ── Concept note ──────────────────────────────────────────────────────────────

# Concept keywords and their display names
CONCEPT_PATTERNS = {
    "新能源出力下降": "新能源出力下降 (New Energy Output Decline)",
    "负荷增加": "负荷增加 (Load Increase)",
    "省间现货交易": "省间现货交易 (Interprovincial Spot Trading)",
    "实时价格波动": "实时价格波动 (Real-time Price Volatility)",
    "检修": "机组检修 (Unit Maintenance)",
    "水电": "水电出力 (Hydro Output)",
    "新能源消纳": "新能源消纳 (New Energy Absorption)",
}


def generate_concept_note(concept_key: str) -> Path:
    """
    Generate/update knowledge/spot_market/03_concepts/{concept_key}.md
    Searches driver facts for mentions of the concept keyword.
    """
    display_name = CONCEPT_PATTERNS.get(concept_key, concept_key)
    note_path = _VAULT_ROOT / "03_concepts" / f"{concept_key}.md"

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT f.report_date, f.province_cn, f.fact_text, f.page_no,
                       d.file_name, f.document_id
                FROM staging.spot_report_facts f
                JOIN staging.spot_report_documents d ON d.id = f.document_id
                WHERE f.fact_type = 'driver'
                  AND f.fact_text ILIKE %s
                ORDER BY f.report_date DESC
                LIMIT 50
                """,
                (f"%{concept_key}%",),
            )
            evidence = cur.fetchall()

    date_min = min(r[0] for r in evidence) if evidence else None
    date_max = max(r[0] for r in evidence) if evidence else None

    # Province frequency count
    prov_counts: dict[str, int] = {}
    for rd, pcn, ft, pg, fn, did in evidence:
        if pcn:
            prov_counts[pcn] = prov_counts.get(pcn, 0) + 1

    freq_lines = [
        f"| {p} | {c} |"
        for p, c in sorted(prov_counts.items(), key=lambda x: -x[1])
    ]

    # Evidence: structured rows (date | province | source | snippet)
    evidence_lines = []
    for rd, pcn, ft, pg, fn, did in evidence[:30]:
        prov_str = pcn or "National"
        snippet = ft[:180].replace("\n", " ")
        evidence_lines.append(
            f"| {rd} | {prov_str} | doc={did} p.{pg} | {snippet} |"
        )

    fm = _frontmatter(
        title=display_name,
        concept=concept_key,
        note_type="concept",
        evidence_count=len(evidence),
        date_range=f"{date_min} → {date_max}" if date_min else "unknown",
        tags=["spot-market", "concept", concept_key],
    )

    sections = [
        fm,
        "",
        f"# Concept: {display_name}",
        "",
        "## Description",
        "",
        f"_Cross-report concept note for occurrences of **{concept_key}** in daily spot reports._",
        "",
        "## Province Frequency",
        "",
    ]
    if freq_lines:
        sections += ["| Province | Occurrences |", "|----------|-------------|"]
        sections += freq_lines
    else:
        sections.append("_No province-level occurrences yet._")

    sections += [
        "",
        "## Evidence  _(source: PDF regex — driver sentences)_",
        "",
        f"Found in {len(evidence)} fact records.",
        "",
    ]
    if evidence_lines:
        sections += ["| Date | Province | Provenance | Driver Sentence |", "|------|----------|-----------|----------------|"]
        sections += evidence_lines
    else:
        sections.append("_No evidence yet._")

    sections += [
        "",
        "## Pattern Analysis",
        "",
        "_Accumulate patterns across reports here._",
        "",
        "## Related Concepts",
        "",
        "_Add [[links]] to related concept notes._",
    ]

    note_path.parent.mkdir(parents=True, exist_ok=True)
    note_path.write_text("\n".join(sections), encoding="utf-8")

    _register_note(
        "concept", concept_key, note_path,
        display_name, None, date_min, date_max,
    )

    return note_path


# ── Index note ────────────────────────────────────────────────────────────────

def generate_index_note() -> Path:
    """Generate knowledge/spot_market/04_indices/index.md — master index."""
    note_path = _VAULT_ROOT / "04_indices" / "index.md"

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*), MIN(report_date_min), MAX(report_date_max) "
                "FROM staging.spot_report_notes WHERE note_type = 'daily_report'"
            )
            daily_count, dmin, dmax = cur.fetchone()

            cur.execute(
                "SELECT note_key FROM staging.spot_report_notes "
                "WHERE note_type = 'province' ORDER BY note_key"
            )
            provinces = [r[0] for r in cur.fetchall()]

            cur.execute(
                "SELECT note_key FROM staging.spot_report_notes "
                "WHERE note_type = 'concept' ORDER BY note_key"
            )
            concepts = [r[0] for r in cur.fetchall()]

            cur.execute(
                "SELECT COUNT(*), COUNT(DISTINCT file_hash) "
                "FROM staging.spot_report_documents WHERE ingest_status = 'parsed'"
            )
            doc_row = cur.fetchone()
            doc_count = doc_row[0] if doc_row else 0

            cur.execute(
                "SELECT COUNT(*) FROM staging.spot_report_chunks"
            )
            chunk_count = (cur.fetchone() or [0])[0]

            cur.execute(
                "SELECT COUNT(*), "
                "SUM(CASE WHEN source_method='spot_daily_bridge' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN source_method='pdf_regex' THEN 1 ELSE 0 END) "
                "FROM staging.spot_report_facts"
            )
            fc_row = cur.fetchone()
            fact_total = fc_row[0] or 0
            fact_bridge = fc_row[1] or 0
            fact_pdf = fc_row[2] or 0

    sections = [
        "---",
        "title: Spot Market Knowledge Pool — Index",
        "note_type: index",
        f"generated_at: {dt.date.today()}",
        "tags:",
        "  - spot-market",
        "  - index",
        "---",
        "",
        "# Spot Market Knowledge Pool",
        "",
        "## Statistics",
        "",
        f"- **PDFs ingested**: {doc_count}",
        f"- **Daily report notes**: {daily_count or 0}",
        f"- **Date range**: {dmin} → {dmax}" if dmin else "- **Date range**: not yet available",
        f"- **Text chunks**: {chunk_count:,}",
        f"- **Total facts**: {fact_total:,}  (bridge: {fact_bridge:,} · pdf-regex: {fact_pdf:,})",
        f"- **Province notes**: {len(provinces)}",
        f"- **Concept notes**: {len(concepts)}",
        "",
        "## Province Notes",
        "",
    ]
    sections += [f"- [[{p}]]" for p in provinces] or ["_None yet._"]
    sections += [
        "",
        "## Concept Notes",
        "",
    ]
    sections += [f"- [[{c}]]" for c in concepts] or ["_None yet._"]
    sections += [
        "",
        "## Daily Reports",
        "",
        f"See [[01_daily_reports/]] for all {daily_count or 0} daily notes.",
    ]

    note_path.parent.mkdir(parents=True, exist_ok=True)
    note_path.write_text("\n".join(sections), encoding="utf-8")
    return note_path
